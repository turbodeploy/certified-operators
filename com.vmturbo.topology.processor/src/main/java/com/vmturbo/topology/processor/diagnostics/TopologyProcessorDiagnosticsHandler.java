package com.vmturbo.topology.processor.diagnostics;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import io.prometheus.client.CollectorRegistry;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.DiscoveredGroupInfo;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagnosticsWriter;
import com.vmturbo.components.common.diagnostics.Diags;
import com.vmturbo.components.common.diagnostics.DiagsZipReader;
import com.vmturbo.identity.store.PersistentIdentityStore;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.TopologyProcessorComponent;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetInfo;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader;
import com.vmturbo.topology.processor.cost.PriceTableUploader;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.IdentifiedEntityDTO;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader.TargetDiscoveredData;
import com.vmturbo.topology.processor.group.discovery.InterpretedGroup;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.DiscoveryDumperSettings;
import com.vmturbo.topology.processor.plan.DiscoveredTemplateDeploymentProfileUploader;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.scheduling.TargetDiscoverySchedule;
import com.vmturbo.topology.processor.targets.InvalidTargetException;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Handle diagnostics for {@link TopologyProcessorComponent}.
 * The topology processor dump is made of a zip file that contains one file for the registered
 * targets, one file for their schedules, and then one file per target with the entities
 * associated with that target.
 */
public class TopologyProcessorDiagnosticsHandler {

    private static final String TARGETS_DIAGS_FILE_NAME = "Targets.diags";
    private static final String SCHEDULES_DIAGS_FILE_NAME = "Schedules.diags";
    private static final String ID_DIAGS_FILE_NAME = "Identity.diags";
    private static final String PROBES_DIAGS_FILE_NAME = "Probes.diags";
    private static final String TARGET_IDENTIFIERS_DIAGS_FILE_NAME = "Target.identifiers.diags";

    private static final String DISCOVERED_CLOUD_COST_NAME = "DiscoveredCloudCost.diags";
    private static final String PRICE_TABLE_NAME = "PriceTables.diags";

    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();
    private static final String SKIPPING_FOLLOWING_TASKS = "remaining diagnostic dump tasks skipped.";

    private final TargetStore targetStore;
    private final PersistentIdentityStore targetPersistentIdentityStore;
    private final Scheduler scheduler;
    private final EntityStore entityStore;
    private final ProbeStore probeStore;
    private final DiscoveredGroupUploader discoveredGroupUploader;
    private final DiscoveredTemplateDeploymentProfileUploader templateDeploymentProfileUploader;
    private final IdentityProvider identityProvider;
    private final DiscoveredCloudCostUploader discoveredCloudCostUploader;
    private final PriceTableUploader priceTableUploader;
    private final DiagnosticsWriter diagnosticsWriter;

    private final Logger logger = LogManager.getLogger();

    TopologyProcessorDiagnosticsHandler(
            @Nonnull final TargetStore targetStore,
            @Nonnull final PersistentIdentityStore targetPersistentIdentityStore,
            @Nonnull final Scheduler scheduler,
            @Nonnull final EntityStore entityStore,
            @Nonnull final ProbeStore probeStore,
            @Nonnull final DiscoveredGroupUploader discoveredGroupUploader,
            @Nonnull final DiscoveredTemplateDeploymentProfileUploader templateDeploymentProfileUploader,
            @Nonnull final IdentityProvider identityProvider,
            @Nonnull final DiscoveredCloudCostUploader discoveredCloudCostUploader,
            @Nonnull final PriceTableUploader priceTableUploader,
            @Nonnull final DiagnosticsWriter diagnosticsWriter) {
        this.targetStore = targetStore;
        this.targetPersistentIdentityStore = targetPersistentIdentityStore;
        this.scheduler = scheduler;
        this.entityStore = entityStore;
        this.probeStore = probeStore;
        this.discoveredGroupUploader = discoveredGroupUploader;
        this.templateDeploymentProfileUploader = templateDeploymentProfileUploader;
        this.identityProvider = identityProvider;
        this.discoveredCloudCostUploader = discoveredCloudCostUploader;
        this.priceTableUploader = priceTableUploader;
        this.diagnosticsWriter = diagnosticsWriter;
    }

    /**
     * Dump TopologyProcessor diagnostics.
     *
     * @param diagnosticZip the ZipOutputStream to dump diags to
     */
    public void dumpDiags(ZipOutputStream diagnosticZip) {
        dumpDiags(diagnosticZip, IdentifiedEntityDTO::toJson, Target::getNoSecretDto);
    }

    /**
     * Dump TopologyProcessor diagnostics, converting entities to JSON and Targets to TargetInfos
     * using the specified functions.
     *
     * @param diagnosticZip the ZipOutputStream to dump diags to
     * @param identifiedEntityMapper a function that converts IdentifiedEntityDTO to a json string
     * @param targetMapper a function that converts Target to TargetInfo
     */
    private void dumpDiags(ZipOutputStream diagnosticZip,
                           Function<IdentifiedEntityDTO, String> identifiedEntityMapper,
                           Function<Target, TargetInfo> targetMapper) {

        // Probes
        try {
            diagnosticsWriter.writeZipEntry(PROBES_DIAGS_FILE_NAME,
                probeStore.getProbes().entrySet().stream()
                    .map(entry -> new ProbeInfoWithId(entry.getKey(), entry.getValue())),
                ProbeInfoWithId.class,
                diagnosticZip
            );
        } catch (DiagnosticsException e) {
            logger.error("Failed writing PROBES_DIAGS_FILE; " + SKIPPING_FOLLOWING_TASKS, e);
            return;
        }

        // Target identifiers in db, skip following tasks if failed to keep consistency.
        try {
            diagnosticsWriter.writeZipEntry(TARGET_IDENTIFIERS_DIAGS_FILE_NAME,
                targetPersistentIdentityStore.collectDiagsStream(), diagnosticZip);
        } catch (DiagnosticsException e) {
            logger.error("Dump target spec oid table failed; " + SKIPPING_FOLLOWING_TASKS, e);
            return;
        }

        // Targets
        List<Target> targets = targetStore.getAll();
        try {
            diagnosticsWriter.writeZipEntry(TARGETS_DIAGS_FILE_NAME,
                targets.stream()
                    .map(targetMapper),
                TargetInfo.class,
                diagnosticZip);
        } catch (DiagnosticsException e) {
            logger.error("Error writing targets diags file; " + SKIPPING_FOLLOWING_TASKS, e);
            return;
        }

        // Schedules
        try {
            diagnosticsWriter.writeZipEntry(SCHEDULES_DIAGS_FILE_NAME,
                targets.stream()
                    .map(Target::getId)
                    .map(scheduler::getDiscoverySchedule)
                    .filter(Optional::isPresent).map(Optional::get),
                TargetDiscoverySchedule.class,
                diagnosticZip);
        } catch (DiagnosticsException e) {
            logger.error("Failed to dump schedules diags;" + SKIPPING_FOLLOWING_TASKS, e);
            return;
        }

        // Discovered costs
        try {
            diagnosticsWriter.writeZipEntry(DISCOVERED_CLOUD_COST_NAME,
                discoveredCloudCostUploader.collectDiagsStream(),
                diagnosticZip);
        } catch (DiagnosticsException e) {
            logger.error("Failed to dump discovered cloud costs. Will continue with others.", e);
        }

        // Discovered price tables.
        try {
            diagnosticsWriter.writeZipEntry(PRICE_TABLE_NAME,
                priceTableUploader.collectDiagsStream(),
                diagnosticZip);
        } catch (DiagnosticsException e) {
            logger.error("Failed to dump discovered cloud costs. Will continue with others.", e);
        }

        final Map<Long, TargetDiscoveredData> discoveredGroupData = discoveredGroupUploader.getDataByTarget();
        final Map<Long, List<DiscoveredGroupInfo>> discoveredGroups = new HashMap<>(discoveredGroupData.size());
        final Map<Long, List<DiscoveredSettingPolicyInfo>> settingPolicies = new HashMap<>(discoveredGroupData.size());
        discoveredGroupData.forEach((targetId, targetData) -> {
            // We only take the discovered groups, not the scanned groups.
            discoveredGroups.put(targetId, targetData.getDiscoveredGroups()
                .map(InterpretedGroup::createDiscoveredGroupInfo)
                .collect(Collectors.toList()));
            // We only take the discovered setting policies, not the scanned ones.
            settingPolicies.put(targetId, targetData.getDiscoveredSettingPolicies().collect(Collectors.toList()));
        });

        final Map<Long, Map<DeploymentProfileInfo, Set<EntityProfileDTO>>> deploymentProfiles =
            templateDeploymentProfileUploader.getDiscoveredDeploymentProfilesByTarget();

        // Files separated by target
        for (Target target : targets) {
            // TODO(Shai): add targetId to the content of the file (not just part of the name)
            final long id = target.getId();
            final Long lastUpdated = entityStore.getTargetLastUpdatedTime(id).orElse(0L);
            final String targetSuffix = "." + id + "-" + lastUpdated + ".diags";

            // Entities
            try {
                final Stream<IdentifiedEntityDTO> objectsToWrite =
                    entityStore.discoveredByTarget(id)
                        .entrySet()
                        .stream()
                        .map(entry -> new IdentifiedEntityDTO(entry.getKey(), entry.getValue()));
                diagnosticsWriter.writeZipEntry("Entities" + targetSuffix,
                    objectsToWrite, IdentifiedEntityDTO.class, diagnosticZip);
            } catch (DiagnosticsException e) {
                logger.error("Error writing entities; " + SKIPPING_FOLLOWING_TASKS + " target " + target.getId(), e);
                return;
            }

            //Discovered Groups
            try {
                diagnosticsWriter.writeZipEntry("DiscoveredGroupsAndPolicies" + targetSuffix,
                    discoveredGroups.getOrDefault(id, Collections.emptyList()).stream(),
                    DiscoveredGroupInfo.class,
                    diagnosticZip
                );
            } catch (DiagnosticsException e) {
                logger.error("Error dumping Discovered Groups And Policies;" +
                    SKIPPING_FOLLOWING_TASKS, e);
                return;
            }

            //Discovered Settings Policies
            try {
                diagnosticsWriter.writeZipEntry("DiscoveredSettingPolicies" + targetSuffix,
                    settingPolicies.get(id).stream(),
                    DiscoveredSettingPolicyInfo.class,
                    diagnosticZip);
            } catch (DiagnosticsException e) {
                logger.error("Error dumping Discovered Setting Policies;" +
                    SKIPPING_FOLLOWING_TASKS, e);
                return;
            }

            // Discovered Deployment Profiles including associations with Templates
            try {
                diagnosticsWriter.writeZipEntry("DiscoveredDeploymentProfilesAndTemplates" + targetSuffix,
                    deploymentProfiles.getOrDefault(id, Collections.emptyMap()).entrySet().stream()
                        .map(entry -> new DeploymentProfileWithTemplate(entry.getKey(), entry.getValue())),
                    DeploymentProfileWithTemplate.class,
                    diagnosticZip);
            } catch (DiagnosticsException e) {
                logger.error("Error dumping Discovered Deployment Profiles and Templates; " +
                    SKIPPING_FOLLOWING_TASKS, e);
                return;
            }
        }

        try {
            diagnosticsWriter.writeZipEntry(ID_DIAGS_FILE_NAME,
                identityProvider.collectDiagsStream(),
                diagnosticZip);
        } catch (DiagnosticsException e) {
            logger.error(e.getErrors());
            return;
        }

        try {
            diagnosticsWriter.writePrometheusMetrics(CollectorRegistry.defaultRegistry, diagnosticZip);
        } catch (DiagnosticsException e) {
            logger.error("Error dumping prometheus metrics;" + SKIPPING_FOLLOWING_TASKS, e);
            return;
        }

        // discovery dumps
        File dumpDirInZip = new File("discoveryDumps");
        if (DiscoveryDumperSettings.DISCOVERY_DUMP_DIRECTORY.isDirectory()) {
            for (String dumpFileName : DiscoveryDumperSettings.DISCOVERY_DUMP_DIRECTORY.list()) {
                File dumpFile = new File(DiscoveryDumperSettings.DISCOVERY_DUMP_DIRECTORY, dumpFileName);
                if (dumpFile.exists() && dumpFile.isFile()) {
                    File entryFileName = new File(dumpDirInZip, dumpFileName);
                    try {
                        diagnosticsWriter.writeZipEntry(entryFileName.toString(),
                            FileUtils.readFileToByteArray(dumpFile), diagnosticZip);
                    } catch (DiagnosticsException | IOException e) {
                        logger.warn("Failed to write discovery dump file {} to diags zip file", dumpFile);
                        logger.catching(Level.DEBUG, e);
                    }
                } else {
                    logger.warn("Not dumping item {} in discovery dump directory; not a regular file", dumpFileName);
                }

            }
        }
    }

    /**
     * Dump TopologyProcessor anonymized diagnostics.
     *
     * @param diagnosticZip the ZipOutputStream to dump diags to
     */
    public void dumpAnonymizedDiags(ZipOutputStream diagnosticZip) {
        // Target identifiers in db.
        try {
            diagnosticsWriter.writeZipEntry(TARGET_IDENTIFIERS_DIAGS_FILE_NAME,
                targetPersistentIdentityStore.collectDiagsStream(), diagnosticZip);
        } catch (DiagnosticsException e) {
            logger.error("Dump target spec oid table failed.", e);
            return;
        }
        // Targets
        List<Target> targets = targetStore.getAll();
        try {
            diagnosticsWriter.writeZipEntry(TARGETS_DIAGS_FILE_NAME,
                                             targets.stream()
                                                    .map(Target::getNoSecretAnonymousDto)
                                                    .map(GSON::toJson),
                                             diagnosticZip);
        } catch (DiagnosticsException e) {
            logger.error("Error dumping " + TARGET_IDENTIFIERS_DIAGS_FILE_NAME, e);
            return;
        }

        // Schedules
        try {
            diagnosticsWriter.writeZipEntry(SCHEDULES_DIAGS_FILE_NAME,
                                             targets.stream()
                                                    .map(Target::getId)
                                                    .map(scheduler::getDiscoverySchedule)
                                                    .filter(Optional::isPresent).map(Optional::get)
                                                    .map(schedule -> GSON.toJson(schedule,
                                                        TargetDiscoverySchedule.class)),
                                             diagnosticZip);
        } catch (DiagnosticsException e) {
            logger.error("Error dumping " + SCHEDULES_DIAGS_FILE_NAME, e);
        }

        // Entities (one file per target)
        for (Target target : targets) {
            // TODO(Shai): add tartgetId to the content of the file (not just part of the name)
            try {
                diagnosticsWriter.writeZipEntry(
                    "Entities." + target.getId() + ".diags",
                    entityStore.discoveredByTarget(target.getId()).entrySet().stream()
                        .map(entry -> new IdentifiedEntityDTO(entry.getKey(), entry.getValue()))
                        .map(IdentifiedEntityDTO::toJsonAnonymized),
                    diagnosticZip);
            } catch (DiagnosticsException e) {
                logger.error("Error writing entities diags.", e);
            }
        }

        // Discovered costs
        try {
            diagnosticsWriter.writeZipEntry(DISCOVERED_CLOUD_COST_NAME,
                discoveredCloudCostUploader.collectDiagsStream(),
                diagnosticZip);
        } catch (DiagnosticsException e) {
            logger.error("Failed to dump discovered cloud costs. Will continue with others.", e);
        }

        // Discovered price tables.
        try {
            diagnosticsWriter.writeZipEntry(PRICE_TABLE_NAME,
                priceTableUploader.collectDiagsStream(),
                diagnosticZip);
        } catch (DiagnosticsException e) {
            logger.error("Failed to dump discovered price tables. Will continue with others.", e);
        }

        try {
            diagnosticsWriter.writeZipEntry(ID_DIAGS_FILE_NAME,
                identityProvider.collectDiagsStream(),
                diagnosticZip);
        } catch (DiagnosticsException e) {
            logger.error(e.getErrors());
        }
    }

    private static final Pattern ENTITIES_PATTERN =
        Pattern.compile("Entities\\.(\\d*)-(\\d*)\\.diags");
    private static final Pattern GROUPS_PATTERN =
        Pattern.compile("DiscoveredGroupsAndPolicies\\.(\\d*)-\\d*\\.diags");
    private static final Pattern SETTING_POLICIES_PATTERN =
        Pattern.compile("DiscoveredSettingPolicies\\.(\\d*)-(\\d*)\\.diags");
    private static final Pattern DEPLOYMENT_PROFILE_PATTERN =
        Pattern.compile("DiscoveredDeploymentProfilesAndTemplates\\.(\\d*)-(\\d*)\\.diags");

    private Comparator<Diags> diagsRestoreComparator =
        Comparator.comparingInt(TopologyProcessorDiagnosticsHandler::diagsRestorePriority);

    /**
     * Retrieve the priority level for restoring a diags file.
     * Lower numbers represent a higher priority, with 1 being the highest priority.
     *
     * @param diags the diagnostics file to determine the load priority for
     * @return the load priority to determine the order to load the given Diags object
     */
    private static Integer diagsRestorePriority(Diags diags) {
        final String diagsName = diags.getName();
        switch (diagsName) {
            case ID_DIAGS_FILE_NAME: return 1;
            case PROBES_DIAGS_FILE_NAME: return 2;
            case TARGET_IDENTIFIERS_DIAGS_FILE_NAME: return 3;
            case TARGETS_DIAGS_FILE_NAME: return 4;
            case SCHEDULES_DIAGS_FILE_NAME: return 5;
            // All other diagnostics files receive equal priority and may be loaded in any order
            default: return 6;
        }
    }

    /**
     * Restore topology processor diagnostics from a {@link ZipInputStream}.
     *
     * @param zis the input stream with compressed diagnostics
     * @return list of restored targets
     */
    public List<Target> restore(InputStream zis) {
        // Ordering is important. The identityProvider diags must be restored before the probes diags.
        // Otherwise, a race condition occurs where if a probe (re)registers after the restore of
        // the probe diags and before the restore of the identityProvider diags, then a probeInfo
        // will be written to Consul with the old ID. This entry will later become a duplicate entry
        // and cause severe problems.
        List<Diags> sortedDiagnostics = Streams.stream(new DiagsZipReader(zis))
            .sorted(diagsRestoreComparator)
            .collect(Collectors.toList());

        for (Diags diags : sortedDiagnostics) {
            final String diagsName = diags.getName();
            final List<String> diagsLines = diags.getLines();
            if (diagsLines == null) {
                continue;
            }
            try {
                switch (diagsName) {
                    case ID_DIAGS_FILE_NAME:
                        identityProvider.restoreDiags(diagsLines);
                        break;
                    case TARGET_IDENTIFIERS_DIAGS_FILE_NAME:
                        restoreTargetIdentifiers(diagsLines);
                        break;
                    case TARGETS_DIAGS_FILE_NAME:
                        restoreTargets(diagsLines);
                        break;
                    case SCHEDULES_DIAGS_FILE_NAME:
                        restoreSchedules(diagsLines);
                        break;
                    case PROBES_DIAGS_FILE_NAME:
                        restoreProbes(diagsLines);
                        break;
                    case DISCOVERED_CLOUD_COST_NAME:
                        discoveredCloudCostUploader.restoreDiags(diagsLines);
                        break;
                    case PRICE_TABLE_NAME:
                        priceTableUploader.restoreDiags(diagsLines);
                        break;
                    default:
                       // Other diags files should match a pattern
                        Matcher entitiesMatcher = ENTITIES_PATTERN.matcher(diagsName);
                        Matcher groupsMatcher = GROUPS_PATTERN.matcher(diagsName);
                        Matcher settingPolicyMatcher = SETTING_POLICIES_PATTERN.matcher(diagsName);
                        Matcher deploymentProfileMatcher = DEPLOYMENT_PROFILE_PATTERN.matcher(diagsName);
                        if (entitiesMatcher.matches()) {
                            long targetId = Long.parseLong(entitiesMatcher.group(1));
                            long lastUpdatedTime = Long.parseLong(entitiesMatcher.group(2));
                            restoreEntities(targetId, lastUpdatedTime, diagsLines);
                        } else if (groupsMatcher.matches()) {
                            long targetId = Long.parseLong(groupsMatcher.group(1));
                            restoreGroupsAndPolicies(targetId, diagsLines);
                        } else if (settingPolicyMatcher.matches()) {
                            long targetId = Long.parseLong(settingPolicyMatcher.group(1));
                            restoreSettingPolicies(targetId, diagsLines);
                        } else if (deploymentProfileMatcher.matches()) {
                            long targetId = Long.parseLong(deploymentProfileMatcher.group(1));
                            restoreTemplatesAndDeploymentProfiles(targetId, diagsLines);
                        } else {
                            logger.warn("Did not recognize diags file {}", diagsName);
                        }
                }
            } catch (Exception e) {
                logger.error("Failed to restore diags file " + diagsName, e);
            }
        }
        return targetStore.getAll();
    }

    /**
     * Clear the target spec oid table and re-populate the data in json-serialized target identifiers.
     *
     * @param serializedTargetIdentifiers a list of json-serialized target identifiers data
     * @throws DiagnosticsException if restore target identifiers into database fails
     */
    @VisibleForTesting
    void restoreTargetIdentifiers(@Nonnull final List<String> serializedTargetIdentifiers)
            throws DiagnosticsException {
        logger.info("Attempting to restore " + serializedTargetIdentifiers.size() + " target identifiers data "
                + "to database");
        targetPersistentIdentityStore.restoreDiags(serializedTargetIdentifiers);
        logger.info("Restored target identifiers into database.");
    }

    /**
     * Clear the target store and re-populate it based on a list of serialized targets.
     *
     * @param serializedTargets a list of json-serialized targets
     */
    @VisibleForTesting
    void restoreTargets(@Nonnull final List<String> serializedTargets) {
        logger.info("Attempting to restore " + serializedTargets.size() + " targets");
        targetStore.removeAllTargets();
        final long restoredTargetsCount = serializedTargets.stream()
            .map(serializedTarget -> {
                try {
                    return GSON.fromJson(serializedTarget, TargetInfo.class);
                } catch (JsonParseException e) {
                    logger.error("Error deserializing target. Error: {}, Target: {}",
                            e.toString(), serializedTarget);
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .map(targetInfo -> {
                try {
                    final Target target = targetStore.restoreTarget(targetInfo.getId(), targetInfo.getSpec());
                    scheduler.setDiscoverySchedule(target.getId(), 365, TimeUnit.DAYS);
                    return target;
                } catch (InvalidTargetException e) {
                    // This shouldn't happen, because the createTarget method we use
                    // here shouldn't do validation.
                    logger.error("Unexpected invalid target exception {} for spec: {}",
                            e.getMessage(), targetInfo.getSpec());
                    return null;
                } catch (TargetNotFoundException e) {
                    // This shouldn't happen, because we restore the target right before setting
                    // the schedule.
                    logger.error("Target not found when attempting to set discovery schedule.", e);
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .count();
        logger.info("Restored {} targets.", restoredTargetsCount);
    }

    /**
     * Repopulate the probe store using a list of serialized probe infos.
     *
     * @param serializedProbes a list of json-serialized probe infos
     */
    private void restoreProbes(@Nonnull final List<String> serializedProbes) {
        logger.info("Restoring {} probes", serializedProbes.size());

        final Map<Long, ProbeInfo> probeMap = serializedProbes.stream()
            .map(serialized -> {
                try {
                    return GSON.fromJson(serialized, ProbeInfoWithId.class);
                } catch (JsonParseException e) {
                    logger.error("Failed to deserialize probe info {} because of error: ",
                        serialized, e);
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(ProbeInfoWithId::getProbeId, ProbeInfoWithId::getProbeInfo));
        probeStore.overwriteProbeInfo(probeMap);
        logger.info("Done restoring {} probes.", probeMap.size());
    }

    /**
     * Restore target discovery schedules from a list of serialized schedules, usually produced
     * via a {@link #dumpDiags(ZipOutputStream) operation.
     *
     * @param serializedSchedules a list of json-serialized schedules
     */
    private void restoreSchedules(@Nonnull final List<String> serializedSchedules) {
        logger.info("Restoring " + serializedSchedules.size() + " schedules");
        Gson gson = new Gson();
        for (String json : serializedSchedules) {
            DiscoverySchedule schedule = gson.fromJson(json, DiscoverySchedule.class);
                try {
                    scheduler.setDiscoverySchedule(
                            schedule.getTargetId(),
                            schedule.getScheduleIntervalMillis(),
                            TimeUnit.MILLISECONDS);
                } catch (TargetNotFoundException e) {
                    logger.warn("While deserializing schedules, target id {} not found",
                        schedule.getTargetId());
                }
        }
        logger.info("Done restoring schedules");
    }

    /**
     * Restore entity DTOs for a given target from a list of serialized entities, usually produced
     * via a {@link #dumpDiags(ZipOutputStream)} operation.
     *
     * @param targetId the target ID that the restored entities will be associated with in
     *        the {@link EntityStore}
     * @param lastUpdatedTime the last updated time to set on the restored entities
     * @param serializedEntities a list of serialized entity DTOs
     * @throws TargetNotFoundException if no target exists with the provided targetId
     * @see EntityStore#entitiesDiscovered(long, long, List) entitiesDiscovered
     */
    private void restoreEntities(final long targetId, final long lastUpdatedTime,
                                @Nonnull final List<String> serializedEntities)
            throws TargetNotFoundException {
        logger.info("Restoring " + serializedEntities.size() + " entities for target " + targetId);
        final Map<Long, EntityDTO> identifiedEntityDTOs = serializedEntities.stream()
            .map(IdentifiedEntityDTO::fromJson)
            .collect(Collectors.toMap(IdentifiedEntityDTO::getOid, IdentifiedEntityDTO::getEntity));

        entityStore.entitiesRestored(targetId, lastUpdatedTime, identifiedEntityDTOs);
        logger.info("Done Restoring entities for target " + targetId);
    }

    /**
     * Restore discovered groups and associated policies for a given target from a list of
     * serialized DiscoveredGroupInfos.
     *
     * @param targetId the target ID that the restored groups and policies will be associated with
     * @param serializedGroups a list of serialized DiscoveredGroupInfos to restore
     */
    private void restoreGroupsAndPolicies(final long targetId,
                                         @Nonnull final List<String> serializedGroups) {
        logger.info("Restoring {} discovered groups for target {}", serializedGroups.size(),
            targetId);

        final List<CommonDTO.GroupDTO> discovered = serializedGroups.stream()
            .map(serialized -> {
                try {
                    return GSON.fromJson(serialized, DiscoveredGroupInfo.class)
                        .getDiscoveredGroup();
                } catch (JsonParseException e) {
                    logger.error("Failed to deserialize discovered group info {} for target {} " +
                        "because of error: ", serialized, targetId, e);
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        discoveredGroupUploader.setTargetDiscoveredGroups(targetId, discovered);

        logger.info("Done restoring {} discovered groups for target {}", discovered.size(),
            targetId);
    }

    /**
     * Restore discovered setting policies for a given target from a list of serialized
     * DiscoveredSettingPolicyInfos.
     *
     * @param targetId the target ID that the restored setting policies will be associated with
     * @param serializedSettingPolicies a list of serialized DiscoveredSettingPolicyInfos to restore
     */
    private void restoreSettingPolicies(final long targetId,
                                        @Nonnull final List<String> serializedSettingPolicies) {
        logger.info("Restoring {} discovered setting policies for target {}",
            serializedSettingPolicies.size(), targetId);

        final List<DiscoveredSettingPolicyInfo> discovered = serializedSettingPolicies.stream()
            .map(serialized -> {
                try {
                    return GSON.fromJson(serialized, DiscoveredSettingPolicyInfo.class);
                } catch (JsonParseException e) {
                    logger.error("Failed to deserialize discovered setting policy info {} for " +
                        "target {} because of error: ", serialized, targetId, e);
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        discoveredGroupUploader.restoreDiscoveredSettingPolicies(targetId, discovered);

        logger.info("Done restoring {} discovered setting policies for target {}",
            discovered.size(), targetId);
    }

    /**
     * Restore discovered templates and deployment profiles for a given target from a list of
     * serialized DeploymentProfileWithTemplates
     *
     * @param targetId the target ID associated with restored templates and deployment profiles
     * @param serialized a list of serialized DeploymentProfileWithTemplates to restore
     */
    private void restoreTemplatesAndDeploymentProfiles(final long targetId,
                                                       @Nonnull final List<String> serialized) {

        logger.info("Restoring {} discovered deployment profiles and associated templates for " +
            "target {}", serialized.size(), targetId);
        Map<DeploymentProfileInfo, Set<EntityProfileDTO>> mappedProfiles = serialized.stream()
            .map(string -> {
                try {
                    return GSON.fromJson(string, DeploymentProfileWithTemplate.class);
                } catch (JsonParseException e) {
                    logger.error("Failed to deserialize deployment profile and template {} for " +
                        "target {} because of error: ", string, targetId, e);
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(DeploymentProfileWithTemplate::getProfile,
                DeploymentProfileWithTemplate::getTemplates));

        templateDeploymentProfileUploader
            .setTargetsTemplateDeploymentProfileInfos(targetId, mappedProfiles);

        logger.info("Done restoring {} discovered deployment profiles and templates for target {}",
            mappedProfiles.size(), targetId);
    }

    /**
     * Internal class used for deserializing schedules. Only care about target ID and the interval.
     */
    private static class DiscoverySchedule {
        long targetId;
        long scheduleIntervalMillis;

        public long getTargetId() {
            return targetId;
        }

        long getScheduleIntervalMillis() {
            return scheduleIntervalMillis;
        }
    }

    /**
     * Internal class used for serializing and deserializing deployment profiles to preserve links
     * to templates
     */
    static class DeploymentProfileWithTemplate {
        private final DeploymentProfileInfo profile;
        private final Set<EntityProfileDTO> templates;

        DeploymentProfileInfo getProfile() {
            return profile;
        }

        Set<EntityProfileDTO> getTemplates() {
            return templates;
        }

        DeploymentProfileWithTemplate(@Nonnull final DeploymentProfileInfo profile,
                                      @Nonnull final Set<EntityProfileDTO> templates) {
            this.profile = Objects.requireNonNull(profile);
            this.templates = Objects.requireNonNull(templates);
        }
    }

    /**
     * Internal class used for serializing and deserializing probe info to preserve
     * links between probes and ids
     */
    static class ProbeInfoWithId {
        private long probeId;
        private ProbeInfo probeInfo;

        public long getProbeId() {
            return probeId;
        }

        ProbeInfoWithId(final long probeId, @Nonnull final ProbeInfo probeInfo) {
            this.probeId = probeId;
            this.probeInfo = probeInfo;
        }

        public ProbeInfo getProbeInfo() {
            return probeInfo;
        }
    }
}

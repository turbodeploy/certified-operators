package com.vmturbo.topology.processor.diagnostics;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
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
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;
import com.google.common.collect.Streams;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import io.prometheus.client.CollectorRegistry;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.DiscoveredGroupInfo;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.components.common.diagnostics.Diagnosable.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.Diags;
import com.vmturbo.components.common.diagnostics.RecursiveZipReader;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.identity.store.PersistentIdentityStore;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.TopologyProcessorComponent;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetInfo;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.IdentifiedEntityDTO;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.DiscoveryDumperSettings;
import com.vmturbo.topology.processor.plan.DiscoveredTemplateDeploymentProfileUploader;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.scheduling.TargetDiscoverySchedule;
import com.vmturbo.topology.processor.targets.InvalidTargetException;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetDeserializationException;
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

    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    private final TargetStore targetStore;
    private final PersistentIdentityStore targetPersistentIdentityStore;
    private final Scheduler scheduler;
    private final EntityStore entityStore;
    private final ProbeStore probeStore;
    private final DiscoveredGroupUploader discoveredGroupUploader;
    private final DiscoveredTemplateDeploymentProfileUploader templateDeploymentProfileUploader;
    private final IdentityProvider identityProvider;
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
            @Nonnull final DiagnosticsWriter diagnosticsWriter) {
        this.targetStore = targetStore;
        this.targetPersistentIdentityStore = targetPersistentIdentityStore;
        this.scheduler = scheduler;
        this.entityStore = entityStore;
        this.probeStore = probeStore;
        this.discoveredGroupUploader = discoveredGroupUploader;
        this.templateDeploymentProfileUploader = templateDeploymentProfileUploader;
        this.identityProvider = identityProvider;
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
        diagnosticsWriter.writeZipEntry(PROBES_DIAGS_FILE_NAME,
            probeStore.getProbes().entrySet().stream()
                .map(entry -> new ProbeInfoWithId(entry.getKey(), entry.getValue()))
                .map(probeInfoWithId -> GSON.toJson(probeInfoWithId, ProbeInfoWithId.class))
                .collect(Collectors.toList()),
            diagnosticZip
        );

        // Target identifiers in db, skip following tasks if failed to keep consistency.
        try {
            diagnosticsWriter.writeZipEntry(TARGET_IDENTIFIERS_DIAGS_FILE_NAME,
                targetPersistentIdentityStore.collectDiags(), diagnosticZip);
        } catch (DiagnosticsException e) {
            logger.error("Dump target spec oid table failed, following dumping tasks will be skipped. {}", e);
            return;
        }

        // Targets
        List<Target> targets = targetStore.getAll();
        diagnosticsWriter.writeZipEntry(TARGETS_DIAGS_FILE_NAME,
            targets.stream()
                .map(targetMapper)
                .map(GSON::toJson)
                .collect(Collectors.toList()),
            diagnosticZip);

        // Schedules
        diagnosticsWriter.writeZipEntry(SCHEDULES_DIAGS_FILE_NAME,
            targets.stream()
                .map(Target::getId)
                .map(scheduler::getDiscoverySchedule)
                .filter(Optional::isPresent).map(Optional::get)
                .map(schedule -> GSON.toJson(schedule, TargetDiscoverySchedule.class))
                .collect(Collectors.toList()),
            diagnosticZip);

        final Map<Long, List<DiscoveredGroupInfo>> discoveredGroups = discoveredGroupUploader
            .getDiscoveredGroupInfoByTarget();
        final Map<Long, Map<DeploymentProfileInfo, Set<EntityProfileDTO>>> deploymentProfiles =
            templateDeploymentProfileUploader.getDiscoveredDeploymentProfilesByTarget();
        final Multimap<Long, DiscoveredSettingPolicyInfo> settingPolicies =
            discoveredGroupUploader.getDiscoveredSettingPolicyInfoByTarget();

        // Files separated by target
        for (Target target : targets) {
            // TODO(Shai): add targetId to the content of the file (not just part of the name)
            final long id = target.getId();
            final Long lastUpdated = entityStore.getTargetLastUpdatedTime(id).orElse(0L);
            final String targetSuffix = "." + id + "-" + lastUpdated + ".diags";

            // Entities
            diagnosticsWriter.writeZipEntry("Entities" + targetSuffix,
                entityStore.discoveredByTarget(id).entrySet().stream()
                    .map(entry -> new IdentifiedEntityDTO(entry.getKey(), entry.getValue()))
                    .map(identifiedEntityMapper)
                    .collect(Collectors.toList()),
                diagnosticZip);

            //Discovered Groups
            diagnosticsWriter.writeZipEntry("DiscoveredGroupsAndPolicies" + targetSuffix,
                discoveredGroups.getOrDefault(id, Collections.emptyList()).stream()
                    .map(groupInfo -> GSON.toJson(groupInfo, DiscoveredGroupInfo.class))
                    .collect(Collectors.toList()),
                diagnosticZip
            );

            //Discovered Settings Policies
            diagnosticsWriter.writeZipEntry("DiscoveredSettingPolicies" + targetSuffix,
                settingPolicies.get(id).stream()
                    .map(settingPolicyInfo ->
                        GSON.toJson(settingPolicyInfo, DiscoveredSettingPolicyInfo.class))
                    .collect(Collectors.toList()),
                diagnosticZip
            );

            // Discovered Deployment Profiles including associations with Templates
            diagnosticsWriter.writeZipEntry("DiscoveredDeploymentProfilesAndTemplates" + targetSuffix,
                deploymentProfiles.getOrDefault(id, Collections.emptyMap()).entrySet().stream()
                    .map(entry -> new DeploymentProfileWithTemplate(entry.getKey(), entry.getValue()))
                    .map(profile -> GSON.toJson(profile, DeploymentProfileWithTemplate.class))
                    .collect(Collectors.toList()),
                diagnosticZip
            );
        }

        try {
            diagnosticsWriter.writeZipEntry(ID_DIAGS_FILE_NAME,
                identityProvider.collectDiags(),
                diagnosticZip);
        } catch (DiagnosticsException e) {
            logger.error(e.getErrors());
        }

        diagnosticsWriter.writePrometheusMetrics(CollectorRegistry.defaultRegistry, diagnosticZip);

        // discovery dumps
        File dumpDirInZip = new File("discoveryDumps");
        for (String dumpFileName: DiscoveryDumperSettings.DISCOVERY_DUMP_DIRECTORY.list()) {
            File dumpFile = new File(DiscoveryDumperSettings.DISCOVERY_DUMP_DIRECTORY, dumpFileName);
            if (dumpFile.exists() && dumpFile.isFile()) {
                File entryFileName = new File(dumpDirInZip, dumpFileName);
                try {
                    diagnosticsWriter.writeZipEntry(entryFileName.toString(), FileUtils.readFileToByteArray(dumpFile), diagnosticZip);
                } catch (IOException e) {
                    logger.warn("Failed to write discovery dump file {} to diags zip file", dumpFile);
                    logger.catching(Level.DEBUG, e);
                }
            } else {
                logger.warn("Not dumping item {} in discovery dump directory; not a regular file", dumpFileName);
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
                targetPersistentIdentityStore.collectDiags(), diagnosticZip);
        } catch (DiagnosticsException e) {
            logger.error("Dump target spec oid table failed. {}", e);
        }
        // Targets
        List<Target> targets = targetStore.getAll();
        diagnosticsWriter.writeZipEntry(TARGETS_DIAGS_FILE_NAME,
                                         targets.stream()
                                                .map(Target::getNoSecretAnonymousDto)
                                                .map(GSON::toJson)
                                                .collect(Collectors.toList()),
                                         diagnosticZip);

        // Schedules
        diagnosticsWriter.writeZipEntry(SCHEDULES_DIAGS_FILE_NAME,
                                         targets.stream()
                                                .map(Target::getId)
                                                .map(scheduler::getDiscoverySchedule)
                                                .filter(Optional::isPresent).map(Optional::get)
                                                .map(schedule -> GSON.toJson(schedule, TargetDiscoverySchedule.class))
                                                .collect(Collectors.toList()),
                                         diagnosticZip);

        // Entities (one file per target)
        for (Target target : targets) {
            // TODO(Shai): add tartgetId to the content of the file (not just part of the name)
            diagnosticsWriter.writeZipEntry("Entities." + target.getId() + ".diags",
                                             entityStore.discoveredByTarget(target.getId()).entrySet().stream()
                                                        .map(entry -> new IdentifiedEntityDTO(entry.getKey(), entry.getValue()))
                                                        .map(IdentifiedEntityDTO::toJsonAnonymized)
                                                        .collect(Collectors.toList()),
                                             diagnosticZip);
        }

        try {
            diagnosticsWriter.writeZipEntry(ID_DIAGS_FILE_NAME,
                identityProvider.collectDiags(),
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

    /**
     * Restore topology processor diagnostics from a {@link ZipInputStream}.
     *
     * @param zis the input stream with compressed diagnostics
     * @return list of restored targets
     * @throws TargetDeserializationException when one target or more cannot be serialized
     * @throws InvalidTargetException when the target is invalid
     * @throws IOException when there is a problem with I/O
     */
    public List<Target> restore(InputStream zis) throws IOException, TargetDeserializationException,
                                                            InvalidTargetException {
        Map<String, List<Diags>> diagnosticsByNameMap = Streams.stream(new RecursiveZipReader(zis))
            .collect(Collectors.groupingBy(Diags::getName));
//        Map<String, Diags> diagnosticsByNameMap = new HashMap<>();
//        new RecursiveZipReader(zis).iterator()
//            .forEachRemaining(diags -> diagnosticsByNameMap.put(diags.getName(), diags));

        // Ordering is important. The identityProvider diags must be restored before the probes diags.
        // Otherwise, a race condition occurs where if a probe (re)registers after the restore of
        // the probe diags and before the restore of the identityProvider diags, then a probeInfo
        // will be written to Consul with the old ID. This entry will later become a duplicate entry
        // and cause severe problems.
        // Process the identityProvider diags (if any) before processing any other diags
        List<Diags> identityDiags = diagnosticsByNameMap.get(ID_DIAGS_FILE_NAME);
        if (!identityDiags.isEmpty()) {
            identityDiags.forEach(diags -> {
                try {
                    identityProvider.restoreDiags(diags.getLines());
                } catch (Exception e) {
                    logger.error("Failed to restore Identity diags.", e);
                }});
            diagnosticsByNameMap.remove(ID_DIAGS_FILE_NAME);
        }

        for (List<Diags> diagsOfSpecificType : diagnosticsByNameMap.values()) {
            for (Diags diags : diagsOfSpecificType) {
                final String diagsName = diags.getName();
                final List<String> diagsLines = diags.getLines();
                if (diagsLines == null) {
                    continue;
                }
                try {
                    if (TARGET_IDENTIFIERS_DIAGS_FILE_NAME.equals(diagsName)) {
                        restoreTargetIdentifiers(diagsLines);
                    } else if (TARGETS_DIAGS_FILE_NAME.equals(diagsName)) {
                        restoreTargets(diagsLines);
                    } else if (SCHEDULES_DIAGS_FILE_NAME.equals(diagsName)) {
                        restoreSchedules(diagsLines);
                    } else if (PROBES_DIAGS_FILE_NAME.equals(diagsName)) {
                        restoreProbes(diagsLines);
                    } else {
                        Matcher entitiesMatcher = ENTITIES_PATTERN.matcher(diagsName);
                        Matcher groupsMatcher = GROUPS_PATTERN.matcher(diagsName);
                        Matcher settingPolicyMatcher = SETTING_POLICIES_PATTERN.matcher(diagsName);
                        Matcher deploymentProfileMatcher = DEPLOYMENT_PROFILE_PATTERN.matcher(diagsName);
                        if (entitiesMatcher.matches()) {
                            long targetId = Long.valueOf(entitiesMatcher.group(1));
                            long lastUpdatedTime = Long.valueOf(entitiesMatcher.group(2));
                            restoreEntities(targetId, lastUpdatedTime, diagsLines);
                        } else if (groupsMatcher.matches()) {
                            long targetId = Long.valueOf(groupsMatcher.group(1));
                            restoreGroupsAndPolicies(targetId, diagsLines);
                        } else if (settingPolicyMatcher.matches()) {
                            long targetId = Long.valueOf(settingPolicyMatcher.group(1));
                            restoreSettingPolicies(targetId, diagsLines);
                        } else if (deploymentProfileMatcher.matches()) {
                            long targetId = Long.valueOf(deploymentProfileMatcher.group(1));
                            restoreTemplatesAndDeploymentProfiles(targetId, diagsLines);
                        } else {
                            logger.warn("Did not recognize diags file {}", diagsName);
                        }
                    }
                } catch (Exception e) {
                    logger.error("Failed to restore diags file {}", diagsName, e);
                }
            }
        }
        return targetStore.getAll();
    }

    /**
     * Clear the target spec oid table and re-populate the data in json-serialized target identifiers.
     *
     * @param serializedTargetIdentifiers a list of json-serialized target identifiers data
     * @throws IdentityStoreException if deserialization fails
     * @throws DiagnosticsException if restore target identifiers into database fails
     */
    @VisibleForTesting
    void restoreTargetIdentifiers(@Nonnull final List<String> serializedTargetIdentifiers)
            throws IdentityStoreException, DiagnosticsException {
        logger.info("Attempting to restore " + serializedTargetIdentifiers.size() + " target identifiers data "
                + "to database");
        targetPersistentIdentityStore.restoreDiags(serializedTargetIdentifiers);
        logger.info("Restored target identifiers into database.");
    }

    /**
     * Clear the target store and re-populate it based on a list of serialized targets.
     *
     * @param serializedTargets a list of json-serialized targets
     * @throws TargetDeserializationException if deserialization fails
     */
    @VisibleForTesting
    void restoreTargets(@Nonnull final List<String> serializedTargets)
                                                            throws TargetDeserializationException {
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
                    return targetStore.restoreTarget(targetInfo.getId(), targetInfo.getSpec());
                } catch (InvalidTargetException e) {
                    // This shouldn't happen, because the createTarget method we use
                    // here shouldn't do validation.
                    logger.error("Unexpected invalid target exception {} for spec: {}",
                            e.getMessage(), targetInfo.getSpec());
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
     * via a {@link #dumpDiags(ZipOutputStream) operation.
     *
     * @param targetId the target ID that the restored entities will be associated with in
     *        the {@link EntityStore}
     * @param serializedEntities a list of serialized entity DTOs
     * @see {@link EntityStore#entitiesDiscovered(long, long, List)
     */
    private void restoreEntities(final long targetId, final long lastUpdatedTime,
                                @Nonnull final List<String> serializedEntities) {
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

        discoveredGroupUploader.setTargetDiscoveredSettingPolicies(targetId, discovered);

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

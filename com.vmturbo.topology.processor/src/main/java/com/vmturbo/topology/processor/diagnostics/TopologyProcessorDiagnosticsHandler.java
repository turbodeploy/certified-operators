package com.vmturbo.topology.processor.diagnostics;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Streams;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonParseException;

import io.prometheus.client.CollectorRegistry;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import common.HealthCheck.HealthState;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealthSubCategory;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.DiscoveredGroupInfo;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.BinaryDiagsRestorable;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagnosticsHandler;
import com.vmturbo.components.common.diagnostics.DiagnosticsWriter;
import com.vmturbo.components.common.diagnostics.Diags;
import com.vmturbo.components.common.diagnostics.DiagsZipReader;
import com.vmturbo.components.common.diagnostics.IDiagnosticsHandlerImportable;
import com.vmturbo.components.common.diagnostics.PrometheusDiagnosticsProvider;
import com.vmturbo.identity.store.PersistentIdentityStore;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.TopologyProcessorComponent;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetInfo;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader;
import com.vmturbo.topology.processor.cost.PriceTableUploader;
import com.vmturbo.topology.processor.discoverydumper.BinaryDiscoveryDumper;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.EntityStore.TargetIncrementalEntities;
import com.vmturbo.topology.processor.entity.IdentifiedEntityDTO;
import com.vmturbo.topology.processor.entity.IncrementalEntityByMessageDTO;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader.TargetDiscoveredData;
import com.vmturbo.topology.processor.group.discovery.InterpretedGroup;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.identity.StaleOidManager;
import com.vmturbo.topology.processor.operation.DiscoveryDumperSettings;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.rpc.TargetHealthRetriever;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.scheduling.TargetDiscoverySchedule;
import com.vmturbo.topology.processor.scheduling.UnsupportedDiscoveryTypeException;
import com.vmturbo.topology.processor.staledata.StalenessInformationProvider;
import com.vmturbo.topology.processor.targets.DuplicateTargetException;
import com.vmturbo.topology.processor.targets.InvalidTargetException;
import com.vmturbo.topology.processor.targets.PersistentTargetSpecIdentityStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.status.TargetStatusTracker;
import com.vmturbo.topology.processor.template.DiscoveredTemplateDeploymentProfileUploader;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService;

/**
 * Handle diagnostics for {@link TopologyProcessorComponent}.
 * The topology processor dump is made of a zip file that contains one file for the registered
 * targets, one file for their schedules, and then one file per target with the entities
 * associated with that target.
 */
public class TopologyProcessorDiagnosticsHandler implements IDiagnosticsHandlerImportable<Void> {

    private static final Logger logger = LogManager.getLogger();
    private static final String TARGETS_DIAGS_FILE_NAME = "Targets.diags";
    private static final String SCHEDULES_DIAGS_FILE_NAME = "Schedules.diags";
    private static final String PROBES_DIAGS_FILE_NAME = "Probes.diags";
    private static final String TARGET_HEALTH_DIAGS_FILE_NAME = "TargetHealth.diags";

    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

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
    private final TopologyPipelineExecutorService topologyPipelineExecutorService;
    private final Map<String, BinaryDiagsRestorable> fixedFilenameBinaryDiagnosticParts;
    private final BinaryDiscoveryDumper binaryDiscoveryDumper;
    private final TargetStatusTracker targetStatusTracker;
    private final StalenessInformationProvider stalenessProvider;
    private final StaleOidManager staleOidManager;
    private final TargetHealthRetriever targetHealthRetriever;

    TopologyProcessorDiagnosticsHandler(@Nonnull final TargetStore targetStore,
            @Nonnull final PersistentIdentityStore targetPersistentIdentityStore,
            @Nonnull final Scheduler scheduler, @Nonnull final EntityStore entityStore,
            @Nonnull final ProbeStore probeStore,
            @Nonnull final DiscoveredGroupUploader discoveredGroupUploader,
            @Nonnull final DiscoveredTemplateDeploymentProfileUploader templateDeploymentProfileUploader,
            @Nonnull final IdentityProvider identityProvider,
            @Nonnull final DiscoveredCloudCostUploader discoveredCloudCostUploader,
            @Nonnull final PriceTableUploader priceTableUploader,
            @Nonnull final TopologyPipelineExecutorService topologyPipelineExecutorService,
            @Nonnull final Map<String, BinaryDiagsRestorable> fixedFilenameBinaryDiagnosticParts,
            final BinaryDiscoveryDumper binaryDiscoveryDumper,
            final TargetStatusTracker targetStatusTracker,
            @Nonnull final StalenessInformationProvider stalenessProvider,
            @Nonnull final StaleOidManager staleOidManager,
            @Nonnull final TargetHealthRetriever targetHealthRetriever) {
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
        this.topologyPipelineExecutorService = topologyPipelineExecutorService;
        this.fixedFilenameBinaryDiagnosticParts = fixedFilenameBinaryDiagnosticParts;
        this.binaryDiscoveryDumper = binaryDiscoveryDumper;
        this.targetStatusTracker = targetStatusTracker;
        this.stalenessProvider = stalenessProvider;
        this.staleOidManager = staleOidManager;
        this.targetHealthRetriever = targetHealthRetriever;
    }

    /**
     * Dump TopologyProcessor diagnostics.
     *
     * @param diagnosticZip the ZipOutputStream to dump diags to
     */
    @Override
    public void dump(@Nonnull ZipOutputStream diagnosticZip) {
        final DiagnosticsWriter diagsWriter = new DiagnosticsWriter(diagnosticZip);

        // Identity information.
        diagsWriter.dumpDiagnosable(identityProvider);

        // Probes
        diagsWriter.writeZipEntry(PROBES_DIAGS_FILE_NAME, probeStore.getProbes()
                .entrySet()
                .stream()
                .map(entry -> new ProbeInfoWithId(entry.getKey(), entry.getValue()))
                .map(probe -> GSON.toJson(probe, ProbeInfoWithId.class))
                .iterator());

        // Target identifiers in db, skip following tasks if failed to keep consistency.
        diagsWriter.dumpDiagnosable(targetPersistentIdentityStore);

        // Targets
        final List<Target> targets = targetStore.getAll();

        saveTargetsInfoToDiags(diagsWriter, targets);

        // Schedules
        diagsWriter.writeZipEntry(SCHEDULES_DIAGS_FILE_NAME, targets.stream()
                .map(Target::getId)
                .map(scheduler::getDiscoverySchedule)
                .map(DiscoverySchedule::from)
                .filter(Objects::nonNull)
                .map(schedule -> GSON.toJson(schedule, DiscoverySchedule.class))
                .iterator());

        // Discovered costs
        diagsWriter.dumpDiagnosable(discoveredCloudCostUploader);
        // Discovered price tables.
        diagsWriter.dumpDiagnosable(priceTableUploader);

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
            final Iterator<String> objectsToWrite = entityStore.discoveredByTarget(id)
                    .entrySet()
                    .stream()
                    .map(entry -> new IdentifiedEntityDTO(entry.getKey(), entry.getValue()))
                    .map(ientity -> GSON.toJson(ientity, IdentifiedEntityDTO.class))
                    .iterator();
            diagsWriter.writeZipEntry("Entities" + targetSuffix, objectsToWrite);
            Optional<TargetIncrementalEntities> incrementalEntities =  entityStore.getIncrementalEntities(id);
            if (incrementalEntities.isPresent()) {
                // Incremental Entities
                LinkedHashMap<Integer, Collection<EntityDTO>> entitiesByMessageId = incrementalEntities.get()
                    .getEntitiesByMessageId();
                final List<String> entitiesToWrite = new ArrayList<>();
                for (Map.Entry<Integer, Collection<EntityDTO>> entityByMessageId : entitiesByMessageId.entrySet()) {
                    for (EntityDTO entityDTO : entityByMessageId.getValue()) {
                        entitiesToWrite.add(IncrementalEntityByMessageDTO
                            .toJson(new IncrementalEntityByMessageDTO(entityByMessageId.getKey(), entityDTO)));
                    }
                }
                diagsWriter.writeZipEntry("IncrementalEntities" + targetSuffix, entitiesToWrite.iterator());
            }

            //Discovered Groups
            diagsWriter.writeZipEntry("DiscoveredGroupsAndPolicies" + targetSuffix, discoveredGroups
                    .getOrDefault(id, Collections.emptyList())
                    .stream()
                    .map(group -> GSON.toJson(group, DiscoveredGroupInfo.class))
                    .iterator());

            //Discovered Settings Policies
            final List<DiscoveredSettingPolicyInfo> targetSettingPolicies = settingPolicies.get(id);
            if (targetSettingPolicies != null) {
                diagsWriter.writeZipEntry("DiscoveredSettingPolicies" + targetSuffix,
                        targetSettingPolicies.stream()
                                .map(snp -> GSON.toJson(snp, DiscoveredSettingPolicyInfo.class))
                                .iterator());
            }

            // Discovered Deployment Profiles including associations with Templates
            diagsWriter.writeZipEntry("DiscoveredDeploymentProfilesAndTemplates" + targetSuffix,
                    deploymentProfiles.getOrDefault(id, Collections.emptyMap())
                            .entrySet()
                            .stream()
                            .map(entry -> new DeploymentProfileWithTemplate(entry.getKey(),
                                    entry.getValue()))
                            .map(template -> GSON.toJson(template,
                                    DeploymentProfileWithTemplate.class))
                            .iterator());
        }


        diagsWriter.writeCustomEntries(binaryDiscoveryDumper);

        diagsWriter.dumpDiagnosable(
                new PrometheusDiagnosticsProvider(CollectorRegistry.defaultRegistry));
        fixedFilenameBinaryDiagnosticParts.values().forEach(diagsWriter::dumpDiagnosable);


        // discovery dumps
        File dumpDirInZip = new File("discoveryDumps");
        if (DiscoveryDumperSettings.DISCOVERY_DUMP_DIRECTORY.isDirectory()) {
            for (String dumpFileName : DiscoveryDumperSettings.DISCOVERY_DUMP_DIRECTORY.list()) {
                File dumpFile = new File(DiscoveryDumperSettings.DISCOVERY_DUMP_DIRECTORY, dumpFileName);
                if (dumpFile.exists() && dumpFile.isFile()) {
                    File entryFileName = new File(dumpDirInZip, dumpFileName);
                    try {
                        diagsWriter.writeZipEntry(entryFileName.toString(),
                                FileUtils.readFileToByteArray(dumpFile));
                    } catch (IOException e) {
                        logger.warn("Failed to write discovery dump file {} to diags zip file", dumpFile);
                        logger.catching(Level.DEBUG, e);
                    }
                } else {
                    logger.warn("Not dumping item {} in discovery dump directory; not a regular file", dumpFileName);
                }

            }
        }
        if (!diagsWriter.getErrors().isEmpty()) {
            diagsWriter.writeZipEntry(DiagnosticsHandler.ERRORS_FILE,
                    diagsWriter.getErrors().iterator());
        }

        // target status details (contains details information about last validation/discovery and info about failed discoveries)
        diagsWriter.dumpDiagnosable(targetStatusTracker);

        diagsWriter.dumpDiagnosable(staleOidManager);
    }

    private void saveTargetsInfoToDiags(DiagnosticsWriter diagsWriter, List<Target> targets) {
        final List<String> targetInfos = new ArrayList<>();
        final List<String> targetHealth = new ArrayList<>();
        for (Target target : targets) {
            try {
                targetInfos.add(GSON.toJson(target.getNoSecretDto(), TargetInfo.class));
                final long targetId = target.getId();
                final TargetHealth lastKnownTargetHealth =
                        stalenessProvider.getLastKnownTargetHealth(targetId);
                if (lastKnownTargetHealth != null) {
                    targetHealth.add(
                            GSON.toJson(new TargetHealthInfo(targetId, lastKnownTargetHealth),
                                    TargetHealthInfo.class));
                }
            } catch (JsonIOException e) {
                logger.error("Failed to parse information to Json for target " + target, e);
            }
        }
        diagsWriter.writeZipEntry(TARGETS_DIAGS_FILE_NAME, targetInfos.iterator());
        diagsWriter.writeZipEntry(TARGET_HEALTH_DIAGS_FILE_NAME, targetHealth.iterator());
    }

    /**
     * Dump TopologyProcessor anonymized diagnostics.
     *
     * @param diagnosticZip the ZipOutputStream to dump diags to
     */
    public void dumpAnonymizedDiags(ZipOutputStream diagnosticZip) {
        final DiagnosticsWriter diagsWriter = new DiagnosticsWriter(diagnosticZip);
        diagsWriter.dumpDiagnosable(identityProvider);
        // Target identifiers in db.
        diagsWriter.dumpDiagnosable(targetPersistentIdentityStore);
        // Targets
        List<Target> targets = targetStore.getAll();
        diagsWriter.writeZipEntry(TARGETS_DIAGS_FILE_NAME,
                targets.stream().map(Target::getNoSecretAnonymousDto).map(GSON::toJson).iterator());

        // Schedules
        diagsWriter.writeZipEntry(SCHEDULES_DIAGS_FILE_NAME, targets.stream()
                .map(Target::getId)
                .map(scheduler::getDiscoverySchedule)
                .map(DiscoverySchedule::from)
                .filter(Objects::nonNull)
                .map(schedule -> GSON.toJson(schedule, DiscoverySchedule.class))
                .iterator());

        // Entities (one file per target)
        for (Target target : targets) {
            // TODO(Shai): add tartgetId to the content of the file (not just part of the name)
            diagsWriter.writeZipEntry("Entities." + target.getId() + ".diags",
                    entityStore.discoveredByTarget(target.getId())
                            .entrySet()
                            .stream()
                            .map(entry -> new IdentifiedEntityDTO(entry.getKey(), entry.getValue()))
                            .map(IdentifiedEntityDTO::toJsonAnonymized)
                            .iterator());
        }

        // Discovered costs
        diagsWriter.dumpDiagnosable(discoveredCloudCostUploader);
        // Discovered price tables.
        diagsWriter.dumpDiagnosable(priceTableUploader);
        if (!diagsWriter.getErrors().isEmpty()) {
            diagsWriter.writeZipEntry(DiagnosticsHandler.ERRORS_FILE,
                    diagsWriter.getErrors().iterator());
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
        // TODO remote this method. We should export diags in an order suitable for restoring
        //final String diagsName = diags.getName();
        String diagsPath = diags.getName();
        final String diagsName = diagsPath.split("/")[diagsPath.split("/").length-1];
        switch (diagsName) {
            case IdentityProviderImpl.ID_DIAGS_FILE_NAME+""+DiagsZipReader.BINARY_DIAGS_SUFFIX:
            case IdentityProviderImpl.ID_DIAGS_FILE_NAME+""+DiagsZipReader.TEXT_DIAGS_SUFFIX:
                return 1;
            case PROBES_DIAGS_FILE_NAME:
                return 2;
            case PersistentTargetSpecIdentityStore.TARGET_IDENTIFIERS_DIAGS_FILE_NAME:
                return 3;
            case TARGETS_DIAGS_FILE_NAME:
                return 4;
            case SCHEDULES_DIAGS_FILE_NAME:
                return 5;
            // All other diagnostics files receive equal priority and may be loaded in any order
            default:
                return 6;
        }
    }

    /**
     * Restore topology processor diagnostics from a {@link ZipInputStream}.
     *
     * @param zis the input stream with compressed diagnostics
     * @return list of restored targets
     */
    @Override
    @Nonnull
    public String restore(@Nonnull InputStream zis, @Nullable Void context) throws DiagnosticsException {
        try {
            topologyPipelineExecutorService.blockBroadcasts(1, TimeUnit.HOURS);
            return internalRestore(zis);
        } finally {
            topologyPipelineExecutorService.unblockBroadcasts();
        }
    }

    @Nonnull
    private String internalRestore(@Nonnull InputStream zis) throws DiagnosticsException {
        // Ordering is important. The identityProvider diags must be restored before the probes diags.
        // Otherwise, a race condition occurs where if a probe (re)registers after the restore of
        // the probe diags and before the restore of the identityProvider diags, then a probeInfo
        // will be written to Consul with the old ID. This entry will later become a duplicate entry
        // and cause severe problems.
        final Stopwatch stopwatch = Stopwatch.createStarted();
        final DiagsZipReader diagsReader = new DiagsZipReader(zis,
            binaryDiscoveryDumper,
            true);
        final List<Diags> sortedDiagnostics = Streams.stream(diagsReader)
            .sorted(diagsRestoreComparator)
            .collect(Collectors.toList());
        final List<String> errors = new ArrayList<>();
        final Map<Long, TargetHealth> targetHealthFromDiags = new HashMap<>();
        for (Diags diags : sortedDiagnostics) {
            String diagsPath = diags.getName();
            final String diagsName = diagsPath.split("/")[diagsPath.split("/").length-1];
            final List<String> diagsLines = diags.getLines() == null ? null : diags.getLines().collect(Collectors.toList());
            final byte[] bytes = diags.getBytes();
            if (Stream.of(diagsLines, bytes).allMatch(Objects::isNull)) {
                continue;
            }
            try {
                switch (diagsName) {
                    // TODO change this switch with mapping from Diagnosable.getFileName()
                    case IdentityProviderImpl.ID_DIAGS_FILE_NAME + DiagsZipReader.TEXT_DIAGS_SUFFIX:
                        identityProvider.restoreStringDiags(diagsLines, null);
                        break;
                    case IdentityProviderImpl.ID_DIAGS_FILE_NAME + DiagsZipReader.BINARY_DIAGS_SUFFIX:
                        identityProvider.restoreDiags(bytes, null);
                        break;
                    case PersistentTargetSpecIdentityStore.TARGET_IDENTIFIERS_DIAGS_FILE_NAME +
                            DiagsZipReader.TEXT_DIAGS_SUFFIX:
                        restoreTargetIdentifiers(diagsLines);
                        break;
                    case TARGETS_DIAGS_FILE_NAME:
                        restoreTargets(diagsLines);
                        break;
                    case TARGET_HEALTH_DIAGS_FILE_NAME:
                        //If target health diags file exists, restore use the file.
                        parseTargetHealth(diagsLines, targetHealthFromDiags);
                        break;
                    case SCHEDULES_DIAGS_FILE_NAME:
                        restoreSchedules(diagsLines);
                        break;
                    case PROBES_DIAGS_FILE_NAME:
                        restoreProbes(diagsLines);
                        break;
                    case DiscoveredCloudCostUploader.DISCOVERED_CLOUD_COST_NAME +
                            DiagsZipReader.TEXT_DIAGS_SUFFIX:
                        discoveredCloudCostUploader.restoreDiags(diagsLines, null);
                        break;
                    case PriceTableUploader.PRICE_TABLE_NAME + DiagsZipReader.TEXT_DIAGS_SUFFIX:
                        priceTableUploader.restoreDiags(diagsLines, null);
                        break;
                    default:
                        // TODO Roman Zimine, Emanuele Maccherani history processing restoration is
                        // incomplete until restoring state from the diagnostics will not restore
                        // property sources (yaml, consul) and reload properties in topology
                        // processor. This should be done in a generic way. JIRA issue to fix this
                        // todo item: https://vmturbo.atlassian.net/browse/OM-60398.
                        final BinaryDiagsRestorable diagnosticPart =
                                        fixedFilenameBinaryDiagnosticParts.get(diagsName);
                        if (diagnosticPart != null) {
                            logger.info("'{}' state will be restored from '{}' file found in diagnostics.",
                                            diagnosticPart.getClass().getSimpleName(),
                                            diagsName);
                            diagnosticPart.restoreDiags(bytes, null);
                            break;
                        }
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
            } catch (RuntimeException | TargetNotFoundException e) {
                logger.error("Failed to restore diags file " + diagsName, e);
                errors.add(e.getLocalizedMessage());
            } catch (DiagnosticsException e) {
                logger.error("Failed to restore diags file " + diagsName, e);
                errors.addAll(e.getErrors());
            }
        }
        //If didn't populate target health data from diags, that means the file is empty or doesn't exist.
        //We make up target health data from restored targets, assuming all the targets are healthy.
        //We need the target health data restored during loading, otherwise the loaded topology environments will always have unhealthy targets,
        //and unhealthy targets won't generate any actions due to delayed_data feature.
        if (targetHealthFromDiags.isEmpty()) {
            makeUpTargetHealth(targetHealthFromDiags);
        }
        restoreTargetHealth(targetHealthFromDiags);
        if (errors.isEmpty()) {
            final String status = "Successfully restored " + targetStore.getAll().size() + " targets in "
                + stopwatch.elapsed(TimeUnit.SECONDS) + " seconds";
            logger.info(status);
            return status;
        } else {
            throw new DiagnosticsException(errors);
        }
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
        targetPersistentIdentityStore.restoreDiags(serializedTargetIdentifiers, null);
        logger.info("Restored target identifiers into database.");
    }

    /**
     * Parse the target health data from file, and populate the data into passed in map.
     * @param serializedTargetHealth String read from diags file
     * @param targetHealthFromDiags The map to populate into
     * @throws DiagnosticsException if can't resotre from file.
     */
    @VisibleForTesting
    void parseTargetHealth(@Nonnull final List<String> serializedTargetHealth, @Nonnull final Map<Long, TargetHealth> targetHealthFromDiags)
            throws DiagnosticsException {
        logger.info("Parsing {} targets health from file", serializedTargetHealth.size());
        targetHealthFromDiags.putAll(
                serializedTargetHealth.stream()
                .map(serialized -> {
                    try {
                        return GSON.fromJson(serialized, TargetHealthInfo.class);
                    } catch (JsonParseException e) {
                        logger.error("Failed to deserialize target health info {} because of error: ",
                                serialized, e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(TargetHealthInfo::getTargetId, TargetHealthInfo::getTargetHealth))
        );
    }

    /**
     * Make up target health data using existing targets information in target store, and populate the health data into passed in map.
     * We assume all the targets in target store are healthy.
     * @param targetHealthMap the map to populate.
     */
    @VisibleForTesting
    void makeUpTargetHealth(@Nonnull final Map<Long, TargetHealth> targetHealthMap) {
        if (!targetHealthMap.isEmpty()) {
            return;
        }
        List<Target> targets = targetStore.getAll();
        for(Target target: targets) {
            TargetHealth health = TargetHealth.newBuilder()
                    .setTargetName(target.getDisplayName())
                    .setHealthState(HealthState.NORMAL)
                    .setSubcategory(TargetHealthSubCategory.DISCOVERY)
                    .setConsecutiveFailureCount(0)
                    .setLastSuccessfulDiscoveryCompletionTime(System.currentTimeMillis())
                    .build();
            targetHealthMap.put(target.getId(), health);
        }
        logger.info("Target health file is missing in diags, made up {} targets health data from restored targets. All the targets are assumed healthy", targetHealthMap.size());
    }

    /**
     * Set target health data from passed in map into target health retriever.
     * @param targetHealthFromDiags
     */
    @VisibleForTesting
    void restoreTargetHealth(@Nonnull final Map<Long, TargetHealth> targetHealthFromDiags) {
        targetHealthRetriever.setHealthFromDiags(targetHealthFromDiags);
        logger.info("Done restoring {} targets into target health retriever.", targetHealthFromDiags.size());
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
                    scheduler.setDiscoverySchedule(target.getId(), DiscoveryType.FULL, 365, TimeUnit.DAYS, false);
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
                } catch (UnsupportedDiscoveryTypeException e) {
                    // this should not happen, since we support FULL discovery for all targets
                    logger.error("{} discovery not supported for target {}", DiscoveryType.FULL,
                        targetInfo.getId());
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
     * via a {@link #dump(ZipOutputStream) operation.
     *
     * @param serializedSchedules a list of json-serialized schedules
     */
    private void restoreSchedules(@Nonnull final List<String> serializedSchedules) {
        logger.info("Restoring " + serializedSchedules.size() + " schedules");
        Gson gson = new Gson();
        for (String json : serializedSchedules) {
            DiscoverySchedule schedule = gson.fromJson(json, DiscoverySchedule.class);
            Map<DiscoveryType, Long> discoveryIntervalByType = new HashMap<>();
            discoveryIntervalByType.put(DiscoveryType.FULL, schedule.getScheduleIntervalMillis());
            if (schedule.getIncrementalIntervalMillis() != null) {
                discoveryIntervalByType.put(DiscoveryType.INCREMENTAL,
                    schedule.getIncrementalIntervalMillis());
            }
            discoveryIntervalByType.forEach((discoveryType, interval) -> {
                try {
                    // don't attempt to discover targets loaded from diags, set the discovery
                    // interval to 365 days in light of OM-47010
                    scheduler.setDiscoverySchedule(schedule.getTargetId(), discoveryType, 365,
                        TimeUnit.DAYS, false);
                } catch (TargetNotFoundException e) {
                    logger.warn("While deserializing schedules, target id {} not found",
                        schedule.getTargetId());
                } catch (UnsupportedDiscoveryTypeException e) {
                    logger.warn("While deserializing schedules, {} discovery is not supported " +
                        "for target {}", discoveryType, schedule.getTargetId());
                }
            });
        }
        logger.info("Done restoring schedules");
    }

    /**
     * Restore entity DTOs for a given target from a list of serialized entities, usually produced
     * via a {@link #dump(ZipOutputStream)} operation.
     *
     * @param targetId the target ID that the restored entities will be associated with in
     *        the {@link EntityStore}
     * @param lastUpdatedTime the last updated time to set on the restored entities
     * @param serializedEntities a list of serialized entity DTOs
     * @throws TargetNotFoundException if no target exists with the provided targetId
     * @see EntityStore#entitiesDiscovered(long, long, int, DiscoveryType, List) entitiesDiscovered
     */
    private void restoreEntities(final long targetId, final long lastUpdatedTime,
                                @Nonnull final List<String> serializedEntities)
            throws TargetNotFoundException {
        logger.info("Restoring " + serializedEntities.size() + " entities for target " + targetId);
        final Map<Long, EntityDTO> identifiedEntityDTOs = serializedEntities.stream()
            .map(IdentifiedEntityDTO::fromJson)
            .collect(Collectors.toMap(IdentifiedEntityDTO::getOid, IdentifiedEntityDTO::getEntity));

        try {
            entityStore.entitiesRestored(targetId, lastUpdatedTime, identifiedEntityDTOs);
            logger.info("Done Restoring entities for target " + targetId);
        } catch (DuplicateTargetException e) {
            logger.warn("Ignoring entities from target {}. It is a duplicate target.", targetId,
                    e);
        }
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
        if (templateDeploymentProfileUploader.ignoreTemplatesForTarget(targetId)) {
            logger.info("Skipping template/deployment profile loading for target {}.", targetId);
        } else {
            logger.info("Restoring {} discovered deployment profiles and associated templates for "
                    + "target {}", serialized.size(), targetId);
            Map<DeploymentProfileInfo, Set<EntityProfileDTO>> mappedProfiles = serialized.stream()
                    .map(string -> {
                        try {
                            return GSON.fromJson(string, DeploymentProfileWithTemplate.class);
                        } catch (JsonParseException e) {
                            logger.error(
                                    "Failed to deserialize deployment profile and template {} for "
                                            + "target {} because of error: ", string, targetId, e);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toMap(DeploymentProfileWithTemplate::getProfile,
                            DeploymentProfileWithTemplate::getTemplates));

            templateDeploymentProfileUploader.setTargetsTemplateDeploymentProfileInfos(targetId,
                    mappedProfiles);

            logger.info(
                    "Done restoring {} discovered deployment profiles and templates for target {}",
                    mappedProfiles.size(), targetId);
        }
    }

    /**
     * Internal class used for deserializing schedules. Only care about target ID and intervals.
     * It always contain the full discovery interval, and may also incremental discovery interval
     * if it's supported by the target. If it's -1, it means it has been disabled by user.
     */
    private static class DiscoverySchedule {
        long targetId;
        // interval for full discovery, do not change name of variable for backward compatibility
        long scheduleIntervalMillis;
        Long incrementalIntervalMillis;

        public long getTargetId() {
            return targetId;
        }

        public void setTargetId(long targetId) {
            this.targetId = targetId;
        }

        long getScheduleIntervalMillis() {
            return scheduleIntervalMillis;
        }

        public void setScheduleIntervalMillis(long scheduleIntervalMillis) {
            this.scheduleIntervalMillis = scheduleIntervalMillis;
        }

        public Long getIncrementalIntervalMillis() {
            return incrementalIntervalMillis;
        }

        public void setIncrementalIntervalMillis(Long incrementalIntervalMillis) {
            this.incrementalIntervalMillis = incrementalIntervalMillis;
        }

        /**
         * Converts the given schedule map to an instance of {@link DiscoverySchedule}.
         *
         * @param scheduleMap map from discovery type to related schedule
         * @return {@link DiscoverySchedule}
         */
        @Nullable
        public static DiscoverySchedule from(
                @Nullable Map<DiscoveryType, TargetDiscoverySchedule> scheduleMap) {
            if (scheduleMap == null || scheduleMap.isEmpty()) {
                return null;
            }

            DiscoverySchedule discoverySchedule = new DiscoverySchedule();

            TargetDiscoverySchedule fullSchedule = scheduleMap.get(DiscoveryType.FULL);
            if (fullSchedule != null) {
                discoverySchedule.setScheduleIntervalMillis(
                    fullSchedule.getScheduleInterval(TimeUnit.MILLISECONDS));
                discoverySchedule.setTargetId(fullSchedule.getTargetId());
            }
            TargetDiscoverySchedule incrementalSchedule = scheduleMap.get(DiscoveryType.INCREMENTAL);
            if (incrementalSchedule != null) {
                discoverySchedule.setIncrementalIntervalMillis(
                    incrementalSchedule.getScheduleInterval(TimeUnit.MILLISECONDS));
                discoverySchedule.setTargetId(incrementalSchedule.getTargetId());
            }
            return discoverySchedule;
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

    /**
     * Internal class used to store information about target health to diagnostics.
     */
    static class TargetHealthInfo {
        private final long targetId;
        private final TargetHealth targetHealth;

        /**
         * Create {@link  TargetHealthInfo} instance.
         *
         * @param targetId target id
         * @param targetHealth {@link TargetHealth} instance describing health target state.
         */
        TargetHealthInfo(long targetId, TargetHealth targetHealth) {
            this.targetId = targetId;
            this.targetHealth = targetHealth;
        }

        public long getTargetId() {
            return targetId;
        }

        public TargetHealth getTargetHealth() {
            return targetHealth;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TargetHealthInfo)) {
                return false;
            }
            TargetHealthInfo that = (TargetHealthInfo)o;
            return targetId == that.targetId && targetHealth.equals(that.targetHealth);
        }

        @Override
        public int hashCode() {
            return Objects.hash(targetId, targetHealth);
        }
    }
}

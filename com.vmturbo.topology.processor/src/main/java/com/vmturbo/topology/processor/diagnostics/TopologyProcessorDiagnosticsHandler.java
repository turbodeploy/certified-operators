package com.vmturbo.topology.processor.diagnostics;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.DiscoveredGroupInfo;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.components.common.diagnostics.Diags;
import com.vmturbo.components.common.diagnostics.RecursiveZipReader;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.topology.processor.TopologyProcessorComponent;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetInfo;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.IdentifiedEntityDTO;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.plan.DiscoveredTemplateDeploymentProfileUploader;
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
 * TODO: add the probes
 */
public class TopologyProcessorDiagnosticsHandler {

    private static final String TARGETS_DIAGS_FILE_NAME = "Targets.diags";
    private static final String SCHEDULES_DIAGS_FILE_NAME = "Schedules.diags";
    private static final String ID_DIAGS_FILE_NAME = "Identity.diags";

    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    private final TargetStore targetStore;
    private final Scheduler scheduler;
    private final EntityStore entityStore;
    private final DiscoveredGroupUploader discoveredGroupUploader;
    private final DiscoveredTemplateDeploymentProfileUploader templateDeploymentProfileUploader;
    private final IdentityProvider identityProvider;
    private final DiagnosticsWriter diagnosticsWriter;

    private final Logger logger = LogManager.getLogger();

    TopologyProcessorDiagnosticsHandler(
        @Nonnull final TargetStore targetStore,
        @Nonnull final Scheduler scheduler,
        @Nonnull final EntityStore entityStore,
        @Nonnull final DiscoveredGroupUploader discoveredGroupUploader,
        @Nonnull final DiscoveredTemplateDeploymentProfileUploader templateDeploymentProfileUploader,
        @Nonnull final IdentityProvider identityProvider,
        @Nonnull final DiagnosticsWriter diagnosticsWriter) {
        this.targetStore = targetStore;
        this.scheduler = scheduler;
        this.entityStore = entityStore;
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

        diagnosticsWriter.writeZipEntry(ID_DIAGS_FILE_NAME, identityProvider.collectDiags(),
            diagnosticZip);
    }

    /**
     * Dump TopologyProcessor anonymized diagnostics.
     *
     * @param diagnosticZip the ZipOutputStream to dump diags to
     */
    public void dumpAnonymizedDiags(ZipOutputStream diagnosticZip) {
        dumpDiags(diagnosticZip, IdentifiedEntityDTO::toJsonAnonymized,
            Target::getNoSecretAnonymousDto);
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
    public List<Target> restore(InputStream zis)
        throws IOException, TargetDeserializationException,
        InvalidTargetException {
        for (Diags diags : new RecursiveZipReader(zis)) {
            String diagsName = diags.getName();
            try {
                if (TARGETS_DIAGS_FILE_NAME.equals(diagsName)) {
                    restoreTargets(diags.getLines());
                } else if (SCHEDULES_DIAGS_FILE_NAME.equals(diagsName)) {
                    restoreSchedules(diags.getLines());
                } else if (ID_DIAGS_FILE_NAME.equals(diagsName)) {
                    try {
                        identityProvider.restoreDiags(diags.getLines());
                    } catch (RuntimeException e) {
                        logger.error("Failed to restore Identity diags.", e);
                    }
                } else {
                    Matcher entitiesMatcher = ENTITIES_PATTERN.matcher(diagsName);
                    Matcher groupsMatcher = GROUPS_PATTERN.matcher(diagsName);
                    Matcher settingPolicyMatcher = SETTING_POLICIES_PATTERN.matcher(diagsName);
                    Matcher deploymentProfileMatcher = DEPLOYMENT_PROFILE_PATTERN.matcher(diagsName);
                    if (entitiesMatcher.matches()) {
                        long targetId = Long.valueOf(entitiesMatcher.group(1));
                        long lastUpdatedTime = Long.valueOf(entitiesMatcher.group(2));
                        restoreEntities(targetId, lastUpdatedTime, diags.getLines());
                    } else if (groupsMatcher.matches()) {
                        long targetId = Long.valueOf(groupsMatcher.group(1));
                        restoreGroupsAndPolicies(targetId, diags.getLines());
                    } else if (settingPolicyMatcher.matches()) {
                        long targetId = Long.valueOf(settingPolicyMatcher.group(1));
                        restoreSettingPolicies(targetId, diags.getLines());
                    } else if (deploymentProfileMatcher.matches()) {
                        long targetId = Long.valueOf(deploymentProfileMatcher.group(1));
                        restoreTemplatesAndDeploymentProfiles(targetId, diags.getLines());
                    } else {
                        logger.warn("Did not recognize diags file {}", diagsName);
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to restore diags file {}", diagsName, e);
            }
        }
        return targetStore.getAll();
    }

    /**
     * Clear the target store and re-populate it based on a list of serialized targets.
     *
     * @param serializedTargets a list of json-serialized targets
     * @throws IOException if there is a problem reading the file
     * @throws TargetDeserializationException if deserialization fails
     */
    public void restoreTargets(List<String> serializedTargets)
            throws TargetDeserializationException, IOException {
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
                    return targetStore.createTarget(targetInfo.getId(), targetInfo.getSpec());
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
     * Restore target discovery schedules from a list of serialized schedules, usually produced
     * via a {@link #dumpDiags(ZipOutputStream) operation.
     *
     * @param serializedSchedules a list of json-serialized shcedules
     * @throws IOException if there is a problem reading the file
     */
    public void restoreSchedules(List<String> serializedSchedules) throws IOException {
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
     * @throws IOException if there is a problem reading the file
     * @see {@link EntityStore#entitiesDiscovered(long, long, List)
     */
    public void restoreEntities(long targetId, long lastUpdatedTime,
                                List<String> serializedEntities) throws IOException {
        logger.info("Restoring " + serializedEntities.size() + " entities for target " + targetId);
        Map<Long, EntityDTO> entitiesMap = Maps.newHashMap();
        serializedEntities.stream()
            .map(IdentifiedEntityDTO::fromJson)
            .forEach(dto -> entitiesMap.put(dto.getOid(), dto.getEntity()));
        entityStore.entitiesRestored(targetId, lastUpdatedTime, entitiesMap);
        logger.info("Done Restoring entities for target " + targetId);
    }

    /**
     * Restore discovered groups and associated policies for a given target from a list of
     * serialized DiscoveredGroupInfos.
     *
     * @param targetId the target ID that the restored groups and policies will be associated with
     * @param serializedGroups a list of serialized DiscoveredGroupInfos to restore
     */
    public void restoreGroupsAndPolicies(final long targetId,
                                         @Nonnull final List<String> serializedGroups) {
        logger.info("Restoring {} discovered groups for target {}", serializedGroups.size(), targetId);

        final List<CommonDTO.GroupDTO> discovered = serializedGroups.stream()
            .map(serialized ->
                GSON.fromJson(serialized, DiscoveredGroupInfo.class).getDiscoveredGroup())
            .collect(Collectors.toList());

        discoveredGroupUploader.setTargetDiscoveredGroups(targetId, discovered);

        logger.info("Done restoring discovered groups for target {}", targetId);
    }

    /**
     * Restore discovered setting policies for a given target from a list of serialized
     * DiscoveredSettingPolicyInfos.
     *
     * @param targetId the target ID that the restored setting policies will be associated with
     * @param serializedSettingPolicies a list of serialized DiscoveredSettingPolicyInfos to restore
     */
    public void restoreSettingPolicies(final long targetId,
                                        @Nonnull final List<String> serializedSettingPolicies) {
        logger.info("Restoring {} discovered setting policies for target {}",
            serializedSettingPolicies.size(), targetId);

        final List<DiscoveredSettingPolicyInfo> discovered = serializedSettingPolicies.stream()
            .map(serialized -> GSON.fromJson(serialized, DiscoveredSettingPolicyInfo.class))
            .collect(Collectors.toList());

        discoveredGroupUploader.setTargetDiscoveredSettingPolicies(targetId, discovered);

        logger.info("Done restoring discovered setting policies for target {}", targetId);
    }

    /**
     * Restore discovered templates and deployment profiles for a given target from a list of
     * serialized DeploymentProfileWithTemplates
     *
     * @param targetId the target ID associated with restored templates and deployment profiles
     * @param serialized a list of serialized DeploymentProfileWithTemplates to restore
     */
    public void restoreTemplatesAndDeploymentProfiles(long targetId, List<String> serialized) {

        logger.info("Restoring {} discovered deployment profiles and associated templates for " +
            "target {}", serialized.size(), targetId);
        Map<DeploymentProfileInfo, Set<EntityProfileDTO>> mappedProfiles = serialized.stream()
            .map(string -> GSON.fromJson(string, DeploymentProfileWithTemplate.class))
            .collect(Collectors.toMap(DeploymentProfileWithTemplate::getProfile,
                DeploymentProfileWithTemplate::getTemplates));

        templateDeploymentProfileUploader
            .setTargetsTemplateDeploymentProfileInfos(targetId, mappedProfiles);

        logger.info("Done restoring discovered deployment profiles and templates for target {}",
            targetId);
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

        public long getScheduleIntervalMillis() {
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
}

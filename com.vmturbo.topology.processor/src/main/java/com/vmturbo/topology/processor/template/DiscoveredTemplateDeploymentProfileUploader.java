package com.vmturbo.topology.processor.template;

import static com.vmturbo.common.protobuf.topology.EnvironmentTypeUtil.CLOUD_PROBE_TYPES;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.EntityProfileToDeploymentProfile;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.UpdateDiscoveredTemplateDeploymentProfileResponse;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.UpdateDiscoveredTemplateDeploymentProfileResponse.TargetProfileIdentities;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.UpdateTargetDiscoveredTemplateDeploymentProfileRequest;
import com.vmturbo.common.protobuf.plan.DiscoveredTemplateDeploymentProfileServiceGrpc.DiscoveredTemplateDeploymentProfileServiceStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.ProfileDTO.DeploymentProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.conversions.typespecific.DesktopPoolInfoMapper;
import com.vmturbo.topology.processor.deployment.profile.DeploymentProfileMapper;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Object is used to send newly available templates and deployment profile data to be stored in plan orchestrator.
 * Templates and DeploymentProfiles are related in a many-to-many fashion. They must be updated together
 * in order to keep both sides of the relationship in sync.
 *
 * TODO: (DavidBlinn 1/31/2018) There is a problem with how we presently handle
 * TODO: discovered groups/policies/settings/templates/deployment profiles etc.
 * TODO: These data are tied with a specific discovery and topology but because they are stored
 * TODO: independently from each other, a discovery that completes in the middle of broadcast may
 * TODO: result in publishing these data from a different discovery than some other part of the
 * TODO: topology (ie the entities in the broadcast for a target may be from discovery A but the
 * TODO: discovered groups in the same broadcast may be from discovery B). These data should all be
 * TODO: stored together and copied together at the the first stage in the broadcast pipeline so
 * TODO: that we can guarantee the topology we publish is internally consistent.
 */
@ThreadSafe
public class DiscoveredTemplateDeploymentProfileUploader implements DiscoveredTemplateDeploymentProfileNotifier {

    private final DiscoveredTemplateDeploymentProfileServiceStub nonBlockingTemplateDeploymentProfileService;

    private static final Logger logger = LogManager.getLogger();

    private final EntityStore entityStore;

    private final TargetStore targetStore;

    private final Object storeLock = new Object();

    // Keeping missing template to make sure we don't overflow the log.
    private Cache<String, Object> loggedMissingTemplateCache;

    private static final int MISSING_TEMPLATE_IN_LINE = 20;

    // Map all discovered templates to list of deployment profile which associate with, for
    // those discovered templates without deployment profiles, it will map to a empty list.
    @GuardedBy("storeLock")
    private final Map<Long, EntityProfileToDeploymentProfileMap> discoveredTemplateToDeploymentProfile =
        new HashMap<>();

    // Contains all discovered deployment profile which have no reference template. Normally, it should
    // not happen, but right now, we allow this case and keep them in database.
    @GuardedBy("storeLock")
    private final Map<Long, Set<DeploymentProfileInfo>> orphanedDeploymentProfile = new HashMap<>();

    // per-target mapping of external to internal profile identifiers
    // maintained across all discoveries and never expiring
    private final Map<Long, Map<String, Long>> target2profileId2oid = new ConcurrentHashMap<>();

    // per-target mapping of external to internal deployment profile identifiers
    private final Map<Long, Map<String, Long>> target2deploymentProfileId2oid = new ConcurrentHashMap<>();

    private final long uploadTimeLimitMs;

    /**
     * Constructs templates deployment profile uploader.
     *
     * @param entityStore Contains information about discovered entities.
     * @param targetStore  Contains information about registered targets.
     * @param templatesDeploymentProfileService rpc service to use for upload template and deployment profile
     * @param uploadTimeLimit Time limit to wait for the upload.
     * @param uploadTimeUnit Time unit for the upload time limit.
     */
    public DiscoveredTemplateDeploymentProfileUploader(
            @Nonnull final EntityStore entityStore,
            @Nonnull final TargetStore targetStore,
            @Nonnull final DiscoveredTemplateDeploymentProfileServiceStub templatesDeploymentProfileService,
            final long uploadTimeLimit,
            @Nonnull final TimeUnit uploadTimeUnit) {
        this.entityStore = Objects.requireNonNull(entityStore);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.nonBlockingTemplateDeploymentProfileService = Objects.requireNonNull(templatesDeploymentProfileService);
        this.loggedMissingTemplateCache = CacheBuilder.newBuilder()
            .expireAfterAccess(6, TimeUnit.HOURS).build();
        this.uploadTimeLimitMs = uploadTimeUnit.toMillis(uploadTimeLimit);
    }

    /**
     * Store discovered templates and deployment profile in memory map and also keep track of relationship
     * between discovered templates with deployment profiles.
     *
     * @param targetId Id of target object.
     * @param entityProfileDTOs A list of discovered templates.
     * @param deploymentProfileDTOs A list of discovered deployment profiles.
     */
    @Override
    public void recordTemplateDeploymentInfo(long targetId,
                                @Nonnull Collection<EntityProfileDTO> entityProfileDTOs,
                                @Nonnull Collection<DeploymentProfileDTO> deploymentProfileDTOs,
                                @Nonnull List<EntityDTO> discoveredEntities) {
        Objects.requireNonNull(entityProfileDTOs);
        Objects.requireNonNull(deploymentProfileDTOs);

        // we may have some missing fields in the entity profiles, such as VCPU speed. Classic has
        // special logic to provide a fallback value for these fields when an entity is created from
        // a profile, but rather than do it during entity creation, we will fill them in during
        // discovery in XL, since we have access to all the discovered entities for the target in
        // memory anyways.
        Collection<EntityProfileDTO> entityProfiles = fillInVMProfileCpuSpeedValues(entityProfileDTOs,
                discoveredEntities);

        final EntityProfileToDeploymentProfileMap templateToDeploymentProfileMap =
            getTemplateToDeploymentProfileMapping(targetId, entityProfiles, deploymentProfileDTOs);
        final Set<DeploymentProfileInfo> deploymentProfileNoTemplates =
            getDeploymentProfileWithoutTemplate(targetId, deploymentProfileDTOs);
        // We synchronized put operation on two Maps to make sure thread safe.
        synchronized (storeLock) {
            discoveredTemplateToDeploymentProfile.put(targetId, templateToDeploymentProfileMap);
            orphanedDeploymentProfile.put(targetId, deploymentProfileNoTemplates);
        }
    }

    /**
     * If any VM {@link EntityProfileDTO}s in the collection passed in happens to be missing
     * a vcpu speed setting, this method will choose a vcpu speed setting for it to use, based on
     * the cpu speeds found on the physical machines discovered on the same target.
     *
     * @param sourceProfiles the set of {@link EntityProfileDTO}s to fill in VM Profile cpu speeds for
     * @return A collection of the same EntityProfileDTO objects, with cpu speeds set on any VM
     * profiles that didn't previous have them.
     */
    private Collection<EntityProfileDTO> fillInVMProfileCpuSpeedValues(Collection<EntityProfileDTO> sourceProfiles,
                                                                       List<EntityDTO> entities) {
        Collection<EntityProfileDTO> adjustedEntityProfiles = new ArrayList<>();
        // if there are any VM profiles missing CPU speeds, we will fill them in.
        float defaultCpuSpeed = -1; // we'll fetch this lazily
        for (EntityProfileDTO entityProfileDTO : sourceProfiles) {
            if (!entityProfileDTO.getVmProfileDTO().hasVCPUSpeed()) {
                // this VM Profile needs to have a vcpu speed set for it.
                // lazily get the default cpu speed if we haven't already
                if (defaultCpuSpeed == -1) {
                    defaultCpuSpeed = getHighestCpuSpeedFromEntities(entities);
                    logger.debug("Highest cpu speed for this target was determined to be {}",
                            defaultCpuSpeed);
                }
                // rebuild this profile w/the speed setting and add it to the collection
                EntityProfileDTO.Builder entityProfileBuilder = entityProfileDTO.toBuilder();
                entityProfileBuilder.getVmProfileDTOBuilder().setVCPUSpeed(defaultCpuSpeed);
                adjustedEntityProfiles.add(entityProfileBuilder.build());
                logger.trace("Defaulting VMProfile {} cpu speed to {}",
                        entityProfileDTO.getDisplayName(), defaultCpuSpeed);
                continue;
            }
            // no adjustment was needed, so add the original profile to the return set
            adjustedEntityProfiles.add(entityProfileDTO);
        }
        return adjustedEntityProfiles;
    }

    /**
     * Given a list of entities, find the highest individual CPU speed for any Physical Machines in
     * the entity list. This function is used to find a default cpu speed to use for any VM
     * templates discovered by the target that also found this list of entities.
     *
     * @param entities the collection of entities to inspect for a highest cpu speed
     * @return the highest cpu speed (in mhz) found across all physical machines in the set of
     * entities
     */
    private float getHighestCpuSpeedFromEntities(List<EntityDTO> entities) {
        // TODO: It'd be nice to start off with the vcpu increment size instead of 0. This would
        // match what classic is doing in the VirtualMachineProfileImpl.getSpeeds() function we are
        // modeling this on. The vcpu increment value would come from the VM Default Settings, which
        // we have on hand during discovery processing, when this code runs. We'll have to figure
        // out how to identify which VM Settings we should use in order to pull the increment into
        // this method.
        int highestCpuSpeed = entities.stream()
                .filter(entity -> (entity.getEntityType() == EntityType.PHYSICAL_MACHINE)
                        && entity.getPhysicalMachineData().hasCpuCoreMhz())
                .mapToInt(entity -> entity.getPhysicalMachineData().getCpuCoreMhz())
                .max()
                .orElse(0);
        return highestCpuSpeed;
    }

    /**
     * Store discovered templates and deployment profile in memory map and also keep track of relationship
     * between discovered templates with deployment profiles.
     *
     * @param targetId Id of target object.
     * @param profileTemplateMap mapping of deployment profile to set of templates
     */
    public void setTargetsTemplateDeploymentProfileInfos(long targetId,
                @Nonnull Map<DeploymentProfileInfo, Set<EntityProfileDTO>> profileTemplateMap) {
        Objects.requireNonNull(profileTemplateMap);

        Map<EntityProfileDTO, Set<DeploymentProfileInfo>> reverseMap = new HashMap<>();

        profileTemplateMap.forEach((profile, templateSet) ->
            templateSet.forEach(template -> {
                if (reverseMap.containsKey(template)) {
                    reverseMap.get(template).add(profile);
                } else {
                    reverseMap.put(template, Sets.newHashSet(profile));
                }
            })
        );
        synchronized (storeLock) {
            discoveredTemplateToDeploymentProfile.put(targetId,
                new EntityProfileToDeploymentProfileMap(reverseMap));
            orphanedDeploymentProfile.put(targetId,
                profileTemplateMap.entrySet().stream()
                    .filter(entry -> entry.getValue().isEmpty()).map(Entry::getKey)
                    .collect(Collectors.toSet())
            );
        }

    }

    @Override
    public void sendTemplateDeploymentProfileData() throws UploadException, InterruptedException {

        final CompletableFuture<Void> completionFuture = new CompletableFuture<>();

        StreamObserver<UpdateDiscoveredTemplateDeploymentProfileResponse> responseObserver =
            new StreamObserver<UpdateDiscoveredTemplateDeploymentProfileResponse>() {
                @Override
                public void onNext(final UpdateDiscoveredTemplateDeploymentProfileResponse response) {
                    for (TargetProfileIdentities identities : response.getTargetProfileIdentitiesList()) {
                       target2profileId2oid.computeIfAbsent(identities.getTargetOid(),
                                                        key -> new ConcurrentHashMap<>())
                                       .putAll(identities.getProfileIdToOidMap());
                       target2deploymentProfileId2oid.computeIfAbsent(identities.getTargetOid(),
                                                        key -> new ConcurrentHashMap<>())
                                       .putAll(identities.getDeploymentProfileIdToOidMap());
                    }
                }

                @Override
                public void onError(final Throwable throwable) {
                    logger.error("Error uploading discovered templates and deployment profile {}.",
                        throwable.getMessage());
                    completionFuture.completeExceptionally(throwable);
                }

                @Override
                public void onCompleted() {
                    logger.info("Uploaded discovered templates and deployment profile.");
                    completionFuture.complete(null);
                }
            };

        final StreamObserver<UpdateTargetDiscoveredTemplateDeploymentProfileRequest> requestObserver =
            nonBlockingTemplateDeploymentProfileService.updateDiscoveredTemplateDeploymentProfile(responseObserver);

        final Set<Long> targetsWithoutDiscoveredTemplateData = targetStore.getAll().stream()
            .map(Target::getId)
            .filter(targetId -> !discoveredTemplateToDeploymentProfile.containsKey(targetId))
            .collect(Collectors.toSet());

        // Synchronized map iterate operation in order to prevent other thread modify map in same time.
        synchronized (storeLock) {
            discoveredTemplateToDeploymentProfile.forEach((targetId, discoveredData) -> {
                if (isTargetEligibleForUpload(targetId)) {
                    final UpdateTargetDiscoveredTemplateDeploymentProfileRequest.Builder targetRequest =
                        UpdateTargetDiscoveredTemplateDeploymentProfileRequest.newBuilder()
                            .setTargetId(targetId);
                    addEntityProfileToDeploymentProfile(discoveredData, targetRequest);
                    targetRequest.addAllDeploymentProfileWithoutTemplates(
                        orphanedDeploymentProfile.getOrDefault(targetId, Collections.emptySet()));
                    targetRequest.build();
                    // Sending the template
                    requestObserver.onNext(targetRequest.build());
                }
            });

            // Send information about targets that have no discovered templates (yet). This will
            // prevent any existing data for those targets from being deleted on restart.
            targetsWithoutDiscoveredTemplateData.forEach(targetId -> {
                if (isTargetEligibleForUpload(targetId)) {
                    requestObserver.onNext(UpdateTargetDiscoveredTemplateDeploymentProfileRequest.newBuilder()
                        .setTargetId(targetId)
                        .setDataAvailable(false)
                        .build());
                }
            });
        }
        // After sending all the templates continue the logic in Plan Orchestrator
        requestObserver.onCompleted();

        try {
            completionFuture.get(uploadTimeLimitMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw new UploadException(e.getCause());
        }
    }

    /**
     * Is the target eligible for uploading templates? Cloud targets should not upload templates
     * and deployment profiles.
     * @param targetId the target id
     * @return true if the target is eligible for uploading templates. False otherwise.
     */
    private boolean isTargetEligibleForUpload(long targetId) {
        // We directly check for the probe type here instead of the probe category of CLOUD_MANAGEMENT
        // because CLOUD_MANAGEMENT probe category includes VMM which might have templates.
        Optional<SDKProbeType> probeType = targetStore.getProbeTypeForTarget(targetId);
        return probeType.isPresent() && !CLOUD_PROBE_TYPES.contains(probeType.get());
    }

    /**
     * Retrieve discovered deployment profiles organized by target, whether or not they are
     * associated with a template.
     *
     * @return a mapping of target id to a mapping of deployment profiles to templates associated
     *         with each discovered profile
     */
    public Map<Long, Map<DeploymentProfileInfo, Set<EntityProfileDTO>>>
                                                        getDiscoveredDeploymentProfilesByTarget() {
        final Map<Long, Map<DeploymentProfileInfo, Set<EntityProfileDTO>>> result = new HashMap<>();
        discoveredTemplateToDeploymentProfile.forEach((targetId, templateToProfileMap) -> {
            final Map<DeploymentProfileInfo, Set<EntityProfileDTO>> interior = new HashMap<>();
            templateToProfileMap.entityProfileDTOSetMap.forEach((template, profileSet) ->
                profileSet.forEach(profile -> {
                    if (interior.containsKey(profile)) {
                        interior.get(profile).add(template);
                    } else {
                        interior.put(profile, Sets.newHashSet(template));
                    }
                })
            );
            result.put(targetId, interior);
        });

        orphanedDeploymentProfile.forEach((targetId, profiles) -> {
            if (result.containsKey(targetId)) {
                profiles.forEach(profile -> result.get(targetId).put(profile, Collections.emptySet()));
            } else {
                final Map<DeploymentProfileInfo, Set<EntityProfileDTO>> interior = new HashMap<>();
                profiles.forEach(profile -> interior.put(profile, Collections.emptySet()));
                result.put(targetId, interior);
            }

        });
        return result;
    }

    /**
     * Remove related discovered templates and deployment profiles from Map when target is deleted.
     *
     * @param targetId Id of target object.
     */
    @Override
    public void deleteTemplateDeploymentProfileByTarget(long targetId) {
        // We synchronized delete operation on two Maps to make sure thread safe.
        synchronized (storeLock) {
            // Remove entry for targetId, templates will be deleted at next broadcast.
            discoveredTemplateToDeploymentProfile.remove(targetId);
            orphanedDeploymentProfile.remove(targetId);
        }
        target2deploymentProfileId2oid.remove(targetId);
        target2profileId2oid.remove(targetId);
    }

    @Override
    public Long getProfileId(long targetId, String profileVendorId) {
        return target2profileId2oid.getOrDefault(targetId, Collections.emptyMap())
                        .get(profileVendorId);
    }

    @Override
    public Long getDeploymentProfileId(long targetId, String deploymentProfileVendorId) {
        return target2deploymentProfileId2oid.getOrDefault(targetId, Collections.emptyMap())
                        .get(deploymentProfileVendorId);
    }

    @Override
    public void patchTopology(@Nonnull Map<Long, TopologyEntity.Builder> topology) {
        for (TopologyEntity.Builder entity : topology.values()) {
            // only one case of patching for now - no generalization
            if (entity.getEntityType() == EntityType.DESKTOP_POOL_VALUE) {
                TopologyEntityDTO.Builder builder = entity.getEntityBuilder();
                String masterImageVendorId = builder
                    .getEntityPropertyMapOrDefault(DesktopPoolInfoMapper.DESKTOP_POOL_TEMPLATE_REFERENCE,
                                                   null);
                if (masterImageVendorId != null) {
                    Set<Long> targets = Sets
                            .union(target2profileId2oid.keySet(),
                                   entity.getEntityBuilder().getOrigin()
                                                   .getDiscoveryOrigin()
                                                   .getDiscoveredTargetDataMap().keySet());
                    for (Long targetId : targets) {
                        Long templateOid = getProfileId(targetId, masterImageVendorId);
                        if (templateOid != null) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Patched template reference '{}' for desktop pool {} into {}",
                                             masterImageVendorId, entity.getOid(), templateOid);
                            }
                            builder.getTypeSpecificInfoBuilder()
                                            .getDesktopPoolBuilder()
                                            .setTemplateReferenceId(templateOid);
                            break;
                        }
                    }
                    builder.removeEntityPropertyMap(DesktopPoolInfoMapper.DESKTOP_POOL_TEMPLATE_REFERENCE);
                }
            }
        }
    }

    /**
     * Generate a Map which key is entity profile, and value is list of attached deployment profile.
     * This map will be sent to Plan component database and the relationship between templates with
     * deployment profile will also be stored.
     *
     * @param targetId Id of target object.
     * @param entityProfileDTOs A list of discovered templates.
     * @param deploymentProfileDTOs A list of discovered deployment profiles.
     * @return map of discovered templates to deployment profiles
     */
    private EntityProfileToDeploymentProfileMap getTemplateToDeploymentProfileMapping(
        long targetId,
        @Nonnull Collection<EntityProfileDTO> entityProfileDTOs,
        @Nonnull Collection<DeploymentProfileDTO> deploymentProfileDTOs) {
        final Map<String, EntityProfileDTO> templateIdMap = Sets.newHashSet(entityProfileDTOs).stream()
            .collect(Collectors.toMap(EntityProfileDTO::getId, Function.identity(),
                    (profile1, profile2) -> {
                logger.warn("Duplicate entity profile id {} in discovery response of target {}! "
                        + "Choosing the first encountered one.", profile1.getId(), targetId);
                return profile1;
        }));
        final Map<EntityProfileDTO, Set<DeploymentProfileDTO>> templateToDeploymentProfileDTOMap =
            buildDefaultEntityProfileMap(entityProfileDTOs);
        List<String> missingTemplates = Lists.newArrayList();
        // Build map of template to list of attached deployment profile
        for (DeploymentProfileDTO deploymentProfile : deploymentProfileDTOs) {
            final List<String> relatedTemplates = deploymentProfile.getRelatedEntityProfileIdList();
            for (String templateId : relatedTemplates) {
                Optional<EntityProfileDTO> template = Optional.ofNullable(templateIdMap.get(templateId));
                template.ifPresent(templateObj -> {
                    Set<DeploymentProfileDTO> deploymentProfileDTOList = templateToDeploymentProfileDTOMap
                        .getOrDefault(templateObj, new HashSet<>());
                    deploymentProfileDTOList.add(deploymentProfile);
                    templateToDeploymentProfileDTOMap.put(templateObj, deploymentProfileDTOList);
                });
                // Making sure we log the missing template without overloading the logger by using a cache
                if (!template.isPresent() && (loggedMissingTemplateCache.getIfPresent(templateId) == null)) {
                    missingTemplates.add(templateId);
                    loggedMissingTemplateCache.put(templateId, new Object());
                }
            }
        }
        if (!missingTemplates.isEmpty()) {
            logMissingTemplate(missingTemplates);
        }
        // Convert DeploymentProfileDTO to DeploymentProfileInfo
        final Map<EntityProfileDTO, Set<DeploymentProfileInfo>> templateToDeploymentProfileInfoMap =
            templateToDeploymentProfileDTOMap.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, entry ->
                    entry.getValue().stream()
                        .map(profile -> DeploymentProfileMapper.convertToDeploymentProfile(targetId,
                            entityStore, profile))
                        .collect(Collectors.toSet())
                ));
        return new EntityProfileToDeploymentProfileMap(templateToDeploymentProfileInfoMap);
    }

    /**
     * Logging error messages for missing templates.
     *
     * @param missingTemplate contains all the missing templates.
     */
    private void logMissingTemplate(List<String> missingTemplate) {
        List<List<String>> missingTemplatePartition =
                Lists.partition(missingTemplate, MISSING_TEMPLATE_IN_LINE);
        missingTemplatePartition.stream().forEach(subList -> logger.error("Missing templates: {}",
                String.join(", ", subList)));
    }

    /**
     * Generate a default Map which key should contains all entity profile and value will be empty set.
     *
     * @param entityProfileDTOs A list of discovered templates.
     * @return Map contains all entity profile as key and value is empty set.
     */
    private Map<EntityProfileDTO, Set<DeploymentProfileDTO>> buildDefaultEntityProfileMap(
        @Nonnull Collection<EntityProfileDTO> entityProfileDTOs) {
        return Sets.newHashSet(entityProfileDTOs).stream()
            .collect(Collectors.toMap(Function.identity(), entry -> new HashSet<>()));
    }


    private void addEntityProfileToDeploymentProfile(
        @Nonnull EntityProfileToDeploymentProfileMap profileMap,
        @Nonnull UpdateTargetDiscoveredTemplateDeploymentProfileRequest.Builder requestBuilder) {
        profileMap.getEntrySet().stream()
            .forEach(entry -> {
                final EntityProfileToDeploymentProfile profile = EntityProfileToDeploymentProfile.newBuilder()
                    .setEntityProfile(entry.getKey())
                    .addAllDeploymentProfile(entry.getValue())
                    .build();
                requestBuilder.addEntityProfileToDeploymentProfile(profile);
            });
    }

    /**
     * Find out all discovered deployment profiles which have no referenced templates. Right now,
     * we also store these deployment profiles into database.
     *
     * @param targetId Id of target object.
     * @param deploymentProfileDTOs A list of discovered deployment profiles.
     * @return Set of discovered deployment profiles which have no referenced templates.
     */
    private Set<DeploymentProfileInfo> getDeploymentProfileWithoutTemplate(
        long targetId, Collection<DeploymentProfileDTO> deploymentProfileDTOs) {
        return deploymentProfileDTOs.stream()
            .filter(deploymentProfileDTO -> deploymentProfileDTO.getRelatedEntityProfileIdList().isEmpty())
            .map(deploymentProfileDTO -> DeploymentProfileMapper.convertToDeploymentProfile(targetId,
                entityStore, deploymentProfileDTO))
            .collect(Collectors.toSet());
    }

    /**
     * A wrapper class contains a Map to represent relationship about which deployment profiles attach
     * to which templates. For discovered templates and deployment profiles, it could be many to many
     * relationship. And this Map will contains all discovered templates and all discovered deployment
     * profiles which have reference templates. For those deployment profiles have no reference templates,
     * it will be store at another Map: orphanedDeploymentProfile.
     */
    private class EntityProfileToDeploymentProfileMap {
        private Map<EntityProfileDTO, Set<DeploymentProfileInfo>> entityProfileDTOSetMap;

        public EntityProfileToDeploymentProfileMap(Map<EntityProfileDTO, Set<DeploymentProfileInfo>> profileMap) {
            this.entityProfileDTOSetMap = profileMap;
        }

        public Set<Entry<EntityProfileDTO, Set<DeploymentProfileInfo>>> getEntrySet() {
            return entityProfileDTOSetMap.entrySet();
        }
    }


    /**
     * Exception thrown when the upload fails.
     */
    public static class UploadException extends Exception {
        UploadException(@Nonnull final Throwable cause) {
            super(cause);
        }
    }
}

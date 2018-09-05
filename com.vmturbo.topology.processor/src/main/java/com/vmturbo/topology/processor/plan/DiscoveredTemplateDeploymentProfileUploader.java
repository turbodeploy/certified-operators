package com.vmturbo.topology.processor.plan;

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
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Sets;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.EntityProfileToDeploymentProfile;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.SetDiscoveredTemplateDeploymentProfileRequest;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.SetTargetDiscoveredTemplateDeploymentProfileRequest;
import com.vmturbo.common.protobuf.plan.DiscoveredTemplateDeploymentProfileServiceGrpc.DiscoveredTemplateDeploymentProfileServiceBlockingStub;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.common.dto.ProfileDTO.DeploymentProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.topology.processor.deployment.profile.DeploymentProfileMapper;
import com.vmturbo.topology.processor.entity.EntityStore;

import io.grpc.StatusRuntimeException;

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

    private final DiscoveredTemplateDeploymentProfileServiceBlockingStub templateDeploymentProfileService;

    private static final Logger logger = LogManager.getLogger();

    private final EntityStore entityStore;

    private final Object storeLock = new Object();

    // Map all discovered templates to list of deployment profile which associate with, for
    // those discovered templates without deployment profiles, it will map to a empty list.
    @GuardedBy("storeLock")
    private final Map<Long, EntityProfileToDeploymentProfileMap> DiscoveredTemplateToDeploymentProfile =
        new HashMap<>();

    // Contains all discovered deployment profile which have no reference template. Normally, it should
    // not happen, but right now, we allow this case and keep them in database.
    @GuardedBy("storeLock")
    private final Map<Long, Set<DeploymentProfileInfo>> orphanedDeploymentProfile = new HashMap<>();

    /**
     * Constructs templates deployment profile uploader.
     *
     * @param templatesDeploymentProfileService rpc service to use for upload template and deployment profile
     */
    public DiscoveredTemplateDeploymentProfileUploader(
        @Nonnull EntityStore entityStore,
        DiscoveredTemplateDeploymentProfileServiceBlockingStub templatesDeploymentProfileService) {
        Objects.requireNonNull(entityStore);
        this.entityStore = entityStore;
        this.templateDeploymentProfileService = Objects.requireNonNull(templatesDeploymentProfileService);
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
    public void setTargetsTemplateDeploymentProfile(long targetId,
                                @Nonnull Collection<EntityProfileDTO> entityProfileDTOs,
                                @Nonnull Collection<DeploymentProfileDTO> deploymentProfileDTOs) {
        Objects.requireNonNull(entityProfileDTOs);
        Objects.requireNonNull(deploymentProfileDTOs);

        final EntityProfileToDeploymentProfileMap templateToDeploymentProfileMap =
            getTemplateToDeploymentProfileMapping(targetId, entityProfileDTOs, deploymentProfileDTOs);
        final Set<DeploymentProfileInfo> deploymentProfileNoTemplates =
            getDeploymentProfileWithoutTemplate(targetId, deploymentProfileDTOs);
        // We synchronized put operation on two Maps to make sure thread safe.
        synchronized (storeLock) {
            DiscoveredTemplateToDeploymentProfile.put(targetId, templateToDeploymentProfileMap);
            orphanedDeploymentProfile.put(targetId, deploymentProfileNoTemplates);
        }

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
            DiscoveredTemplateToDeploymentProfile.put(targetId,
                new EntityProfileToDeploymentProfileMap(reverseMap));
            orphanedDeploymentProfile.put(targetId,
                profileTemplateMap.entrySet().stream()
                    .filter(entry -> entry.getValue().isEmpty()).map(Entry::getKey)
                    .collect(Collectors.toSet())
            );
        }

    }

    /**
     * Upload discovered templates and deployment profiles to Plan component. And upload will be happen
     * at broadcast time.
     *
     * @throws CommunicationException
     */
    @Override
    public void sendTemplateDeploymentProfileData() throws CommunicationException {
        final SetDiscoveredTemplateDeploymentProfileRequest.Builder request =
            SetDiscoveredTemplateDeploymentProfileRequest.newBuilder();
        // Synchronized map iterate operation in order to prevent other thread modify map in same time.
        synchronized (storeLock) {
            DiscoveredTemplateToDeploymentProfile.entrySet().stream()
                .forEach(entry -> {
                    final SetTargetDiscoveredTemplateDeploymentProfileRequest.Builder targetRequest =
                        SetTargetDiscoveredTemplateDeploymentProfileRequest.newBuilder()
                            .setTargetId(entry.getKey());
                    addEntityProfileToDeploymentProfile(entry.getValue(), targetRequest);
                    Optional.ofNullable(orphanedDeploymentProfile.get(entry.getKey()))
                        .ifPresent(targetRequest::addAllDeploymentProfileWithoutTemplates);
                    targetRequest.build();
                    request.addTargetRequest(targetRequest);
                });
        }
        try {
            templateDeploymentProfileService.setDiscoveredTemplateDeploymentProfile(request.build());
            logger.info("Uploaded discovered templates and deployment profile.");
        } catch (StatusRuntimeException e) {
            throw new CommunicationException("Unable to upload templates and deployment profile.", e);
        }

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
        DiscoveredTemplateToDeploymentProfile.forEach((targetId, templateToProfileMap) -> {
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
            DiscoveredTemplateToDeploymentProfile.remove(targetId);
            orphanedDeploymentProfile.remove(targetId);
        }
    }

    @Override
    public Map<Long, Set<EntityProfileDTO>> getTargetToEntityProfilesMap() {
        return DiscoveredTemplateToDeploymentProfile.entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey(), entry -> entry.getValue().getEntityProfiles()));
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
                if (!template.isPresent()) {
                    logger.error("DeploymentProfile {} related entity profile {} is not found",
                        deploymentProfile.getId(), templateId);
                }
            }
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
        @Nonnull SetTargetDiscoveredTemplateDeploymentProfileRequest.Builder requestBuilder) {
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

        public Set<EntityProfileDTO> getEntityProfiles() {
            return entityProfileDTOSetMap.keySet();
        }
    }

}

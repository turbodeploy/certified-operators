package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Mapper for getting aspects for entity or group.
 */
public class EntityAspectMapper {

    private final Logger logger = LogManager.getLogger();

    private Map<Integer, List<IAspectMapper>> aspectMappers;

    private final long realtimeTopologyContextId;

    public EntityAspectMapper(@Nonnull final StorageTierAspectMapper storageTierAspectMapper,
                              @Nonnull final VirtualVolumeAspectMapper virtualVolumeAspectMapper,
                              @Nonnull final CloudAspectMapper cloudAspectMapper,
                              @Nonnull final VirtualMachineAspectMapper virtualMachineMapper,
                              @Nonnull final DesktopPoolAspectMapper desktopPoolAspectMapper,
                              @Nonnull final MasterImageEntityAspectMapper masterImageEntityAspectMapper,
                              @Nonnull final PhysicalMachineAspectMapper physicalMachineAspectMapper,
                              @Nonnull final StorageAspectMapper storageAspectMapper,
                              @Nonnull final DiskArrayAspectMapper diskArrayAspectMapper,
                              @Nonnull final LogicalPoolAspectMapper logicalPoolAspectMapper,
                              @Nonnull final StorageControllerAspectMapper storageControllerAspectMapper,
                              @Nonnull final PortsAspectMapper portsAspectMapper,
                              @Nonnull final DatabaseAspectMapper databaseAspectMapper,
                              @Nonnull final DatabaseServerAspectMapper databaseServerAspectMapper,
                              @Nonnull final RegionAspectMapper regionAspectMapper,
                              @Nonnull final WorkloadControllerAspectMapper workloadControllerAspectMapper,
                              @Nonnull final ComputeTierAspectMapper computeTierAspectMapper,
                              @Nonnull final DatabaseServerTierAspectMapper databaseServerTierAspectMapper,
                              @Nonnull final DatabaseTierAspectMapper databaseTierAspectMapper,
                              @Nonnull final BusinessUserAspectMapper businessUserAspectMapper,
                              @Nonnull final VirtualVolumeEntityAspectMapper virtualVolumeEntityAspecMapper,
                              @Nonnull final CloudCommitmentAspectMapper cloudCommitmentAspectMapper,
                              @Nonnull final ContainerPlatformContextAspectMapper containerPlatformContextAspectMapper,
                              @Nonnull final ApplicationServiceAspectMapper appServiceAspectMapper,
                              @Nonnull final CloudApplicationAspectMapper cloudApplicationAspectMapper,
                              final long realtimeTopologyContextId) {
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        aspectMappers = new ImmutableMap.Builder<Integer, List<IAspectMapper>>()
            .put(EntityType.DATABASE_VALUE, ImmutableList.of(
                databaseAspectMapper,
                cloudAspectMapper))
            .put(EntityType.DATABASE_SERVER_VALUE, ImmutableList.of(
                databaseServerAspectMapper,
                cloudAspectMapper))
            .put(EntityType.DISK_ARRAY_VALUE, ImmutableList.of(
                diskArrayAspectMapper))
            .put(EntityType.LOGICAL_POOL_VALUE, ImmutableList.of(
                logicalPoolAspectMapper))
            .put(EntityType.STORAGE_CONTROLLER_VALUE, ImmutableList.of(
                storageControllerAspectMapper))
            .put(EntityType.NETWORK_VALUE, ImmutableList.of(
                portsAspectMapper))
            .put(EntityType.IO_MODULE_VALUE, ImmutableList.of(
                portsAspectMapper))
            .put(EntityType.PHYSICAL_MACHINE_VALUE, ImmutableList.of(
                physicalMachineAspectMapper))
            .put(EntityType.STORAGE_VALUE, ImmutableList.of(
                storageAspectMapper,
                virtualVolumeAspectMapper,
                cloudAspectMapper))
            .put(EntityType.STORAGE_TIER_VALUE, ImmutableList.of(
                storageTierAspectMapper,
                cloudAspectMapper,
                virtualVolumeAspectMapper))
            .put(EntityType.SWITCH_VALUE, ImmutableList.of(
                portsAspectMapper))
            .put(EntityType.VIRTUAL_MACHINE_VALUE, ImmutableList.of(
                virtualMachineMapper,
                cloudAspectMapper,
                containerPlatformContextAspectMapper,
                virtualVolumeAspectMapper,
                desktopPoolAspectMapper,
                masterImageEntityAspectMapper))
            .put(EntityType.DESKTOP_POOL_VALUE, ImmutableList.of(
                desktopPoolAspectMapper,
                masterImageEntityAspectMapper))
            .put(EntityType.VIRTUAL_VOLUME_VALUE, ImmutableList.of(
                virtualVolumeAspectMapper,
                cloudAspectMapper,
                virtualVolumeEntityAspecMapper))
            .put(EntityType.REGION_VALUE, ImmutableList.of(
                regionAspectMapper))
            .put(EntityType.CONTAINER_VALUE,
                ImmutableList.of(containerPlatformContextAspectMapper))
            .put(EntityType.CONTAINER_SPEC_VALUE,
                ImmutableList.of(containerPlatformContextAspectMapper))
            .put(EntityType.CONTAINER_POD_VALUE,
                ImmutableList.of(containerPlatformContextAspectMapper))
            .put(EntityType.WORKLOAD_CONTROLLER_VALUE,
                ImmutableList.of(workloadControllerAspectMapper,
                                containerPlatformContextAspectMapper))
            .put(EntityType.NAMESPACE_VALUE,
                ImmutableList.of(containerPlatformContextAspectMapper))
            .put(EntityType.SERVICE_VALUE,
                    ImmutableList.of(containerPlatformContextAspectMapper))
            .put(EntityType.COMPUTE_TIER_VALUE, ImmutableList.of(
                    computeTierAspectMapper))
            .put(EntityType.DATABASE_SERVER_TIER_VALUE, ImmutableList.of(
                    databaseServerTierAspectMapper))
            .put(EntityType.DATABASE_TIER_VALUE, ImmutableList.of(
                    databaseTierAspectMapper))
            .put(EntityType.BUSINESS_USER_VALUE, ImmutableList.of(
                    businessUserAspectMapper))
            // TODO (Cloud PaaS): ASP "legacy" APPLICATION_COMPONENT support, OM-83212
            //  can remove APPLICATION_COMPONENT_VALUE when legacy support not needed
            .put(EntityType.APPLICATION_COMPONENT_VALUE, ImmutableList.of(
                    cloudAspectMapper, appServiceAspectMapper))
            .put(EntityType.CLOUD_COMMITMENT_VALUE, ImmutableList.of(
                    cloudCommitmentAspectMapper))
            .put(EntityType.APPLICATION_COMPONENT_SPEC_VALUE, ImmutableList.of(
                    cloudAspectMapper, cloudApplicationAspectMapper))
            .put(EntityType.VIRTUAL_MACHINE_SPEC_VALUE, ImmutableList.of(
                    cloudAspectMapper, appServiceAspectMapper))
            .build();
    }

    /**
     * Get a map of aspectMapper -> list of entity types supported.
     *
     * @param requestedAspects restrict result to these aspects, if non-null
     * @return a map containing aspectMapper -> supported entity types
     */
    public Map<IAspectMapper, List<Integer>> getMappersAndEntityTypes(@Nullable Collection<String> requestedAspects) {
        Map<IAspectMapper, List<Integer>> reverseMap = new HashMap();

        aspectMappers.forEach((entityType, mappers) -> {
            mappers.forEach(mapper -> {
                if (requestedAspects == null ||
                    requestedAspects.isEmpty() ||
                    requestedAspects.contains(mapper.getAspectName().getApiName())) {
                    reverseMap.computeIfAbsent(mapper, e -> new ArrayList<>()).add(entityType);
                }
            });
        });

        return reverseMap;
    }

    /**
     * Get all aspects for a given entity.
     *
     * @param entity the entity to get aspect for
     * @return all aspects mapped by aspect name
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    @Nonnull
    public Map<AspectName, EntityAspect> getAspectsByEntity(@Nonnull TopologyEntityDTO entity)
            throws InterruptedException, ConversionException {
        return getAspectsByEntities(Collections.singletonList(entity), null).get(entity.getOid());
    }

    /**
     * Get a specific aspect for a given entity.
     *
     * @param entity the entity to get aspect for
     * @param aspectName the name of the aspect to gets
     * @return {@link EntityAspect} for the given entity and aspect name
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    @Nullable
    public EntityAspect getAspectByEntity(@Nonnull TopologyEntityDTO entity,
            @Nonnull AspectName aspectName) throws InterruptedException, ConversionException {
        return getAspectByEntity(entity.getEntityType(), mapper -> mapper.mapEntityToAspect(entity),
                aspectName);
    }

    /**
     * Get a specific aspect for a given entity.
     *
     * @param entityType entity type
     * @param applicator mapper
     * @param aspectName the name of the aspect to get
     * @return {@link EntityAspect} for the given entity and aspect name
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    @Nullable
    private EntityAspect getAspectByEntity(int entityType,
            @Nonnull AspectMapperApplicator applicator, @Nonnull AspectName aspectName)
            throws ConversionException, InterruptedException {
        final List<IAspectMapper> mappers = this.aspectMappers.get(entityType);
        if (mappers == null) {
            logger.debug("Aspect with name: {} for entity: {} not found", aspectName, entityType);
            return null;
        }
        // look for the aspect by that name and apply it; or else return null
        for (IAspectMapper mapper: this.aspectMappers.get(entityType)) {
            if (mapper.getAspectName().equals(aspectName)) {
                return applicator.applyAspectMapper(mapper);
            }
        }
        return null;
    }

    /**
     * Get the collection of {@link IAspectMapper} corresponding to the members of a (homogeneous) group
     *
     * @param members the members of a group to get mappers for
     * @return the collection of {@link IAspectMapper} corresponding to the given group
     */
    @Nonnull
    public List<IAspectMapper> getGroupMemberMappers(@Nonnull List<TopologyEntityDTO> members) {
        // TODO: handle groups with more than one entity type such as resource groups
        return getEntityTypeMappers(members.stream()
            .map(TopologyEntityDTO::getEntityType)
            .collect(Collectors.toList()));
    }

    /**
     * Get the collection of {@link IAspectMapper} corresponding to a set of types.
     * Only homogeneous groups are supported, so checking the type of the first member is sufficient
     * to determine the appropriate collection of {@link IAspectMapper}
     *
     * @param types for which the corresponding {@link IAspectMapper}s should be retrieved
     * @return a list of {@link IAspectMapper} corresponding to the type specified
     */
    @Nonnull
    public List<IAspectMapper> getEntityTypeMappers(@Nonnull List<Integer> types) {
        // TODO: handle groups with more than one entity type such as resource groups
        return types.stream()
                .findFirst()
                .map(t -> aspectMappers.getOrDefault(t, Collections.emptyList()))
                .orElse(Collections.emptyList());
    }

    /**
     * Get all aspects for a group and return as a mapping from aspect name to aspect DTO.
     *
     * @param members the members of a group to get aspect for
     * @return all aspects mapped by {@link AspectName}
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    @Nonnull
    public Map<AspectName, EntityAspect> getAspectsByGroup(@Nonnull List<TopologyEntityDTO> members)
            throws InterruptedException, ConversionException {
        return getAspectsByGroupUsingMappers(members, getGroupMemberMappers(members));
    }

    /**
     * Get a group of aspects specified by {@param mappers} for a group and
     * return as a mapping from aspect name to aspect DTO.
     *
     * @param members the members of a group to get aspect for
     * @param mappers the list of aspect mappers that should be applied to each group member
     * @return all aspects mapped by aspect name
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    @Nonnull
    public Map<AspectName, EntityAspect> getAspectsByGroupUsingMappers(
            @Nonnull List<TopologyEntityDTO> members, @Nonnull List<IAspectMapper> mappers)
            throws InterruptedException, ConversionException {
        final Map<AspectName, EntityAspect> aspects = new HashMap<>();
        for (IAspectMapper aspectMapper: mappers) {
            if (aspectMapper.supportsGroup()) {
                EntityAspect entityAspect = aspectMapper.mapEntitiesToAspect(members);
                if (entityAspect != null) {
                    aspects.put(aspectMapper.getAspectName(), entityAspect);
                }
            }
        }
        return aspects;
    }

    /**
     * Get a specific aspect for a group, given all members of the group and the name of the aspect
     * to get.
     *
     * @param members the members of a group to get aspect for
     * @param aspectName the name of the aspect to get
     * @return EntityAspect DTO for the given group and aspect name
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    @Nullable
    public EntityAspect getAspectByGroup(@Nonnull List<TopologyEntityDTO> members,
            @Nonnull String aspectName) throws InterruptedException, ConversionException {
        List<IAspectMapper> mappers = getGroupMemberMappers(members);
        for (IAspectMapper mapper: mappers) {
            if (mapper.supportsGroup() && mapper.getAspectName().getApiName().equals(aspectName)) {
                return mapper.mapEntitiesToAspect(members);
            }
        }
        return null;
    }

    /**
     * Get aspects for a list of arbitrary {@link TopologyEntityDTO} and return as
     * a mapping from OID to aspect name to aspect DTO.
     *
     * @param entities the entities for which to return aspects
     * @param aspectsList the {@link IAspectMapper}s to apply to each entity provided
     * @return A map of entity OID, to a map of aspect name to EntityAspect DTO
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    @Nonnull
    public Map<Long, Map<AspectName, EntityAspect>> getAspectsByEntities(
            @Nonnull Collection<TopologyEntityDTO> entities, @Nullable Collection<String> aspectsList)
            throws InterruptedException, ConversionException {
        // a mapping from aspectMapper -> list of entity types supported by that aspect mapper.
        Map<IAspectMapper, List<Integer>> mappers = getMappersAndEntityTypes(aspectsList);
        Map<IAspectMapper, List<TopologyEntityDTO>> entitiesByMapper = new HashMap<>();
        entities.forEach(entity -> {
            mappers.forEach((mapper, supportedEntityTypes) -> {
                if (supportedEntityTypes.contains(entity.getEntityType())) {
                    entitiesByMapper.computeIfAbsent(mapper, x -> new ArrayList<>()).add(entity);
                }
            });
        });

        // initialize the map that we will return
        Map<Long, Map<AspectName, EntityAspect>> aspects = entities.stream()
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, e -> new HashMap<>()));

        // iterate through aspect mappers, and feed each mapper all entities supported by that mapper.
        for (Map.Entry<IAspectMapper, List<TopologyEntityDTO>> entry : entitiesByMapper.entrySet()) {
            IAspectMapper mapper = entry.getKey();
            List<TopologyEntityDTO> matchingEntities = entry.getValue();

            // We will first call mapper.mapEntityToAspectBatch(), which handles entities in bulk.
            // If mapEntityToAspectBatch() is not implemented for this mapper, it will return an empty Optional.
            // In that case, revert to mapper.mapEntityToAspect(), which handles entities one at a time.
            Optional<Map<Long, EntityAspect>> aspectMap = mapper.mapEntityToAspectBatch(matchingEntities);
            if (aspectMap.isPresent()) {
                aspectMap.get().forEach((oid, aspect) -> {
                    aspects.get(oid).put(mapper.getAspectName(), aspect);
                });
            } else {
                for (TopologyEntityDTO entity : matchingEntities) {
                    EntityAspect aspect = mapper.mapEntityToAspect(entity);
                    if (aspect != null) {
                        aspects.get(entity.getOid()).put(mapper.getAspectName(), aspect);
                    }
                }
            }
        }

        return aspects;
    }

    /**
     * Convert a list of partial entities to aspect for real time topology.
     *
     * @param entities list of partial entities
     * @param aspectName name of the aspect
     *
     * @return map containing aspects with key as oid
     * @throws InterruptedException if thread is interrupted
     * @throws ConversionException if there is error in conversion of DTOs.
     */
    @Nonnull
    public Map<Long, EntityAspect> getAspectsByEntitiesPartial(
            @Nonnull final Collection<ApiPartialEntity> entities,
            @Nonnull final AspectName aspectName)
            throws InterruptedException, ConversionException {
        return getAspectsByEntitiesPartial(entities, aspectName, null);
    }

    /**
     * Convert a list of partial entities to aspect based on the given topology context.
     * If topology context is null, real time topology is used.
     *
     * @param entities list of partial entities
     * @param aspectName name of the aspect
     * @param topologyContextId the topology context id
     *
     * @return map containing aspects with key as oid
     * @throws InterruptedException if thread is interrupted
     * @throws ConversionException if there is error in conversion of DTOs.
     */
    @Nonnull
    public Map<Long, EntityAspect> getAspectsByEntitiesPartial(
            @Nonnull final Collection<ApiPartialEntity> entities,
            @Nonnull final AspectName aspectName,
            @Nullable final Long topologyContextId)
            throws InterruptedException, ConversionException {

        Map<Long, EntityAspect> ret = new HashMap<>();
        getAspectsByEntitiesPartial(entities, Collections.singletonList(aspectName.getApiName()),
                                    getPlanContextId(topologyContextId))
                .forEach((oid, aspects) -> {
                    EntityAspect aspect = aspects.get(aspectName);
                    if (aspect != null) {
                        ret.put(oid, aspect);
                    }
                });
        return ret;
    }

    private Optional<Long> getPlanContextId(@Nullable final Long topologyContextId) {
        if (topologyContextId == null || topologyContextId == realtimeTopologyContextId) {
            return Optional.empty();
        }
        return Optional.of(topologyContextId);
    }

    /**
     * Get aspects for a list of arbitrary {@link TopologyEntityDTO} and return as
     * a mapping from OID to aspect name to aspect DTO.
     *
     * @param entities the entities for which to return aspects
     * @param aspectsList the {@link IAspectMapper}s to apply to each entity provided
     * @param planContextId an optional plan context id
     *
     * @return A map of entity OID, to a map of aspect name to EntityAspect DTO
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    @Nonnull
    public Map<Long, Map<AspectName, EntityAspect>> getAspectsByEntitiesPartial(
            @Nonnull final Collection<ApiPartialEntity> entities,
            @Nullable final Collection<String> aspectsList,
            @Nonnull final Optional<Long> planContextId)
            throws InterruptedException, ConversionException {
        // a mapping from aspectMapper -> list of entity types supported by that aspect mapper.
        Map<IAspectMapper, List<Integer>> mappers = getMappersAndEntityTypes(aspectsList);
        Map<IAspectMapper, List<ApiPartialEntity>> entitiesByMapper = new HashMap<>();
        Set<ApiPartialEntity> entitySet = new HashSet<>(entities);

        entitySet.forEach(entity -> {
            mappers.forEach((mapper, supportedEntityTypes) -> {
                if (supportedEntityTypes.contains(entity.getEntityType())) {
                    entitiesByMapper.computeIfAbsent(mapper, x -> new ArrayList<>()).add(entity);
                }
            });
        });

        // initialize the map that we will return
        Map<Long, Map<AspectName, EntityAspect>> aspects = entitySet.stream()
                .collect(Collectors.toMap(ApiPartialEntity::getOid, e -> new HashMap<>()));

        // iterate through aspect mappers, and feed each mapper all entities supported by that mapper.
        for (Map.Entry<IAspectMapper, List<ApiPartialEntity>> entry : entitiesByMapper.entrySet()) {
            IAspectMapper mapper = entry.getKey();
            List<ApiPartialEntity> matchingEntities = entry.getValue();

            // We will first call mapper.mapEntityToAspectBatch(), which handles entities in bulk.
            // If mapEntityToAspectBatch() is not implemented for this mapper, it will return an empty Optional.
            // In that case, revert to mapper.mapEntityToAspect(), which handles entities one at a time.
            try {
                Optional<Map<Long, EntityAspect>> aspectMap = planContextId.isPresent()
                        ? mapper.mapPlanEntityToAspectBatchPartial(matchingEntities, planContextId.get())
                        : mapper.mapEntityToAspectBatchPartial(matchingEntities);
                aspectMap.ifPresent(
                        longEntityAspectMap -> longEntityAspectMap
                                .forEach((oid, aspect) -> aspects.get(oid)
                                        .put(mapper.getAspectName(), aspect)));
            } catch (InvalidOperationException e) {
                logger.error("Failed to map plan entity aspect: {}.", e.getMessage());
            }
        }

        return aspects;
    }

    /**
     * Applicator for the aspect mappers.
     */
    @FunctionalInterface
    private interface AspectMapperApplicator {
        @Nullable
        EntityAspect applyAspectMapper(@Nonnull IAspectMapper aspectMapper)
                throws InterruptedException, ConversionException;
    }
}

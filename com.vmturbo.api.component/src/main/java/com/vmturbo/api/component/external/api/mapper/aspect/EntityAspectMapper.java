package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Mapper for getting aspects for entity or group.
 */
public class EntityAspectMapper {

    private final Logger logger = LogManager.getLogger();

    private Map<Integer, List<IAspectMapper>> aspectMappers;

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
                              @Nonnull final RegionAspectMapper regionAspectMapper) {

        aspectMappers = new ImmutableMap.Builder<Integer, List<IAspectMapper>>()
            .put(EntityType.DATABASE_VALUE, ImmutableList.of(
                databaseAspectMapper,
                cloudAspectMapper))
            .put(EntityType.DATABASE_SERVER_VALUE, ImmutableList.of(
                databaseAspectMapper,
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
                virtualVolumeAspectMapper,
                desktopPoolAspectMapper,
                masterImageEntityAspectMapper))
            .put(EntityType.DESKTOP_POOL_VALUE, ImmutableList.of(
                desktopPoolAspectMapper,
                masterImageEntityAspectMapper))
            .put(EntityType.VIRTUAL_VOLUME_VALUE, ImmutableList.of(
                virtualVolumeAspectMapper))
            .put(EntityType.REGION_VALUE, ImmutableList.of(
                regionAspectMapper))
            .build();
    }

    /**
     * Get a group of aspects specified by {@param mappers} for a given entity.
     *
     * @param entity the entity to get aspect for
     * @param mappers the {@link IAspectMapper}s that should be applied to the entity in question
     * @return all aspects mapped by aspect name
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    @Nonnull
    public Map<AspectName, EntityAspect> getAspectsByEntityUsingMappers(
            @Nonnull TopologyEntityDTO entity, @Nullable List<IAspectMapper> mappers)
            throws InterruptedException, ConversionException {
        final Map<AspectName, EntityAspect> aspects = new HashMap<>();
        if (!CollectionUtils.isEmpty(mappers)) {
            for (IAspectMapper aspectMapper: mappers) {
                EntityAspect entityAspect = aspectMapper.mapEntityToAspect(entity);
                if (entityAspect != null) {
                    final AspectName aspectName = aspectMapper.getAspectName();
                    aspects.put(aspectName, entityAspect);
                    logger.debug("Added aspect " + aspectName + " to " + entity.getEntityType()
                            + " entity " + entity.getOid());
                }
            }
        }
        return aspects;
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
        return getAspectsByEntityUsingMappers(entity, aspectMappers.get(entity.getEntityType()));
    }

    /**
     * Get a specific aspect for a given entity.
     *
     * @param entity the entity to get aspect for
     * @param aspectName the name of the aspect to get
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
     * @param entity the entity to get aspect for
     * @param aspectName the name of the aspect to get
     * @return {@link EntityAspect} for the given entity and aspect name
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    @Nullable
    public EntityAspect getAspectByEntity(@Nonnull ApiPartialEntity entity,
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
     * To optimize performance, certain {@link IAspectMapper} implementations support generating a single aspect
     * representing a group of entities, then mapping that aspect to a collection where each element represents a
     * unique entity. If this functionality is supported by all the mappers that correspond to a given entity type,
     * it should be leveraged.
     *
     * @param classInstances of a given {@link TopologyEntityDTO} implementation for which to retrieve aspects
     * @param groupAspectEnabledMappers a list of {@link IAspectMapper} used to compute aspects of {@param classInstances}
     * @return a map of UUID to map of aspect name to aspect value
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    @Nonnull
    public Map<String, Map<AspectName, EntityAspect>> getExpandedAspectsByGroupUsingMappers(
            @Nonnull List<TopologyEntityDTO> classInstances,
            @Nullable List<IAspectMapper> groupAspectEnabledMappers)
            throws InterruptedException, ConversionException {
        final Map<String, Map<AspectName, EntityAspect>> uuidToAspectMap = new HashMap<>();
        if (CollectionUtils.isEmpty(groupAspectEnabledMappers)) {
            return uuidToAspectMap;
        }
        Map<AspectName, EntityAspect> aspectsByGroup = getAspectsByGroupUsingMappers(classInstances, groupAspectEnabledMappers);
        groupAspectEnabledMappers.forEach(mapper -> {
            AspectName aspectName = mapper.getAspectName();
            if (aspectsByGroup.containsKey(aspectName)) {
                Map<String, EntityAspect> uuidToAspect = mapper.mapOneToManyAspects(
                    classInstances, aspectsByGroup.get(aspectName));
                uuidToAspect.entrySet().stream().forEach(uuidToAspectEntry ->
                    uuidToAspectMap.computeIfAbsent(uuidToAspectEntry.getKey(), key -> Maps.newHashMap())
                        .put(aspectName, uuidToAspectEntry.getValue()));
            }
        });
        return uuidToAspectMap;
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
     * Get aspects for an arbitrary {@link TopologyEntityDTO} and return as
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
            @Nonnull List<TopologyEntityDTO> entities, @Nonnull List<IAspectMapper> aspectsList)
            throws InterruptedException, ConversionException {
        final Map<Long, Map<AspectName, EntityAspect>> oidToAspectMapMap = new HashMap<>();
        for (TopologyEntityDTO entity: entities) {
            Map<AspectName, EntityAspect> aspects =
                    getAspectsByEntityUsingMappers(entity, aspectsList);
            if (!aspects.isEmpty()) {
                oidToAspectMapMap.put(entity.getOid(), aspects);
            }
        }
        return oidToAspectMapMap;
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

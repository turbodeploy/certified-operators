package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.enums.AspectName;
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
     * Get all aspects for a given entity.
     *
     * @param entity the entity to get aspect for
     * @param aspectsToInclude a set of {@link AspectName}s to include, or null to include all aspects
     * @return all aspects mapped by aspect name
     */
    @Nonnull
    public Map<AspectName, EntityAspect> getAspectsByEntity(@Nonnull TopologyEntityDTO entity,
                                                        @Nullable Set<AspectName> aspectsToInclude) {
        final Map<AspectName, EntityAspect> aspects = new HashMap<>();
        // Find the list of entity mappers that may apply to this entity type
        final List<IAspectMapper> mappers =
            aspectMappers.getOrDefault(entity.getEntityType(), Collections.emptyList());
        mappers.forEach(aspectMapper -> {
            // Check if this aspect should be included. A null aspectsToInclude list means
            // include all aspects. Otherwise, include only those aspects specified in the list.
            final AspectName aspectName = aspectMapper.getAspectName();
            if (aspectsToInclude == null || aspectsToInclude.contains(aspectName)) {
                EntityAspect entityAspect = aspectMapper.mapEntityToAspect(entity);
                if (entityAspect != null) {
                    aspects.put(aspectName, entityAspect);
                    logger.debug("Added aspect " + aspectName + " to " + entity.getEntityType()
                        + " entity " + entity.getOid());
                }
            }
        });
        return aspects;
    }

    /**
     * Get a specific aspect for a given entity.
     *
     * @param entity the entity to get aspect for
     * @param aspectName the name of the aspect to get
     * @return {@link EntityAspect} for the given entity and aspect name
     */
    @Nullable
    public EntityAspect getAspectByEntity(@Nonnull TopologyEntityDTO entity,
            @Nonnull AspectName aspectName) {
        return getAspectByEntity(entity.getEntityType(), mapper -> mapper.mapEntityToAspect(entity),
                aspectName);
    }

    /**
     * Get a specific aspect for a given entity.
     *
     * @param entity the entity to get aspect for
     * @param aspectName the name of the aspect to get
     * @return {@link EntityAspect} for the given entity and aspect name
     */
    @Nullable
    public EntityAspect getAspectByEntity(@Nonnull ApiPartialEntity entity,
            @Nonnull AspectName aspectName) {
        return getAspectByEntity(entity.getEntityType(), mapper -> mapper.mapEntityToAspect(entity),
                aspectName);
    }

    /**
     * Get a specific aspect for a given entity.
     *
     * @param entityType entity type
     * @param mapper mapper
     * @param aspectName the name of the aspect to get
     * @return {@link EntityAspect} for the given entity and aspect name
     */
    @Nullable
    private EntityAspect getAspectByEntity(int entityType,
            @Nonnull Function<IAspectMapper, EntityAspect> mapper, @Nonnull AspectName aspectName) {
        final List<IAspectMapper> mappers = this.aspectMappers.get(entityType);
        if (mappers == null) {
            logger.debug("Aspect with name: {} for entity: {} not found", aspectName, entityType);
            return null;
        }
        // look for the aspect by that name and apply it; or else return null
        return mappers.stream()
                .filter(m -> m.getAspectName().equals(aspectName))
                .findAny()
                .map(mapper)
                .orElse(null);
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
        return members.stream()
                .map(TopologyEntityDTO::getEntityType)
                .findFirst()
                .map(t -> aspectMappers.getOrDefault(t, Collections.emptyList()))
                .orElse(Collections.emptyList());
    }

    /**
     * Get all aspects for a group and return as a mapping from aspect name to aspect DTO.
     * Only homogeneous groups are supported, so checking the type of the first member is sufficient
     * to determine the appropriate collection of {@link IAspectMapper}
     *
     * @param members the members of a group to get aspect for
     * @return all aspects mapped by aspect name
     */
    @Nonnull
    public Map<AspectName, EntityAspect> getAspectsByGroup(@Nonnull List<TopologyEntityDTO> members) {
        return getAspectsByGroup(members, null);
    }

    /**
     * Get all aspects for a group and return as a mapping from aspect name to aspect DTO.
     * Only homogeneous groups are supported, so checking the type of the first member is sufficient
     * to determine the appropriate collection of {@link IAspectMapper}
     *
     * @param members the members of a group to get aspect for
     * @param aspectsToInclude a set of {@link AspectName}s to include, or null to include all aspects
     * @return all aspects mapped by aspect name
     */
    public Map<AspectName, EntityAspect> getAspectsByGroup(@Nonnull List<TopologyEntityDTO> members,
                                                       @Nullable Set<AspectName> aspectsToInclude) {
        final Map<AspectName, EntityAspect> aspects = new HashMap<>();
        List<IAspectMapper> mappers = getGroupMemberMappers(members);
        mappers
            .stream()
            .filter(aspectMapper -> aspectsToInclude == null
                || aspectsToInclude.contains(aspectMapper.getAspectName()))
            .forEach(aspectMapper -> {
            if (aspectMapper.supportsGroup()) {
                EntityAspect entityAspect = aspectMapper.mapEntitiesToAspect(members);
                if (entityAspect != null) {
                    aspects.put(aspectMapper.getAspectName(), entityAspect);
                }
            }
        });
        return aspects;
    }

    /**
     * To optimize performance, certain {@link IAspectMapper} implementations support generating a single aspect
     * representing a group of entities, then mapping that aspect to a collection where each element represents a
     * unique entity. If this functionality is supported by all the mappers that correspond to a given entity type,
     * it should be leveraged.
     *
     * @param classInstances of a given {@link TopologyEntityDTO} implementation for which to retrieve aspects
     * @param aspectsToInclude a set of {@link AspectName}s to include, or null to include all aspects
     * @return a map of UUID to map of aspect name to aspect value
     */
    @Nonnull
    public Map<String, Map<AspectName, EntityAspect>> getExpandedAspectsByGroup(
                @Nonnull List<TopologyEntityDTO> classInstances,
                @Nullable final Set<AspectName> aspectsToInclude) {
        final Map<String, Map<AspectName, EntityAspect>> uuidToAspectMap = Maps.newHashMap();
        List<IAspectMapper> mappers = getGroupMemberMappers(classInstances);
        // If all mappers support group aspect expansion...
        if (!mappers.stream().map(x -> x.supportsGroupAspectExpansion()).collect(Collectors.toSet()).contains(false)) {
            Map<AspectName, EntityAspect> aspectsByGroup = getAspectsByGroup(classInstances);
            mappers.stream()
                .filter(aspectsToInclude::contains)
                .forEach(mapper -> {
                    AspectName aspectName = mapper.getAspectName();
                    if (aspectsByGroup.containsKey(aspectName)) {
                        Map<String, EntityAspect> uuidToAspect = mapper.mapOneToManyAspects(aspectsByGroup.get(aspectName));
                        if (uuidToAspectMap.isEmpty()) {
                            uuidToAspectMap.putAll(uuidToAspect.entrySet().stream().collect(Collectors.toMap(
                                e -> e.getKey(),
                                e -> Collections.singletonMap(aspectName, e.getValue()))
                            ));
                        } else {
                            uuidToAspect.entrySet().stream().forEach(entry ->
                                uuidToAspectMap.get(entry.getKey()).put(aspectName, entry.getValue()));
                        }
                    }
                });
        }
        return uuidToAspectMap;
    }

    /**
     * Get a specific aspect for a group, given all members of the group and the name of the aspect
     * to get.
     *
     * @param members the members of a group to get aspect for
     * @param aspectName the name of the aspect to get
     * @return EntityAspect DTO for the given group and aspect name
     */
    @Nullable
    public EntityAspect getAspectByGroup(@Nonnull List<TopologyEntityDTO> members, @Nonnull String aspectName) {
        List<IAspectMapper> mappers = getGroupMemberMappers(members);
        return mappers.stream()
            .filter(mapper -> mapper.supportsGroup() && mapper.getAspectName().equals(aspectName))
            .findAny()
            .map(applicableMapper -> applicableMapper.mapEntitiesToAspect(members))
            .orElse(null);
    }

    /**
     * Get all aspects for an arbitrary {@link TopologyEntityDTO} and return as
     * a mapping from OID to aspect name to aspect DTO.
     *
     * @param entities the entities for which to return aspects
     * @param aspectsToInclude a set of {@link AspectName}s to include, or null to include all aspects
     * @return A map of entity OID, to a map of aspect name to EntityAspect DTO
     */
    @Nonnull
    public Map<Long, Map<AspectName, EntityAspect>> getAspectsByEntities(
            @Nonnull List<TopologyEntityDTO> entities,
            @Nullable Set<AspectName> aspectsToInclude) {
        final Map<Long, Map<AspectName, EntityAspect>> oidToAspectMapMap = new HashMap<>();
        entities.stream().forEach(entity -> oidToAspectMapMap.put(
            entity.getOid(), getAspectsByEntity(entity, aspectsToInclude)));
        return oidToAspectMapMap;
    }
}

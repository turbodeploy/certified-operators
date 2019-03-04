package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Mapper for getting aspects for entity or group.
 */
public class EntityAspectMapper {

    private final Logger logger = LogManager.getLogger();

    private static Map<Integer, List<IAspectMapper>> ASPECT_MAPPERS;

    public EntityAspectMapper(@Nonnull final StorageTierAspectMapper storageTierAspectMapper,
                              @Nonnull final VirtualVolumeAspectMapper virtualVolumeAspectMapper,
                              @Nonnull final CloudAspectMapper cloudAspectMapper,
                              @Nonnull final VirtualMachineAspectMapper virtualMachineMapper,
                              @Nonnull final PhysicalMachineAspectMapper physicalMachineAspectMapper,
                              @Nonnull final StorageAspectMapper storageAspectMapper,
                              @Nonnull final PortsAspectMapper portsAspectMapper,
                              @Nonnull final DiskArrayAspectMapper diskArrayAspectMapper,
                              @Nonnull final LogicalPoolAspectMapper logicalPoolAspectMapper,
                              @Nonnull final DatabaseAspectMapper databaseAspectMapper,
                              @Nonnull final VirtualDisksAspectMapper virtualDisksAspectMapper) {

        ASPECT_MAPPERS = new ImmutableMap.Builder<Integer, List<IAspectMapper>>()
            .put(EntityType.DATABASE_VALUE, ImmutableList.of(
                databaseAspectMapper,
                cloudAspectMapper))
            .put(EntityType.DATABASE_SERVER_VALUE, ImmutableList.of(
                databaseAspectMapper,
                cloudAspectMapper))
            .put(EntityType.DISK_ARRAY_VALUE, ImmutableList.of(
                diskArrayAspectMapper))
            .put(EntityType.IO_MODULE_VALUE, ImmutableList.of(
                portsAspectMapper))
            .put(EntityType.LOGICAL_POOL_VALUE, ImmutableList.of(
                logicalPoolAspectMapper))
            .put(EntityType.NETWORK_VALUE, ImmutableList.of(
                portsAspectMapper))
            .put(EntityType.PHYSICAL_MACHINE_VALUE, ImmutableList.of(
                physicalMachineAspectMapper))
            .put(EntityType.STORAGE_VALUE, ImmutableList.of(
                storageAspectMapper,
                virtualDisksAspectMapper,
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
                virtualDisksAspectMapper))
            .build();
    }

    /**
     * Get all aspects for a given entity.
     *
     * @param entity the entity to get aspect for
     * @return all aspects mapped by aspect name
     */
    @Nonnull
    public Map<String, EntityAspect> getAspectsByEntity(@Nonnull TopologyEntityDTO entity) {
        final Map<String, EntityAspect> aspects = new HashMap<>();
        if (ASPECT_MAPPERS.containsKey(entity.getEntityType())) {
            ASPECT_MAPPERS.get(entity.getEntityType()).forEach(aspectMapper -> {
                EntityAspect entityAspect = aspectMapper.mapEntityToAspect(entity);
                if (entityAspect != null) {
                    aspects.put(aspectMapper.getAspectName(), entityAspect);
                }
            });
        }
        return aspects;
    }

    /**
     * Get a specific aspect for a given entity.
     *
     * @param entity the entity to get aspect for
     * @param aspectName the name of the aspect to get
     * @return EntityAspect DTO for the given entity and aspect name
     */
    @Nullable
    public EntityAspect getAspectByEntity(@Nonnull TopologyEntityDTO entity, @Nonnull String aspectName) {
        List<IAspectMapper> aspectMappers = ASPECT_MAPPERS.get(entity.getEntityType());
        if (aspectMappers == null) {
            logger.warn("Aspect with name: " + aspectName + " for entity: " + entity.getOid() +
                " not found");
            return null;
        }
        // look for the aspect by that name and apply it; or else return null
        return aspectMappers.stream()
            .filter(mapper -> mapper.getAspectName().equals(aspectName))
            .findAny()
            .map(mapper -> mapper.mapEntityToAspect(entity))
            .orElse(null);
    }

    /**
     * Get all aspects for a group and return as a mapping from aspect name to aspect DTO.
     *
     * @param members the members of a group to get aspect for
     * @return all aspects mapped by aspect name
     */
    @Nonnull
    public Map<String, EntityAspect> getAspectsByGroup(@Nonnull List<TopologyEntityDTO> members) {
        int entityType = members.stream().map(TopologyEntityDTO::getEntityType).findFirst().get();
        final Map<String, EntityAspect> aspects = new HashMap<>();
        if (ASPECT_MAPPERS.containsKey(entityType)) {
            ASPECT_MAPPERS.get(entityType).forEach(aspectMapper -> {
                if (aspectMapper.supportsGroup()) {
                    EntityAspect entityAspect = aspectMapper.mapEntitiesToAspect(members);
                    if (entityAspect != null) {
                        aspects.put(aspectMapper.getAspectName(), entityAspect);
                    }
                }
            });
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
     */
    @Nullable
    public EntityAspect getAspectByGroup(@Nonnull List<TopologyEntityDTO> members, @Nonnull String aspectName) {
        int entityType = members.stream().map(TopologyEntityDTO::getEntityType).findFirst().get();
        List<IAspectMapper> aspectMappers = ASPECT_MAPPERS.get(entityType);
        if (aspectMappers == null) {
            logger.warn("Aspect: " + aspectName + " is not supported for group type: " + entityType);
            return null;
        }
        return aspectMappers.stream()
            .filter(mapper -> mapper.supportsGroup() && mapper.getAspectName().equals(aspectName))
            .findAny()
            .map(applicableMapper -> applicableMapper.mapEntitiesToAspect(members))
            .orElse(null);
    }
}

package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
                              @Nonnull final CloudAspectMapper cloudAspectMapper) {
        // todo: add more aspect mappers here when they are implemented
        ASPECT_MAPPERS = ImmutableMap.of(
                EntityType.STORAGE_TIER_VALUE, ImmutableList.of(
                        storageTierAspectMapper,
                        virtualVolumeAspectMapper,
                        cloudAspectMapper
                ),
                EntityType.VIRTUAL_MACHINE_VALUE, ImmutableList.of(
                        virtualVolumeAspectMapper
                )
        );
    }

    /**
     * Get all aspects for a given entity and return as a mapping from aspect name to aspect DTO.
     *
     * @param entity the entity to get aspect for
     * @return all aspects mapped by aspect name
     */
    public Map<String, EntityAspect> getAspectsByEntity(@Nonnull TopologyEntityDTO entity) {
        if (!ASPECT_MAPPERS.containsKey(entity.getEntityType())) {
            logger.warn("Aspect is not supported for entity type: {}",
                    EntityType.forNumber(entity.getEntityType()));
            return Collections.emptyMap();
        }

        final Map<String, EntityAspect> aspects = new HashMap<>();
        ASPECT_MAPPERS.get(entity.getEntityType()).forEach(aspectMapper -> {
            EntityAspect entityAspect = aspectMapper.map(entity);
            if (entityAspect != null) {
                aspects.put(aspectMapper.getAspectName(), entityAspect);
            }
        });
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
        if (!ASPECT_MAPPERS.containsKey(entity.getEntityType())) {
            logger.warn("Aspect is not supported for entity type: {}",
                    EntityType.forNumber(entity.getEntityType()));
            return null;
        }

        Optional<IAspectMapper> aspectMapper = ASPECT_MAPPERS.get(entity.getEntityType()).stream()
                .filter(mapper -> mapper.getAspectName().equals(aspectName))
                .findAny();
        if (!aspectMapper.isPresent()) {
            logger.warn("Aspect: {} is not supported for entity type: {}", aspectName,
                    EntityType.forNumber(entity.getEntityType()));
            return null;
        }
        return aspectMapper.get().map(entity);
    }

    /**
     * Get all aspects for a group and return as a mapping from aspect name to aspect DTO.
     *
     * @param members the members of a group to get aspect for
     * @return all aspects mapped by aspect name
     */
    public Map<String, EntityAspect> getAspectsByGroup(@Nonnull List<TopologyEntityDTO> members) {
        int entityType = members.stream().map(TopologyEntityDTO::getEntityType).findFirst().get();
        if (!ASPECT_MAPPERS.containsKey(entityType)) {
            return Collections.emptyMap();
        }

        final Map<String, EntityAspect> aspects = new HashMap<>();
        ASPECT_MAPPERS.get(entityType).forEach(aspectMapper -> {
            if (aspectMapper.supportsGroup()) {
                EntityAspect entityAspect = aspectMapper.map(members);
                if (entityAspect != null) {
                    aspects.put(aspectMapper.getAspectName(), entityAspect);
                }
            }
        });
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
        if (!ASPECT_MAPPERS.containsKey(entityType)) {
            logger.warn("Aspect is not supported for group type: {} ", EntityType.forNumber(entityType));
            return null;
        }

        Optional<IAspectMapper> aspectMapper = ASPECT_MAPPERS.get(entityType).stream()
                .filter(mapper -> mapper.supportsGroup() && mapper.getAspectName().equals(aspectName))
                .findAny();
        if (!aspectMapper.isPresent()) {
            logger.warn("Aspect: {} is not supported for group type: {}", aspectName,
                    EntityType.forNumber(entityType));
            return null;
        }
        return aspectMapper.get().map(members);
    }
}

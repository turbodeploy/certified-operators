package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

public interface IAspectMapper {

    /**
     * Map a single entity into one entity aspect object.
     *
     * @param entity the entity to get aspect for
     * @return the entity aspect for the given entity, or null if no aspect for this entity
     */
    @Nullable
    EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity);

    /**
     * Map a list of entities into a single entity aspect object. This needs to be implemented if
     * {@link IAspectMapper#supportsGroup()} returns true.
     *
     * @param entities list of entities to get aspect for, which are members of a group
     * @return the entity aspect for given list of entities
     */
    default EntityAspect mapEntitiesToAspect(@Nonnull final List<TopologyEntityDTO> entities) {
        return null;
    }

    /**
     * Returns the aspect name that can be used for filtering.
     *
     * @return the name of the aspect
     */
    @Nonnull
    String getAspectName();

    /**
     * Defines whether or not this aspect mapper supports group aspect. If this is true, then
     * {@link IAspectMapper#mapEntitiesToAspect(List)} need to be implemented.
     *
     * @return true if group aspect is supported, otherwise false
     */
    default boolean supportsGroup() {
        return false;
    }

    /**
     * Static method to define whether or not the entity belongs to Cloud service.
     *
     * @return true if the entity is belonged to Cloud service
     */
    static boolean isCloudEntity(@Nonnull final TopologyEntityDTO entity) {
        return entity.hasEnvironmentType() &&
            entity.getEnvironmentType() == EnvironmentType.CLOUD;
    }

    /**
     * Static method to define whether or not the entity belongs to Cloud service.
     *
     * @return true if the entity is belonged to Cloud service
     */
    static boolean isCloudEntity(@Nonnull final ApiPartialEntity entity) {
        return entity.hasEnvironmentType() &&
            entity.getEnvironmentType() == EnvironmentType.CLOUD;
    }
}

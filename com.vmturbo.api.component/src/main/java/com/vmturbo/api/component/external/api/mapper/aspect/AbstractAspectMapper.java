package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Abstract {@link IAspectMapper} with common logic needed in derived classes.
 */
public abstract class AbstractAspectMapper implements IAspectMapper {

    @Override
    @Nullable
    public EntityAspect mapEntityToAspect(@Nonnull ApiPartialEntity entity) {
        return null;
    }

    /**
     * Determine if the entity belongs to a cloud service.
     *
     * @param entity the {@link TopologyEntityDTO}
     * @return true if the entity belongs to a cloud service
     */
    protected static boolean isCloudEntity(@Nonnull final TopologyEntityDTO entity) {
        return entity.hasEnvironmentType() && entity.getEnvironmentType() == EnvironmentType.CLOUD;
    }

    /**
     * Determine if the entity belongs to a cloud service.
     *
     * @param entity the {@link ApiPartialEntity}
     * @return true if the entity belongs to a cloud service
     */
    protected static boolean isCloudEntity(@Nonnull final ApiPartialEntity entity) {
        return entity.hasEnvironmentType() && entity.getEnvironmentType() == EnvironmentType.CLOUD;
    }
}

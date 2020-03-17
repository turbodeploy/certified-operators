package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEntityType;

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

    /**
     * Create {@link BaseApiDTO} by {@link MinimalEntity}.
     *
     * @param minimalEntity the {@link MinimalEntity}
     * @return the {@link BaseApiDTO}
     */
    @Nonnull
    protected static BaseApiDTO createBaseApiDTO(@Nonnull MinimalEntity minimalEntity) {
        final BaseApiDTO baseApiDTO = new BaseApiDTO();
        baseApiDTO.setUuid(String.valueOf(minimalEntity.getOid()));
        baseApiDTO.setDisplayName(minimalEntity.getDisplayName());
        baseApiDTO.setClassName(UIEntityType.fromType(minimalEntity.getEntityType()).apiStr());
        return baseApiDTO;
    }

    /**
     * Create {@link BaseApiDTO} by {@link Grouping}.
     *
     * @param grouping the {@link Grouping}
     * @return the {@link BaseApiDTO}
     */
    @Nonnull
    protected static BaseApiDTO createBaseApiDTO(@Nonnull Grouping grouping) {
        final BaseApiDTO baseApiDTO = new BaseApiDTO();
        baseApiDTO.setUuid(String.valueOf(grouping.getId()));
        final GroupDTO.GroupDefinition groupDefinition = grouping.getDefinition();
        baseApiDTO.setDisplayName(groupDefinition.getDisplayName());
        baseApiDTO.setClassName(GroupMapper.convertGroupTypeToApiType(groupDefinition.getType()));
        return baseApiDTO;
    }
}

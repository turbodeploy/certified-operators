package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.dto.entityaspect.CloudAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class CloudAspectMapper implements IAspectMapper {

    @Override
    @Nullable
    public EntityAspect mapEntityToAspect(@Nonnull TopologyEntityDTO entity) {
        // this aspect only applies to cloud service entities

        if (!isCloudEntity(entity)) {
            return null;
        }
        final CloudAspectApiDTO aspect = new CloudAspectApiDTO();
        if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
            // todo: fill the aspect fields
        }
        return aspect;
    }

    @Override
    public @Nonnull String getAspectName() {
        return "cloudAspect";
    }

    private boolean isCloudEntity(@Nonnull final TopologyEntityDTO entity) {
        return entity.getEnvironmentType() == EnvironmentType.CLOUD;
    }
}

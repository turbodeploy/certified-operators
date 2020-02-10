package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.STEntityAspectApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.LogicalPoolInfo;

/**
 * Topology Extension data related to logical pool.
 **/
public class LogicalPoolAspectMapper extends DiskCommonAspectMapper {
    @Override
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        final STEntityAspectApiDTO aspect = new STEntityAspectApiDTO();
        if (!entity.getTypeSpecificInfo().hasLogicalPool()) {
            return aspect;
        }
        final LogicalPoolInfo lpInfo = entity.getTypeSpecificInfo().getLogicalPool();

        if (lpInfo.hasDiskTypeInfo()) {
            fillDiskTypeInfo(aspect, lpInfo.getDiskTypeInfo());
        }

        return aspect;
    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.STORAGE;
    }
}

package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.STEntityAspectApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DiskArrayInfo;

/**
 * Topology Extension data related to disk array.
 **/
public class DiskArrayAspectMapper extends DiskCommonAspectMapper {
    @Override
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        final STEntityAspectApiDTO aspect = new STEntityAspectApiDTO();
        if (!entity.getTypeSpecificInfo().hasDiskArray()) {
            return aspect;
        }
        final DiskArrayInfo daInfo = entity.getTypeSpecificInfo().getDiskArray();

        if (daInfo.hasDiskTypeInfo()) {
            fillDiskTypeInfo(aspect, daInfo.getDiskTypeInfo());
        }

        return aspect;
    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.STORAGE;
    }
}

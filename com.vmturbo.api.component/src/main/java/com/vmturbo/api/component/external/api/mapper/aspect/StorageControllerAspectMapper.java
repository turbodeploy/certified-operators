package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.STEntityAspectApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageControllerInfo;

/**
 * Topology Extension data related to storage controller.
 **/
public class StorageControllerAspectMapper extends DiskCommonAspectMapper {
    @Override
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        final STEntityAspectApiDTO aspect = new STEntityAspectApiDTO();
        if (!entity.getTypeSpecificInfo().hasStorageController()) {
            return aspect;
        }
        final StorageControllerInfo scInfo = entity.getTypeSpecificInfo().getStorageController();

        if (scInfo.hasDiskTypeInfo()) {
            fillDiskTypeInfo(aspect, scInfo.getDiskTypeInfo());
        }

        return aspect;
    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.STORAGE;
    }
}

package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.STEntityAspectApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

public class StorageTierAspectMapper implements IAspectMapper {

    @Override
    public EntityAspect map(@Nonnull TopologyEntityDTO entity) {
        // todo: map more attributes if needed
        STEntityAspectApiDTO aspect = new STEntityAspectApiDTO();
        aspect.setDisplayName(entity.getDisplayName());
        aspect.setName(String.valueOf(entity.getOid()));
        return aspect;
    }

    @Override
    public @Nonnull String getAspectName() {
        return "storageAspect";
    }
}

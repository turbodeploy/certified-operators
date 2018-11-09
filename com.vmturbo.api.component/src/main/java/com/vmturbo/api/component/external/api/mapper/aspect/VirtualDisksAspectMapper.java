package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.VirtualDisksAspectApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

public class VirtualDisksAspectMapper implements IAspectMapper {

    @Override
    public EntityAspect mapEntityToAspect(@Nonnull TopologyEntityDTO entity) {
        // todo: map more attributes if needed
        VirtualDisksAspectApiDTO aspect = new VirtualDisksAspectApiDTO();
        // TODO: set the fields
        return aspect;
    }

    @Override
    public @Nonnull String getAspectName() {
        return "storageAspect";
    }
}

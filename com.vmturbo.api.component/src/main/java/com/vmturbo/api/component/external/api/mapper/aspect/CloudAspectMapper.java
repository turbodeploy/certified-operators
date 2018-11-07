package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

public class CloudAspectMapper implements IAspectMapper {

    @Override
    public EntityAspect map(@Nonnull TopologyEntityDTO entity) {
        //todo: implement this when CloudAspect is used
        return null;
    }

    @Override
    public @Nonnull String getAspectName() {
        return "cloudAspect";
    }
}

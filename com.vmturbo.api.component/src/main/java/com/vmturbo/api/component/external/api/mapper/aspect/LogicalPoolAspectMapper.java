package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.STEntityAspectApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Topology Extension data related to LogicalPools.
 **/
public class LogicalPoolAspectMapper implements IAspectMapper {
    @Override
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        STEntityAspectApiDTO aspect = new STEntityAspectApiDTO();
        // TODO: set the aspect fields
        return aspect;
    }

    @Nonnull
    @Override
    public String getAspectName() {
        return "logicalPoolAspect";
    }
}

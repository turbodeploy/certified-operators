package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.dto.entityaspect.CloudAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.PortsAspectApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Map topology extension data that are related to ports.
 **/
public class PortsAspectMapper implements IAspectMapper {
    @Nullable
    @Override
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        final PortsAspectApiDTO aspect = new PortsAspectApiDTO();
        // TODO: populate fields
        return aspect;
    }

    @Override
    public boolean supportsGroup() {
        return true;
    }

    @Nonnull
    @Override
    public String getAspectName() {
        return "portsAspect";
    }
}

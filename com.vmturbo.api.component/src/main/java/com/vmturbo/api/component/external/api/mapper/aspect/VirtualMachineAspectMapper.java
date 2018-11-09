package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.VMEntityAspectApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;

/**
 * Topology Extension data related to Virtual Machine.
 **/
public class VirtualMachineAspectMapper implements IAspectMapper {
    @Override
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        final VMEntityAspectApiDTO aspect = new VMEntityAspectApiDTO();
        if (entity.getTypeSpecificInfo().hasVirtualMachine()) {
            final VirtualMachineInfo virtualMachineInfo = entity.getTypeSpecificInfo()
                .getVirtualMachine();
            if (!virtualMachineInfo.getIpAddressesList().isEmpty()) {
                aspect.setIp(virtualMachineInfo
                    .getIpAddressesList()
                    .stream().map(IpAddress::getIpAddress)
                    .collect(Collectors.toList()));
            }
            // todo: handle cloud OS names in a friendly way
            if (virtualMachineInfo.hasGuestOsType()) {
                aspect.setOs(virtualMachineInfo.getGuestOsType().name());
            }
        }
        // TODO: set other fields
        return aspect;
    }

    @Nonnull
    @Override
    public String getAspectName() {
        return "virtualMachineAspect";
    }
}

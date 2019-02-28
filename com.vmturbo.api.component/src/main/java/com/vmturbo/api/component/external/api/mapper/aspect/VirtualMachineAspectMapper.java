package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.VMEntityAspectApiDTO;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;

/**
 * Topology Extension data related to Virtual Machine.
 **/
public class VirtualMachineAspectMapper implements IAspectMapper {

    private final SearchServiceBlockingStub searchServiceBlockingStub;

    public VirtualMachineAspectMapper(final SearchServiceBlockingStub searchServiceBlockingStub) {
        this.searchServiceBlockingStub = searchServiceBlockingStub;
    }

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
            if (virtualMachineInfo.hasGuestOsInfo()) {
                aspect.setOs(virtualMachineInfo.getGuestOsInfo().getGuestOsName());
            }
            if (virtualMachineInfo.hasNumCpus()) {
                aspect.setNumVCPUs(virtualMachineInfo.getNumCpus());
            }
            // TODO: missing ebsOptimized, businessUserSessions
        }
        return aspect;
    }

    @Nonnull
    @Override
    public String getAspectName() {
        return "virtualMachineAspect";
    }
}

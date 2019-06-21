package com.vmturbo.api.component.external.api.mapper.aspect;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.VMEntityAspectApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

public class VirtualMachineAspectMapperTest extends BaseAspectMapperTest {

    private static final long CONNECTED_PROCESSOR_POOL_ID = 123L;
    private static final String CONNECTED_PROCESSOR_POOL_NAME = "processor pool";
    private static final String CONNECTED_NETWORK_NAME_1 = "network 1";
    private static final String CONNECTED_NETWORK_NAME_2 = "network 2";

    private static final List<String> CONNECTED_ENTITY_NAME_LIST = ImmutableList.of(
            CONNECTED_NETWORK_NAME_1, CONNECTED_NETWORK_NAME_2);
    private static final List<String> IP_ADDRESSES = ImmutableList.of("1.2.3.4", "5.6.7.8");

    @Test
    public void testMapEntityToAspect() {
        final TypeSpecificInfo typeSpecificInfo = TypeSpecificInfo.newBuilder()
            .setVirtualMachine(VirtualMachineInfo.newBuilder()
                .setGuestOsInfo(OS.newBuilder()
                    .setGuestOsType(OSType.LINUX)
                    .setGuestOsName(OSType.LINUX.name()))
                .setNumCpus(4)
                .addIpAddresses(IpAddress.newBuilder().setIpAddress(IP_ADDRESSES.get(0)))
                .addIpAddresses(IpAddress.newBuilder().setIpAddress(IP_ADDRESSES.get(1)))
                .addConnectedNetworks(CONNECTED_NETWORK_NAME_1)
                .addConnectedNetworks(CONNECTED_NETWORK_NAME_2)
            )
            .build();
        final TopologyEntityDTO.Builder topologyEntityDTO = topologyEntityDTOBuilder(
            EntityType.VIRTUAL_MACHINE, typeSpecificInfo);

        VirtualMachineAspectMapper testMapper = new VirtualMachineAspectMapper();
        // act
        final EntityAspect resultAspect = testMapper.mapEntityToAspect(topologyEntityDTO.build());
        // assert
        assertTrue(resultAspect instanceof VMEntityAspectApiDTO);
        final VMEntityAspectApiDTO vmAspect = (VMEntityAspectApiDTO) resultAspect;
        assertEquals(OSType.LINUX.name(), vmAspect.getOs());
        assertEquals(IP_ADDRESSES, vmAspect.getIp());
        assertEquals(4, vmAspect.getNumVCPUs().intValue());
        assertEquals(CONNECTED_ENTITY_NAME_LIST, vmAspect.getConnectedNetworks().stream()
            .map(BaseApiDTO::getDisplayName).collect(Collectors.toList()));
    }

}
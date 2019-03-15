package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;

public class VirtualMachineInfoMapperTest {

    private static final String CONNECTED_NETWORK_NAME_1 = "network 1";
    private static final String CONNECTED_NETWORK_NAME_2 = "network 2";
    private static final List<String> CONNECTED_ENTITY_NAME_LIST = ImmutableList.of(
        CONNECTED_NETWORK_NAME_1, CONNECTED_NETWORK_NAME_2);

    @Test
    public void testExtractTypeSpecificInfo() {
        // arrange
        final EntityDTOOrBuilder vmEntityDTO = EntityDTO.newBuilder()
            .setVirtualMachineData(VirtualMachineData.newBuilder()
                .setBillingType(VirtualMachineData.VMBillingType.BIDDING)
                .addAllConnectedNetwork(CONNECTED_ENTITY_NAME_LIST)
                .build());
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
            .setVirtualMachine(VirtualMachineInfo.newBuilder()
                .setGuestOsInfo(OS.newBuilder()
                    .setGuestOsType(OSType.UNKNOWN_OS)
                    .setGuestOsName(""))
                .setTenancy(Tenancy.DEFAULT)
                .setNumCpus(4)
                .setBillingType(VirtualMachineData.VMBillingType.BIDDING)
                .addAllConnectedNetworks(CONNECTED_ENTITY_NAME_LIST)
                .build())
            .build();
        final VirtualMachineInfoMapper testBuilder = new VirtualMachineInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(vmEntityDTO,
            ImmutableMap.of(SupplyChainConstants.NUM_CPUS, "4"));
        // assert
        assertThat(result, equalTo(expected));
    }
}
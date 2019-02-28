package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class VirtualMachineInfoMapperTest {

    @Test
    public void testExtractTypeSpecificInfo() {
        // arrange
        final EntityDTOOrBuilder vmEntityDTO = EntityDTO.newBuilder()
                .setVirtualMachineData(VirtualMachineData.newBuilder()
                        .setBillingType(VirtualMachineData.VMBillingType.BIDDING)
                        .build());
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
                .setVirtualMachine(VirtualMachineInfo.newBuilder()
                        .setGuestOsInfo(OS.newBuilder()
                            .setGuestOsType(OSType.UNKNOWN_OS)
                            .setGuestOsName(""))
                        .setTenancy(Tenancy.DEFAULT)
                        .setNumCpus(4)
                        .setBillingType(VirtualMachineData.VMBillingType.BIDDING)
                        .build())
                .build();
        final VirtualMachineInfoMapper testBuilder = new VirtualMachineInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(vmEntityDTO,
                ImmutableMap.of("numCpus", "4"));
        // assert
        assertThat(result, equalTo(expected));
    }
}
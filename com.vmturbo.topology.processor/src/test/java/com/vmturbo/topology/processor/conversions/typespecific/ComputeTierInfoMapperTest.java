package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ComputeTierData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ComputeTierData.DedicatedStorageNetworkState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

public class ComputeTierInfoMapperTest {


    public static final String FAMILY = "FAMILY";
    private static final int NUM_COUPONS = 42;

    @Test
    public void testExtractTypeSpecificInfo() {
        // arrange
        final EntityDTOOrBuilder computeTierEntityDTO = EntityDTO.newBuilder()
                .setComputeTierData(ComputeTierData.newBuilder()
                        .setFamily(FAMILY)
                        .setDedicatedStorageNetworkState(DedicatedStorageNetworkState.CONFIGURED_ENABLED)
                        .setNumCoupons(NUM_COUPONS)
                        .build());
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
                .setComputeTier(ComputeTierInfo.newBuilder()
                        .setFamily(FAMILY)
                        .setDedicatedStorageNetworkState(DedicatedStorageNetworkState.CONFIGURED_ENABLED)
                        .setNumCoupons(NUM_COUPONS)
                        .build())
                .build();
        final ComputeTierInfoMapper testBuilder = new ComputeTierInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(computeTierEntityDTO);
        // assert
        assertThat(result, equalTo(expected));
    }
}
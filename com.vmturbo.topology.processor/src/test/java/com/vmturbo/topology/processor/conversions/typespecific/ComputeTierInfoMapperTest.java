package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Collections;

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
    private static final int NUM_CORES = 64;

    @Test
    public void testExtractTypeSpecificInfo() {
        // arrange
        final EntityDTOOrBuilder computeTierEntityDTO = EntityDTO.newBuilder()
                .setComputeTierData(ComputeTierData.newBuilder()
                        .setFamily(FAMILY)
                        .setDedicatedStorageNetworkState(DedicatedStorageNetworkState.CONFIGURED_ENABLED)
                        .setNumCoupons(NUM_COUPONS)
                        .setNumCores(NUM_CORES)
                        .build());
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
                .setComputeTier(ComputeTierInfo.newBuilder()
                        .setFamily(FAMILY)
                        .setDedicatedStorageNetworkState(DedicatedStorageNetworkState.CONFIGURED_ENABLED)
                        .setNumCoupons(NUM_COUPONS)
                        .setNumCores(NUM_CORES)
                        .build())
                .build();
        final ComputeTierInfoMapper testBuilder = new ComputeTierInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(computeTierEntityDTO,
                Collections.emptyMap());
        // assert
        assertThat(result, equalTo(expected));
    }
}
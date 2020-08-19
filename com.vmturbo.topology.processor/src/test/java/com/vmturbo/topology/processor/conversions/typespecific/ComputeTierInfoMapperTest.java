package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.Collections;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.Architecture;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo.SupportedCustomerInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualizationType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ComputeTierData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ComputeTierData.DedicatedStorageNetworkState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.InstanceDiskType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

public class ComputeTierInfoMapperTest {


    private static final String FAMILY = "FAMILY";
    private static final String QUOTA_FAMILY = "QUOTA_FAMILY";
    private static final int NUM_COUPONS = 42;
    private static final int NUM_CORES = 64;
    private static final int NUM_INSTANCE_DISKS = 2;
    private static final int INSTANCE_DISK_SIZE = 100;
    private static final InstanceDiskType INSTANCE_DISK_TYPE = InstanceDiskType.HDD;

    @Test
    public void testExtractTypeSpecificInfo() {
        // arrange
        final EntityDTOOrBuilder computeTierEntityDTO = createEntityDTOBuilder();
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
                .setComputeTier(ComputeTierInfo.newBuilder()
                        .setFamily(FAMILY)
                        .setQuotaFamily(QUOTA_FAMILY)
                        .setDedicatedStorageNetworkState(DedicatedStorageNetworkState.CONFIGURED_ENABLED)
                        .setNumCoupons(NUM_COUPONS)
                        .setNumCores(NUM_CORES)
                        .setSupportedCustomerInfo(SupportedCustomerInfo.getDefaultInstance())
                        .setNumInstanceDisks(NUM_INSTANCE_DISKS)
                        .setInstanceDiskType(INSTANCE_DISK_TYPE)
                        .setInstanceDiskSizeGb(INSTANCE_DISK_SIZE)
                        .setBurstableCPU(false)
                        .build())
                .build();
        final ComputeTierInfoMapper testBuilder = new ComputeTierInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(computeTierEntityDTO,
                Collections.emptyMap());
        // assert
        assertThat(result, equalTo(expected));
    }

    /**
     * Test that the pre-requisite data is created in ComputeTierInfo.
     */
    @Test
    public void testExtractTypeSpecificInfoForPreReqData() {
        // arrange
        final EntityDTOOrBuilder computeTier = createEntityDTOBuilder();
        final ComputeTierInfoMapper testBuilder = new ComputeTierInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(computeTier,
            ImmutableMap.of(
                "enhancedNetworkingType", "ENA",
                "NVMeRequired", "true",
                "architecture", "32-bit or 64-bit",
                "supportedVirtualizationType", "PVM_AND_HVM"));
        // assert
        assertEquals(true, result.getComputeTier().getSupportedCustomerInfo().getSupportsOnlyEnaVms());
        assertEquals(true, result.getComputeTier().getSupportedCustomerInfo().getSupportsOnlyNVMeVms());
        assertEquals(ImmutableSet.of(Architecture.ARM_64, Architecture.BIT_64, Architecture.BIT_32),
            Sets.newHashSet(result.getComputeTier().getSupportedCustomerInfo().getSupportedArchitecturesList()));
        assertEquals(ImmutableSet.of(VirtualizationType.HVM, VirtualizationType.PVM),
            Sets.newHashSet(result.getComputeTier().getSupportedCustomerInfo().getSupportedVirtualizationTypesList()));
    }

    private EntityDTOOrBuilder createEntityDTOBuilder() {
        return EntityDTO.newBuilder()
            .setComputeTierData(ComputeTierData.newBuilder()
                .setFamily(FAMILY)
                .setQuotaFamily(QUOTA_FAMILY)
                .setDedicatedStorageNetworkState(DedicatedStorageNetworkState.CONFIGURED_ENABLED)
                .setNumCoupons(NUM_COUPONS)
                .setNumCores(NUM_CORES)
                .setNumInstanceDisks(NUM_INSTANCE_DISKS)
                .setInstanceDiskType(INSTANCE_DISK_TYPE)
                .setInstanceDiskSizeGb(INSTANCE_DISK_SIZE)
                .build());
    }
}
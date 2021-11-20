package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.Architecture;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo.ScalingPenalty;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo.SupportedCustomerInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualizationType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ScalingPenaltyReason;
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
    private static final Map<String, String> entityPropertyMap = ImmutableMap.of(
            "enhancedNetworkingType", "ENA",
            "NVMeRequired", "true",
            "architecture", "32-bit or 64-bit",
            "supportedVirtualizationType", "PVM_AND_HVM");

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
                        .addInstanceDiskCounts(NUM_INSTANCE_DISKS)
                        .setInstanceDiskType(INSTANCE_DISK_TYPE)
                        .setInstanceDiskSizeGb(INSTANCE_DISK_SIZE)
                        .setBurstableCPU(false)
                        .setScalePenalty(ScalingPenalty.newBuilder()
                                .setPenalty(0)
                                .addReasons(ScalingPenaltyReason.CORE_CONSTRAINED_TIER).build())
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
            entityPropertyMap);
        // assert
        assertTrue(result.getComputeTier().getSupportedCustomerInfo().getSupportsOnlyEnaVms());
        assertTrue(result.getComputeTier().getSupportedCustomerInfo().getSupportsOnlyNVMeVms());
        assertEquals(ImmutableSet.of(Architecture.ARM_64, Architecture.BIT_64, Architecture.BIT_32),
            Sets.newHashSet(result.getComputeTier().getSupportedCustomerInfo().getSupportedArchitecturesList()));
        assertEquals(ImmutableSet.of(VirtualizationType.HVM, VirtualizationType.PVM),
            Sets.newHashSet(result.getComputeTier().getSupportedCustomerInfo().getSupportedVirtualizationTypesList()));
    }

    private EntityDTOOrBuilder createEntityDTOBuilder() {
        return EntityDTO.newBuilder()
            .setComputeTierData(createComputeTierDataBuilder(true, ImmutableList.of(NUM_INSTANCE_DISKS))
                .build());
    }

    /**
     * ComputeTierMapper has some backward compatibility logic to set older deprecated
     * numInstanceDisks field in addition to the newer instanceDiskCounts fields. This test is to
     * verify that values in ComputeTierData are being set correctly.
     */
    @Test
    public void instanceDiskCountMapper() {
        final List<Integer> newDiskCounts = ImmutableList.of(2, 4, 16);
        final List<Integer>  oldDiskCount = ImmutableList.of(8);
        final ComputeTierInfoMapper testBuilder = new ComputeTierInfoMapper();

        // Set disk counts in new field of probe DTO's ComputeTierData and verify that it got
        // mapped correctly in the TP's ComputeTierInfo.
        EntityDTOOrBuilder computeTier = EntityDTO.newBuilder()
                .setComputeTierData(createComputeTierDataBuilder(true, newDiskCounts)
                        .build());
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(computeTier,
                entityPropertyMap);
        assertFalse(result.getComputeTier().hasNumInstanceDisks());
        assertEquals(newDiskCounts, result.getComputeTier().getInstanceDiskCountsList());

        // Test backward compatibility. Set value in old field, and verify it get picked up in the
        // new field (instanceDiskCounts).
        computeTier = EntityDTO.newBuilder()
                .setComputeTierData(createComputeTierDataBuilder(false, oldDiskCount)
                        .build());
        result = testBuilder.mapEntityDtoToTypeSpecificInfo(computeTier, entityPropertyMap);
        assertFalse(result.getComputeTier().hasNumInstanceDisks());
        assertEquals(oldDiskCount, result.getComputeTier().getInstanceDiskCountsList());
    }

    /**
     * A bit of refactoring to enable it to be called with both older (deprecated) numInstanceDisk
     * field as well as the new instanceDiskCounts field. Creates ComputeTierData based on inputs.
     *
     * @param newField Whether value should be set in the new field.
     * @param diskCounts Set of disk counts to set.
     * @return ComputeTierData builder.
     */
    private ComputeTierData.Builder createComputeTierDataBuilder(boolean newField, List<Integer> diskCounts) {
        ComputeTierData.Builder builder = ComputeTierData.newBuilder()
                        .setFamily(FAMILY)
                        .setQuotaFamily(QUOTA_FAMILY)
                        .setDedicatedStorageNetworkState(DedicatedStorageNetworkState.CONFIGURED_ENABLED)
                        .setNumCoupons(NUM_COUPONS)
                        .setNumCores(NUM_CORES)
                        .setInstanceDiskType(INSTANCE_DISK_TYPE)
                        .setInstanceDiskSizeGb(INSTANCE_DISK_SIZE)
                        .setScalePenalty(ComputeTierData.ScalingPenalty.newBuilder()
                                .addReasons(EntityDTO.ScalingPenaltyReason.CORE_CONSTRAINED_TIER).build());
        if (newField) {
            return builder.addAllInstanceDiskCounts(diskCounts);
        }
        return builder.setNumInstanceDisks(diskCounts.get(0));
    }
}
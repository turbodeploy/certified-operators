package com.vmturbo.reserved.instance.coverage.allocator;

import static com.vmturbo.reserved.instance.coverage.allocator.AzureAllocationTopologyTest.BUSINESS_ACCOUNT;
import static com.vmturbo.reserved.instance.coverage.allocator.AzureAllocationTopologyTest.COMPUTE_TIER_SMALL;
import static com.vmturbo.reserved.instance.coverage.allocator.AzureAllocationTopologyTest.REGION;
import static com.vmturbo.reserved.instance.coverage.allocator.AzureAllocationTopologyTest.RI_BOUGHT_SMALL;
import static com.vmturbo.reserved.instance.coverage.allocator.AzureAllocationTopologyTest.RI_SPEC_SMALL;
import static com.vmturbo.reserved.instance.coverage.allocator.AzureAllocationTopologyTest.VIRTUAL_MACHINE_SMALL_A;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

import java.util.Collections;

import org.junit.Test;

import com.google.common.collect.ImmutableTable;

import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

public class ReservedInstanceCoverageAllocatorAzureFuncTest extends AbstractReservedInstanceCoverageAllocatorTest {


    @Test
    public void testDirectNonSizeFlexibleAssignment() {
        final CoverageTopology coverageTopology = generateCoverageTopology(
                SDKProbeType.AZURE,
                Collections.singleton(RI_BOUGHT_SMALL),
                Collections.singleton(RI_SPEC_SMALL),
                COMPUTE_TIER_SMALL,
                REGION,
                VIRTUAL_MACHINE_SMALL_A,
                BUSINESS_ACCOUNT);

        /*
         * Invoke SUT
         */
        final ReservedInstanceCoverageAllocator allocator = ReservedInstanceCoverageAllocator.newBuilder()
                .coverageTopology(coverageTopology)
                .coverageProvider(() -> ImmutableTable.of())
                .build();

        final ReservedInstanceCoverageAllocation allocationResult = allocator.allocateCoverage();

        /*
         * Asserts
         */
        assertThat(allocationResult.allocatorCoverageTable().size(), equalTo(1));
        assertThat(allocationResult.allocatorCoverageTable().rowMap(),
                hasEntry(VIRTUAL_MACHINE_SMALL_A.getOid(), Collections.singletonMap(
                        RI_BOUGHT_SMALL.getId(), 1.0)));
    }

    /**
     * Tests a direct non-ISF RI -> Windows VM assignment
     */
    @Test
    public void testPlatformFlexibleAssignment() {

        final TopologyEntityDTO virtualMachineWindows = VIRTUAL_MACHINE_SMALL_A.toBuilder()
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setVirtualMachine(VirtualMachineInfo.newBuilder()
                                .setGuestOsInfo(OS.newBuilder()
                                        .setGuestOsType(OSType.WINDOWS))
                                .setTenancy(Tenancy.DEFAULT)))
                .build();

        final CoverageTopology coverageTopology = generateCoverageTopology(
                SDKProbeType.AZURE,
                Collections.singleton(RI_BOUGHT_SMALL),
                Collections.singleton(RI_SPEC_SMALL),
                COMPUTE_TIER_SMALL,
                REGION,
                virtualMachineWindows,
                BUSINESS_ACCOUNT);

        /*
         * Invoke SUT
         */
        final ReservedInstanceCoverageAllocator allocator = ReservedInstanceCoverageAllocator.newBuilder()
                .coverageTopology(coverageTopology)
                .coverageProvider(() -> ImmutableTable.of())
                .build();

        final ReservedInstanceCoverageAllocation allocationResult = allocator.allocateCoverage();

        /*
         * Asserts
         */
        assertThat(allocationResult.allocatorCoverageTable().size(), equalTo(1));
        assertThat(allocationResult.allocatorCoverageTable().rowMap(),
                hasEntry(virtualMachineWindows.getOid(), Collections.singletonMap(
                        RI_BOUGHT_SMALL.getId(), 1.0)));
    }
}

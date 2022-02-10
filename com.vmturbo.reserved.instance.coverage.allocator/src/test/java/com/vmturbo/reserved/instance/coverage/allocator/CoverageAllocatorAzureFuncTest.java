package com.vmturbo.reserved.instance.coverage.allocator;

import static com.vmturbo.reserved.instance.coverage.allocator.AzureAllocationTopologyTest.AZURE_SERVICE_PROVIDER_TEST;
import static com.vmturbo.reserved.instance.coverage.allocator.AzureAllocationTopologyTest.BILLING_FAMILY_GROUPS;
import static com.vmturbo.reserved.instance.coverage.allocator.AzureAllocationTopologyTest.BUSINESS_ACCOUNT;
import static com.vmturbo.reserved.instance.coverage.allocator.AzureAllocationTopologyTest.COMPUTE_TIER_SMALL;
import static com.vmturbo.reserved.instance.coverage.allocator.AzureAllocationTopologyTest.REGION;
import static com.vmturbo.reserved.instance.coverage.allocator.AzureAllocationTopologyTest.RI_BOUGHT_SMALL;
import static com.vmturbo.reserved.instance.coverage.allocator.AzureAllocationTopologyTest.RI_SPEC_SMALL;
import static com.vmturbo.reserved.instance.coverage.allocator.AzureAllocationTopologyTest.VIRTUAL_MACHINE_SMALL_A;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.Collections;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

/**
 * Tests the commitment allocator for for Azure.
 */
public class CoverageAllocatorAzureFuncTest extends AbstractCoverageAllocatorTest {

    /**
     * Setup method for tests.
     */
    @Before
    public void testSetup() {
        when(groupMemberRetriever.getGroupsWithMembers(any())).thenReturn(
                Collections.singletonList(BILLING_FAMILY_GROUPS));
    }

    @Test
    public void testDirectNonSizeFlexibleAssignment() {
        final CoverageTopology coverageTopology = generateCoverageTopology(
                AZURE_SERVICE_PROVIDER_TEST,
                Collections.singleton(RI_BOUGHT_SMALL),
                Collections.singleton(RI_SPEC_SMALL),
                groupMemberRetriever,
                COMPUTE_TIER_SMALL,
                REGION,
                VIRTUAL_MACHINE_SMALL_A,
                BUSINESS_ACCOUNT);

        /*
         * Invoke SUT
         */
        final CloudCommitmentCoverageAllocator allocator = allocatorFactory.createAllocator(
                CoverageAllocationConfig.builder()
                        .coverageTopology(coverageTopology)
                        .build());

        final CloudCommitmentCoverageAllocation allocationResult = allocator.allocateCoverage();

        /*
         * Asserts
         */
        final Table<Long, Long, CloudCommitmentAmount> expectedAllocations = ImmutableTable.<Long, Long, CloudCommitmentAmount>builder()
                .put(VIRTUAL_MACHINE_SMALL_A.getOid(), RI_BOUGHT_SMALL.getId(), CloudCommitmentAmount.newBuilder()
                        .setCoupons(1.0)
                        .build())
                .build();
        assertThat(allocationResult.allocatorCoverageTable().size(), equalTo(1));
        assertThat(allocationResult.allocatorCoverageTable(), equalTo(expectedAllocations));
    }

    /**
     * Tests a direct non-ISF RI -> Windows VM assignment.
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
                AZURE_SERVICE_PROVIDER_TEST,
                Collections.singleton(RI_BOUGHT_SMALL),
                Collections.singleton(RI_SPEC_SMALL),
                groupMemberRetriever,
                COMPUTE_TIER_SMALL,
                REGION,
                virtualMachineWindows,
                BUSINESS_ACCOUNT);

        /*
         * Invoke SUT
         */
        final CloudCommitmentCoverageAllocator allocator = allocatorFactory.createAllocator(
                CoverageAllocationConfig.builder()
                        .coverageTopology(coverageTopology)
                        .build());

        final CloudCommitmentCoverageAllocation allocationResult = allocator.allocateCoverage();

        /*
         * Asserts
         */
        final Table<Long, Long, CloudCommitmentAmount> expectedAllocations = ImmutableTable.<Long, Long, CloudCommitmentAmount>builder()
                .put(virtualMachineWindows.getOid(), RI_BOUGHT_SMALL.getId(), CloudCommitmentAmount.newBuilder()
                        .setCoupons(1.0)
                        .build())
                .build();
        assertThat(allocationResult.allocatorCoverageTable().size(), equalTo(1));
        assertThat(allocationResult.allocatorCoverageTable(), equalTo(expectedAllocations));
    }
}

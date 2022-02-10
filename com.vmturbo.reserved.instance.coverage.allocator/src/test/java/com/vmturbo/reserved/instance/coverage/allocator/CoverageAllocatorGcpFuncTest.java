package com.vmturbo.reserved.instance.coverage.allocator;

import static com.vmturbo.reserved.instance.coverage.allocator.GcpAllocationTopologyTest.ACCOUNT_A_SCOPED_COMMITMENT;
import static com.vmturbo.reserved.instance.coverage.allocator.GcpAllocationTopologyTest.ACCOUNT_B_BF_SCOPED_COMMITMENT;
import static com.vmturbo.reserved.instance.coverage.allocator.GcpAllocationTopologyTest.AVAILABILITY_ZONE_A;
import static com.vmturbo.reserved.instance.coverage.allocator.GcpAllocationTopologyTest.BILLING_FAMILY_GROUPS;
import static com.vmturbo.reserved.instance.coverage.allocator.GcpAllocationTopologyTest.BUSINESS_ACCOUNT_A;
import static com.vmturbo.reserved.instance.coverage.allocator.GcpAllocationTopologyTest.BUSINESS_ACCOUNT_B;
import static com.vmturbo.reserved.instance.coverage.allocator.GcpAllocationTopologyTest.COMPUTE_CLOUD_SERVICE;
import static com.vmturbo.reserved.instance.coverage.allocator.GcpAllocationTopologyTest.COMPUTE_TIER_SMALL;
import static com.vmturbo.reserved.instance.coverage.allocator.GcpAllocationTopologyTest.GCP_SERVICE_PROVIDER_TEST;
import static com.vmturbo.reserved.instance.coverage.allocator.GcpAllocationTopologyTest.REGION;
import static com.vmturbo.reserved.instance.coverage.allocator.GcpAllocationTopologyTest.VIRTUAL_MACHINE_SMALL_A;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.Collections;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommodityBought;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

public class CoverageAllocatorGcpFuncTest extends AbstractCoverageAllocatorTest {

    /**
     * Setup method for tests.
     */
    @Before
    public void setup() {
        when(groupMemberRetriever.getGroupsWithMembers(any())).thenReturn(Collections.singletonList(BILLING_FAMILY_GROUPS));
    }

    @Test
    public void testAccountScopedCud() {

        final CoverageTopology coverageTopology = generateCoverageTopology(
                GCP_SERVICE_PROVIDER_TEST,
                Collections.emptySet(),
                Collections.emptySet(),
                ImmutableSet.of(ACCOUNT_A_SCOPED_COMMITMENT),
                groupMemberRetriever,
                COMPUTE_TIER_SMALL,
                AVAILABILITY_ZONE_A,
                REGION,
                VIRTUAL_MACHINE_SMALL_A,
                BUSINESS_ACCOUNT_A,
                COMPUTE_CLOUD_SERVICE,
                ACCOUNT_A_SCOPED_COMMITMENT);

        /*
         * Invoke SUT
         */
        final CloudCommitmentCoverageAllocator allocator = allocatorFactory.createAllocator(
                CoverageAllocationConfig.builder()
                        .coverageTopology(coverageTopology)
                        .build());

        final CloudCommitmentCoverageAllocation allocationResult = allocator.allocateCoverage();

        assertThat(allocationResult.allocatorCoverageTable().size(), equalTo(1));

        final CommittedCommodityBought expectVCoreAmount = CommittedCommodityBought.newBuilder()
                .setCommodityType(CommodityType.NUM_VCORE)
                .setCapacity(1)
                .build();
        final CommittedCommodityBought expectMemProvisionAmount = CommittedCommodityBought.newBuilder()
                .setCommodityType(CommodityType.MEM_PROVISIONED)
                .setCapacity(2.0)
                .build();
        assertTrue(allocationResult.allocatorCoverageTable().containsRow(VIRTUAL_MACHINE_SMALL_A.getOid()));
        final CloudCommitmentAmount actualAllocation = Iterables.getOnlyElement(allocationResult.allocatorCoverageTable().values());
        assertThat(actualAllocation.getCommoditiesBought().getCommodityList(), containsInAnyOrder(expectVCoreAmount, expectMemProvisionAmount));
    }

    @Test
    public void testBFScopedCud() {

        final CoverageTopology coverageTopology = generateCoverageTopology(
                GCP_SERVICE_PROVIDER_TEST,
                Collections.emptySet(),
                Collections.emptySet(),
                ImmutableSet.of(ACCOUNT_B_BF_SCOPED_COMMITMENT),
                groupMemberRetriever,
                COMPUTE_TIER_SMALL,
                AVAILABILITY_ZONE_A,
                REGION,
                VIRTUAL_MACHINE_SMALL_A,
                BUSINESS_ACCOUNT_A,
                BUSINESS_ACCOUNT_B,
                COMPUTE_CLOUD_SERVICE,
                ACCOUNT_B_BF_SCOPED_COMMITMENT);

        /*
         * Invoke SUT
         */
        final CloudCommitmentCoverageAllocator allocator = allocatorFactory.createAllocator(
                CoverageAllocationConfig.builder()
                        .coverageTopology(coverageTopology)
                        .build());

        final CloudCommitmentCoverageAllocation allocationResult = allocator.allocateCoverage();

        assertThat(allocationResult.allocatorCoverageTable().size(), equalTo(1));

        final CommittedCommodityBought expectVCoreAmount = CommittedCommodityBought.newBuilder()
                .setCommodityType(CommodityType.NUM_VCORE)
                .setCapacity(1)
                .build();
        final CommittedCommodityBought expectMemProvisionAmount = CommittedCommodityBought.newBuilder()
                .setCommodityType(CommodityType.MEM_PROVISIONED)
                .setCapacity(1.0)
                .build();
        assertTrue(allocationResult.allocatorCoverageTable().containsRow(VIRTUAL_MACHINE_SMALL_A.getOid()));
        final CloudCommitmentAmount actualAllocation = Iterables.getOnlyElement(allocationResult.allocatorCoverageTable().values());
        assertThat(actualAllocation.getCommoditiesBought().getCommodityList(), containsInAnyOrder(expectVCoreAmount, expectMemProvisionAmount));
    }
}

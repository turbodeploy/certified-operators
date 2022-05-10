package com.vmturbo.reserved.instance.coverage.allocator;

import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.AVAILIBILITY_ZONE_A;
import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.AWS_SERVICE_PROVIDER_TEST;
import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.BILLING_FAMILY_GROUPS;
import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.BUSINESS_ACCOUNT;
import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.BUSINESS_ACCOUNT_B;
import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.COMPUTER_TIER_MEDIUM;
import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.COMPUTE_TIER_SMALL;
import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.OID_PROVIDER;
import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.REGION;
import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.RI_BOUGHT_SMALL_REGIONAL;
import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.RI_SPEC_SMALL_REGIONAL;
import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.VIRTUAL_MACHINE_SMALL_A;
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
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

public class CoverageAllocatorAwsFuncTest extends AbstractCoverageAllocatorTest {

    /**
     * Setup method for tests.
     */
    @Before
    public void testSetup() {
        when(groupMemberRetriever.getGroupsWithMembers(any())).thenReturn(Collections.singletonList(BILLING_FAMILY_GROUPS));
    }

    @Test
    public void testDirectZonalAssignment() {

        final ReservedInstanceBought zonalRiBought = ReservedInstanceBought.newBuilder()
                .setId(OID_PROVIDER.incrementAndGet())
                .setReservedInstanceBoughtInfo(RI_BOUGHT_SMALL_REGIONAL
                    .getReservedInstanceBoughtInfo()
                    .toBuilder()
                    .setAvailabilityZoneId(AVAILIBILITY_ZONE_A.getOid()))
                .build();


        final CoverageTopology coverageTopology = generateCoverageTopology(
                AWS_SERVICE_PROVIDER_TEST,
                Collections.singleton(zonalRiBought),
                Collections.singleton(RI_SPEC_SMALL_REGIONAL),
                Collections.emptySet(),
                groupMemberRetriever,
                COMPUTE_TIER_SMALL,
                AVAILIBILITY_ZONE_A,
                REGION,
                VIRTUAL_MACHINE_SMALL_A,
                BUSINESS_ACCOUNT);

        final CloudCommitmentCoverageAllocator allocator = allocatorFactory.createAllocator(
                CoverageAllocationConfig.builder()
                        .coverageTopology(coverageTopology)
                        .build());

        final CloudCommitmentCoverageAllocation allocationResult = allocator.allocateCoverage();

        /*
         * Asserts
         */
        final Table<Long, Long, CloudCommitmentAmount> expectedAllocations = ImmutableTable.of(
                VIRTUAL_MACHINE_SMALL_A.getOid(),
                zonalRiBought.getId(),
                CloudCommitmentAmount.newBuilder()
                        .setCoupons(1.0)
                        .build());
        assertThat(allocationResult.allocatorCoverageTable().size(), equalTo(1));
        assertThat(allocationResult.allocatorCoverageTable(), equalTo(expectedAllocations));
    }

    @Test
    public void testSharedZonalAssignment() {
        final ReservedInstanceBought zonalRiBoughtB = ReservedInstanceBought.newBuilder()
                .setId(OID_PROVIDER.incrementAndGet())
                .setReservedInstanceBoughtInfo(RI_BOUGHT_SMALL_REGIONAL
                        .getReservedInstanceBoughtInfo()
                        .toBuilder()
                        .setAvailabilityZoneId(AVAILIBILITY_ZONE_A.getOid())
                        .setBusinessAccountId(BUSINESS_ACCOUNT_B.getOid()))
                .build();

        final CoverageTopology coverageTopology = generateCoverageTopology(
                AWS_SERVICE_PROVIDER_TEST,
                Collections.singleton(zonalRiBoughtB),
                Collections.singleton(RI_SPEC_SMALL_REGIONAL),
                Collections.emptySet(),
                groupMemberRetriever,
                COMPUTE_TIER_SMALL,
                AVAILIBILITY_ZONE_A,
                REGION,
                VIRTUAL_MACHINE_SMALL_A,
                BUSINESS_ACCOUNT,
                BUSINESS_ACCOUNT_B);


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
        final Table<Long, Long, CloudCommitmentAmount> expectedAllocations = ImmutableTable.of(
                VIRTUAL_MACHINE_SMALL_A.getOid(),
                zonalRiBoughtB.getId(),
                CloudCommitmentAmount.newBuilder()
                        .setCoupons(1.0)
                        .build());
        assertThat(allocationResult.allocatorCoverageTable().size(), equalTo(1));
        assertThat(allocationResult.allocatorCoverageTable(), equalTo(expectedAllocations));
    }

    /**
     * Test direct regional RI with account scope.
     */
    @Test
    public void testDirectRegionalAssignmentAccountScope() {

        final ReservedInstanceBought accountScopedRIBought = ReservedInstanceBought.newBuilder()
                .setId(OID_PROVIDER.incrementAndGet())
                .setReservedInstanceBoughtInfo(RI_BOUGHT_SMALL_REGIONAL
                        .getReservedInstanceBoughtInfo()
                        .toBuilder()
                        .setReservedInstanceScopeInfo(ReservedInstanceScopeInfo.newBuilder()
                                .setShared(false)
                                .addApplicableBusinessAccountId(BUSINESS_ACCOUNT.getOid())))
                .build();

        final CoverageTopology coverageTopology = generateCoverageTopology(
                AWS_SERVICE_PROVIDER_TEST,
                Collections.singleton(accountScopedRIBought),
                Collections.singleton(RI_SPEC_SMALL_REGIONAL),
                Collections.emptySet(),
                groupMemberRetriever,
                COMPUTE_TIER_SMALL,
                AVAILIBILITY_ZONE_A,
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
        final Table<Long, Long, CloudCommitmentAmount> expectedAllocations = ImmutableTable.of(
                VIRTUAL_MACHINE_SMALL_A.getOid(),
                accountScopedRIBought.getId(),
                CloudCommitmentAmount.newBuilder()
                        .setCoupons(1.0)
                        .build());
        assertThat(allocationResult.allocatorCoverageTable().size(), equalTo(1));
        assertThat(allocationResult.allocatorCoverageTable(), equalTo(expectedAllocations));
    }

    @Test
    public void testDirectRegionalAssignment() {

        final CoverageTopology coverageTopology = generateCoverageTopology(
                AWS_SERVICE_PROVIDER_TEST,
                Collections.singleton(RI_BOUGHT_SMALL_REGIONAL),
                Collections.singleton(RI_SPEC_SMALL_REGIONAL),
                Collections.emptySet(),
                groupMemberRetriever,
                COMPUTE_TIER_SMALL,
                AVAILIBILITY_ZONE_A,
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
        final Table<Long, Long, CloudCommitmentAmount> expectedAllocations = ImmutableTable.of(
                VIRTUAL_MACHINE_SMALL_A.getOid(),
                RI_BOUGHT_SMALL_REGIONAL.getId(),
                CloudCommitmentAmount.newBuilder()
                        .setCoupons(1.0)
                        .build());
        assertThat(allocationResult.allocatorCoverageTable().size(), equalTo(1));
        assertThat(allocationResult.allocatorCoverageTable(), equalTo(expectedAllocations));
    }

    @Test
    public void testSharedRegionalAssignment() {

        final ReservedInstanceBought regionalRIB = ReservedInstanceBought.newBuilder()
                .setId(OID_PROVIDER.incrementAndGet())
                .setReservedInstanceBoughtInfo(RI_BOUGHT_SMALL_REGIONAL
                        .getReservedInstanceBoughtInfo()
                        .toBuilder()
                        .setBusinessAccountId(BUSINESS_ACCOUNT_B.getOid()))
                .build();

        final CoverageTopology coverageTopology = generateCoverageTopology(
                AWS_SERVICE_PROVIDER_TEST,
                Collections.singleton(regionalRIB),
                Collections.singleton(RI_SPEC_SMALL_REGIONAL),
                Collections.emptySet(),
                groupMemberRetriever,
                COMPUTE_TIER_SMALL,
                AVAILIBILITY_ZONE_A,
                REGION,
                VIRTUAL_MACHINE_SMALL_A,
                BUSINESS_ACCOUNT,
                BUSINESS_ACCOUNT_B);

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
        final Table<Long, Long, CloudCommitmentAmount> expectedAllocations = ImmutableTable.of(
                VIRTUAL_MACHINE_SMALL_A.getOid(),
                regionalRIB.getId(),
                CloudCommitmentAmount.newBuilder()
                        .setCoupons(1.0)
                        .build());
        assertThat(allocationResult.allocatorCoverageTable().size(), equalTo(1));
        assertThat(allocationResult.allocatorCoverageTable(), equalTo(expectedAllocations));
    }

    @Test
    public void testPlatformMismatch() {

        final TopologyEntityDTO virtualMachineWindows = VIRTUAL_MACHINE_SMALL_A.toBuilder()
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setVirtualMachine(VirtualMachineInfo.newBuilder()
                                .setGuestOsInfo(OS.newBuilder()
                                        .setGuestOsType(OSType.WINDOWS))
                                .setTenancy(Tenancy.DEFAULT)))
                .build();

        final CoverageTopology coverageTopology = generateCoverageTopology(
                AWS_SERVICE_PROVIDER_TEST,
                Collections.singleton(RI_BOUGHT_SMALL_REGIONAL),
                Collections.singleton(RI_SPEC_SMALL_REGIONAL),
                Collections.emptySet(),
                groupMemberRetriever,
                COMPUTE_TIER_SMALL,
                AVAILIBILITY_ZONE_A,
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
        assertThat(allocationResult.allocatorCoverageTable().size(), equalTo(0));

    }

    @Test
    public void testTenancyMismatch() {

        final TopologyEntityDTO virtualMachineWindows = VIRTUAL_MACHINE_SMALL_A.toBuilder()
                .setOid(OID_PROVIDER.incrementAndGet())
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setVirtualMachine(VirtualMachineInfo.newBuilder()
                                .setGuestOsInfo(OS.newBuilder()
                                        .setGuestOsType(OSType.LINUX))
                                .setTenancy(Tenancy.DEDICATED)))
                .build();

        final CoverageTopology coverageTopology = generateCoverageTopology(
                AWS_SERVICE_PROVIDER_TEST,
                Collections.singleton(RI_BOUGHT_SMALL_REGIONAL),
                Collections.singleton(RI_SPEC_SMALL_REGIONAL),
                Collections.emptySet(),
                groupMemberRetriever,
                COMPUTE_TIER_SMALL,
                AVAILIBILITY_ZONE_A,
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
        assertThat(allocationResult.allocatorCoverageTable().size(), equalTo(0));

    }

    @Test
    public void testInstanceSizeFlexibility() {
        final ReservedInstanceSpec riSpec = RI_SPEC_SMALL_REGIONAL.toBuilder()
                .setId(OID_PROVIDER.incrementAndGet())
                .setReservedInstanceSpecInfo(RI_SPEC_SMALL_REGIONAL.getReservedInstanceSpecInfo()
                        .toBuilder()
                        .setSizeFlexible(true))
                .build();
        final ReservedInstanceBought sizeFlexibleRI = ReservedInstanceBought.newBuilder()
                .setId(OID_PROVIDER.incrementAndGet())
                .setReservedInstanceBoughtInfo(RI_BOUGHT_SMALL_REGIONAL
                        .getReservedInstanceBoughtInfo()
                        .toBuilder()
                        .setReservedInstanceSpec(riSpec.getId())
                        .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                                .setNumberOfCoupons(4)
                                .build()))
                .build();

        final TopologyEntityDTO virtualMachineMedium = VIRTUAL_MACHINE_SMALL_A.toBuilder()
                .setOid(OID_PROVIDER.incrementAndGet())
                .clearCommoditiesBoughtFromProviders()
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE)
                        .setProviderId(COMPUTER_TIER_MEDIUM.getOid()))
                .build();
        final TopologyEntityDTO businessAccount = BUSINESS_ACCOUNT.toBuilder()
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(virtualMachineMedium.getOid())
                        .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setConnectionType(ConnectionType.OWNS_CONNECTION))
                .build();

        final CoverageTopology coverageTopology = generateCoverageTopology(
                AWS_SERVICE_PROVIDER_TEST,
                Collections.singleton(sizeFlexibleRI),
                Collections.singleton(riSpec),
                Collections.emptySet(),
                groupMemberRetriever,
                COMPUTE_TIER_SMALL,
                COMPUTER_TIER_MEDIUM,
                AVAILIBILITY_ZONE_A,
                REGION,
                VIRTUAL_MACHINE_SMALL_A,
                virtualMachineMedium,
                businessAccount);

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
                .put(VIRTUAL_MACHINE_SMALL_A.getOid(), sizeFlexibleRI.getId(), CloudCommitmentAmount.newBuilder()
                        .setCoupons(1.0)
                        .build())
                .put(virtualMachineMedium.getOid(), sizeFlexibleRI.getId(), CloudCommitmentAmount.newBuilder()
                        .setCoupons(2.0)
                        .build())
                .build();
        assertThat(allocationResult.allocatorCoverageTable().size(), equalTo(2));
        assertThat(allocationResult.allocatorCoverageTable(), equalTo(expectedAllocations));

    }

    @Test
    public void testPreviousCoverageBetweenEntityAndRI() {

        final CoverageTopology coverageTopology = generateCoverageTopology(
                AWS_SERVICE_PROVIDER_TEST,
                Collections.singleton(RI_BOUGHT_SMALL_REGIONAL),
                Collections.singleton(RI_SPEC_SMALL_REGIONAL),
                Collections.emptySet(),
                groupMemberRetriever,
                COMPUTE_TIER_SMALL,
                AVAILIBILITY_ZONE_A,
                REGION,
                VIRTUAL_MACHINE_SMALL_A,
                BUSINESS_ACCOUNT);

        /*
         * Invoke SUT
         */
        final CloudCommitmentCoverageAllocator allocator = allocatorFactory.createAllocator(
                CoverageAllocationConfig.builder()
                        .coverageTopology(coverageTopology)
                        .sourceCoverage(ImmutableTable.of(
                                VIRTUAL_MACHINE_SMALL_A.getOid(),
                                RI_BOUGHT_SMALL_REGIONAL.getId(),
                                CloudCommitmentAmount.newBuilder()
                                        .setCoupons(0.5)
                                        .build()))
                        .build());

        final CloudCommitmentCoverageAllocation allocationResult = allocator.allocateCoverage();

        /*
         * Asserts
         */
        final Table<Long, Long, CloudCommitmentAmount> expectedAllocations = ImmutableTable.<Long, Long, CloudCommitmentAmount>builder()
                .put(VIRTUAL_MACHINE_SMALL_A.getOid(), RI_BOUGHT_SMALL_REGIONAL.getId(), CloudCommitmentAmount.newBuilder()
                        .setCoupons(.5)
                        .build())
                .build();
        assertThat(allocationResult.allocatorCoverageTable().size(), equalTo(1));
        assertThat(allocationResult.allocatorCoverageTable(), equalTo(expectedAllocations));
    }
}

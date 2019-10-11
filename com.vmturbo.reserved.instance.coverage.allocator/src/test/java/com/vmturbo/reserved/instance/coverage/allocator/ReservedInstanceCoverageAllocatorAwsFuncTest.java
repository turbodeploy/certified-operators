package com.vmturbo.reserved.instance.coverage.allocator;

import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.AVAILIBILITY_ZONE_A;
import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.BUSINESS_ACCOUNT;
import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.COMPUTER_TIER_MEDIUM;
import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.COMPUTE_TIER_SMALL;
import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.OID_PROVIDER;
import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.REGION;
import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.RI_BOUGHT_SMALL_REGIONAL;
import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.RI_SPEC_SMALL_REGIONAL;
import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.VIRTUAL_MACHINE_SMALL_A;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

import java.util.Collections;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
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


public class ReservedInstanceCoverageAllocatorAwsFuncTest extends AbstractReservedInstanceCoverageAllocatorTest{



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
                Collections.singleton(zonalRiBought),
                Collections.singleton(RI_SPEC_SMALL_REGIONAL),
                COMPUTE_TIER_SMALL,
                AVAILIBILITY_ZONE_A,
                REGION,
                VIRTUAL_MACHINE_SMALL_A,
                BUSINESS_ACCOUNT);

        final ReservedInstanceCoverageAllocator allocator = ReservedInstanceCoverageAllocator.newBuilder()
                .coverageTopology(coverageTopology)
                .coverageProvider(createCoverageProvider(
                        ImmutableMap.of(VIRTUAL_MACHINE_SMALL_A.getOid(), 1.0),
                        ImmutableMap.of(zonalRiBought.getId(), 1.0)))
                .build();

        final ReservedInstanceCoverageAllocation allocationResult = allocator.allocateCoverage();

        /*
         * Asserts
         */
        assertThat(allocationResult.allocatorCoverageTable().size(), equalTo(1));
        assertThat(allocationResult.allocatorCoverageTable().rowMap(),
                hasEntry(VIRTUAL_MACHINE_SMALL_A.getOid(), Collections.singletonMap(
                        zonalRiBought.getId(), 1.0)));
    }

    @Test
    public void testSharedZonalAssignment() {
        final TopologyEntityDTO businessAccountB = BUSINESS_ACCOUNT.toBuilder()
                .setOid(OID_PROVIDER.incrementAndGet())
                .clearConnectedEntityList()
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(BUSINESS_ACCOUNT.getOid())
                        .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setConnectionType(ConnectionType.OWNS_CONNECTION))
                .build();
        final ReservedInstanceBought zonalRiBoughtB = ReservedInstanceBought.newBuilder()
                .setId(OID_PROVIDER.incrementAndGet())
                .setReservedInstanceBoughtInfo(RI_BOUGHT_SMALL_REGIONAL
                        .getReservedInstanceBoughtInfo()
                        .toBuilder()
                        .setAvailabilityZoneId(AVAILIBILITY_ZONE_A.getOid())
                        .setBusinessAccountId(businessAccountB.getOid()))
                .build();

        final CoverageTopology coverageTopology = generateCoverageTopology(
                Collections.singleton(zonalRiBoughtB),
                Collections.singleton(RI_SPEC_SMALL_REGIONAL),
                COMPUTE_TIER_SMALL,
                AVAILIBILITY_ZONE_A,
                REGION,
                VIRTUAL_MACHINE_SMALL_A,
                BUSINESS_ACCOUNT,
                businessAccountB);


        /*
         * Invoke SUT
         */
        final ReservedInstanceCoverageAllocator allocator = ReservedInstanceCoverageAllocator.newBuilder()
                .coverageTopology(coverageTopology)
                .coverageProvider(createCoverageProvider(
                        ImmutableMap.of(VIRTUAL_MACHINE_SMALL_A.getOid(), 1.0),
                        ImmutableMap.of(zonalRiBoughtB.getId(), 1.0)))
                .build();

        final ReservedInstanceCoverageAllocation allocationResult = allocator.allocateCoverage();

        /*
         * Asserts
         */
        assertThat(allocationResult.allocatorCoverageTable().size(), equalTo(1));
        assertThat(allocationResult.allocatorCoverageTable().rowMap(),
                hasEntry(VIRTUAL_MACHINE_SMALL_A.getOid(), Collections.singletonMap(
                        zonalRiBoughtB.getId(), 1.0)));
    }

    @Test
    public void testDirectRegionalAssignment() {

        final CoverageTopology coverageTopology = generateCoverageTopology(
                Collections.singleton(RI_BOUGHT_SMALL_REGIONAL),
                Collections.singleton(RI_SPEC_SMALL_REGIONAL),
                COMPUTE_TIER_SMALL,
                AVAILIBILITY_ZONE_A,
                REGION,
                VIRTUAL_MACHINE_SMALL_A,
                BUSINESS_ACCOUNT);

        /*
         * Invoke SUT
         */
        final ReservedInstanceCoverageAllocator allocator = ReservedInstanceCoverageAllocator.newBuilder()
                .coverageTopology(coverageTopology)
                .coverageProvider(createCoverageProvider(
                        ImmutableMap.of(VIRTUAL_MACHINE_SMALL_A.getOid(), 1.0),
                        ImmutableMap.of(RI_BOUGHT_SMALL_REGIONAL.getId(), 1.0)))
                .build();

        final ReservedInstanceCoverageAllocation allocationResult = allocator.allocateCoverage();

        /*
         * Asserts
         */
        assertThat(allocationResult.allocatorCoverageTable().size(), equalTo(1));
        assertThat(allocationResult.allocatorCoverageTable().rowMap(),
                hasEntry(VIRTUAL_MACHINE_SMALL_A.getOid(), Collections.singletonMap(
                        RI_BOUGHT_SMALL_REGIONAL.getId(), 1.0)));
    }

    @Test
    public void testSharedRegionalAssignment() {

        final TopologyEntityDTO businessAccountB = BUSINESS_ACCOUNT.toBuilder()
                .setOid(OID_PROVIDER.incrementAndGet())
                .clearConnectedEntityList()
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(BUSINESS_ACCOUNT.getOid())
                        .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setConnectionType(ConnectionType.OWNS_CONNECTION))
                .build();

        final ReservedInstanceBought regionalRIB = ReservedInstanceBought.newBuilder()
                .setId(OID_PROVIDER.incrementAndGet())
                .setReservedInstanceBoughtInfo(RI_BOUGHT_SMALL_REGIONAL
                        .getReservedInstanceBoughtInfo()
                        .toBuilder()
                        .setBusinessAccountId(businessAccountB.getOid()))
                .build();

        final CoverageTopology coverageTopology = generateCoverageTopology(
                Collections.singleton(regionalRIB),
                Collections.singleton(RI_SPEC_SMALL_REGIONAL),
                COMPUTE_TIER_SMALL,
                AVAILIBILITY_ZONE_A,
                REGION,
                VIRTUAL_MACHINE_SMALL_A,
                BUSINESS_ACCOUNT,
                businessAccountB);

        /*
         * Invoke SUT
         */
        final ReservedInstanceCoverageAllocator allocator = ReservedInstanceCoverageAllocator.newBuilder()
                .coverageTopology(coverageTopology)
                .coverageProvider(createCoverageProvider(
                        ImmutableMap.of(VIRTUAL_MACHINE_SMALL_A.getOid(), 1.0),
                        ImmutableMap.of(regionalRIB.getId(), 1.0)))
                .build();

        final ReservedInstanceCoverageAllocation allocationResult = allocator.allocateCoverage();

        /*
         * Asserts
         */
        assertThat(allocationResult.allocatorCoverageTable().size(), equalTo(1));
        assertThat(allocationResult.allocatorCoverageTable().rowMap(),
                hasEntry(VIRTUAL_MACHINE_SMALL_A.getOid(), Collections.singletonMap(
                        regionalRIB.getId(), 1.0)));
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
                Collections.singleton(RI_BOUGHT_SMALL_REGIONAL),
                Collections.singleton(RI_SPEC_SMALL_REGIONAL),
                COMPUTE_TIER_SMALL,
                AVAILIBILITY_ZONE_A,
                REGION,
                virtualMachineWindows,
                BUSINESS_ACCOUNT);

        /*
         * Invoke SUT
         */
        final ReservedInstanceCoverageAllocator allocator = ReservedInstanceCoverageAllocator.newBuilder()
                .coverageTopology(coverageTopology)
                .coverageProvider(createCoverageProvider(
                        ImmutableMap.of(VIRTUAL_MACHINE_SMALL_A.getOid(), 1.0),
                        ImmutableMap.of(RI_BOUGHT_SMALL_REGIONAL.getId(), 1.0)))
                .build();

        final ReservedInstanceCoverageAllocation allocationResult = allocator.allocateCoverage();

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
                Collections.singleton(RI_BOUGHT_SMALL_REGIONAL),
                Collections.singleton(RI_SPEC_SMALL_REGIONAL),
                COMPUTE_TIER_SMALL,
                AVAILIBILITY_ZONE_A,
                REGION,
                virtualMachineWindows,
                BUSINESS_ACCOUNT);

        /*
         * Invoke SUT
         */
        final ReservedInstanceCoverageAllocator allocator = ReservedInstanceCoverageAllocator.newBuilder()
                .coverageTopology(coverageTopology)
                .coverageProvider(createCoverageProvider(
                        ImmutableMap.of(VIRTUAL_MACHINE_SMALL_A.getOid(), 1.0),
                        ImmutableMap.of(RI_BOUGHT_SMALL_REGIONAL.getId(), 1.0)))
                .build();

        final ReservedInstanceCoverageAllocation allocationResult = allocator.allocateCoverage();

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
                        .setReservedInstanceSpec(riSpec.getId()))
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
                Collections.singleton(sizeFlexibleRI),
                Collections.singleton(riSpec),
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
        final ReservedInstanceCoverageAllocator allocator = ReservedInstanceCoverageAllocator.newBuilder()
                .coverageTopology(coverageTopology)
                .coverageProvider(createCoverageProvider(
                        ImmutableMap.of(VIRTUAL_MACHINE_SMALL_A.getOid(), 1.0,
                                virtualMachineMedium.getOid(), 2.0),
                        ImmutableMap.of(sizeFlexibleRI.getId(), 4.0)))
                .build();

        final ReservedInstanceCoverageAllocation allocationResult = allocator.allocateCoverage();

        /*
         * Asserts
         */
        /*
         * Asserts
         */
        assertThat(allocationResult.allocatorCoverageTable().size(), equalTo(2));
        assertThat(allocationResult.allocatorCoverageTable().rowMap(),
                hasEntry(VIRTUAL_MACHINE_SMALL_A.getOid(), Collections.singletonMap(
                        sizeFlexibleRI.getId(), 1.0)));
        assertThat(allocationResult.allocatorCoverageTable().rowMap(),
                hasEntry(virtualMachineMedium.getOid(), Collections.singletonMap(
                        sizeFlexibleRI.getId(), 2.0)));

    }
}

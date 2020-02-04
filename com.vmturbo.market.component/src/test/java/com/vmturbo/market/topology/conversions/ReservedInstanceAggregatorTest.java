package com.vmturbo.market.topology.conversions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.group.api.ImmutableGroupAndMembers;
import com.vmturbo.market.topology.RiDiscountedMarketTier;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;


public class ReservedInstanceAggregatorTest {

    private static final long REGION_1 = 10;
    private static final int REGION_2 = 11;
    private static final int ZONE_1 = 20;
    private static final int ACCOUNT_1 = 15;
    private static final OSType OS_1 = OSType.LINUX;
    private static final Tenancy TENANCY_1 = Tenancy.DEFAULT;
    private static final long TIER_1 = 50;
    private static final String FAMILY_1 = "LINUX";
    private static final long BILLING_FAMILY_ID = 444L;

    private final Map<Long, TopologyEntityDTO> topology = Mockito.spy(new HashMap<>());
    private final ReservedInstanceBought riBought1 = ReservedInstanceBought.newBuilder().setId(10)
        .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder().setAvailabilityZoneId(ZONE_1)
            .setBusinessAccountId(ACCOUNT_1)
            .setNumBought(10)
            .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder().setNumberOfCoupons(100)
                .setNumberOfCouponsUsed(70).build()).build()).build();
    private final ReservedInstanceSpec riSpec1 = ReservedInstanceSpec.newBuilder().setId(11)
        .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder().setOs(OS_1)
            .setSizeFlexible(true)
            .setRegionId(REGION_1)
            .setTenancy(TENANCY_1)
            .setTierId(TIER_1).build()).build();
    private final ReservedInstanceData riData1 = new ReservedInstanceData(riBought1, riSpec1);
    private final ReservedInstanceData riData2 = new ReservedInstanceData(riBought1, riSpec1);

    @Test
    public void testReservedInstanceKey() {
        ReservedInstanceBought riBought2 = ReservedInstanceBought.newBuilder().setId(10)
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder().setAvailabilityZoneId(ZONE_1)
                        .setBusinessAccountId(ACCOUNT_1)
                        .setNumBought(10)
                        .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder().setNumberOfCoupons(100)
                                .setNumberOfCouponsUsed(70).build()).build()).build();
        ReservedInstanceSpec riSpec2 = ReservedInstanceSpec.newBuilder().setId(11)
                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder().setOs(OS_1)
                        .setSizeFlexible(true)
                        .setRegionId(REGION_2)
                        .setTenancy(TENANCY_1)
                        .setTierId(TIER_1).build()).build();

        when(topology.get(TIER_1)).thenReturn(
                TopologyEntityDTO.newBuilder().setOid(TIER_1)
                    .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setComputeTier(
                                ComputeTierInfo.newBuilder().setFamily(FAMILY_1)
                                .build())
                        .build())
                .build());

        ReservedInstanceData riData3 = new ReservedInstanceData(riBought2, riSpec2);

        final ReservedInstanceKey riKey1 = new ReservedInstanceKey(riData1, FAMILY_1,
                BILLING_FAMILY_ID);
        final ReservedInstanceKey riKey2 = new ReservedInstanceKey(riData2, FAMILY_1,
                BILLING_FAMILY_ID);
        final ReservedInstanceKey riKey3 = new ReservedInstanceKey(riData3, FAMILY_1,
                BILLING_FAMILY_ID);
        final TopologyEntityDTO computeTier = TopologyEntityDTO.newBuilder().setOid(TIER_1)
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setComputeTier(
                        ComputeTierInfo.newBuilder().setFamily(FAMILY_1)
                                .build()).build()).build();
        final Set<Long> applicableBusinessAccounts = Collections.singleton((long)ACCOUNT_1);
        ReservedInstanceAggregate riAgg1 = new ReservedInstanceAggregate(riData1, riKey1,
                computeTier, applicableBusinessAccounts);
        ReservedInstanceAggregate riAgg2 = new ReservedInstanceAggregate(riData2, riKey2,
                computeTier, applicableBusinessAccounts);
        assertEquals(riAgg1, riAgg2);

        ReservedInstanceAggregate riAgg3 = new ReservedInstanceAggregate(riData3, riKey3,
                computeTier, applicableBusinessAccounts);
        assertNotEquals(riAgg1, riAgg3);

        Set<ReservedInstanceAggregate> aggregates = new HashSet<>();
        aggregates.add(riAgg1);
        assertTrue(aggregates.contains(riAgg2));

        TopologyEntityDTO region1 = TopologyEntityDTO.newBuilder().setOid(REGION_1)
                .setEntityType(EntityDTO.EntityType.REGION_VALUE)
                .build();
        TopologyEntityDTO region2 = TopologyEntityDTO.newBuilder().setOid(REGION_2)
                .setEntityType(EntityDTO.EntityType.REGION_VALUE)
                .build();
        RiDiscountedMarketTier riMarketTier1 = new RiDiscountedMarketTier(topology.get(TIER_1), region1, riAgg1);
        RiDiscountedMarketTier riMarketTier2 = new RiDiscountedMarketTier(topology.get(TIER_1), region1, riAgg2);
        RiDiscountedMarketTier riMarketTier3 = new RiDiscountedMarketTier(topology.get(TIER_1), region2, riAgg3);
        assertEquals(riMarketTier1, riMarketTier2);
        assertNotEquals(riMarketTier1, riMarketTier3);
    }

    /**
     * If tierId_ of riSpec of riData is not in topology, then do nothing for this riData
     * Related bug: OM-43481
     */
    @Test
    public void testAggregateRis() {
        TopologyInfo topoInfo = TopologyInfo.newBuilder().build();
        CloudCostData ccd = mock(CloudCostData.class);
        when(ccd.getExistingRiBought()).thenReturn(Collections.singletonList(riData1));
        final TopologyEntityCloudTopology cloudTopology =
                Mockito.mock(TopologyEntityCloudTopology.class);
        when(cloudTopology.getBillingFamilyForEntity(anyLong()))
                .thenReturn(Optional.of(ImmutableGroupAndMembers.builder()
                        .group(Grouping.newBuilder().setId(1111L).build())
                        .entities(Collections.emptyList())
                        .members(Collections.emptyList())
                        .build()));
        ReservedInstanceAggregator ria = new ReservedInstanceAggregator(ccd, topology,
                cloudTopology);
        assertEquals(0, ria.aggregate(topoInfo).size());
        TopologyEntityDTO computeTier = TopologyEntityDTO.newBuilder().setOid(TIER_1)
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setComputeTier(
                ComputeTierInfo.newBuilder().setFamily(FAMILY_1)
                    .build())
                .build())
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityType(EntityType.REGION_VALUE)
                        .setConnectedEntityId(REGION_1)
                        .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                        .build())
            .build();
        topology.put(TIER_1, computeTier);
        topology.put(REGION_1, computeTier);
        assertEquals(1, ria.aggregate(topoInfo).size());
    }
}
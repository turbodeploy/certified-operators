package com.vmturbo.market.topology.conversions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.market.topology.RiDiscountedMarketTier;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;


public class ReservedInstanceAggregatorTest {

    private static final int REGION_1 = 10;
    private static final int REGION_2 = 11;
    private static final int ZONE_1 = 20;
    private static final int ACCOUNT_1 = 15;
    private static final OSType OS_1 = OSType.LINUX;
    private static final Tenancy TENANCY_1 = Tenancy.DEDICATED;
    private static final long TIER_1 = 50;
    private static final String FAMILY_1 = "LINUX";

    private final Map<Long, TopologyEntityDTO> topology = mock(Map.class);
    private final ReservedInstanceBought riBought1 = ReservedInstanceBought.newBuilder().setId(10)
        .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder().setAvailabilityZoneId(ZONE_1)
            .setBusinessAccountId(ACCOUNT_1)
            .setNumBought(10)
            .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder().setNumberOfCoupons(100)
                .setNumberOfCouponsUsed(70).build()).build()).build();
    private final ReservedInstanceSpec riSpec1 = ReservedInstanceSpec.newBuilder().setId(11)
        .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder().setOs(OS_1)
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

        CloudCostData ccd = mock(CloudCostData.class);
        ReservedInstanceAggregate riAgg1 = new ReservedInstanceAggregate(riData1, topology);
        ReservedInstanceAggregate riAgg2 = new ReservedInstanceAggregate(riData2, topology);
        assertTrue(riAgg1.equals(riAgg2));

        ReservedInstanceAggregate riAgg3 = new ReservedInstanceAggregate(riData3, topology);
        assertFalse(riAgg1.equals(riAgg3));

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
        assertTrue(riMarketTier1.equals(riMarketTier2));
        assertFalse(riMarketTier1.equals(riMarketTier3));
    }

    /**
     * If tierId_ of riSpec of riData is not in topology, then do nothing for this riData
     * Related bug: OM-43481
     */
    @Test
    public void testAggregateRis() {
        CloudCostData ccd = mock(CloudCostData.class);
        when(ccd.getAllRiBought()).thenReturn(Arrays.asList(riData1));
        ReservedInstanceAggregator ria = new ReservedInstanceAggregator(ccd, topology);
        assertFalse(ria.aggregateRis());
        assertEquals(0, ria.riAggregates.size());


        TopologyEntityDTO computeTier = TopologyEntityDTO.newBuilder().setOid(TIER_1)
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setComputeTier(
                ComputeTierInfo.newBuilder().setFamily(FAMILY_1)
                    .build())
                .build())
            .build();
        when(topology.get(TIER_1)).thenReturn(computeTier);
        when(topology.get(new Long(REGION_1))).thenReturn(computeTier);
        assertTrue(ria.aggregateRis());
        assertEquals(1, ria.riAggregates.size());
    }
}
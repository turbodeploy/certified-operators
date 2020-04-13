package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.calculator;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalyzerConstantsTest;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceHistoricalDemandDataType;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.AccountGroupingIdentifier;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.ImmutableAccountGroupingIdentifier;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.ImmutableRIBuyDemandCluster;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.ImmutableRIBuyRegionalContext;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyDemandCluster;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyHistoricalDemandProvider;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyRegionalContext;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.RegionalRIMatcherCache;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.ReservedInstanceInventoryMatcher;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Class to test {@link RIBuyDemandCalculator}.
 */
public class RIBuyDemandCalculatorTest {

    private static final long REGION_OID = 73397477051451L;
    private static final long REGION_OID2 = 73397477051452L;
    private static final long ACCOUNT_OID = 73397477051545L;

    private static final TopologyEntityDTO REGION = TopologyEntityDTO.newBuilder()
            .setOid(REGION_OID)
            .setDisplayName("region")
            .setEntityType(EntityType.REGION_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(2222)
                    .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                    .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .build();

    private static final ReservedInstanceType RI_TYPE =
            ReservedInstanceType.newBuilder().setTermYears(1).build();
    private static final Cost.ReservedInstanceSpecInfo riSpecInfo =
            Cost.ReservedInstanceSpecInfo.newBuilder()
                    .setOs(OSType.RHEL)
                    .setTenancy(Tenancy.DEFAULT)
                    .setType(RI_TYPE)
                    .setPlatformFlexible(true)
                    .build();

    private static final Cost.ReservedInstanceSpec RI_TO_PURCHASE =
            Cost.ReservedInstanceSpec.newBuilder().setReservedInstanceSpecInfo(riSpecInfo).build();

    private static final AccountGroupingIdentifier ACCOUNT_GROUPING =
            ImmutableAccountGroupingIdentifier.builder()
                    .groupingType(AccountGroupingIdentifier.AccountGroupingType.BILLING_FAMILY)
                    .id(1)
                    .tag("AccountGrouping")
                    .build();

    private static final RIBuyDemandCluster RI_BUY_DEMAND_CLUSTER =
            ImmutableRIBuyDemandCluster.builder()
                    .accountOid(ACCOUNT_OID)
                    .platform(OSType.LINUX)
                    .tenancy(Tenancy.DEFAULT)
                    .computeTier(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_NANO)
                    .regionOrZoneOid(REGION_OID)
                    .build();

    private static final RIBuyDemandCluster RI_BUY_DEMAND_CLUSTER2 =
            ImmutableRIBuyDemandCluster.builder()
                    .accountOid(ACCOUNT_OID)
                    .platform(OSType.LINUX)
                    .tenancy(Tenancy.DEFAULT)
                    .computeTier(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_NANO)
                    .regionOrZoneOid(REGION_OID2)
                    .build();

    private static final RIBuyRegionalContext REGIONAL_CONTEXT =
            ImmutableRIBuyRegionalContext.builder()
                    .region(REGION)
                    .riSpecToPurchase(RI_TO_PURCHASE)
                    .computeTier(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_NANO)
                    .accountGroupingId(ACCOUNT_GROUPING)
                    .contextTag("Context")
                    .analysisTag("Analysis")
                    .addDemandClusters(RI_BUY_DEMAND_CLUSTER)
                    .addDemandClusters(RI_BUY_DEMAND_CLUSTER2)
                    .build();

    /**
     * Test {@link RIBuyDemandCalculator#calculateUncoveredDemand}.
     */
    @Test
    public void testCalculateUncoveredDemand() {

        final RegionalRIMatcherCache regionalRIMatcherCache = mock(RegionalRIMatcherCache.class);

        final ReservedInstanceInventoryMatcher reservedInstanceInventoryMatcher =
                mock(ReservedInstanceInventoryMatcher.class);

        when(reservedInstanceInventoryMatcher.matchToDemandContext(any(), any(), any())).thenReturn(
                ImmutableSet.of());

        when(regionalRIMatcherCache.getOrCreateRIInventoryMatcherForRegion(anyLong())).thenReturn(
                reservedInstanceInventoryMatcher);

        final float[] demand = {-1, 1, -1, 4.2f, -1, 1, 0.9f, 1, 1, 1};
        final float[] demand2 = {2, -1, 2, 1f, 2.8f, 1, 1f, -1, -1, -1};

        final float[] normalizedDemand = {2, 1, 2, 5.2f, 2.8f, 2, 1.9f, 1, 1, 1};

        final RIBuyHistoricalDemandProvider demandProvider = mock(RIBuyHistoricalDemandProvider.class);
        when(demandProvider.getDemand(eq(RI_BUY_DEMAND_CLUSTER), any())).thenReturn(demand);
        when(demandProvider.getDemand(eq(RI_BUY_DEMAND_CLUSTER2), any())).thenReturn(demand2);

        final RIBuyDemandCalculator riBuyDemandCalculator = new RIBuyDemandCalculator(demandProvider,
                ReservedInstanceHistoricalDemandDataType.ALLOCATION, regionalRIMatcherCache, 1f);
        final RIBuyDemandCalculationInfo riBuyDemandCalculationInfo =
                riBuyDemandCalculator.calculateUncoveredDemand(REGIONAL_CONTEXT);

        Assert.assertEquals(10, riBuyDemandCalculationInfo.activeHours());
        Assert.assertArrayEquals(normalizedDemand, riBuyDemandCalculationInfo.aggregateUncoveredDemand(),
                0.01f);
        Assert.assertArrayEquals(normalizedDemand,
                riBuyDemandCalculationInfo.uncoveredDemandByComputeTier()
                        .values()
                        .iterator()
                        .next(), 0.01f);
    }
}

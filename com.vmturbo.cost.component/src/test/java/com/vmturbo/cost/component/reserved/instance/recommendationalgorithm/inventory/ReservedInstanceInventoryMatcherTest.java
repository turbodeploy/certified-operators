package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalyzerConstantsTest;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.AccountGroupingIdentifier;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.AccountGroupingIdentifier.AccountGroupingType;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.ImmutableAccountGroupingIdentifier;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.ImmutableRIBuyDemandCluster;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.ImmutableRIBuyRegionalContext;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyDemandCluster;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyRegionalContext;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.reserved.instance.coverage.allocator.rules.RICoverageRuleConfig;

/**
 * Class to test {@link ReservedInstanceInventoryMatcher}.
 */
public class ReservedInstanceInventoryMatcherTest {

    private static final long REGION_OID = 73397477051451L;
    private static final long ZONE_OID = 73397477051452L;
    private static final long ACCOUNT_OID = 73397477051545L;
    private static final long RI_SPEC_OID = 3333;
    private static final long RI_OID = 44440;
    private static final long RI_OID2 = 44441;

    private static final ReservedInstanceBoughtInfo RESERVED_INSTANCE_BOUGHT_INFO_1 =
            ReservedInstanceBoughtInfo.newBuilder()
                    .setBusinessAccountId(ACCOUNT_OID)
                    .setAvailabilityZoneId(ZONE_OID)
                    .setReservedInstanceSpec(RI_SPEC_OID)
                    .build();

    private static final ReservedInstanceBoughtInfo RESERVED_INSTANCE_BOUGHT_INFO_2 =
            ReservedInstanceBoughtInfo.newBuilder()
                    .setBusinessAccountId(ACCOUNT_OID)
                    .setAvailabilityZoneId(ZONE_OID)
                    .setReservedInstanceSpec(RI_SPEC_OID)
                    .build();

    private static final ReservedInstanceBought RESERVED_INSTANCE_BOUGHT_1 =
            ReservedInstanceBought.newBuilder()
                    .setId(RI_OID)
                    .setReservedInstanceBoughtInfo(RESERVED_INSTANCE_BOUGHT_INFO_1)
                    .build();


    private static final ReservedInstanceBought RESERVED_INSTANCE_BOUGHT_2 =
            ReservedInstanceBought.newBuilder()
                    .setId(RI_OID2)
                    .setReservedInstanceBoughtInfo(RESERVED_INSTANCE_BOUGHT_INFO_2)
                    .build();

    private static final TopologyEntityDTO REGION = TopologyEntityDTO.newBuilder()
            .setOid(REGION_OID)
            .setDisplayName("region")
            .setEntityType(EntityType.REGION_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(ZONE_OID)
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
                    .groupingType(AccountGroupingType.BILLING_FAMILY)
                    .id(ACCOUNT_OID)
                    .tag("AccountGrouping")
                    .build();

    private static final AccountGroupingIdentifier ACCOUNT_GROUPING2 =
            ImmutableAccountGroupingIdentifier.builder()
                    .groupingType(AccountGroupingType.STANDALONE_ACCOUNT)
                    .id(ACCOUNT_OID)
                    .tag("AccountGrouping")
                    .build();

    private static final RIBuyDemandCluster RI_BUY_DEMAND_CLUSTER =
            ImmutableRIBuyDemandCluster.builder()
                    .accountOid(ACCOUNT_OID)
                    .platform(OSType.LINUX)
                    .tenancy(Tenancy.DEFAULT)
                    .computeTier(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_NANO)
                    .regionOrZoneOid(ZONE_OID)
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
                    .build();

    /**
     * Test {@link ReservedInstanceInventoryMatcher#matchToDemandContext}.
     */
    @Test
    public void testMatchToDemandContext() {

        final Collection<ReservedInstanceBought> riBoughtInstances =
                ImmutableSet.of(RESERVED_INSTANCE_BOUGHT_1, RESERVED_INSTANCE_BOUGHT_2);

        final Map<Long, AccountGroupingIdentifier> riOidToAccountGroupingId =
                ImmutableMap.of(RI_OID, ACCOUNT_GROUPING, RI_OID2, ACCOUNT_GROUPING2);

        final ReservedInstanceSpecMatcher riSpecMatcher = mock(ReservedInstanceSpecMatcher.class);
        when(riSpecMatcher.matchDemandContextToRISpecs(any(), any(), any())).thenReturn(
                ImmutableSet.of(RI_SPEC_OID));

        final ReservedInstanceInventoryMatcher matcher =
                new ReservedInstanceInventoryMatcher(riSpecMatcher, riBoughtInstances,
                        riOidToAccountGroupingId);


        final RICoverageRuleConfig matchingRule = mock(RICoverageRuleConfig.class);
        // test shared scope
        when(matchingRule.isSharedScope()).thenReturn(true);
        when(matchingRule.isZoneScoped()).thenReturn(true);
        final Set<ReservedInstanceBought> result =
                matcher.matchToDemandContext(matchingRule, REGIONAL_CONTEXT, RI_BUY_DEMAND_CLUSTER);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(RESERVED_INSTANCE_BOUGHT_1, result.iterator().next());

        // test non-shared scope
        when(matchingRule.isSharedScope()).thenReturn(false);
        final Set<ReservedInstanceBought> result2 =
                matcher.matchToDemandContext(matchingRule, REGIONAL_CONTEXT, RI_BUY_DEMAND_CLUSTER);
        Assert.assertEquals(1, result2.size());
        Assert.assertEquals(RESERVED_INSTANCE_BOUGHT_2, result2.iterator().next());
    }
}

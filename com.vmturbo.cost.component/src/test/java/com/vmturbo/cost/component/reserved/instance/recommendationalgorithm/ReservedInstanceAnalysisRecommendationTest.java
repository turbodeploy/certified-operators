package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import static org.junit.Assert.assertEquals;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.AccountGroupingIdentifier;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.ImmutableAccountGroupingIdentifier;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.ImmutableRIBuyRegionalContext;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyRegionalContext;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * This class tests methods in the ReservedInstanceAnalysisRecommendation class.
 */
public class ReservedInstanceAnalysisRecommendationTest {

    private static final String RECOMMENDATION_TAG = "Tag";
    private static final String ACTION_GOAL = "Goal";
    private static final String CONTEXT_TAG = "Context";
    private static final String ANALYSIS_TAG = "Analysis";
    private static final String ACCOUNT_GROUPING_TAG = "AccountGrouping";

    private RIBuyRegionalContext regionalContext;
    private ImmutablePricingProviderResult pricing;
    private TopologyEntityDTO region;
    private AccountGroupingIdentifier accountGrouping;
    private CloudCostDTO.ReservedInstanceType riType;
    private Cost.ReservedInstanceSpecInfo riSpecInfo;
    private Cost.ReservedInstanceSpec riToPurchase;

    /**
     * This method tests the creation of ReservedInstanceAnalysisRecommendation.
     */
    @Test
    public void testReservedInstanceAnalysisRecommendation() {
        region = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.REGION_VALUE)
                .setOid(ReservedInstanceAnalyzerConstantsTest.REGION_AWS_OHIO_OID)
                .setDisplayName("test region")
                .build();
        accountGrouping = ImmutableAccountGroupingIdentifier.builder()
                .groupingType(AccountGroupingIdentifier.AccountGroupingType.BILLING_FAMILY)
                .id(1)
                .tag(ACCOUNT_GROUPING_TAG)
                .build();
        riType = CloudCostDTO.ReservedInstanceType.newBuilder()
                .setTermYears(1)
                .build();
        riSpecInfo = Cost.ReservedInstanceSpecInfo.newBuilder()
                .setOs(OSType.RHEL)
                .setTenancy(Tenancy.DEFAULT)
                .setType(riType)
                .setPlatformFlexible(true).build();
        riToPurchase = Cost.ReservedInstanceSpec.newBuilder()
                .setReservedInstanceSpecInfo(riSpecInfo)
                .build();
        regionalContext = ImmutableRIBuyRegionalContext.builder()
                .region(region)
                .riSpecToPurchase(riToPurchase)
                .computeTier(ReservedInstanceAnalyzerConstantsTest.COMPUTE_TIER_T2_NANO)
                .accountGroupingId(accountGrouping)
                .contextTag(CONTEXT_TAG)
                .analysisTag(ANALYSIS_TAG)
                .build();

        final long accountId = 1111;
        final float onDemandRate = 0.6f;
        final float upFrontRate = 3f;
        final float recurringRate = 0f;
        pricing = ImmutablePricingProviderResult.builder()
                .reservedInstanceRecurringRate(recurringRate)
                .reservedInstanceUpfrontRate(upFrontRate)
                .onDemandRate(onDemandRate)
                .build();

        final int count = 1;
        final float hourlySavings = 0.1f;
        final float avgHourlyCouponDemand = 0.5f;
        final int totalHours = 10;
        final int activeHours = 6;
        final int coupons = 2;
        final float couponsUsed = 1.3f;
        final float riUtilization = couponsUsed / coupons;
        ReservedInstanceAnalysisRecommendation recommendation =
                new ReservedInstanceAnalysisRecommendation(
                        123,
                        RECOMMENDATION_TAG,
                        ACTION_GOAL,
                        regionalContext,
                        accountId,
                        count,
                        pricing,
                        hourlySavings,
                        avgHourlyCouponDemand,
                        totalHours,
                        activeHours,
                        coupons,
                        couponsUsed);

        assertEquals(ACTION_GOAL, recommendation.getActionGoal());
        assertEquals(RECOMMENDATION_TAG, recommendation.getLogTag());
        assertEquals(avgHourlyCouponDemand, recommendation.getAverageHourlyCouponDemand(), 0);
        assertEquals(activeHours, recommendation.getActiveHours());
        assertEquals(count, recommendation.getCount());
        assertEquals(onDemandRate, recommendation.getOnDemandHourlyCost(), 0);
        assertEquals(hourlySavings, recommendation.getHourlyCostSavings(), 0);
        assertEquals(riSpecInfo.getOs(), recommendation.getPlatform());
        assertEquals(regionalContext.regionOid(), recommendation.getRegion());
        assertEquals(coupons, recommendation.getRiNormalizedCoupons());
        assertEquals(couponsUsed, recommendation.getRiNormalizedCouponsUsed(), 0);
        assertEquals(riUtilization, recommendation.getRiUtilization(), 0);
        assertEquals(riSpecInfo.getTenancy(), recommendation.getTenancy());
        assertEquals(riSpecInfo.getType().getTermYears(), recommendation.getTermInYears());

        assertEquals(1.3f,
                recommendation.getRiBoughtInfo().getReservedInstanceBoughtCoupons().getNumberOfCouponsUsed(), 0.001);
        assertEquals(2,
                recommendation.getRiBoughtInfo().getReservedInstanceBoughtCoupons().getNumberOfCoupons(), 0);
    }
}

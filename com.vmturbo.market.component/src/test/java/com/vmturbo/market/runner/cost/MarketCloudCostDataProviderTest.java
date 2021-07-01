package com.vmturbo.market.runner.cost;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.CostMoles;
import com.vmturbo.common.protobuf.cost.EntityUptime;
import com.vmturbo.common.protobuf.cost.EntityUptime.EntityUptimeDTO;
import com.vmturbo.common.protobuf.cost.EntityUptime.GetEntityUptimeByFilterRequest;
import com.vmturbo.common.protobuf.cost.EntityUptimeMoles;
import com.vmturbo.common.protobuf.cost.Pricing;
import com.vmturbo.common.protobuf.cost.PricingMoles;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CommonCost;

/**
 * Tests for MarketCloudCostDataProvider.
 */
public class MarketCloudCostDataProviderTest {

    private final TopologyEntityInfoExtractor topologyEntityInfoExtractor =
        mock(TopologyEntityInfoExtractor.class);
    private final DiscountApplicator.DiscountApplicatorFactory discountApplicatorFactory =
        mock(DiscountApplicator.DiscountApplicatorFactory.class);
    private static final TopologyDTO.TopologyEntityDTO CLOUD_VM_1 =
        TopologyDTO.TopologyEntityDTO.newBuilder()
            .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD).setOid(73695157440640L)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).build();
    private static final TopologyDTO.TopologyEntityDTO CLOUD_VM_2 =
        TopologyDTO.TopologyEntityDTO.newBuilder()
            .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD).setOid(73695157440630L)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).build();
    private final CostMoles.CostServiceMole costService = spy(new CostMoles.CostServiceMole());
    private final PricingMoles.PricingServiceMole pricingService =
        spy(new PricingMoles.PricingServiceMole());
    private CostMoles.ReservedInstanceBoughtServiceMole riBoughtService;
    private CostMoles.BuyReservedInstanceServiceMole buyRIService;
    private CostMoles.ReservedInstanceSpecServiceMole riSpecService;
    private CostMoles.ReservedInstanceUtilizationCoverageServiceMole riUtilizationCoverageService;
    private EntityUptimeMoles.EntityUptimeServiceMole entityUptimeService;

    /**
     * Prepare mocks that need preparing.
     */
    @Before
    public void setup() {
        riBoughtService = spy(new CostMoles.ReservedInstanceBoughtServiceMole());
        buyRIService = spy(new CostMoles.BuyReservedInstanceServiceMole());
        riSpecService = spy(new CostMoles.ReservedInstanceSpecServiceMole());
        riUtilizationCoverageService =
            spy(new CostMoles.ReservedInstanceUtilizationCoverageServiceMole());
        entityUptimeService = spy(new EntityUptimeMoles.EntityUptimeServiceMole());
    }

    /**
     * An entity is covered by 2 RIs, but they are not in scope.
     * Test that MarketCloudCostDataProvider.filterCouponsCoveredByRi filters out these RIs because
     * they are not in scope.
     */
    @Test
    public void testFilterCouponsCoveredByRi() {
        Map<Long, EntityReservedInstanceCoverage> coverageMap = Maps.newHashMap();
        coverageMap.put(100L, EntityReservedInstanceCoverage.newBuilder().setEntityId(100L)
            .putCouponsCoveredByRi(1L, 16)
            .putCouponsCoveredByRi(2L, 32).build());
        Set<Long> riBoughtIds = Sets.newHashSet();

        Map<Long, EntityReservedInstanceCoverage> filteredCoverageMap =
            MarketCloudCostDataProvider.filterCouponsCoveredByRi(coverageMap, riBoughtIds);

        assertTrue(filteredCoverageMap.get(100L).getCouponsCoveredByRiMap().isEmpty());
    }

    /**
     * This test is focused on testing the MarketCloudCostDataProviderTest::getCloudCostData to ensure
     * that the entityRICoverage that we have retrieved from the cost component is limited to the scope
     * of entities present in the cloudTopology.
     * In this case, the cloud topology contains one VM with an ID 73695157440640.This is also a plan
     * topology. The entity RI coverage contains entry for two entities 73695157440640 and
     * 73695157440641. At the end of this method we have to ensure that the entity RI coverage of
     * 73695157440640 is picked up as its the VM in scope of the cloud topology.
     *
     * @throws CloudCostDataProvider.CloudCostDataRetrievalException Exception of type CloudCostDataRetrievalException.
     * @throws IOException Exception of type IOException.
     */
    @Test
    public void testGetCloudCostData()
                    throws CloudCostDataProvider.CloudCostDataRetrievalException, IOException {

        final TopologyEntityCloudTopology cloudTopology = mock(TopologyEntityCloudTopology.class);
        when(cloudTopology.getAllEntitiesOfType(EntityType.VIRTUAL_MACHINE_VALUE))
            .thenReturn(ImmutableList.of(CLOUD_VM_1, CLOUD_VM_2));
        when(cloudTopology.getEntity(73695157440640L)).thenReturn(Optional.of(CLOUD_VM_1));
        when(cloudTopology.getEntity(73695157440641L)).thenReturn(Optional.empty());

        // RI Bought Info
        final Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo reservedInstanceBoughtInfo =
                        buildRIBoughtInfo(
                                        "aws::ap-northeast-2::RI::6874f611-51c3-4ca0-ac4f-9017628664e1",
                                        2, 706875679016785L, 37.0, 0.0, 0.0, 2.0, 0.0, "t3.nano",
                                        0.004223744292237443, 0.0065);

        Cost.ReservedInstanceBought riBought1 = Cost.ReservedInstanceBought.newBuilder()
                        .setReservedInstanceBoughtInfo(reservedInstanceBoughtInfo).setId(706875669213360L).build();

        Cost.GetReservedInstanceBoughtForAnalysisResponse getReservedInstanceBoughtForAnalysisResponse =
                        Cost.GetReservedInstanceBoughtForAnalysisResponse.newBuilder().addReservedInstanceBought(riBought1).build();
        when(riBoughtService.getReservedInstanceBoughtForAnalysis(Matchers.any())).thenReturn(getReservedInstanceBoughtForAnalysisResponse);

        // Buy RI recommendations
        final Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo riBoughtInfoBuyRI =
                        buildRIBoughtInfo(StringUtils.EMPTY, 1, 706875679016785L, 5454.00048828125,
                                        0.6226027607917786, 0.0, 16.0, 16.0, StringUtils.EMPTY,
                                        0.6226027607917786, 0.6580000519752502);
        Cost.ReservedInstanceBought buyRI1 = Cost.ReservedInstanceBought.newBuilder()
                        .setReservedInstanceBoughtInfo(riBoughtInfoBuyRI).setId(707129005948688L).build();
        Cost.GetBuyReservedInstancesByFilterResponse getBuyReservedInstancesByFilterResponse = Cost.GetBuyReservedInstancesByFilterResponse.newBuilder()
                        .addReservedInstanceBoughts(buyRI1).build();
        when(buyRIService.getBuyReservedInstancesByFilter(Matchers.any())).thenReturn(getBuyReservedInstancesByFilterResponse);

        // RI Specs
        Cost.ReservedInstanceSpecInfo riSpecInfo = buildRISpecInfo(CloudCostDTO.ReservedInstanceType.OfferingClass.STANDARD,
                        CommonCost.PaymentOption.ALL_UPFRONT, 1, CloudCostDTO.Tenancy.DEFAULT,
                        CloudCostDTO.OSType.WINDOWS_WITH_SQL_STANDARD);
        final Cost.ReservedInstanceSpec riSpec =
                        Cost.ReservedInstanceSpec.newBuilder().setId(706875679016785L)
                                        .setReservedInstanceSpecInfo(riSpecInfo).build();
        Cost.GetReservedInstanceSpecByIdsResponse getReservedInstanceSpecByIdsResponse =
                        Cost.GetReservedInstanceSpecByIdsResponse.newBuilder().addReservedInstanceSpec(riSpec).build();
        when(riSpecService.getReservedInstanceSpecByIds(Matchers.any())).thenReturn(getReservedInstanceSpecByIdsResponse);

        //Entity To RI Coverage
        EntityReservedInstanceCoverage entityToRIMapping1 = EntityReservedInstanceCoverage.newBuilder().setEntityId(73695157440640L).setEntityCouponCapacity(8).build();
        EntityReservedInstanceCoverage entityToRIMapping2 = EntityReservedInstanceCoverage.newBuilder().setEntityId(73695157440641L).setEntityCouponCapacity(8).build();
        Cost.GetEntityReservedInstanceCoverageResponse getEntityReservedInstanceCoverageResponse =
                        Cost.GetEntityReservedInstanceCoverageResponse.newBuilder()
                                        .putCoverageByEntityId(73695157440640L, entityToRIMapping1)
                                        .putCoverageByEntityId(73695157440641L, entityToRIMapping2)
                                        .build();
        // set up EntityUptimeDTO
        EntityUptimeDTO expectedUptimeDTO = EntityUptimeDTO.newBuilder()
                                    .setTotalDurationMs(60000)
                                    .setCreationTimeMs(10000)
                                    .setUptimeDurationMs(50000)
                                    .setUptimePercentage(90D).build();
        EntityUptime.GetEntityUptimeByFilterResponse uptimeByFilterResponse =
                EntityUptime.GetEntityUptimeByFilterResponse.newBuilder()
                                    .setDefaultUptime(EntityUptimeDTO.getDefaultInstance())
                                    .putEntityUptimeByOid(CLOUD_VM_1.getOid(), expectedUptimeDTO)
                                    .build();
        when(entityUptimeService.getEntityUptimeByFilter(Matchers.any(GetEntityUptimeByFilterRequest.class)))
                .thenReturn(uptimeByFilterResponse);

        when(riUtilizationCoverageService.getEntityReservedInstanceCoverage(Matchers.any())).thenReturn(getEntityReservedInstanceCoverageResponse);

        GrpcTestServer mockServer = GrpcTestServer.newServer(riBoughtService, pricingService, costService, buyRIService,
                                    riSpecService, riUtilizationCoverageService, entityUptimeService);
        mockServer.start();

        TopologyDTO.TopologyInfo topoInfo = TopologyDTO.TopologyInfo.newBuilder().setTopologyContextId(1000L)
                        .setTopologyType(TopologyDTO.TopologyType.PLAN)
                        .build();

        AccountPricingData<TopologyDTO.TopologyEntityDTO> accountPricingData = new AccountPricingData<>(DiscountApplicator.noDiscount(),
                        Pricing.PriceTable.getDefaultInstance(), 144179052733936L, 15L, 20L);
        MarketPricingResolver marketPricingResolver = mock(MarketPricingResolver.class);
        final ImmutableMap<Long, AccountPricingData<TopologyDTO.TopologyEntityDTO>> accountPricingDataByBusinessAccountOid =
                        ImmutableMap.of(73578741418069L, accountPricingData);
        when(marketPricingResolver.getAccountPricingDataByBusinessAccount(cloudTopology)).thenReturn(accountPricingDataByBusinessAccountOid);

        MarketCloudCostDataProvider marketCloudCostDataProvider = new MarketCloudCostDataProvider(mockServer.getChannel(),
                        discountApplicatorFactory, topologyEntityInfoExtractor);
        CloudCostDataProvider.CloudCostData<TopologyDTO.TopologyEntityDTO> cloudCostData =
                        marketCloudCostDataProvider.getCloudCostData(topoInfo, cloudTopology,
                                        topologyEntityInfoExtractor);

        final Map<Long, EntityReservedInstanceCoverage> currentRiCoverage =
                        cloudCostData.getCurrentRiCoverage();
        assertEquals(1, currentRiCoverage.size());
        assertTrue(currentRiCoverage.containsKey(73695157440640L));
        // verify entity uptime for id with entity uptime
        verifyUptimePercentage(cloudCostData, CLOUD_VM_1.getOid(),
            expectedUptimeDTO.getUptimePercentage());
        verifyUptimePercentage(cloudCostData, CLOUD_VM_2.getOid(),
            EntityUptimeDTO.getDefaultInstance().getUptimePercentage());
        // now check real time
        topoInfo = topoInfo.toBuilder().setTopologyType(TopologyType.REALTIME).build();
        cloudCostData = marketCloudCostDataProvider.getCloudCostData(topoInfo, cloudTopology,
                        topologyEntityInfoExtractor);
        verifyUptimePercentage(cloudCostData, CLOUD_VM_1.getOid(),
            expectedUptimeDTO.getUptimePercentage());
        verifyUptimePercentage(cloudCostData, CLOUD_VM_2.getOid(),
            EntityUptimeDTO.getDefaultInstance().getUptimePercentage());
    }

    /**
     * Test that for a Migrate to Cloud plan entity uptime is always set to 100%.
     *
     * @throws IOException never
     * @throws CloudCostDataRetrievalException never ever
     */
    @Test
    public void testMigrateCloudPlanUptimePercentage() throws IOException, CloudCostDataRetrievalException {
        final TopologyEntityCloudTopology cloudTopology = mock(TopologyEntityCloudTopology.class);
        when(cloudTopology.getAllEntitiesOfType(EntityType.VIRTUAL_MACHINE_VALUE)).thenReturn(ImmutableList.of(CLOUD_VM_1));
        when(cloudTopology.getEntity(CLOUD_VM_1.getOid())).thenReturn(Optional.of(CLOUD_VM_1));

        TopologyDTO.TopologyInfo topoInfo = TopologyDTO.TopologyInfo.newBuilder()
            .setTopologyType(TopologyDTO.TopologyType.PLAN)
            .setPlanInfo(PlanTopologyInfo.newBuilder()
                .setPlanProjectType(PlanProjectType.CLOUD_MIGRATION)
                .setPlanType(PlanProjectType.CLOUD_MIGRATION.toString())
                .setPlanSubType(StringConstants.CLOUD_MIGRATION_PLAN__ALLOCATION))
            .build();
        final EntityUptimeDTO lessThanFullUptime = EntityUptimeDTO.newBuilder()
            .setUptimePercentage(90D).build();
        final EntityUptime.GetEntityUptimeByFilterResponse uptimeByFilterResponse =
            EntityUptime.GetEntityUptimeByFilterResponse.newBuilder()
                .putEntityUptimeByOid(CLOUD_VM_1.getOid(), lessThanFullUptime)
                .build();
        when(entityUptimeService.getEntityUptimeByFilter(Matchers.any(GetEntityUptimeByFilterRequest.class)))
            .thenReturn(uptimeByFilterResponse);
        final GrpcTestServer mockServer = GrpcTestServer.newServer(costService, pricingService,
            riBoughtService, buyRIService, riSpecService, riUtilizationCoverageService,
            entityUptimeService);
        mockServer.start();

        final MarketCloudCostDataProvider marketCloudCostDataProvider =
            new MarketCloudCostDataProvider(mockServer.getChannel(), discountApplicatorFactory,
                topologyEntityInfoExtractor);
        final CloudCostDataProvider.CloudCostData<TopologyDTO.TopologyEntityDTO> result1 =
            marketCloudCostDataProvider.getCloudCostData(topoInfo, cloudTopology,
                topologyEntityInfoExtractor);
        verifyUptimePercentage(result1, CLOUD_VM_1.getOid(), 100d);

        topoInfo = topoInfo.toBuilder().setPlanInfo(PlanTopologyInfo.newBuilder()
            .setPlanProjectType(PlanProjectType.CLOUD_MIGRATION)
            .setPlanType(PlanProjectType.CLOUD_MIGRATION.toString())
            .setPlanSubType(StringConstants.CLOUD_MIGRATION_PLAN__ALLOCATION))
            .build();

        final CloudCostDataProvider.CloudCostData<TopologyDTO.TopologyEntityDTO> result2 =
            marketCloudCostDataProvider.getCloudCostData(topoInfo, cloudTopology,
                topologyEntityInfoExtractor);
        verifyUptimePercentage(result2, CLOUD_VM_1.getOid(), 100d);
    }

    private Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo buildRIBoughtInfo(String probeRIId,
        int numBought, long riSpecId, double fixedCost, double usageCostPerHour,
        double recurringCostPerHour, double numberOfCoupons, double numberOfCouponsUsed,
        String displayName, double amortizedCostPerHour, double onDemandRatePerHour) {
        Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo riBoughtInfo =
            Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.newBuilder().setBusinessAccountId(73556963040147L)
                .setProbeReservedInstanceId(probeRIId)
                .setStartTime(1582646400000L)
                .setNumBought(numBought)
                .setReservedInstanceSpec(riSpecId)
                .setReservedInstanceBoughtCost(
                    Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost
                        .newBuilder()
                        .setFixedCost(getCurrencyAmount(fixedCost))
                        .setUsageCostPerHour(getCurrencyAmount(usageCostPerHour))
                        .setRecurringCostPerHour(getCurrencyAmount(recurringCostPerHour)))
                .setReservedInstanceBoughtCoupons(
                    Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons
                        .newBuilder()
                        .setNumberOfCoupons(numberOfCoupons)
                        .setNumberOfCouponsUsed(numberOfCouponsUsed)
                        .build())
                .setDisplayName(displayName)
                .setReservedInstanceScopeInfo(
                    Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo
                        .newBuilder().setShared(true)
                        .build())
                .setReservedInstanceDerivedCost(
                    Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceDerivedCost
                        .newBuilder()
                        .setAmortizedCostPerHour(getCurrencyAmount(amortizedCostPerHour))
                        .setOnDemandRatePerHour(getCurrencyAmount(onDemandRatePerHour)).build())
                .setEndTime(1612273391000L)
                .build();
        return riBoughtInfo;
    }

    private Cost.ReservedInstanceSpecInfo buildRISpecInfo(CloudCostDTO.ReservedInstanceType.OfferingClass offeringClass,
        CommonCost.PaymentOption paymentOption, int termInYears, CloudCostDTO.Tenancy tenancy,
        CloudCostDTO.OSType osType) {
        Cost.ReservedInstanceSpecInfo riSpecInfo = Cost.ReservedInstanceSpecInfo.newBuilder()
            .setType(CloudCostDTO.ReservedInstanceType.newBuilder()
                .setOfferingClass(offeringClass)
                .setPaymentOption(paymentOption)
                .setTermYears(termInYears)
                .build())
            .setTenancy(tenancy)
            .setOs(osType)
            .setTierId(73556963039728L)
            .setRegionId(73556963039687L)
            .setPlatformFlexible(false)
            .setSizeFlexible(false)
            .build();
        return riSpecInfo;
    }

    private CommonCost.CurrencyAmount getCurrencyAmount(double cost) {
        return CommonCost.CurrencyAmount.newBuilder().setCurrency(123).setAmount(cost).build();
    }

    private void verifyUptimePercentage(CloudCostData data, Long oid, Double expectedUptime) {
        Double actualEntityUptime = data.getEntityUptimePercentage(oid);
        assertEquals(expectedUptime, actualEntityUptime, 0.0001D);
    }
}

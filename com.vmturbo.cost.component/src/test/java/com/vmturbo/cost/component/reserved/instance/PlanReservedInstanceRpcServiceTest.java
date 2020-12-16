package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtCountByTemplateResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceCostStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceCostStat;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Cost.UpdatePlanBuyReservedInstanceCostsRequest;
import com.vmturbo.common.protobuf.cost.Cost.UpdatePlanBuyReservedInstanceCostsResponse;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.entity.cost.PlanProjectedEntityCostStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;

/**
 * Tests for the {@link PlanReservedInstanceRpcService}.
 */
public class PlanReservedInstanceRpcServiceTest {
    private static final long PLAN_ID = 11111L;
    private static final long RI_SPEC_ID = 2222L;
    private static final long REGION1_OID = 789456L;
    private static final long TIER_ID = 3333L;
    private static final long RI_BOUGHT_COUNT = 4L;
    private static final double DELTA = 0.01;

    private PlanReservedInstanceStore planReservedInstanceStore =
                    Mockito.mock(PlanReservedInstanceStore.class);

    private ReservedInstanceSpecStore reservedInstanceSpecStore =
                    Mockito.mock(ReservedInstanceSpecStore.class);

    private BuyReservedInstanceStore buyReservedInstanceStore = mock(BuyReservedInstanceStore.class);

    private PlanProjectedEntityCostStore planProjectedEntityCostStore =
            mock(PlanProjectedEntityCostStore.class);

    private PlanProjectedRICoverageAndUtilStore planProjectedRICoverageAndUtilStore =
            mock(PlanProjectedRICoverageAndUtilStore.class);

    private PlanReservedInstanceRpcService service = new PlanReservedInstanceRpcService(
            planReservedInstanceStore, buyReservedInstanceStore, reservedInstanceSpecStore,
            planProjectedEntityCostStore, planProjectedRICoverageAndUtilStore);

    private static final ReservedInstanceBoughtInfo RI_INFO_1 = ReservedInstanceBoughtInfo.newBuilder()
                    .setBusinessAccountId(123L)
                    .setProbeReservedInstanceId("bar")
                    .setReservedInstanceSpec(101L)
                    .setAvailabilityZoneId(100L)
                    .setNumBought(10)
                    .setReservedInstanceBoughtCost(ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost.newBuilder()
                                    .setFixedCost(CloudCostDTO.CurrencyAmount.newBuilder().setAmount(0))
                                    .setRecurringCostPerHour(CloudCostDTO.CurrencyAmount.newBuilder().setAmount(0.25)))
                    .setDisplayName("t101.small")
                    .build();

    private static final ReservedInstanceBoughtInfo RI_INFO_2 = ReservedInstanceBoughtInfo.newBuilder()
                    .setBusinessAccountId(456L)
                    .setProbeReservedInstanceId("foo")
                    .setReservedInstanceSpec(102L)
                    .setAvailabilityZoneId(100L)
                    .setNumBought(20)
                    .setReservedInstanceBoughtCost(ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost
                                    .newBuilder()
                                    .setFixedCost(CloudCostDTO.CurrencyAmount.newBuilder().setAmount(15))
                                    .setRecurringCostPerHour(CloudCostDTO.CurrencyAmount.newBuilder().setAmount(0.25)))
                    .setDisplayName("t102.large")
                    .build();

    /**
     * Set up a test GRPC server.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(service);

    private PlanReservedInstanceServiceBlockingStub client;

    /**
     * Initialize instances before test.
     */
    @Before
    public void setup() {
        client = PlanReservedInstanceServiceGrpc.newBlockingStub(grpcServer.getChannel());
        Mockito.when(planReservedInstanceStore.getPlanReservedInstanceCountByRISpecIdMap(PLAN_ID))
                        .thenReturn(Collections.singletonMap(RI_SPEC_ID, RI_BOUGHT_COUNT));
        Mockito.when(reservedInstanceSpecStore.getReservedInstanceSpecByIds(any()))
                        .thenReturn(Collections.singletonList(createRiSpec()));
        final Cost.ReservedInstanceCostStat riCostStat = Cost.ReservedInstanceCostStat.newBuilder().setFixedCost(90.0D)
                        .setRecurringCost(0.20D).setAmortizedCost(0.30213D)
                        .setSnapshotTime(Clock.systemUTC().instant().toEpochMilli()).build();
        Mockito.when(planReservedInstanceStore.getPlanReservedInstanceAggregatedCosts(PLAN_ID))
                        .thenReturn(riCostStat);

        final Cost.ReservedInstanceCostStat riBuyCostStat = Cost.ReservedInstanceCostStat.newBuilder().setFixedCost(50.0D)
                        .setRecurringCost(0.10D).setAmortizedCost(0.1057077626D)
                        .setSnapshotTime(Clock.systemUTC().instant().toEpochMilli()).build();
        Mockito.when(buyReservedInstanceStore.queryBuyReservedInstanceCostStats(any()))
                        .thenReturn(Collections.singletonList(riBuyCostStat));

    }

    /**
     * Tests get plan reserved instance bought count by template type.
     */
    @Test
    public void testGetPlanReservedInstanceBoughtCountByTemplateType() {
        final GetPlanReservedInstanceBoughtCountRequest request =
                        GetPlanReservedInstanceBoughtCountRequest.newBuilder().setPlanId(PLAN_ID).build();
        final GetPlanReservedInstanceBoughtCountByTemplateResponse response = client
                        .getPlanReservedInstanceBoughtCountByTemplateType(request);
        Assert.assertNotNull(response);
        final Map<Long, Long> riBoughtCountByTierId = response.getReservedInstanceCountMapMap();
        Assert.assertFalse(riBoughtCountByTierId.isEmpty());
        Assert.assertEquals(Long.valueOf(TIER_ID), riBoughtCountByTierId.keySet().iterator().next());
        Assert.assertEquals(RI_BOUGHT_COUNT, riBoughtCountByTierId.get(TIER_ID), DELTA);
    }

    private static ReservedInstanceSpec createRiSpec() {
        return ReservedInstanceSpec.newBuilder()
                        .setId(RI_SPEC_ID)
                        .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                                        .setRegionId(REGION1_OID)
                                        .setTierId(TIER_ID)
                                        .build())
                        .build();
    }

    /**
     * Tests get plan reserved instance cost stats including buy RIs.
     */
    @Test
    public void testGetPlanReservedInstanceCostStatsWithBuyRI() {
        final GetPlanReservedInstanceCostStatsRequest riCostRequest =
                        GetPlanReservedInstanceCostStatsRequest.newBuilder()
                                        .setPlanId(PLAN_ID)
                                        .setIncludeBuyRi(true)
                                        .build();
        final GetPlanReservedInstanceCostStatsResponse response = client.getPlanReservedInstanceCostStats(riCostRequest);
        final List<ReservedInstanceCostStat> statsList = response.getStatsList();
        Assert.assertNotNull(statsList);
        Assert.assertEquals(2, statsList.size());
        final ReservedInstanceCostStat currentStats = statsList.get(0);
        Assert.assertEquals(90.0, currentStats.getFixedCost(), DELTA);
        Assert.assertEquals(0.2, currentStats.getRecurringCost(), DELTA);
        Assert.assertEquals(0.30213, currentStats.getAmortizedCost(), DELTA);
        final ReservedInstanceCostStat projectedStats = statsList.get(1);
        Assert.assertEquals(140.0, projectedStats.getFixedCost(), DELTA);
        Assert.assertEquals(0.3, projectedStats.getRecurringCost(), DELTA);
        Assert.assertEquals(0.4078, projectedStats.getAmortizedCost(), DELTA);
    }

    /**
     * Tests get plan reserved instance cost stats without buy RIs.
     */
    @Test
    public void testGetPlanReservedInstanceCostStatsWithoutBuyRI() {
        final GetPlanReservedInstanceCostStatsRequest riCostRequest =
                        GetPlanReservedInstanceCostStatsRequest.newBuilder()
                                        .setPlanId(PLAN_ID)
                                        .setIncludeBuyRi(false)
                                        .build();
        final GetPlanReservedInstanceCostStatsResponse response = client.getPlanReservedInstanceCostStats(riCostRequest);
        final List<ReservedInstanceCostStat> statsList = response.getStatsList();
        Assert.assertNotNull(statsList);
        Assert.assertEquals(2, statsList.size());
        final ReservedInstanceCostStat currentStats = statsList.get(0);
        Assert.assertEquals(90.0, currentStats.getFixedCost(), DELTA);
        Assert.assertEquals(0.2, currentStats.getRecurringCost(), DELTA);
        Assert.assertEquals(0.30213, currentStats.getAmortizedCost(), DELTA);
        final ReservedInstanceCostStat projectedStats = statsList.get(1);
        Assert.assertEquals(90.0, projectedStats.getFixedCost(), DELTA);
        Assert.assertEquals(0.2, projectedStats.getRecurringCost(), DELTA);
        Assert.assertEquals(0.30213, projectedStats.getAmortizedCost(), DELTA);
    }

    /**
     * Tests retrieval of RIs/Coupons included in OCP plans.
     */
    @Test
    public void testGetSavedIncludedReservedInstanceBought() {
        final long planId = 1234567L;
        final List<ReservedInstanceBought> planReservedInstanceBought =
                        Arrays.asList(ReservedInstanceBought.newBuilder()
                                      .setReservedInstanceBoughtInfo(RI_INFO_1).build(),
                    ReservedInstanceBought.newBuilder().setReservedInstanceBoughtInfo(RI_INFO_2)
                                    .build());

        // insert the plan RIs to plan data store.
        final UploadRIDataRequest insertRiRequest =
                        UploadRIDataRequest
                                        .newBuilder()
                                        .setTopologyContextId(planId)
                                        .addAllReservedInstanceBought(planReservedInstanceBought)
                                        .build();

        client.insertPlanReservedInstanceBought(insertRiRequest);

        // The added RIs have Region ID == 101L and 102L.
        Set<Long> scopeIds = new HashSet<>();
        scopeIds.add(101L);
        scopeIds.add(102L);
        scopeIds.add(103L);

        // setup what's expected to be returned on going through the plan path.
        when(planReservedInstanceStore.getReservedInstanceBoughtByPlanId(planId))
                    .thenReturn(planReservedInstanceBought);

        // The RIs stored in db by plan id are the plan included RIs/ Coupons (2 in this case).
        // When getSaved is true, the plan RIs are fetched.
        List<ReservedInstanceBought> riBought1 = client
                        .getPlanReservedInstanceBought(GetPlanReservedInstanceBoughtRequest
                                        .newBuilder()
                                        .setPlanId(planId)
                                        .build()).getReservedInstanceBoughtsList();

        assertEquals(2, riBought1.size());
    }

    /**
     * Tests to make sure projected entity costs and RI coverage used coupons are successfully
     * getting updated for MPC plan BuyRI scenario.
     */
    @Test
    public void updatePlanBuyReservedInstanceCosts() {
        final UpdatePlanBuyReservedInstanceCostsRequest request =
                UpdatePlanBuyReservedInstanceCostsRequest.newBuilder().setPlanId(PLAN_ID).build();

        // 1. Check for empty coverage map, should be 0 rows updated.
        final Map<Long, EntityReservedInstanceCoverage> coverageMap = new HashMap<>();
        when(planProjectedRICoverageAndUtilStore.getPlanProjectedRiCoverage(eq(PLAN_ID), any()))
                .thenReturn(coverageMap);
        UpdatePlanBuyReservedInstanceCostsResponse response =
                client.updatePlanBuyReservedInstanceCosts(request);
        assertNotNull(response);
        assertEquals(0, response.getUpdateCount());

        // 2. Check to make sure we return 0 update counts if there are no entity costs.
        long vmId = 1001L;
        long riId = 2001L;
        int riTotalCoupons = 8;
        double riUsedCoupons = 8d;
        coverageMap.put(vmId, EntityReservedInstanceCoverage.newBuilder().setEntityId(vmId)
                .setEntityCouponCapacity(riTotalCoupons)
                .putCouponsCoveredByRi(riId, riUsedCoupons).build());
        final Map<Long, EntityCost> originalEntityCosts = new HashMap<>();
        when(planProjectedEntityCostStore.getPlanProjectedEntityCosts(any(), eq(PLAN_ID)))
                .thenReturn(originalEntityCosts);
        response = client.updatePlanBuyReservedInstanceCosts(request);
        assertNotNull(response);
        assertEquals(0, response.getUpdateCount());

        // 3. Test with real VM costs set.
        double onDemandComputeCost = 0.0054;
        double onDemandLicenseCost = 0.0012;
        final CurrencyAmount computeRate = CurrencyAmount.newBuilder()
                .setAmount(onDemandComputeCost)
                .setCurrency(CurrencyAmount.getDefaultInstance().getCurrency())
                .build();
        final CurrencyAmount licenseRate = CurrencyAmount.newBuilder()
                .setAmount(onDemandLicenseCost)
                .setCurrency(CurrencyAmount.getDefaultInstance().getCurrency())
                .build();
        final CurrencyAmount totalRate = CurrencyAmount.newBuilder()
                .setAmount(onDemandComputeCost + onDemandLicenseCost)
                .setCurrency(CurrencyAmount.getDefaultInstance().getCurrency())
                .build();
        final EntityCost vmCost = EntityCost.newBuilder()
                .setAssociatedEntityId(vmId)
                .setAssociatedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addComponentCost(ComponentCost.newBuilder()
                        .setCategory(CostCategory.ON_DEMAND_COMPUTE)
                        .setCostSource(CostSource.ON_DEMAND_RATE)
                        .setAmount(computeRate))
                .addComponentCost(ComponentCost.newBuilder()
                        .setCategory(CostCategory.ON_DEMAND_LICENSE)
                        .setCostSource(CostSource.ON_DEMAND_RATE)
                        .setAmount(licenseRate))
                .setTotalAmount(totalRate)
                .build();
        originalEntityCosts.put(vmId, vmCost);
        final Map<Long, Double> aggregatedCoverage = new HashMap<>();
        aggregatedCoverage.put(vmId, riUsedCoupons);
        when(planProjectedRICoverageAndUtilStore.getAggregatedEntityRICoverage(any()))
                .thenReturn(aggregatedCoverage);
        when(planProjectedRICoverageAndUtilStore.updatePlanProjectedRiCoverage(eq(PLAN_ID), any()))
                .thenReturn(1);
        when(planProjectedEntityCostStore.updatePlanProjectedEntityCosts(eq(PLAN_ID), any()))
                .thenReturn(1);
        response = client.updatePlanBuyReservedInstanceCosts(request);
        assertNotNull(response);
        assertEquals(1, response.getUpdateCount());
    }
}

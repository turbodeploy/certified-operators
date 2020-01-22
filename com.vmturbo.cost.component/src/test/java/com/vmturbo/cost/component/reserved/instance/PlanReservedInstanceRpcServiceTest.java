package com.vmturbo.cost.component.reserved.instance;

import static org.mockito.Mockito.mock;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtCountByTemplateResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceCostStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceCostStat;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Tests for the {@link PlanReservedInstanceRpcService}.
 */
public class PlanReservedInstanceRpcServiceTest {
    private static final long PLAN_ID = 11111L;
    private static final String RI_NAME = "t1000.medium";
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

    private PlanReservedInstanceRpcService service = new PlanReservedInstanceRpcService(
                    planReservedInstanceStore, buyReservedInstanceStore);

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
                        .thenReturn(Collections.singletonMap(RI_NAME, RI_BOUGHT_COUNT));
        Mockito.when(reservedInstanceSpecStore.getReservedInstanceSpecByIds(Matchers.any()))
                        .thenReturn(Collections.singletonList(createRiSpec()));
        final Cost.ReservedInstanceCostStat riCostStat = Cost.ReservedInstanceCostStat.newBuilder().setFixedCost(90.0D)
                        .setRecurringCost(0.20D).setAmortizedCost(0.30213D)
                        .setSnapshotTime(Clock.systemUTC().instant().toEpochMilli()).build();
        Mockito.when(planReservedInstanceStore.getPlanReservedInstanceAggregatedCosts(PLAN_ID))
                        .thenReturn(riCostStat);

        final Cost.ReservedInstanceCostStat riBuyCostStat = Cost.ReservedInstanceCostStat.newBuilder().setFixedCost(50.0D)
                        .setRecurringCost(0.10D).setAmortizedCost(0.1057077626D)
                        .setSnapshotTime(Clock.systemUTC().instant().toEpochMilli()).build();
        Mockito.when(buyReservedInstanceStore.queryBuyReservedInstanceCostStats(Matchers.any()))
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
        final Map<String, Long> riBoughtCountByTierId = response.getReservedInstanceCountMapMap();
        Assert.assertFalse(riBoughtCountByTierId.isEmpty());
        Assert.assertEquals(RI_NAME, riBoughtCountByTierId.keySet().iterator().next());
        Assert.assertEquals(RI_BOUGHT_COUNT, riBoughtCountByTierId.get(RI_NAME), DELTA);
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
}

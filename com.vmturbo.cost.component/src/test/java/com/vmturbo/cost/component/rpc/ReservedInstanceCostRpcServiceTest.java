package com.vmturbo.cost.component.rpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.util.CollectionUtils;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.ReservedInstanceCostServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.reserved.instance.BuyReservedInstanceStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;

/**
 * Test class for ReservedInstanceCostRpcService.
 */
public class ReservedInstanceCostRpcServiceTest {

    private static final double DELTA = 0.1D;

    // static clock value for ease of testing
    private Clock clock = Clock.fixed(Instant.now(), ZoneId.of("-05:00"));

    private ReservedInstanceBoughtStore reservedInstanceBoughtStore = mock(ReservedInstanceBoughtStore.class);

    private BuyReservedInstanceStore buyReservedInstanceStore = mock(BuyReservedInstanceStore.class);

    private ReservedInstanceCostRpcService reservedInstanceCostRpcService = new ReservedInstanceCostRpcService(reservedInstanceBoughtStore,
                    buyReservedInstanceStore, clock);

    /**
     * Test gRPC server for the rpc service ReservedInstanceCostRpcService.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(reservedInstanceCostRpcService);

    private ReservedInstanceCostServiceGrpc.ReservedInstanceCostServiceBlockingStub client;

    private Cost.ReservedInstanceCostStat boughtRICostStat;

    private Cost.ReservedInstanceCostStat riBuyCostStat;

    /**
     * Setup the test client stub for testing the rpc service. Also mock the db responses for
     * ReservedInstanceBoughtStore and BuyReservedInstanceStore.
     */
    @Before
    public void setup() {
        client = ReservedInstanceCostServiceGrpc.newBlockingStub(grpcServer.getChannel());

        boughtRICostStat = Cost.ReservedInstanceCostStat.newBuilder().setFixedCost(10.0D).setRecurringCost(0.25D).setAmortizedCost(0.2511415525D).setSnapshotTime(clock.instant().toEpochMilli()).build();
        when(reservedInstanceBoughtStore.queryReservedInstanceBoughtCostStats(any())).thenReturn(
                        Collections.singletonList(boughtRICostStat));

        riBuyCostStat = Cost.ReservedInstanceCostStat.newBuilder().setFixedCost(50.0D).setRecurringCost(0.10D).setAmortizedCost(0.1057077626D).setSnapshotTime(clock.instant().toEpochMilli()).build();
        when(buyReservedInstanceStore.queryBuyReservedInstanceCostStats(any())).thenReturn(Collections.singletonList(riBuyCostStat));
    }

    /**
     * Test getReservedInstanceCostStats with QueryLatest: True, IncludeProjected: True, IncludeBuyRI: True,
     * GroupBy SNAPSHOT_TIME.
     */
    @Test
    public void testGetReservedInstanceCostStatsWithLatestAndBuyRIGroupBySnapshotTime() {
        final long projectedSnapshotTime = clock.instant().plus(1, ChronoUnit.HOURS).toEpochMilli();

        final Cost.GetReservedInstanceCostStatsRequest getReservedInstanceCostStatsRequest = Cost.GetReservedInstanceCostStatsRequest.newBuilder().setTimeWindow(
                        Cost.StatsRequestTimeWindow.newBuilder().setQueryLatest(true).build()).setIncludeProjected(true).setIncludeBuyRi(true).setAccountFilter(
                        Cost.AccountFilter.newBuilder().addAccountId(123L).build()).setGroupBy(
                        Cost.GetReservedInstanceCostStatsRequest.GroupBy.SNAPSHOT_TIME).build();

        final Cost.GetReservedInstanceCostStatsResponse reservedInstanceCostStats = client.getReservedInstanceCostStats(getReservedInstanceCostStatsRequest);

        final Cost.ReservedInstanceCostStat expectedCostStat = Cost.ReservedInstanceCostStat.newBuilder()
                        .setFixedCost(60.0D)
                        .setRecurringCost(0.35D)
                        .setAmortizedCost(0.3568493151D)
                        .setSnapshotTime(projectedSnapshotTime).build();

        final List<Cost.ReservedInstanceCostStat> statsList = reservedInstanceCostStats.getStatsList();
        assertFalse(CollectionUtils.isEmpty(statsList));
        assertEquals(2, statsList.size());
        final Cost.ReservedInstanceCostStat currentSnapshotCostStat = statsList.get(0);
        final Cost.ReservedInstanceCostStat projectedSnapshotCostStat = statsList.get(1);

        verifyCostStats(currentSnapshotCostStat, boughtRICostStat);
        verifyCostStats(projectedSnapshotCostStat, expectedCostStat);
    }

    /**
     * Test getReservedInstanceCostStats with QueryLatest: True, IncludeProjected: True, IncludeBuyRI: False,
     * GroupBy SNAPSHOT_TIME.
     */
    @Test
    public void testGetReservedInstanceCostStatsWithLatestAndNoBuyRIGroupBySnapshotTime() {
        final long projectedSnapshotTime = clock.instant().plus(1, ChronoUnit.HOURS).toEpochMilli();

        final Cost.GetReservedInstanceCostStatsRequest getReservedInstanceCostStatsRequest = Cost.GetReservedInstanceCostStatsRequest.newBuilder().setTimeWindow(
                        Cost.StatsRequestTimeWindow.newBuilder().setQueryLatest(true).build()).setIncludeProjected(true).setIncludeBuyRi(false).setAccountFilter(
                        Cost.AccountFilter.newBuilder().addAccountId(123L).build()).setGroupBy(
                        Cost.GetReservedInstanceCostStatsRequest.GroupBy.SNAPSHOT_TIME).build();

        final Cost.GetReservedInstanceCostStatsResponse reservedInstanceCostStats = client.getReservedInstanceCostStats(getReservedInstanceCostStatsRequest);

        final Cost.ReservedInstanceCostStat expectedCostStat = Cost.ReservedInstanceCostStat.newBuilder()
                        .setFixedCost(10.0D)
                        .setRecurringCost(0.25D)
                        .setAmortizedCost(0.2511415525D)
                        .setSnapshotTime(projectedSnapshotTime).build();

        final List<Cost.ReservedInstanceCostStat> statsList = reservedInstanceCostStats.getStatsList();
        assertFalse(CollectionUtils.isEmpty(statsList));
        assertEquals(2, statsList.size());
        final Cost.ReservedInstanceCostStat currentSnapshotCostStat = statsList.get(0);
        final Cost.ReservedInstanceCostStat projectedSnapshotCostStat = statsList.get(1);

        verifyCostStats(currentSnapshotCostStat, boughtRICostStat);
        verifyCostStats(projectedSnapshotCostStat, expectedCostStat);
    }

    /**
     * Test getReservedInstanceCostStats with QueryLatest: True, IncludeProjected: False, IncludeBuyRI: False,
     * GroupBy SNAPSHOT_TIME.
     */
    @Test
    public void testGetReservedInstanceCostStatsWithLatestAndNoProjectedGroupBySnapshotTime() {
        final Cost.GetReservedInstanceCostStatsRequest getReservedInstanceCostStatsRequest = Cost.GetReservedInstanceCostStatsRequest.newBuilder().setTimeWindow(
                        Cost.StatsRequestTimeWindow.newBuilder().setQueryLatest(true).build()).setIncludeProjected(false).setIncludeBuyRi(true).setAccountFilter(
                        Cost.AccountFilter.newBuilder().addAccountId(123L).build()).setGroupBy(
                        Cost.GetReservedInstanceCostStatsRequest.GroupBy.SNAPSHOT_TIME).build();

        final Cost.GetReservedInstanceCostStatsResponse reservedInstanceCostStats = client.getReservedInstanceCostStats(getReservedInstanceCostStatsRequest);

        final List<Cost.ReservedInstanceCostStat> statsList = reservedInstanceCostStats.getStatsList();
        assertFalse(CollectionUtils.isEmpty(statsList));
        assertEquals(1, statsList.size());

        final Cost.ReservedInstanceCostStat currentSnapshotCostStat = statsList.get(0);
        verifyCostStats(currentSnapshotCostStat, boughtRICostStat);
    }

    /**
     * Test getReservedInstanceCostStats with QueryLatest: False, IncludeProjected: True, IncludeBuyRI: True,
     * GroupBy SNAPSHOT_TIME.
     */
    @Test
    public void testGetReservedInstanceCostStatsWithNoLatestAndProjectedGroupBySnapshotTime() {
        final long projectedSnapshotTime = clock.instant().plus(1, ChronoUnit.HOURS).toEpochMilli();

        final Cost.GetReservedInstanceCostStatsRequest getReservedInstanceCostStatsRequest = Cost.GetReservedInstanceCostStatsRequest.newBuilder().setTimeWindow(
                        Cost.StatsRequestTimeWindow.newBuilder().setQueryLatest(false).build()).setIncludeProjected(true).setIncludeBuyRi(true).setAccountFilter(
                        Cost.AccountFilter.newBuilder().addAccountId(123L).build()).setGroupBy(
                        Cost.GetReservedInstanceCostStatsRequest.GroupBy.SNAPSHOT_TIME).build();

        final Cost.GetReservedInstanceCostStatsResponse reservedInstanceCostStats = client.getReservedInstanceCostStats(getReservedInstanceCostStatsRequest);


        final Cost.ReservedInstanceCostStat expectedCostStat = Cost.ReservedInstanceCostStat.newBuilder()
                        .setFixedCost(60.0D)
                        .setRecurringCost(0.35D)
                        .setAmortizedCost(0.3568493151D)
                        .setSnapshotTime(projectedSnapshotTime).build();

        final List<Cost.ReservedInstanceCostStat> statsList = reservedInstanceCostStats.getStatsList();
        assertFalse(CollectionUtils.isEmpty(statsList));
        assertEquals(1, statsList.size());

        final Cost.ReservedInstanceCostStat projectedSnapshotCostStat = statsList.get(0);
        verifyCostStats(projectedSnapshotCostStat, expectedCostStat);
    }

    /**
     * Test getReservedInstanceCostStats with QueryLatest: False, IncludeProjected: False, IncludeBuyRI: True,
     * GroupBy SNAPSHOT_TIME.
     */
    @Test
    public void testGetReservedInstanceCostStatsWithNoLatestAndNoProjectedGroupBySnapshotTime() {
        final Cost.GetReservedInstanceCostStatsRequest getReservedInstanceCostStatsRequest = Cost.GetReservedInstanceCostStatsRequest.newBuilder().setTimeWindow(
                        Cost.StatsRequestTimeWindow.newBuilder().setQueryLatest(false).build()).setIncludeProjected(false).setIncludeBuyRi(true).setAccountFilter(
                        Cost.AccountFilter.newBuilder().addAccountId(123L).build()).setGroupBy(
                        Cost.GetReservedInstanceCostStatsRequest.GroupBy.SNAPSHOT_TIME).build();

        final Cost.GetReservedInstanceCostStatsResponse reservedInstanceCostStats = client.getReservedInstanceCostStats(getReservedInstanceCostStatsRequest);
        final List<Cost.ReservedInstanceCostStat> statsList = reservedInstanceCostStats.getStatsList();
        assertTrue(CollectionUtils.isEmpty(statsList));
    }

    /**
     * Test getReservedInstanceCostStats with QueryLatest: True, IncludeProjected: True, IncludeBuyRI: True,
     * GroupBy NONE.
     */
    @Test
    public void testGetReservedInstanceCostStatsWithLatestAndNoBuyRIGroupByNone() {
        final Cost.GetReservedInstanceCostStatsRequest getReservedInstanceCostStatsRequest = Cost.GetReservedInstanceCostStatsRequest.newBuilder().setTimeWindow(
                        Cost.StatsRequestTimeWindow.newBuilder().setQueryLatest(true).build()).setIncludeProjected(true).setIncludeBuyRi(true).setAccountFilter(
                        Cost.AccountFilter.newBuilder().addAccountId(123L).build()).setGroupBy(
                        Cost.GetReservedInstanceCostStatsRequest.GroupBy.NONE).build();

        final Cost.GetReservedInstanceCostStatsResponse reservedInstanceCostStats = client.getReservedInstanceCostStats(getReservedInstanceCostStatsRequest);
        final List<Cost.ReservedInstanceCostStat> statsList = reservedInstanceCostStats.getStatsList();
        assertEquals(3, statsList.size());
    }

    private void verifyCostStats(Cost.ReservedInstanceCostStat reservedInstanceCostStat, Cost.ReservedInstanceCostStat expectedCostStat) {
        assertEquals(expectedCostStat.getFixedCost(), reservedInstanceCostStat.getFixedCost(), DELTA);
        assertEquals(expectedCostStat.getRecurringCost(), reservedInstanceCostStat.getRecurringCost(), DELTA);
        assertEquals(expectedCostStat.getAmortizedCost(), reservedInstanceCostStat.getAmortizedCost(), DELTA);
        assertEquals(expectedCostStat.getSnapshotTime(), reservedInstanceCostStat.getSnapshotTime(), DELTA);
    }
}

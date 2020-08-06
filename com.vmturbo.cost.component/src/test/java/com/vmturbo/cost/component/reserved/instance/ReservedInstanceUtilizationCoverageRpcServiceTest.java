package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoveredEntitiesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoveredEntitiesResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoveredEntitiesResponse.EntitiesCoveredByReservedInstance;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord.StatValue;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.TimeFrameCalculator;

/**
 * Test the ReservedInstanceUtilizationCoverageRpcService public methods which get coverage
 * statistics.
 */
public class ReservedInstanceUtilizationCoverageRpcServiceTest {

    private static final long REAL_TIME_TOPOLOGY_CONTEXT_ID = 777777L;

    private static final long PLAN_ID = 123456L;

    private ReservedInstanceUtilizationStore reservedInstanceUtilizationStore
        = mock(ReservedInstanceUtilizationStore.class);

    private ReservedInstanceCoverageStore reservedInstanceCoverageStore
        = mock(ReservedInstanceCoverageStore.class);

    private ProjectedRICoverageAndUtilStore projectedRICoverageStore
        = mock(ProjectedRICoverageAndUtilStore.class);

    private EntityReservedInstanceMappingStore entityReservedInstanceMappingStore
            = mock(EntityReservedInstanceMappingStore.class);

    private final AccountRIMappingStore accountRIMappingStore = mock(AccountRIMappingStore.class);

    private PlanProjectedRICoverageAndUtilStore planProjectedRICoverageAndUtilStore = mock(PlanProjectedRICoverageAndUtilStore.class);

    private TimeFrameCalculator timeFrameCalculator = mock(TimeFrameCalculator.class);

    private ReservedInstanceUtilizationCoverageRpcService service =
        new ReservedInstanceUtilizationCoverageRpcService(
               reservedInstanceUtilizationStore,
               reservedInstanceCoverageStore,
               projectedRICoverageStore,
               entityReservedInstanceMappingStore,
               accountRIMappingStore,
               planProjectedRICoverageAndUtilStore,
               timeFrameCalculator,
               REAL_TIME_TOPOLOGY_CONTEXT_ID);

    /**
     * Set up a test GRPC server.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(service);

    private ReservedInstanceUtilizationCoverageServiceBlockingStub client;


    private static final float riCoverageLatestUsedValue = 550;
    private static final float riCoverageLatestCapacity = 1000;
    private long riCoverageLatestSnapshotDate;

    private static final float riCoverageHistoricUsedValue = 230;
    private static final float riCoverageHistoricCapacity = 1000;

    private static final float riUtilizationLatestUsedValue = 550;
    private static final float riUtilizationLatestCapacity = 800;
    private long riUtilizationLatestSnapshotDate;
    private static final float riUtilizationHistoricUsedValue = 230;
    private static final float riUtilizationHistoricCapacity = 800;

    private long now;

    private static final long msInDay = 24 * 60 * 60 * 1000;

    /**
     * Set up the ReservedInstanceUtilizationCoverageRpcService stub for use in the tests.
     */
    @Before
    public void setup() {
        client = ReservedInstanceUtilizationCoverageServiceGrpc
                        .newBlockingStub(grpcServer.getChannel());
        now = System.currentTimeMillis();
        mockRICoverageStore();
        mockRIUtilizationStore();
    }

    private void mockRIUtilizationStore() {
        riUtilizationLatestSnapshotDate = now - 50;
        final long riUtilizationHistoricCoverageSnapshotDate = now - msInDay + 10;
        final ReservedInstanceStatsRecord latestRICoverageStats =
                createRIStatsRecord(riUtilizationLatestCapacity, riUtilizationLatestUsedValue,
                        riUtilizationLatestSnapshotDate);
        final ReservedInstanceStatsRecord historicalRIUtilizationStats =
                createRIStatsRecord(riUtilizationHistoricCapacity, riUtilizationHistoricUsedValue,
                        riUtilizationHistoricCoverageSnapshotDate);

        final List<ReservedInstanceStatsRecord> historicalStatsList = new ArrayList<>();
        historicalStatsList.add(historicalRIUtilizationStats);
        when(reservedInstanceUtilizationStore.getReservedInstanceUtilizationStatsRecords(any()))
                .thenReturn(historicalStatsList)
                .thenReturn(Lists.newArrayList(latestRICoverageStats));

        //set projected to return nothing
        final ReservedInstanceStatsRecord projectedRICoverageStatRecord =
            createRIStatsRecord(0, 0,
                    now + 50);
        when(projectedRICoverageStore.getReservedInstanceCoverageStats(any(), anyBoolean(), anyLong()))
                .thenReturn(Optional.of(projectedRICoverageStatRecord));
    }

    private void mockRICoverageStore() {
        riCoverageLatestSnapshotDate = now - 50;
        final long riCoverageHistoricSnapshotDate = now - msInDay + 10;
        final ReservedInstanceStatsRecord latestRICoverageStats =
                createRIStatsRecord(riCoverageLatestCapacity, riCoverageLatestUsedValue,
                        riCoverageLatestSnapshotDate);
        final ReservedInstanceStatsRecord historicalRICoverageStats =
                createRIStatsRecord(riCoverageHistoricCapacity, riCoverageHistoricUsedValue,
                        riCoverageHistoricSnapshotDate);

        final List<ReservedInstanceStatsRecord> historicalStatsList = new ArrayList<>();
        historicalStatsList.add(historicalRICoverageStats);
        when(reservedInstanceCoverageStore.getReservedInstanceCoverageStatsRecords(any()))
                .thenReturn(historicalStatsList)
                .thenReturn(Lists.newArrayList(latestRICoverageStats));

        //set projected to return nothing
        final ReservedInstanceStatsRecord projectedRIUtilizationStatRecord =
                createRIStatsRecord(0, 0,
                        now + 50);
        when(projectedRICoverageStore.getReservedInstanceUtilizationStats(any(), anyBoolean(), anyLong()))
                .thenReturn(Optional.of(projectedRIUtilizationStatRecord));
    }

    private ReservedInstanceStatsRecord createRIStatsRecord(final float capacity,
                                                            final float used,
                                                            final long snapShotTime) {
        final Cost.ReservedInstanceStatsRecord.Builder statsRecord =
                Cost.ReservedInstanceStatsRecord.newBuilder();
        statsRecord.setCapacity(StatValue.newBuilder()
                .setTotal(capacity)
                .setAvg(capacity)
                .setMax(capacity)
                .setMin(capacity));
        statsRecord.setValues(StatValue.newBuilder()
                .setTotal(used)
                .setAvg(used)
                .setMax(used)
                .setMin(used));
        statsRecord.setSnapshotDate(snapShotTime);
        return statsRecord.build();
    }

    /**
     * Verify that the expected coverage Map is returned when we invoke the
     * getEntityReservedInstanceCoverage. We mock the underlying store's return value and expect
     * that return value to be passed back in the response.
     */
    @Test
    public void testGetEntityReservedInstanceCoverage() {
        final Coverage coverage1 = Coverage.newBuilder().setCoveredCoupons(2).setReservedInstanceId(1).build();
        final Coverage coverage2 = Coverage.newBuilder().setCoveredCoupons(2).setReservedInstanceId(2).build();
        final Map<Long, Set<Coverage>> rICoverageByEntity = ImmutableMap.of(1L, Sets.newHashSet(coverage1, coverage2));

        when(entityReservedInstanceMappingStore.getRICoverageByEntity(any())).thenReturn(rICoverageByEntity);
        when(reservedInstanceCoverageStore.getEntitiesCouponCapacity(any()))
                .thenReturn(ImmutableMap.of(1L, 4d));

        final GetEntityReservedInstanceCoverageResponse response =
            client.getEntityReservedInstanceCoverage(
                GetEntityReservedInstanceCoverageRequest.getDefaultInstance());

        assertEquals(1, response.getCoverageByEntityIdMap().size());
        assertEquals(4, response.getCoverageByEntityIdMap().get(1L).getEntityCouponCapacity());
        assertEquals(2, response.getCoverageByEntityIdMap().get(1L).getCouponsCoveredByRiMap().size());
    }

    /**
     * Test that latest, historical and projected stats are added for RI coverage.
     */
    @Test
    public void testLatestRICoverageStatsAdded() {
        GetReservedInstanceCoverageStatsRequest request =
                GetReservedInstanceCoverageStatsRequest.newBuilder()
                        .setStartDate(now - msInDay)
                        .setEndDate(now + msInDay)
                        .build();
        final GetReservedInstanceCoverageStatsResponse response =
                client.getReservedInstanceCoverageStats(request);
        Assert.assertFalse(response.getReservedInstanceStatsRecordsList().isEmpty());
        final List<ReservedInstanceStatsRecord> riCoverageStats =
                response.getReservedInstanceStatsRecordsList();
        // response should contain 3 entries, historical, latest and projected
        Assert.assertEquals(3, riCoverageStats.size());

        // latest values
        ReservedInstanceStatsRecord latestRecord = riCoverageStats.stream()
                .filter(stat -> stat.getSnapshotDate() == riCoverageLatestSnapshotDate)
                .findAny().orElse(null);
        Assert.assertNotNull(latestRecord);
        Assert.assertEquals(riCoverageLatestCapacity, latestRecord.getCapacity().getMax(), 0);
    }

    /**
     * Test that latest, historical and projected stats are added for RI Utilization.
     */
    @Test
    public void testLatestRIUtilizationStatsAdded() {
        GetReservedInstanceUtilizationStatsRequest request =
                GetReservedInstanceUtilizationStatsRequest.newBuilder()
                        .setStartDate(now - msInDay)
                        .setEndDate(now + msInDay)
                        .build();
        final GetReservedInstanceUtilizationStatsResponse response =
                client.getReservedInstanceUtilizationStats(request);
        Assert.assertFalse(response.getReservedInstanceStatsRecordsList().isEmpty());
        final List<ReservedInstanceStatsRecord> riUtilizationStats =
                response.getReservedInstanceStatsRecordsList();
        // response should contain 3 entries, historical, latest and projected
        Assert.assertEquals(3, riUtilizationStats.size());

        // latest values
        ReservedInstanceStatsRecord latestRecord = riUtilizationStats.stream()
                .filter(stat -> stat.getSnapshotDate() == riUtilizationLatestSnapshotDate)
                .findAny().orElse(null);
        Assert.assertNotNull(latestRecord);
        Assert.assertEquals(riUtilizationLatestCapacity, latestRecord.getCapacity().getMax(), 0);
    }

    /**
     * Test that projected stats are added for Plan RI Utilization.
     */
    @Test
    public void testPlanRIUtilizationStats() {
        final long projectedSnapshotTime = now + 50;
        final ReservedInstanceStatsRecord planProjectedRICoverageStats =
                        createRIStatsRecord(200, 100, projectedSnapshotTime);

        when(planProjectedRICoverageAndUtilStore.getPlanReservedInstanceUtilizationStatsRecords(eq(PLAN_ID), anyList()))
                        .thenReturn(Lists.newArrayList(planProjectedRICoverageStats));
        GetReservedInstanceUtilizationStatsRequest request =
                        GetReservedInstanceUtilizationStatsRequest.newBuilder()
                                        .setTopologyContextId(PLAN_ID)
                                        .build();
        final GetReservedInstanceUtilizationStatsResponse response =
                        client.getReservedInstanceUtilizationStats(request);
        Assert.assertFalse(response.getReservedInstanceStatsRecordsList().isEmpty());
        final List<ReservedInstanceStatsRecord> riUtilizationStats =
                        response.getReservedInstanceStatsRecordsList();
        // response should contain 2 entries: historical and projected
        Assert.assertEquals(2, riUtilizationStats.size());

        // latest values
        ReservedInstanceStatsRecord projectedRecord = riUtilizationStats.stream()
                        .filter(stat -> stat.getSnapshotDate() == projectedSnapshotTime)
                        .findAny().orElse(null);
        Assert.assertNotNull(projectedRecord);
        Assert.assertEquals(200, projectedRecord.getCapacity().getMax(), 0);
        Assert.assertEquals(100, projectedRecord.getValues().getMax(), 0);
    }

    /**
     * Test that projected stats are added for Plan RI Coverage.
     */
    @Test
    public void testPlanRICoverageStats() {
        final long projectedSnapshotTime = now + 50;
        final ReservedInstanceStatsRecord planProjectedRICoverageStats =
                        createRIStatsRecord(10, 6.5f, projectedSnapshotTime);

        when(planProjectedRICoverageAndUtilStore.getPlanReservedInstanceCoverageStatsRecords(eq(PLAN_ID), anyList()))
                        .thenReturn(Lists.newArrayList(planProjectedRICoverageStats));
        GetReservedInstanceCoverageStatsRequest request =
                        GetReservedInstanceCoverageStatsRequest.newBuilder()
                                        .setTopologyContextId(PLAN_ID)
                                        .build();
        final GetReservedInstanceCoverageStatsResponse response =
                        client.getReservedInstanceCoverageStats(request);
        Assert.assertFalse(response.getReservedInstanceStatsRecordsList().isEmpty());
        final List<ReservedInstanceStatsRecord> riCoverageStats =
                        response.getReservedInstanceStatsRecordsList();
        // response should contain 2 entries: historical and projected
        Assert.assertEquals(2, riCoverageStats.size());

        // latest values
        ReservedInstanceStatsRecord projectedRecord = riCoverageStats.stream()
                        .filter(stat -> stat.getSnapshotDate() == projectedSnapshotTime)
                        .findAny().orElse(null);
        Assert.assertNotNull(projectedRecord);
        Assert.assertEquals(10, projectedRecord.getCapacity().getMax(), 0);
        Assert.assertEquals(6.5, projectedRecord.getValues().getMax(), 0);
    }

    /**
     * Test for {@link ReservedInstanceUtilizationCoverageRpcService#getReservedInstanceCoveredEntities}.
     */
    @Test
    public void testGetReservedInstanceCoveredEntities() {
        ImmutableMap.<List<Long>, Map<Long, Set<Long>>>of(Arrays.asList(1L, 2L, 3L),
                ImmutableMap.of(1L, ImmutableSet.of(4L, 5L, 6L)), Collections.emptyList(),
                Collections.emptyMap()).forEach(
                (reservedInstances, reservedInstanceToCoveredEntities) -> Mockito.when(
                        entityReservedInstanceMappingStore.getEntitiesCoveredByReservedInstances(
                                reservedInstances)).thenReturn(reservedInstanceToCoveredEntities));
        ImmutableMap.<List<Long>, Map<Long, Set<Long>>>of(Arrays.asList(1L, 2L, 3L),
                ImmutableMap.of(1L, ImmutableSet.of(40L, 45L, 50L), 2L,
                        ImmutableSet.of(25L, 10L, 15L, 20L)), Collections.emptyList(),
                Collections.emptyMap()).forEach(
                (reservedInstances, reservedInstanceToCoveredEntities) -> Mockito.when(
                        accountRIMappingStore.getUndiscoveredAccountsCoveredByReservedInstances(
                                reservedInstances)).thenReturn(reservedInstanceToCoveredEntities));
        Assert.assertEquals(GetReservedInstanceCoveredEntitiesResponse.getDefaultInstance(),
                client.getReservedInstanceCoveredEntities(
                        GetReservedInstanceCoveredEntitiesRequest.getDefaultInstance()));

        final Map<Long, EntitiesCoveredByReservedInstance> entitiesCoveredByReservedInstancesMap =
                client.getReservedInstanceCoveredEntities(
                        GetReservedInstanceCoveredEntitiesRequest.newBuilder()
                                .addReservedInstanceId(1)
                                .addReservedInstanceId(2)
                                .addReservedInstanceId(3)
                                .build()).getEntitiesCoveredByReservedInstancesMap();
        Assert.assertEquals(ImmutableSet.of(1L, 2L),
                entitiesCoveredByReservedInstancesMap.keySet());
        final EntitiesCoveredByReservedInstance coveredByReservedInstance1 =
                entitiesCoveredByReservedInstancesMap.get(1L);
        Assert.assertEquals(3, coveredByReservedInstance1.getCoveredUndiscoveredAccountIdCount());
        Assert.assertEquals(3, coveredByReservedInstance1.getCoveredEntityIdCount());
        Assert.assertThat(coveredByReservedInstance1.getCoveredEntityIdList(),
                CoreMatchers.hasItems(4L, 5L, 6L));
        Assert.assertThat(coveredByReservedInstance1.getCoveredUndiscoveredAccountIdList(),
                CoreMatchers.hasItems(40L, 45L, 50L));
        final EntitiesCoveredByReservedInstance coveredByReservedInstance2 =
                entitiesCoveredByReservedInstancesMap.get(2L);
        Assert.assertEquals(4, coveredByReservedInstance2.getCoveredUndiscoveredAccountIdCount());
        Assert.assertThat(coveredByReservedInstance2.getCoveredUndiscoveredAccountIdList(),
                CoreMatchers.hasItems(25L, 10L, 15L, 20L));
    }
}

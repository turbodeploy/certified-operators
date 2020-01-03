package com.vmturbo.cost.component.reserved.instance;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord.StatValue;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCoverageFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceUtilizationFilter;

/**
 * Test the ReservedInstanceUtilizationCoverageRpcService public methods which get coverage
 * statistics.
 */
public class ReservedInstanceUtilizationCoverageRpcServiceTest {

    private ReservedInstanceUtilizationStore reservedInstanceUtilizationStore
        = mock(ReservedInstanceUtilizationStore.class);

    private ReservedInstanceCoverageStore reservedInstanceCoverageStore
        = mock(ReservedInstanceCoverageStore.class);

    private ProjectedRICoverageAndUtilStore projectedRICoverageStore
        = mock(ProjectedRICoverageAndUtilStore.class);

    private EntityReservedInstanceMappingStore entityReservedInstanceMappingStore
        = mock(EntityReservedInstanceMappingStore.class);

    private TimeFrameCalculator timeFrameCalculator = mock(TimeFrameCalculator.class);

    private ReservedInstanceUtilizationCoverageRpcService service =
        new ReservedInstanceUtilizationCoverageRpcService(
               reservedInstanceUtilizationStore,
               reservedInstanceCoverageStore,
               projectedRICoverageStore,
               entityReservedInstanceMappingStore,
               timeFrameCalculator);

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
        when(projectedRICoverageStore.getReservedInstanceCoverageStats(any(), anyBoolean()))
                .thenReturn(projectedRICoverageStatRecord);
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
        when(projectedRICoverageStore.getReservedInstanceUtilizationStats(any(), anyBoolean()))
                .thenReturn(projectedRIUtilizationStatRecord);
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
}

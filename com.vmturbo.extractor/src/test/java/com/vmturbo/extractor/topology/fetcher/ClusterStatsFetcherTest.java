package com.vmturbo.extractor.topology.fetcher;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsChunk;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.MetricType;
import com.vmturbo.extractor.schema.tables.Entity;
import com.vmturbo.extractor.schema.tables.Metric;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointTestRule;

/**
 * Tests of the ClusterStatsFetcherTest class.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ExtractorDbConfig.class, ExtractorDbBaseConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = {"enableReporting=true"})
public class ClusterStatsFetcherTest {

    @Autowired
    private ExtractorDbConfig dbConfig;

    private static final long ONE_DAY_IN_MILLIS = TimeUnit.DAYS.toMillis(1);

    private DbEndpoint ingesterEndpoint;
    private DbEndpoint queryEndpoint;
    private final int headroomMaxBackfillingDays = 2;
    private final int headroomCheckIntervalHrs = 24;

    /**
     * Rule to manage configured endpoints for tests.
     */
    @Rule
    @ClassRule
    public static DbEndpointTestRule endpointRule = new DbEndpointTestRule("extractor");

    private static final StatSnapshot STAT_SNAPSHOT = StatSnapshot.newBuilder()
        .setSnapshotDate(Clock.systemUTC().millis())
        .build();

    private StatsHistoryServiceMole statsHistoryServiceSpy = spy(new StatsHistoryServiceMole());

    /** Rule to manage Grpc server. */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(statsHistoryServiceSpy);

    private StatsHistoryServiceBlockingStub historyServiceStub;

    /** set up mocked history service.
     *
     * @throws UnsupportedDialectException if the endpoint is mis-configured
     * @throws SQLException                if there's a problem
     * @throws InterruptedException        if interrupted
     */
    @Before
    public void before() throws InterruptedException, SQLException, UnsupportedDialectException {
        historyServiceStub = StatsHistoryServiceGrpc.newBlockingStub(grpcServer.getChannel());
        this.ingesterEndpoint = dbConfig.ingesterEndpoint();
        this.queryEndpoint = dbConfig.queryEndpoint();
        endpointRule.addEndpoints(ingesterEndpoint, queryEndpoint);
    }

    /**
     * Check that fetching cluster stats works properly.
     */
    @Test
    public void testClusterStatsAreProperlyFetched() {
        final long clusterId1 = 1L;
        final long clusterId2 = 2L;
        final int totalRecordCount = 2;
        // mock internal history gRPC call
        final AtomicReference<List<EntityStats>> gd = new AtomicReference<>();
        final ClusterStatsFetcherFactory factory =
            new ClusterStatsFetcherFactory(historyServiceStub, queryEndpoint, 2, TimeUnit.HOURS);

        final List<ClusterStatsResponse> clusterStatsResponse =
            getClusterStatsResponse(Arrays.asList(clusterId1, clusterId2));

        when(statsHistoryServiceSpy.getClusterStats(anyObject())).thenReturn(clusterStatsResponse);

        Instant tomorrow = Instant.ofEpochMilli(new Date().getTime()).plus(Duration.ofDays(1));
        factory.getClusterStatsFetcher(gd::set, tomorrow.toEpochMilli()).fetchAndConsume();
        List<EntityStats> clusterStats = gd.get();
        assertEquals(totalRecordCount, clusterStats.size());

        final ArgumentCaptor<ClusterStatsRequest> request =
            ArgumentCaptor.forClass(ClusterStatsRequest.class);
        verify(statsHistoryServiceSpy).getClusterStats(request.capture(), any());
        assertEquals(tomorrow.toEpochMilli(), request.getValue().getStats().getEndDate());
        Instant now = Instant.ofEpochMilli(new Date().getTime());
        assertEquals(truncateToDay(now.toEpochMilli()), request.getValue().getStats().getStartDate());
    }

    /**
     * Check that fetching cluster stats works properly.
     * @throws UnsupportedDialectException if the endpoint is mis-configured
     * @throws SQLException                if there's a problem
     * @throws InterruptedException        if interrupted
     */
    @Test
    public void testBackfillingData() throws UnsupportedDialectException, InterruptedException,
        SQLException {
        OffsetDateTime yesterday = OffsetDateTime.now().minus(Duration.ofDays(1));
        final AtomicReference<List<EntityStats>> gd = new AtomicReference<>();
        final long clusterId1 = 1L;
        final List<ClusterStatsResponse> clusterStatsResponse =
            getClusterStatsResponse(Collections.singletonList(clusterId1));
        final ClusterStatsFetcherFactory factory = new ClusterStatsFetcherFactory(historyServiceStub,
            queryEndpoint, headroomCheckIntervalHrs, TimeUnit.HOURS);

        when(statsHistoryServiceSpy.getClusterStats(anyObject())).thenReturn(clusterStatsResponse);

        ingesterEndpoint.dslContext().insertInto(Metric.METRIC, Metric.METRIC.TIME,
            Metric.METRIC.ENTITY_OID, Metric.METRIC.ENTITY_TYPE, Metric.METRIC.TYPE).values(
                    yesterday, clusterId1, EntityType.COMPUTE_CLUSTER, MetricType.CPU_HEADROOM).execute();

        ingesterEndpoint.dslContext().insertInto(Entity.ENTITY, Entity.ENTITY.TYPE,
            Entity.ENTITY.NAME, Entity.ENTITY.OID, Entity.ENTITY.FIRST_SEEN,
            Entity.ENTITY.LAST_SEEN).values(EntityType.VIRTUAL_MACHINE, "cluster", clusterId1, OffsetDateTime.now(),
            OffsetDateTime.now()).execute();

        Instant now = Instant.ofEpochMilli(new Date().getTime());
        factory.getClusterStatsFetcher(gd::set, now.toEpochMilli()).fetchAndConsume();

        final ArgumentCaptor<ClusterStatsRequest> request =
            ArgumentCaptor.forClass(ClusterStatsRequest.class);
        verify(statsHistoryServiceSpy).getClusterStats(request.capture(), any());
        assertEquals(truncateToDay(now.toEpochMilli()),
            request.getValue().getStats().getStartDate());
        assertEquals(now.toEpochMilli(),
            request.getValue().getStats().getEndDate());
    }

    /**
     * Test that if we have older stats than the time range expressed in
     * MAX_BACKFILLING_TIMERANGE we won't try to backfill the data for those timestamps.
     * MAX_BACKFILLING_TIMERANGE.
     *
     * @throws UnsupportedDialectException if the endpoint is mis-configured
     * @throws SQLException                if there's a problem
     * @throws InterruptedException        if interrupted
     */
    @Test
    public void testBackfillingDataExceedBuffer() throws UnsupportedDialectException,
        InterruptedException, SQLException {
        OffsetDateTime nowMinusMaxBackfillingTime =
            OffsetDateTime.now().minus(Duration.ofDays(headroomMaxBackfillingDays));
        final AtomicReference<List<EntityStats>> gd = new AtomicReference<>();
        final long clusterId1 = 1L;
        final List<ClusterStatsResponse> clusterStatsResponse =
            getClusterStatsResponse(Collections.singletonList(clusterId1));
        final ClusterStatsFetcherFactory factory =
            new ClusterStatsFetcherFactory(historyServiceStub, queryEndpoint,
                headroomCheckIntervalHrs, TimeUnit.HOURS);

        when(statsHistoryServiceSpy.getClusterStats(anyObject())).thenReturn(clusterStatsResponse);


        ingesterEndpoint.dslContext().insertInto(Metric.METRIC, Metric.METRIC.TIME,
            Metric.METRIC.ENTITY_OID, Metric.METRIC.ENTITY_TYPE, Metric.METRIC.TYPE).values(
                    nowMinusMaxBackfillingTime, 1L, EntityType.COMPUTE_CLUSTER, MetricType.CPU_HEADROOM).execute();

        ingesterEndpoint.dslContext().insertInto(Entity.ENTITY, Entity.ENTITY.TYPE,
            Entity.ENTITY.NAME, Entity.ENTITY.OID, Entity.ENTITY.FIRST_SEEN,
            Entity.ENTITY.LAST_SEEN).values(EntityType.VIRTUAL_MACHINE, "cluster", clusterId1,
            OffsetDateTime.now(),
            OffsetDateTime.now()).execute();

        Instant now = Instant.ofEpochMilli(new Date().getTime());
        factory.getClusterStatsFetcher(gd::set, now.toEpochMilli()).fetchAndConsume();

        final ArgumentCaptor<ClusterStatsRequest> request =
            ArgumentCaptor.forClass(ClusterStatsRequest.class);
        verify(statsHistoryServiceSpy).getClusterStats(request.capture(), any());
        assertEquals(request.getValue().getStats().getStartDate(),
            truncateToDay(now.toEpochMilli()));
    }

    private List<ClusterStatsResponse> getClusterStatsResponse(List<Long> oids) {
        final List<ClusterStatsResponse> clusterStatsResponse = new ArrayList<>();

        oids.forEach(oid -> clusterStatsResponse.add(ClusterStatsResponse.newBuilder().setSnapshotsChunk(EntityStatsChunk.newBuilder()
            .addSnapshots(EntityStats.newBuilder()
                .setOid(oid)
                .addStatSnapshots(STAT_SNAPSHOT).build())).build()));
        clusterStatsResponse.add(ClusterStatsResponse.newBuilder().setPaginationResponse(PaginationResponse.newBuilder()
            .setTotalRecordCount(oids.size())).build());
        return clusterStatsResponse;
    }

    private static long truncateToDay(long time) {
        return (time / ONE_DAY_IN_MILLIS) * ONE_DAY_IN_MILLIS;
    }
}

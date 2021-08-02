package com.vmturbo.topology.processor.history.movingstats;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import it.unimi.dsi.fastutil.longs.LongSets;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord.ThrottlingCapacityMovingStatistics;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord.ThrottlingCommodityMovingStatistics;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord.ThrottlingMovingStatisticsRecord;

/**
 * Tests for {@link MovingStatisticsPersistenceTask}.
 */
public class MovingStatisticsPersistenceTaskTest extends MovingStatisticsBaseTest {

    private final Clock clock = mock(Clock.class);
    private MovingStatisticsPersistenceTask persistenceTask;

    /**
     * Initializes the tests.
     *
     * @throws IOException if error occurred while creating gRPC server
     */
    @Before
    public void init() throws IOException {
        final StatsHistoryServiceMole history = Mockito.spy(new StatsHistoryServiceMole());
        final GrpcTestServer grpcServer = GrpcTestServer.newServer(history);
        grpcServer.start();
        final StatsHistoryServiceStub statsHistoryClient = StatsHistoryServiceGrpc.newStub(grpcServer.getChannel());
        persistenceTask = new MovingStatisticsPersistenceTask(
                statsHistoryClient, clock, Pair.create(0L, 0L), false,
                movingStatisticsHistoricalEditorConfig(clock,
                        Collections.singletonList(THROTTLING_SAMPLER_CONFIGURATION)));
    }

    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Test successful parse.
     *
     * @throws IOException on parsing exception
     */
    @Test
    public void testParseSuccess() throws IOException {
        final MovingStatisticsRecord record = MovingStatisticsRecord.newBuilder()
            .setEntityOid(1L)
            .setThrottlingRecord(ThrottlingMovingStatisticsRecord.newBuilder()
                .setActiveVcpuCapacity(1.0)
                .addCapacityRecords(ThrottlingCapacityMovingStatistics.newBuilder()
                    .setLastSampleTimestamp(2L)
                    .setSampleCount(50)
                    .setVcpuCapacity(1.0)
                    .setThrottlingStatistics(ThrottlingCommodityMovingStatistics.newBuilder()
                        .setFastMovingAverage(100.0)
                        .setSlowMovingAverage(200.0)
                        .setMaxSample(500.0))
                    .setVcpuStatistics(ThrottlingCommodityMovingStatistics.newBuilder()
                        .setFastMovingAverage(1000.0)
                        .setSlowMovingAverage(2000.0)
                        .setMaxSample(5000.0))
                )).build();
        final MovingStatistics stats = MovingStatistics.newBuilder()
            .addStatisticRecords(record)
            .build();
        final InputStream inputStream = new ByteArrayInputStream(stats.toByteArray());
        final Map<EntityCommodityFieldReference, MovingStatisticsRecord> parseResult =
            persistenceTask.parse(0L, inputStream, LongSets.EMPTY_SET, false);
        final Entry<EntityCommodityFieldReference, MovingStatisticsRecord> result =
            parseResult.entrySet().iterator().next();

        assertEquals(1L, result.getKey().getEntityOid());
        assertEquals(CommodityType.VCPU_VALUE, result.getKey().getCommodityType().getType());

        assertEquals(1L, result.getValue().getEntityOid());
        assertEquals(record, result.getValue());
    }

    /**
     * Test invalid proto parse.
     *
     * @throws IOException on parsing exception
     */
    @Test
    public void testParseFailure() throws IOException {
        expectedException.expect(IOException.class);

        final byte[] invalidData = new byte[] { 2, 5, 7 };
        final InputStream inputStream = new ByteArrayInputStream(invalidData);
        persistenceTask.parse(0L, inputStream, LongSets.EMPTY_SET, false);
    }
}

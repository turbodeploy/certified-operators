package com.vmturbo.topology.processor.history;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;

import it.unimi.dsi.fastutil.longs.LongSets;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.stats.Stats.MovingStatisticsChunk;
import com.vmturbo.common.protobuf.stats.Stats.SetMovingStatisticsResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord.ThrottlingCapacityMovingStatistics;
import com.vmturbo.topology.processor.history.moving.statistics.MovingStatisticsDto.MovingStatistics.MovingStatisticsRecord.ThrottlingMovingStatisticsRecord;

/**
 * Tests for {@link AbstractBlobsPersistenceTask}.
 */
public class AbstractBlobsPersistenceTaskTest {

    private static final int LOADING_CHUNK_SIZE = 1;
    private static final int CALCULATION_CHUNK_SIZE = 256;
    private static final int GRPC_STREAM_TIMEOUT_SECONDS = 120;
    private static final int BLOB_READ_WRITE_CHUNK_SIZE_KB = 120;

    private static final Clock clock = mock(Clock.class);

    private static final MovingStatisticsRecord record = MovingStatisticsRecord.newBuilder()
        .setEntityOid(1L)
        .setThrottlingRecord(ThrottlingMovingStatisticsRecord.newBuilder()
            .setActiveVcpuCapacity(1.0)
            .addCapacityRecords(ThrottlingCapacityMovingStatistics.newBuilder()
                .setLastSampleTimestamp(2L)
                .setSampleCount(50)
                .setVcpuCapacity(1.0)
                .setThrottlingFastMovingAverage(100.0)
                .setThrottlingSlowMovingAverage(200.0)
                .setThrottlingMaxSample(500.0))
            ).build();

    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final TestConfig config = new TestConfig(clock);

    private StatsHistoryServiceStub statsHistoryServiceStub;

    private final StatsHistoryServiceMole statsHistoryServiceMole = Mockito.spy(
            new StatsHistoryServiceMole());

    /**
     * Grpc server for mocking services. The rule handles starting it and cleaning it up.
     */
    @Rule
    public final GrpcTestServer grpcServer = GrpcTestServer.newServer(statsHistoryServiceMole);

    /**
     * Set up test environment.
     *
     * @throws IOException if failed to start grpc server
     */
    @Before
    public void init() throws IOException {
        statsHistoryServiceStub = StatsHistoryServiceGrpc.newStub(grpcServer.getChannel());
    }

    /**
     * Test successful save functionality.
     *
     * @throws HistoryCalculationException on error
     * @throws InterruptedException on error
     */
    @Test
    public void testSaveSuccess() throws HistoryCalculationException, InterruptedException {
        @SuppressWarnings("unchecked")
        final CallStreamObserver<MovingStatisticsChunk> streamObserver =
            (CallStreamObserver<MovingStatisticsChunk>)mock(CallStreamObserver.class);
        when(streamObserver.isReady()).thenReturn(true);

        final BlobPersister persister = new BlobPersister(streamObserver, null, null,
                statsHistoryServiceStub);

        persister.save(MovingStatistics.newBuilder().addStatisticRecords(record).build(),
            0L, config);
        verify(persister.streamObserver, atLeastOnce()).onNext(any(MovingStatisticsChunk.class));
    }

    /**
     * Test that errors that occur during save are correctly propagated.
     *
     * @throws HistoryCalculationException on error
     * @throws InterruptedException on error
     */
    @Test
    public void testErrorDuringSave() throws HistoryCalculationException, InterruptedException {
        @SuppressWarnings("unchecked")
        final CallStreamObserver<MovingStatisticsChunk> streamObserver =
            (CallStreamObserver<MovingStatisticsChunk>)mock(CallStreamObserver.class);
        when(streamObserver.isReady()).thenReturn(true);

        final BlobPersister persister = new BlobPersister(streamObserver, new Exception("My Error"), null, statsHistoryServiceStub);

        expectedException.expect(HistoryCalculationException.class);
        expectedException.expectMessage("Failed in WriterObserver for BlobPersister with start timestamp 0");
        persister.save(MovingStatistics.newBuilder().addStatisticRecords(record).build(),
            0L, config);
    }

    /**
     * Test successful load functionality.
     *
     * @throws HistoryCalculationException on error
     * @throws InterruptedException on error
     */
    @Test
    public void testLoadSuccess() throws HistoryCalculationException, InterruptedException {
        final BlobPersister persister = new BlobPersister(null, null,
            record.toByteString(), statsHistoryServiceStub);
        final Map<EntityCommodityFieldReference, MovingStatisticsRecord> loadedRecords
            = persister.load(Collections.emptyList(), config, LongSets.EMPTY_SET);

        assertEquals(1, loadedRecords.size());
        final Entry<EntityCommodityFieldReference, MovingStatisticsRecord> result
            = loadedRecords.entrySet().iterator().next();

        assertEquals(record.getEntityOid(), result.getKey().getEntityOid());
        assertEquals(record, result.getValue());
    }

    /**
     * Test error occurring during load.
     *
     * @throws HistoryCalculationException on error
     * @throws InterruptedException on error
     */
    @Test
    public void testErrorDuringLoad() throws HistoryCalculationException, InterruptedException {
        final BlobPersister persister = new BlobPersister(null, null,
            ByteString.copyFrom(new byte[] {1, 2, 3}), statsHistoryServiceStub);

        expectedException.expect(HistoryCalculationException.class);
        expectedException.expectMessage("Failed to deserialize BlobPersister blob for 0");
        persister.load(Collections.emptyList(), config, LongSets.EMPTY_SET);
    }

    /**
     * Simple config with mostly hardcoded values for testing.
     */
    private static class TestConfig extends AbstractBlobsHistoricalEditorConfig {
        /**
         * Construct the configuration for history editors.
         *
         * @param clock provides information about current time
         */
        private TestConfig(@Nonnull Clock clock) {
            super(LOADING_CHUNK_SIZE, CALCULATION_CHUNK_SIZE, 777777L, clock,
                BlobPersistingCachingHistoricalEditorTest.createKvConfig("foo"),
                GRPC_STREAM_TIMEOUT_SECONDS, BLOB_READ_WRITE_CHUNK_SIZE_KB);
        }

        @Override
        protected String getDiagnosticsEnabledPropertyName() {
            return "foo";
        }
    }

    /**
     * {@link AbstractBlobsPersistenceTask} implementation for testing.
     */
    private static class BlobPersister
        extends AbstractBlobsPersistenceTask<MovingStatistics, MovingStatisticsRecord,
        MovingStatisticsChunk, SetMovingStatisticsResponse, TestConfig> {

        private final StreamObserver<MovingStatisticsChunk> streamObserver;
        private final Throwable errorToAdd;
        private final ByteString getRequestChunkBytes;

        /**
         * Construct the task to load blobs data for the 'full window' from the persistent store.
         *
         * @param streamObserver StreamObserver to use as result of makeSetRequest call.
         *                       Should be a mock.
         * @param errorToAdd     Error to add to WriterObserver on save. If null, no error
         *                       will be added and instead the observer will be completed.
         * @param getRequestChunkBytes Bytes to insert into chunk for response to getRequest call.
         * @param statsHistoryServiceStub stats history client
         */
        private BlobPersister(@Nullable final StreamObserver<MovingStatisticsChunk> streamObserver,
                @Nullable final Throwable errorToAdd,
                @Nullable final ByteString getRequestChunkBytes,
                final StatsHistoryServiceStub statsHistoryServiceStub) {
            super(statsHistoryServiceStub, clock, Pair.create(0L, 0L), false);
            this.streamObserver = streamObserver;
            this.errorToAdd = errorToAdd;
            this.getRequestChunkBytes = getRequestChunkBytes;
        }

        @Override
        protected void makeGetRequest(@Nonnull StatsHistoryServiceStub statsHistoryClient,
                                      @Nonnull TestConfig config, long startTimestamp,
                                      @Nonnull ReaderObserver observer) {
            MovingStatisticsChunk chunk = MovingStatisticsChunk.newBuilder()
                .setStartTimestamp(0L)
                .setContent(getRequestChunkBytes)
                .build();
            observer.onNext(chunk);
            observer.onCompleted();
        }

        @Override
        protected StreamObserver<MovingStatisticsChunk>
        makeSetRequest(@Nonnull StatsHistoryServiceStub statsHistoryClient, @Nonnull WriterObserver observer) {
            doAnswer(invocation -> {
                if (errorToAdd != null) {
                    observer.onError(errorToAdd);
                } else {
                    observer.onCompleted();
                }
                return null;
            }).when(streamObserver).onCompleted();

            return streamObserver;
        }

        @Nonnull
        @Override
        protected Map<EntityCommodityFieldReference, MovingStatisticsRecord>
        parse(long startTimestamp, @Nonnull InputStream source, @Nonnull Set<Long> oidsToUse,
              boolean enableExpiredOidFiltering) throws IOException {
            final MovingStatisticsRecord rec = MovingStatisticsRecord.parseFrom(source);
            final EntityCommodityFieldReference ref = new EntityCommodityFieldReference(rec.getEntityOid(),
                CommodityType.newBuilder().setType(1).build(), null, CommodityField.USED);

            return ImmutableMap.of(ref, rec);
        }

        @Override
        protected byte[] toByteArray(@Nonnull MovingStatistics stats) {
            return Objects.requireNonNull(stats).toByteArray();
        }

        @Override
        protected int getCount(@Nonnull MovingStatistics stats) {
            return 0;
        }

        @Override
        protected MovingStatisticsChunk newChunk(long startTimestamp, long periodMs,
                                                 @Nonnull ByteString payload) {
            return MovingStatisticsChunk.newBuilder().setStartTimestamp(startTimestamp)
                .setContent(payload).build();
        }

        @Override
        protected ByteString getChunkContent(@Nonnull MovingStatisticsChunk chunk) {
            return chunk.getContent();
        }

        @Override
        protected long getChunkPeriod(@Nonnull MovingStatisticsChunk chunk) {
            return 0;
        }
    }
}

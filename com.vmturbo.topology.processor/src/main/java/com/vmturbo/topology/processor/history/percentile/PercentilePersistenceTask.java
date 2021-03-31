package com.vmturbo.topology.processor.history.percentile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.stats.Stats.GetPercentileCountsRequest;
import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk;
import com.vmturbo.common.protobuf.stats.Stats.SetPercentileCountsResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.Units;
import com.vmturbo.commons.utils.ThrowingFunction;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.topology.processor.history.AbstractStatsLoadingTask;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.InvalidHistoryDataException;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;

/**
 * Persist the percentile data to/from historydb.
 */
public class PercentilePersistenceTask extends
                AbstractStatsLoadingTask<PercentileHistoricalEditorConfig, PercentileRecord> {
    /**
     * Timestamp for TOTAL percentile data in database.
     */
    public static final long TOTAL_TIMESTAMP = 0;
    private static final Logger logger = LogManager.getLogger();
    private static final long waitForChannelReadinessIntervalMs = 1;
    private static final long TOTAL_START_TIMESTAMP = 0L;
    private static final String FAILED_TO_PERCENTILE_DATA =
                    "Failed to %s percentile data for %s start timestamp";
    private static final String PERSIST = "persist";
    private final StatsHistoryServiceStub statsHistoryClient;
    private final long startTimestamp;
    private long lastCheckpointMs;

    /**
     * Construct the task to load percentile data for the 'full window' from the persistent store.
     *
     * @param statsHistoryClient persistent store grpc interface
     * @param range range from start timestamp till end timestamp for which we need
     *                 to request data from DB.
     */
    public PercentilePersistenceTask(@Nonnull StatsHistoryServiceStub statsHistoryClient,
                    @Nonnull Pair<Long, Long> range) {
        this.statsHistoryClient = statsHistoryClient;
        final Long startMs = range.getFirst();
        this.startTimestamp = startMs == null ? TOTAL_START_TIMESTAMP : startMs;
    }

    /**
     * Returns start timestamp for which task has been configured.
     *
     * @return start timestamp from which we are going to get data from DB.
     */
    protected long getStartTimestamp() {
        return startTimestamp;
    }

    /**
     * Getter for {@link PercentilePersistenceTask#lastCheckpointMs} field.
     *
     * @return checkpoint timestamp of last {@link PercentilePersistenceTask#load(Collection, PercentileHistoricalEditorConfig)}
     * or zero if {@link PercentilePersistenceTask#load(Collection, PercentileHistoricalEditorConfig)} wasn't called.
     */
    public long getLastCheckpointMs() {
        return lastCheckpointMs;
    }

    @Override
    public Map<EntityCommodityFieldReference, PercentileRecord>
           load(@Nonnull Collection<EntityCommodityReference> commodities,
                @Nonnull PercentileHistoricalEditorConfig config)
                           throws HistoryCalculationException, InterruptedException {
        ReaderObserver observer = new ReaderObserver(config.getGrpcStreamTimeoutSec());
        statsHistoryClient
                .getPercentileCounts(GetPercentileCountsRequest.newBuilder()
                                .setStartTimestamp(startTimestamp)
                                .setChunkSize((int)(config.getBlobReadWriteChunkSizeKb() * Units.KBYTE))
                                .build(), observer);
        final ByteArrayOutputStream baos = observer.getResult();
        checkRemoteError(observer, "load", startTimestamp);
        try (ByteArrayInputStream source = new ByteArrayInputStream(baos.toByteArray())) {
            final Map<EntityCommodityFieldReference, PercentileRecord> result =
                            parse(startTimestamp, source, PercentileCounts::parseFrom);
            logger.trace("Loaded {} percentile commodity entries for timestamp {}", result.size(),
                            startTimestamp);
            return result;
        } catch (InvalidProtocolBufferException e) {
            throw new InvalidHistoryDataException("Failed to deserialize percentile blob for "
                            + startTimestamp, e);
        } catch (IOException e) {
            throw new HistoryCalculationException(
                            "Failed to read percentile blob for " + startTimestamp, e);
        }
    }

    /**
     * Parses raw bytes to create mapping from field reference to {@link PercentileRecord}
     * instance.
     *
     * @param startTimestamp timestamp for which source loaded
     * @param source data that have been loaded
     * @param parser method that need to be used to create {@link PercentileCounts}
     *                 instance from {@link InputStream}.
     * @return mapping from field reference to appropriate percentile record instance.
     * @throws IOException in case of error while parsing percentile records.
     */
    @Nonnull
    protected static Map<EntityCommodityFieldReference, PercentileRecord> parse(long startTimestamp,
                    @Nonnull InputStream source,
                    @Nonnull ThrowingFunction<InputStream, PercentileCounts, IOException> parser)
                    throws IOException {
        // parse the source
        final Map<EntityCommodityFieldReference, PercentileRecord> result = new HashMap<>();
        final PercentileCounts counts = parser.apply(source);
        if (counts == null) {
            throw new InvalidProtocolBufferException(String.format("Cannot parse '%s' for '%s' timestamp",
                            PercentileCounts.class.getSimpleName(), startTimestamp));
        }
        for (PercentileRecord record : counts.getPercentileRecordsList()) {
            final CommodityType.Builder commTypeBuilder =
                            CommodityType.newBuilder().setType(record.getCommodityType());
            if (record.hasKey()) {
                commTypeBuilder.setKey(record.getKey());
            }
            final Long provider = record.hasProviderOid() ? record.getProviderOid() : null;
            final EntityCommodityFieldReference fieldRef =
                            new EntityCommodityFieldReference(record.getEntityOid(),
                                            commTypeBuilder.build(), provider, CommodityField.USED);
            result.put(fieldRef, record);
        }
        return result;
    }

    /**
     * Store the percentile blob.
     * Percentile is the only editor as of now that will store data explicitly,
     * not using utilizations from the broadcast.
     *
     * @param counts payload
     * @param periodMs the observation window length covered by this data blob
     * @param config configuration settings
     * @throws HistoryCalculationException when failed to persist
     * @throws InterruptedException when interrupted
     *
     * @implNote If <code>counts</code> is empty then no store is performed.
     */
    public void save(@Nonnull PercentileCounts counts,
                     long periodMs,
                     @Nonnull PercentileHistoricalEditorConfig config)
                    throws HistoryCalculationException, InterruptedException {
        final Stopwatch sw = Stopwatch.createStarted();
        WriterObserver observer = new WriterObserver(config.getGrpcStreamTimeoutSec());
        StreamObserver<PercentileChunk> writer = statsHistoryClient.setPercentileCounts(observer);

        byte[] data = counts.toByteArray();
        int chunkSize = (int)(config.getBlobReadWriteChunkSizeKb() * Units.KBYTE);
        for (int i = 0; i <= data.length / chunkSize; ++i) {
            int position = i * chunkSize;
            int size = Math.min(chunkSize, data.length - position);
            ByteString payload = ByteString.copyFrom(data, position, size);
            observer.checkIoAvailability((CallStreamObserver<PercentileChunk>)writer);
            writer.onNext(PercentileChunk.newBuilder().setStartTimestamp(startTimestamp)
                          .setPeriod(periodMs).setContent(payload).build());
            checkRemoteError(observer, PERSIST, startTimestamp);
        }
        writer.onCompleted();
        observer.waitForCompletion();
        checkRemoteError(observer, PERSIST, startTimestamp);

        logger.debug("Saved {} percentile commodity entries for timestamp {} in {}",
                     counts::getPercentileRecordsCount, () -> startTimestamp, sw::toString);
    }

    private static void checkRemoteError(ErrorObserver<?> observer, String type,
                    long startTimestamp) throws HistoryCalculationException, InterruptedException {
        final Throwable error = observer.getError();
        if (error != null) {
            throw new HistoryCalculationException(
                            String.format(FAILED_TO_PERCENTILE_DATA, type, startTimestamp), error);
        }
    }

    /**
     * Grpc stream observer that retains last occurred exception.
     *
     * @param <T> stream data type
     */
    private abstract class ErrorObserver<T> implements StreamObserver<T> {
        private final CountDownLatch cond = new CountDownLatch(1);
        protected final long timeoutSec;
        protected final long startMs;
        protected Throwable error;
        protected int chunkNo;

        /**
         * Construct the observer instance.
         *
         * @param timeoutSec timeout to wait for operations
         */
        ErrorObserver(long timeoutSec) {
            this.timeoutSec = timeoutSec;
            this.startMs = System.currentTimeMillis();
        }

        @Override
        public void onError(Throwable throwable) {
            this.error = throwable;
            cond.countDown();
        }

        @Override
        public void onCompleted() {
            cond.countDown();
        }

        /**
         * Get the error that occurred during sending or receiving data, if any.
         *
         * @return null if no error
         * @throws InterruptedException when interrupted
         * @throws HistoryCalculationException when timed out
         */
        public Throwable getError() throws HistoryCalculationException, InterruptedException {
            return error;
        }

        /**
         * Awaits for command completion.
         *
         * @throws InterruptedException when interrupted
         * @throws HistoryCalculationException when timed out
         */
        public void waitForCompletion() throws InterruptedException, HistoryCalculationException {
            if (!cond.await(timeoutSec, TimeUnit.SECONDS)) {
                throw new HistoryCalculationException("Timed out reading percentile data");
            }
        }

        /**
         * Test whether we can proceed with stream IO operation, optionally wait for buffer
         * availability if call observer is specified, but no more than given timeout.
         *
         * @param callObserver call observer, null for reading
         * @throws HistoryCalculationException when general timeout occurs
         * @throws InterruptedException when interrupted
         */
        public void checkIoAvailability(@Nullable CallStreamObserver<PercentileChunk> callObserver)
                        throws HistoryCalculationException, InterruptedException {
            if (Thread.interrupted()) {
                throw new InterruptedException("Persisting percentile data was interrupted");
            }
            if (callObserver != null) {
                while (!callObserver.isReady()) {
                    Thread.sleep(waitForChannelReadinessIntervalMs);
                    checkTimeout();
                }
            } else {
                checkTimeout();
            }
        }

        private void checkTimeout() throws HistoryCalculationException {
            if (System.currentTimeMillis() - startMs > TimeUnit.SECONDS.toMillis(timeoutSec)) {
                throw new HistoryCalculationException("Percentile I/O operation timeout exceeded");
            }
        }
    }

    /**
     * Notifications handler for sending up percentile data stream.
     */
    private class WriterObserver extends ErrorObserver<SetPercentileCountsResponse> {
        /**
         * Construct the observer instance.
         *
         * @param timeoutSec timeout to wait for operations
         */
        WriterObserver(long timeoutSec) {
            super(timeoutSec);
        }


        @Override
        public void onNext(SetPercentileCountsResponse value) {
            if (logger.isTraceEnabled()) {
                logger.trace("Sent percentile blob chunk {} for timestamp {}", chunkNo++,
                             startTimestamp);
            }
        }
    }

    /**
     * Notifications handler for receiving the percentile data stream.
     */
    @VisibleForTesting
    class ReaderObserver extends ErrorObserver<PercentileChunk> {
        private final ByteArrayOutputStream result = new ByteArrayOutputStream();

        /**
         * Construct the observer instance.
         *
         * @param timeoutSec timeout to wait for operations
         */
        ReaderObserver(long timeoutSec) {
            super(timeoutSec);
        }

        /**
         * Get the received data.
         *
         * @return received byte array
         * @throws InterruptedException when interrupted
         * @throws HistoryCalculationException when timed out
         */
        @Nonnull
        public ByteArrayOutputStream getResult() throws InterruptedException, HistoryCalculationException {
            waitForCompletion();
            return result;
        }

        @Override
        public void onNext(PercentileChunk chunk) {
            try {
                checkIoAvailability(null);
                chunk.getContent().writeTo(result);
                lastCheckpointMs = chunk.getPeriod();
                if (logger.isTraceEnabled()) {
                    logger.trace("Received percentile blob chunk {} for timestamp {}", chunkNo++,
                                 startTimestamp);
                }
            } catch (IOException | HistoryCalculationException | InterruptedException e) {
                error = e;
            }
        }
    }
}

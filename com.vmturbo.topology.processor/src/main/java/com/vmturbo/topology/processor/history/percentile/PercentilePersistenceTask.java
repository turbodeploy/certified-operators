package com.vmturbo.topology.processor.history.percentile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.topology.processor.history.AbstractStatsLoadingTask;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
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
        // parse the data
        Map<EntityCommodityFieldReference, PercentileRecord> result = new HashMap<>();
        try {
            ByteArrayOutputStream baos = observer.getResult();
            if (observer.getError() != null) {
                throw new HistoryCalculationException("Failed to load percentile data for " + startTimestamp,
                                                      observer.getError());
            }
            PercentileCounts counts = PercentileCounts.parseFrom(baos.toByteArray());
            for (PercentileRecord record : counts.getPercentileRecordsList()) {
                CommodityType.Builder commTypeBuilder = CommodityType.newBuilder()
                                .setType(record.getCommodityType());
                if (record.hasKey()) {
                    commTypeBuilder.setKey(record.getKey());
                }
                Long provider = record.hasProviderOid() ? record.getProviderOid() : null;
                EntityCommodityFieldReference fieldRef =
                       new EntityCommodityFieldReference(record.getEntityOid(),
                                                         commTypeBuilder.build(),
                                                         provider,
                                                         CommodityField.USED);
                result.put(fieldRef, record);
            }
        } catch (InvalidProtocolBufferException e) {
            throw new HistoryCalculationException("Failed to deserialize percentile blob for " + startTimestamp);
        }
        logger.debug("Loaded {} percentile commodity entries for timestamp {}", result.size(),
                     startTimestamp);
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
        if (counts.getPercentileRecordsList().isEmpty()) {
            logger.debug("There is no percentile commodity entries to save for timestamp {}. Skipping actual write",
                         startTimestamp);
            return;
        }

        Stopwatch sw = Stopwatch.createStarted();
        WriterObserver observer = new WriterObserver(config.getGrpcStreamTimeoutSec());
        StreamObserver<PercentileChunk> writer = statsHistoryClient.setPercentileCounts(observer);

        byte[] data = counts.toByteArray();
        int chunkSize = (int)(config.getBlobReadWriteChunkSizeKb() * Units.KBYTE);
        for (int i = 0; i <= data.length / chunkSize; ++i) {
            int position = i * chunkSize;
            int size = Math.min(chunkSize, data.length - position);
            if (size <= 0) {
                break;
            }
            ByteString payload = ByteString.copyFrom(data, position, size);
            observer.checkIoAvailability((CallStreamObserver<PercentileChunk>)writer);
            writer.onNext(PercentileChunk.newBuilder().setStartTimestamp(startTimestamp)
                          .setPeriod(periodMs).setContent(payload).build());
            if (observer.getError() != null) {
                throw new HistoryCalculationException("Failed to persist percentile data",
                                                      observer.getError());
            }
        }
        writer.onCompleted();

        logger.debug("Saved {} percentile commodity entries for timestamp {} in {}",
                     counts::getPercentileRecordsCount, () -> startTimestamp, sw::toString);
    }


    /**
     * Grpc stream observer that retains last occurred exception.
     *
     * @param <T> stream data type
     */
    private abstract class ErrorObserver<T> implements StreamObserver<T> {
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
        }

        /**
         * Get the error that occurred during sending or receiving data, if any.
         *
         * @return null if no error
         */
        public Throwable getError() {
            return error;
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

        @Override
        public void onCompleted() {}
    }

    /**
     * Notifications handler for receiving the percentile data stream.
     */
    @VisibleForTesting
    class ReaderObserver extends ErrorObserver<PercentileChunk> {
        private final ByteArrayOutputStream result = new ByteArrayOutputStream();
        private final CountDownLatch cond = new CountDownLatch(1);

        /**
         * Construct the observer instance.
         *
         * @param timeoutSec timeout to wait for operations
         */
        ReaderObserver(long timeoutSec) {
            super(timeoutSec);
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

        @Override
        public void onError(Throwable throwable) {
            super.onError(throwable);
            cond.countDown();
        }

        @Override
        public void onCompleted() {
            cond.countDown();
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
            if (cond.await(timeoutSec, TimeUnit.SECONDS)) {
                return result;
            } else {
                throw new HistoryCalculationException("Timed out reading percentile data");
            }
        }
    }
}

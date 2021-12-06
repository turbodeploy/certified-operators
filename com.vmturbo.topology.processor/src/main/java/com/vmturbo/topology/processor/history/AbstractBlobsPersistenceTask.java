package com.vmturbo.topology.processor.history;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.commons.Units;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.topology.processor.history.exceptions.HistoryCalculationException;
import com.vmturbo.topology.processor.history.exceptions.HistoryPersistenceException;
import com.vmturbo.topology.processor.history.exceptions.InvalidHistoryDataException;

/**
 * Persist the blobs data to/from historydb.
 *
 * @param <DataT> per-commodity field historical data to cache that wraps DbValue with runtime info
 * @param <DbRecordT> database record type
 * @param <ChunkT> streaming chunk type
 * @param <ResponseT> get stats response type
 * @param <ConfigT> config type
 */
public abstract class AbstractBlobsPersistenceTask<DataT, DbRecordT, ChunkT, ResponseT,
        ConfigT extends AbstractBlobsHistoricalEditorConfig>
        extends AbstractStatsLoadingTask<ConfigT, DbRecordT> {
    /**
     * Timestamp for TOTAL blobs data in database.
     */
    public static final long TOTAL_TIMESTAMP = 0;
    private static final Logger logger = LogManager.getLogger();
    private static final String FAILED_MESSAGE = "Failed in %s for %s with start timestamp %s";
    private final StatsHistoryServiceStub statsHistoryClient;
    private final long startTimestamp;
    private final Clock clock;
    private long lastCheckpointMs;
    private boolean enableOidFiltering;

    /**
     * Construct the task to load blobs data for the 'full window' from the persistent store.
     * @param statsHistoryClient persistent store grpc interface
     * @param clock Clock for timing.
     * @param range range from start timestamp till end timestamp for which we need
     * @param enableExpiredOidFiltering feature flag to filter out expired oids
     */
    public AbstractBlobsPersistenceTask(@Nonnull StatsHistoryServiceStub statsHistoryClient,
                                        @Nonnull Clock clock,
                                        @Nonnull Pair<Long, Long> range, final boolean enableExpiredOidFiltering) {
        this.statsHistoryClient = Objects.requireNonNull(statsHistoryClient);
        final Long startMs = Objects.requireNonNull(range).getFirst();
        this.startTimestamp = startMs == null ? TOTAL_TIMESTAMP : startMs;
        this.enableOidFiltering = enableExpiredOidFiltering;
        this.clock = clock;
    }

    /**
     * Returns start timestamp for which task has been configured.
     *
     * @return start timestamp from which we are going to get data from DB.
     */
    public long getStartTimestamp() {
        return startTimestamp;
    }

    /**
     * Getter for {@link AbstractBlobsPersistenceTask#lastCheckpointMs} field.
     *
     * @return checkpoint timestamp of last {@link IHistoryLoadingTask#load(Collection, Object, Set)}
     * or zero if {@link IHistoryLoadingTask#load(Collection, Object, Set)} wasn't called.
     */
    public long getLastCheckpointMs() {
        return lastCheckpointMs;
    }

    @Override
    public Map<EntityCommodityFieldReference, DbRecordT> load(
            @Nonnull Collection<EntityCommodityReference> commodities,
            @Nonnull ConfigT config, @Nonnull final Set<Long> oidsToUse)
            throws HistoryCalculationException, InterruptedException {
        ReaderObserver observer = new ReaderObserver(clock, config.getGrpcStreamTimeoutSec());
        makeGetRequest(statsHistoryClient, config, startTimestamp, observer);
        try (ByteArrayOutputStream baos = observer.getResult()) {
            observer.checkRemoteError(getClass().getSimpleName(), startTimestamp);
            try (ByteArrayInputStream source = new ByteArrayInputStream(baos.toByteArray())) {
                final Map<EntityCommodityFieldReference, DbRecordT> result =
                    parse(startTimestamp, source, oidsToUse, this.enableOidFiltering);
                logger.trace("Loaded {} {}} commodity entries for timestamp {}", result.size(),
                    getClass().getSimpleName(), startTimestamp);
                return result;
            } catch (InvalidProtocolBufferException e) {
                throw new InvalidHistoryDataException("Failed to deserialize " + getClass().getSimpleName()
                    + " blob for " + startTimestamp, e);
            }
        } catch (IOException e) {
            throw new HistoryPersistenceException(
                "Failed to read " + getClass().getSimpleName() + " blob for " + startTimestamp, e);
        }
    }

    /**
     * Store the blob data.
     * Blob data are stored explicitly, not using utilization's from the broadcast.
     *
     * @param stats payload
     * @param periodMs the observation window length covered by this data blob
     * @param config configuration settings
     * @throws HistoryCalculationException when failed to persist
     * @throws InterruptedException when interrupted
     *
     * @implNote If <code>counts</code> is empty then no store is performed.
     */
    public void save(@Nonnull DataT stats, long periodMs, @Nonnull ConfigT config)
            throws HistoryCalculationException, InterruptedException {
        final Stopwatch sw = Stopwatch.createStarted();
        WriterObserver observer = new WriterObserver(clock, config.getGrpcStreamTimeoutSec());
        StreamObserver<ChunkT> writer = makeSetRequest(statsHistoryClient, observer);

        byte[] data = toByteArray(stats);
        int chunkSize = (int)(config.getBlobReadWriteChunkSizeKb() * Units.KBYTE);
        for (int i = 0; i <= data.length / chunkSize; ++i) {
            int position = i * chunkSize;
            int size = Math.min(chunkSize, data.length - position);
            ByteString payload = ByteString.copyFrom(data, position, size);
            observer.checkIoAvailability((CallStreamObserver<ChunkT>)writer);
            writer.onNext(newChunk(startTimestamp, periodMs, payload));
            observer.checkRemoteError(getClass().getSimpleName(), startTimestamp);
        }
        writer.onCompleted();
        observer.waitForCompletion();
        observer.checkRemoteError(getClass().getSimpleName(), startTimestamp);

        logger.debug("Saved {} {} commodity entries for timestamp {} in {}",
            () -> getCount(stats), () -> getClass().getSimpleName(),
            () -> startTimestamp, sw::toString);
    }

    /**
     * Parse a map of {@link EntityCommodityFieldReference} to {@code DbValue}s from
     * a source input stream.
     *
     * @param startTimestamp The startTimestamp associated with the blob being parsed from the InputStream.
     * @param source The InputStream source from which to read and parse the blob.
     * @param parser The parser to use to parse the records.
     * @param oidsToUse The oids containing live entities. Records for entities not in this list will be dropped
     *                  if enableExpiredOidFiltering is enabled.
     * @param recordToOidFunc Function to get the entity OID from the DBValue record.
     * @param recordToRefFunc Function that generates a {@link EntityCommodityFieldReference} from a DBValue record.
     * @param enableExpiredOidFiltering Whether to filter entities that are not live.
     * @param <DbValueT> The individual database record type (ie PercentileRecord).
     * @return mapping from field reference to appropriate DbValue record instance.
     * @throws IOException in case of error while parsing the protobuf blob.
     */
    protected static <DbValueT>
    Map<EntityCommodityFieldReference, DbValueT> parseDbRecords(long startTimestamp,
                                                                @Nonnull InputStream source,
                                                                @Nonnull com.vmturbo.commons.utils.ThrowingFunction<InputStream, Iterable<DbValueT>, IOException> parser,
                                                                @Nullable Set<Long> oidsToUse,
                                                                @Nonnull Function<DbValueT, Long> recordToOidFunc,
                                                                @Nonnull Function<DbValueT, EntityCommodityFieldReference> recordToRefFunc,
                                                                boolean enableExpiredOidFiltering) throws IOException {
        // parse the source
        final Map<EntityCommodityFieldReference, DbValueT> result = new HashMap<>();
        final Iterable<DbValueT> records = parser.apply(source);
        int filteredOidsCount = 0;
        for (DbValueT record : records) {
            //TODO: Remove enableExpiredOidFiltering flag once OM-69698 has been tested in staging
            if (enableExpiredOidFiltering && oidsToUse != null && !oidsToUse.contains(recordToOidFunc.apply(record))) {
                filteredOidsCount += 1;
                continue;
            }

            final EntityCommodityFieldReference fieldRef = recordToRefFunc.apply(record);
            result.put(fieldRef, record);

        }
        Level level = filteredOidsCount > 0 ? Level.INFO : Level.DEBUG;
        logger.log(level, "{} expired oids were filtered out from blob with timestamp {}",
            filteredOidsCount, startTimestamp);
        return result;
    }

    /**
     * Makes a get request to retrieve the stats from the history component.
     *
     * @param statsHistoryClient persistent store grpc interface
     * @param config the associated editor configuration
     * @param startTimestamp the start timestamp of the request
     * @param observer the {@link ReaderObserver} to process the read data
     */
    protected abstract void makeGetRequest(@Nonnull StatsHistoryServiceStub statsHistoryClient,
            @Nonnull ConfigT config, long startTimestamp, @Nonnull ReaderObserver observer);

    /**
     * Makes a set request to the history component to store the stats.
     *
     * @param statsHistoryClient persistent store grpc interface
     * @param observer the {@link WriterObserver} for streaming error handling and debug logging
     * @return a {@link StreamObserver} to write data onto
     */
    protected abstract StreamObserver<ChunkT> makeSetRequest(
            @Nonnull StatsHistoryServiceStub statsHistoryClient, @Nonnull WriterObserver observer);

    /**
     * Parses raw bytes to create mapping from field reference to {@link DbRecordT} instance.
     *
     * @param startTimestamp timestamp for which source loaded
     * @param source data that have been loaded
     * @param oidsToUse non expired oids that should be used
     * @param enableExpiredOidFiltering feature flag to filter out expired oids
     * @return mapping from field reference to appropriate blobs record instance.
     * @throws IOException in case of error while parsing blobs records.
     */
    @Nonnull
    protected abstract Map<EntityCommodityFieldReference, DbRecordT> parse(long startTimestamp,
            @Nonnull InputStream source, @Nonnull Set<Long> oidsToUse,
            boolean enableExpiredOidFiltering) throws IOException;

    /**
     * Converts the given stats object to a byte array.
     *
     * @param stats the given stats data
     * @return the corresponding byte array
     */
    protected abstract byte[] toByteArray(@Nonnull DataT stats);

    /**
     * Returns the count of the stats.
     *
     * @param stats the given stats data
     * @return the count of the stats
     */
    protected abstract int getCount(@Nonnull DataT stats);

    /**
     * Constructs a new chunk based on the given parameters.
     *
     * @param startTimestamp the start timestamp of the chunk
     * @param periodMs the period of the chunk in milliseconds
     * @param payload the payload of the chunk
     * @return the constructed chunk
     */
    protected abstract ChunkT newChunk(long startTimestamp, long periodMs, @Nonnull ByteString payload);

    /**
     * Returns the content of the given chunk.
     *
     * @param chunk the chunk
     * @return the content of the chunk
     */
    protected abstract ByteString getChunkContent(@Nonnull ChunkT chunk);

    /**
     * Returns the period of the chunk.
     *
     * @param chunk the chunk
     * @return the period of the chunk
     */
    protected abstract long getChunkPeriod(@Nonnull ChunkT chunk);

    /**
     * Notifications handler for sending up blobs data stream.
     */
    protected class WriterObserver extends ErrorObserver<ResponseT> {
        private static final long waitForChannelReadinessIntervalMs = 1;

        /**
         * Construct the observer instance.
         *
         * @param clock The clock for timing
         * @param timeoutSec timeout to wait for operations
         */
        WriterObserver(@Nonnull Clock clock, long timeoutSec) {
            super(clock, timeoutSec);
        }

        @Override
        public void onNext(ResponseT value) {
            if (logger.isTraceEnabled()) {
                logger.trace("Sent {} blob chunk {} for timestamp {}", getClass().getSimpleName(), chunkNo++,
                             startTimestamp);
            }
        }

        /**
         * Test whether we can proceed with stream IO operation, optionally wait for buffer
         * availability if call observer is specified, but no more than given timeout.
         *
         * @param callObserver call observer, null for reading
         * @throws HistoryPersistenceException when general timeout occurs
         * @throws InterruptedException when interrupted
         */
        protected void checkIoAvailability(CallStreamObserver<?> callObserver)
                throws HistoryPersistenceException, InterruptedException {
            if (callObserver == null) {
                logger.warn("Call observer is null; skip checking the IO availability. "
                        + "But this shouldn't happen so we should investigate why.");
                return;
            }

            if (Thread.interrupted()) {
                throw new InterruptedException("Persisting blobs data on the history component was interrupted");
            }
            while (!callObserver.isReady()) {
                checkRemoteError(callObserver.getClass().getSimpleName(), startTimestamp);
                Thread.sleep(waitForChannelReadinessIntervalMs);
                checkTimeout();
            }
        }
    }

    /**
     * Notifications handler for receiving the blobs data stream.
     */
    protected class ReaderObserver extends ErrorObserver<ChunkT> {
        private final ByteArrayOutputStream result = new ByteArrayOutputStream();

        /**
         * Construct the observer instance.
         *
         * @param clock The clock for timing
         * @param timeoutSec timeout to wait for operations
         */
        ReaderObserver(@Nonnull Clock clock, long timeoutSec) {
            super(clock, timeoutSec);
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
        public void onNext(ChunkT chunk) {
            try {
                if (Thread.interrupted()) {
                    throw new InterruptedException("Reading blobs data on the history component was interrupted");
                }
                checkTimeout();
                getChunkContent(chunk).writeTo(result);
                lastCheckpointMs = getChunkPeriod(chunk);
                if (logger.isTraceEnabled()) {
                    logger.trace("Received {} blob chunk {} for timestamp {}", getClass().getSimpleName(),
                            chunkNo++, startTimestamp);
                }
            } catch (IOException | HistoryCalculationException | InterruptedException e) {
                error = e;
            }
        }
    }

    /**
     * Grpc stream observer that retains last occurred exception.
     *
     * @param <T> stream data type
     */
    private abstract static class ErrorObserver<T> implements StreamObserver<T> {
        private final CountDownLatch cond = new CountDownLatch(1);
        private final long timeoutSec;
        private final long startMs;
        private final Clock clock;
        protected Throwable error;
        protected int chunkNo;

        /**
         * Construct the observer instance.
         *
         * @param clock The clock for timing
         * @param timeoutSec timeout to wait for operations
         */
        ErrorObserver(@Nonnull Clock clock,  long timeoutSec) {
            this.timeoutSec = timeoutSec;
            this.startMs = clock.millis();
            this.clock = clock;
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
         * Awaits for command completion.
         *
         * @throws InterruptedException when interrupted
         * @throws HistoryCalculationException when timed out
         */
        protected void waitForCompletion() throws InterruptedException, HistoryCalculationException {
            if (!cond.await(timeoutSec, TimeUnit.SECONDS)) {
                throw new HistoryCalculationException("Timed out reading blobs data");
            }
        }

        /**
         * Checks the remote error and if present throws an {@link HistoryCalculationException}.
         *
         * @param statsRelatedClassName the name of the stats class for logging purpose
         * @param startTimestamp the start timestamp for logging purpose
         * @throws HistoryPersistenceException if such an error is encountered
         */
        protected void checkRemoteError(String statsRelatedClassName, long startTimestamp)
                throws HistoryPersistenceException {
            if (error != null) {
                throw new HistoryPersistenceException(String.format(FAILED_MESSAGE,
                        getClass().getSimpleName(), statsRelatedClassName, startTimestamp), error);
            }
        }

        /**
         * Checks if we have waited long enough to declare timeout.
         *
         * @throws HistoryPersistenceException if such an error is encountered
         */
        protected void checkTimeout() throws HistoryPersistenceException {
            if (clock.millis() - startMs > TimeUnit.SECONDS.toMillis(timeoutSec)) {
                throw new HistoryPersistenceException("Stream I/O operation from history component timeout exceeded");
            }
        }
    }
}

/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats.readers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Clock;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;

import com.vmturbo.history.stats.RequestBasedReader;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * {@link AbstractBlobsReader} reads information from blobs-based database such as percentile.
 * For more information, please look at:
 * <a href="https://vmturbo.atlassian.net/wiki/spaces/XD/pages/944930872/Historical+Data+XL+D2+-+Historical+Analysis+in+XL+-+Details#HistoricalDataXLD2-HistoricalAnalysisinXL-Details-Otherchanges">HistoricalDataXLD2-HistoricalAnalysisinXL-Details-Otherchanges</a>.
 *
 * @param <Q> type of the API request that will be processed and data from history will be returned.
 * @param <C> type of the record chunks that will be returned by the reader.
 * @param <R> type of the records returned from the database.
 */
public abstract class AbstractBlobsReader<Q, C, R extends Record> implements RequestBasedReader<Q, C> {
    private static final Logger LOGGER = LogManager.getLogger();
    private final int timeToWaitNetworkReadinessMs;
    private final long grpcTimeoutMs;
    private final Clock clock;
    private final DataMetricSummary metric;
    private final Table<R> blobsTable;
    private final String recordClassName;
    private final DSLContext dsl;

    /**
     * Creates {@link AbstractBlobsReader} instance.
     *
     * @param timeToWaitNetworkReadinessMs time to wait network buffer readiness to send
     *         accept
     *         next chunk for sending over the network.
     * @param grpcTimeoutMs GRPC interaction timeout in milliseconds
     * @param clock provides information about current time.
     * @param dsl provides connection to database.
     * @param metric the {@link DataMetricSummary} recording the performance.
     * @param blobsTable the database table storing the data blobs.
     * @param recordClassName the simple class name that represents the database records.
     */
    public AbstractBlobsReader(int timeToWaitNetworkReadinessMs, long grpcTimeoutMs,
            @Nonnull Clock clock, @Nonnull DSLContext dsl,
            @Nonnull DataMetricSummary metric, @Nonnull Table<R> blobsTable,
            @Nonnull String recordClassName) {
        this.timeToWaitNetworkReadinessMs = timeToWaitNetworkReadinessMs;
        this.grpcTimeoutMs = grpcTimeoutMs;
        this.clock = Objects.requireNonNull(clock, "Clock cannot be null");
        this.dsl = Objects.requireNonNull(dsl, "DSLContext cannot be null");
        this.metric = Objects.requireNonNull(metric, "DataMetricSummary cannot be null");
        this.blobsTable = Objects.requireNonNull(blobsTable, "Blobs table cannot be null");
        this.recordClassName = Objects.requireNonNull(recordClassName, "Record class name cannot be null");
    }

    @Override
    public void processRequest(@Nonnull Q request, @Nonnull StreamObserver<C> responseObserver) {
        try (DataMetricTimer dataMetricTimer = metric.startTimer()) {
            try {
                dsl.transaction(trans -> {
                    final Stopwatch dbReading = Stopwatch.createStarted();
                    final Result<R> blobsRecords = queryData(trans.dsl(), request);
                    LOGGER.debug("Read '{}' '{}s' for '{}' in '{}'", blobsRecords::size,
                            () -> recordClassName, () -> getRequestInfo(request), dbReading::stop);
                    final int amountOfRecords = blobsRecords.size();
                    if (amountOfRecords < 1) {
                        LOGGER.warn("There is no {} information for '{}'", recordClassName,
                                getRequestInfo(request));
                        responseObserver.onCompleted();
                        return;
                    }
                    final ServerCallStreamObserver<C> serverObserver =
                            (ServerCallStreamObserver<C>)responseObserver;
                    final long totalProcessed = readData(blobsRecords, serverObserver,
                            getRequestChunkSize(request));
                    LOGGER.debug(
                            "{} data read '{}' bytes from database for '{}' in '{}' seconds from '{}' records",
                            () -> recordClassName, () -> totalProcessed,
                            () -> getRequestInfo(request), dataMetricTimer::getTimeElapsedSecs,
                            blobsRecords::size);
                    responseObserver.onCompleted();
                });
            } catch (DataAccessException ex) {
                LOGGER.error("Cannot extract data from database table {} for '{}'",
                        blobsTable.getName(), getRequestInfo(request), ex);
                responseObserver.onError(
                        Status.INTERNAL.withDescription(ex.getMessage()).asException());
            }
        }
    }

    private long readData(@Nonnull Collection<R> records,
            @Nonnull ServerCallStreamObserver<C> serverObserver, int chunkSize) {
        long totalProcessed = 0;
        final LinkedList<R> blobsRecords = new LinkedList<>(records);
        final R anyRecord = blobsRecords.iterator().next();
        final long startTimestamp = getRecordStartTimestamp(anyRecord);
        final long period = getRecordAggregationWindowLength(anyRecord);
        final long operationStart = clock.millis();
        try {
            R record;
            while ((record = blobsRecords.poll()) != null) {
                final Stopwatch recordRead = Stopwatch.createStarted();
                if (period > 0 && getRecordAggregationWindowLength(record) != period) {
                    final String error = String.format("Blob for '%s' is corrupted. Its chunk#%s "
                            + "has period '%s', initial chunk had period '%s'",
                            getRecordStartTimestamp(record), getRecordChunkIndex(record),
                            getRecordAggregationWindowLength(record), period);
                    sendError(serverObserver, error);
                    return totalProcessed;
                }
                final byte[] sourceData = getRecordData(record);
                long subChunk = 0;
                int recordProcessed = 0;
                while (recordProcessed < sourceData.length) {
                    if (Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException();
                    }
                    if (clock.millis() - grpcTimeoutMs > operationStart) {
                        final String message = String.format(
                                "Cannot read %s data for start timestamp '%s' and period '%s' because of exceeding GRPC timeout '%s' ms",
                                recordClassName, startTimestamp, period, grpcTimeoutMs);
                        sendError(serverObserver, message);
                        return totalProcessed;
                    }
                    if (!serverObserver.isReady()) {
                        Thread.sleep(timeToWaitNetworkReadinessMs);
                        continue;
                    }
                    final ByteString data = ByteString.readFrom(
                            new ByteArrayInputStream(sourceData, recordProcessed, chunkSize));
                    totalProcessed += data.size();
                    recordProcessed += data.size();
                    subChunk++;
                    serverObserver.onNext(buildChunk(period, startTimestamp, data));
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("{}#{} sub-chunk#{} for {} processed({}/{})",
                                recordClassName, getRecordChunkIndex(record),
                                (int)Math.ceil(recordProcessed / (float)chunkSize), startTimestamp,
                                recordProcessed, sourceData.length);
                    }
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("{} read '{}' bytes from database for '{}' timestamp in '{}' seconds from '{}' chunks",
                            recordClassName, recordProcessed, startTimestamp,
                            recordRead.elapsed(TimeUnit.SECONDS), subChunk);
                }
            }
        } catch (IOException ex) {
            LOGGER.error("Cannot split the data from table '{}' into chunks for start timestamp '{}' and period '{}'",
                    blobsTable, startTimestamp, period, ex);
            serverObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
        } catch (InterruptedException e) {
            sendError(serverObserver, String.format(
                    "Thread has been interrupted while reading %s data for start timestamp '%s' and period '%s'",
                    recordClassName, startTimestamp, period));
            Thread.currentThread().interrupt();
        }
        return totalProcessed;
    }

    private void sendError(@Nonnull ServerCallStreamObserver<C> serverObserver, String message) {
        LOGGER.error(message);
        serverObserver.onError(Status.INTERNAL.withDescription(message).asException());
    }

    /**
     * Queries and fetches the request data from the database.
     *
     * @param context the {@link DSLContext} of the database
     * @param request the request
     * @return the resulting data from the query
     */
    protected abstract Result<R> queryData(@Nonnull DSLContext context, @Nonnull Q request);

    /**
     * Gets the start timestamp of the record.
     *
     * @param record the record
     * @return the start timestamp of the record
     */
    protected abstract long getRecordStartTimestamp(@Nonnull R record);

    /**
     * Gets the aggregation window length of the given record.
     *
     * @param record the record
     * @return the aggregation window length if any, or 0 if not applicable
     */
    protected abstract long getRecordAggregationWindowLength(@Nonnull R record);

    /**
     * Gets the chunk index of the record.
     *
     * @param record the record
     * @return the chunk size of the record
     */
    protected abstract int getRecordChunkIndex(@Nonnull R record);

    /**
     * Gets the data of the record.
     *
     * @param record the record
     * @return the data of the record
     */
    protected abstract byte[] getRecordData(@Nonnull R record);

    /**
     * Builds a chunk of records.
     *
     * @param period the aggregation window length or 0 if none
     * @param startTimestamp the start timestamp
     * @param data the data of the chunk of records
     * @return the chunk of records
     */
    protected abstract C buildChunk(long period, long startTimestamp, ByteString data);

    /**
     * Gets the request info for logging purpose.
     *
     * @param request that will be processed
     * @return the request info to log
     */
    protected abstract String getRequestInfo(@Nonnull Q request);

    /**
     * Gets the chunk size of the request.
     *
     * @param request that will be processed
     * @return the chunk size of the request
     */
    protected abstract int getRequestChunkSize(@Nonnull Q request);
}

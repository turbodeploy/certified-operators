/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats.readers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.stats.Stats.GetPercentileCountsRequest;
import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk;
import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk.Builder;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.tables.PercentileBlobs;
import com.vmturbo.history.schema.abstraction.tables.records.PercentileBlobsRecord;
import com.vmturbo.history.stats.RequestBasedReader;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * {@link PercentileReader} reads percentile information from database. For more information,
 * please, look at:
 * <a href="https://vmturbo.atlassian.net/wiki/spaces/XD/pages/944930872/Historical+Data+XL+D2+-+Historical+Analysis+in+XL+-+Details#HistoricalDataXLD2-HistoricalAnalysisinXL-Details-Otherchanges">HistoricalDataXLD2-HistoricalAnalysisinXL-Details-Otherchanges</a>.
 */
public class PercentileReader
                implements RequestBasedReader<GetPercentileCountsRequest, PercentileChunk> {
    private static final PercentileBlobs PERCENTILE_BLOBS_TABLE = PercentileBlobs.PERCENTILE_BLOBS;
    private static final Logger LOGGER = LogManager.getLogger();
    private final int timeToWaitNetworkReadinessMs;
    private final long grpcTimeoutMs;
    private final Clock clock;
    private final HistorydbIO historydbIO;

    /**
     * Creates {@link PercentileReader} instance.
     *
     * @param timeToWaitNetworkReadinessMs time to wait network buffer readiness to
     *                 send accept next chunk for sending over the network.
     * @param grpcTimeoutMs GRPC interaction timeout in milliseconds
     * @param clock provides information about current time.
     * @param historydbIO provides connection to database.
     */
    public PercentileReader(int timeToWaitNetworkReadinessMs, long grpcTimeoutMs,
                    @Nonnull Clock clock, @Nonnull HistorydbIO historydbIO) {
        this.timeToWaitNetworkReadinessMs = timeToWaitNetworkReadinessMs;
        this.grpcTimeoutMs = grpcTimeoutMs;
        this.clock = Objects.requireNonNull(clock, "Clock cannot be null");
        this.historydbIO = Objects.requireNonNull(historydbIO, "HistorydbIO cannot be null");
    }

    @Override
    public void processRequest(@Nonnull GetPercentileCountsRequest request,
                    @Nonnull StreamObserver<PercentileChunk> responseObserver) {
        try (DataMetricTimer dataMetricTimer = SharedMetrics.PERCENTILE_READING.startTimer()) {
            final long startTimestamp = request.getStartTimestamp();
            try (Connection connection = historydbIO.transConnection();
                            DSLContext context = historydbIO.using(connection)) {
                final Stopwatch dbReading = Stopwatch.createStarted();
                final Result<PercentileBlobsRecord> percentileBlobsRecords =
                                context.selectFrom(PERCENTILE_BLOBS_TABLE)
                                                .where(PERCENTILE_BLOBS_TABLE.START_TIMESTAMP
                                                                .eq(startTimestamp))
                                                .orderBy(PERCENTILE_BLOBS_TABLE.CHUNK_INDEX)
                                                .fetch();
                LOGGER.debug("Read '{}' '{}s' for '{}' in '{}'",
                                percentileBlobsRecords::size,
                                PercentileBlobsRecord.class::getSimpleName, () -> startTimestamp,
                                dbReading::stop);
                final int amountOfRecords = percentileBlobsRecords.size();
                if (amountOfRecords < 1) {
                    final String message =
                                    String.format("There is no percentile information for '%s' timestamp",
                                                    startTimestamp);
                    LOGGER.warn(message);
                    responseObserver.onCompleted();
                    return;
                }
                final ServerCallStreamObserver<PercentileChunk> serverObserver =
                                (ServerCallStreamObserver<PercentileChunk>)responseObserver;
                final AtomicInteger totalProcessed = new AtomicInteger();

                /*
                 * This is required to avoid java.lang.OutOfMemoryError: Direct buffer memory
                 * by using io.grpc.stub.CallStreamObserver#isReady.
                 * Please, consider https://github.com/grpc/grpc-java/issues/1216 for more details.
                 */
                final Runnable readyHandler =
                                new PercentileReaderHandler(percentileBlobsRecords, totalProcessed,
                                                serverObserver, request.getChunkSize());
                serverObserver.setOnReadyHandler(readyHandler);
                readyHandler.run();
                LOGGER.debug("Percentile data read '{}' bytes from database for '{}' timestamp in '{}' seconds from '{}' records",
                                () -> totalProcessed, () -> startTimestamp,
                                dataMetricTimer::getTimeElapsedSecs, percentileBlobsRecords::size);
                responseObserver.onCompleted();
            } catch (VmtDbException | SQLException | DataAccessException ex) {
                LOGGER.error("Cannot extract data from the database for '{}'", startTimestamp, ex);
                responseObserver.onError(
                                Status.INTERNAL.withDescription(ex.getMessage()).asException());
            }
        }
    }

    /**
     * {@link PercentileReaderHandler} sends the data read from DB record over the GRPC, considering
     * unavailability of the network.
     */
    private class PercentileReaderHandler implements Runnable {
        private final Collection<PercentileBlobsRecord> records;
        private final AtomicInteger totalProcessed;
        private final ServerCallStreamObserver<PercentileChunk> serverObserver;
        private final int chunkSize;

        /**
         * Creates {@link PercentileReaderHandler} instance.
         *
         * @param records non-empty collection of database records that are going to
         *                 be send to the API caller.
         * @param totalProcessed amount of processed bytes in one blob of percentile
         *                 data.
         * @param serverObserver observer that will be used to send data over gRPC.
         * @param chunkSize size of the data that caller side ready to accept in one
         *                 gRPC message.
         */
        private PercentileReaderHandler(@Nonnull Collection<PercentileBlobsRecord> records,
                        @Nonnull AtomicInteger totalProcessed,
                        @Nonnull ServerCallStreamObserver<PercentileChunk> serverObserver,
                        int chunkSize) {
            this.records = records;
            this.totalProcessed = totalProcessed;
            this.serverObserver = serverObserver;
            this.chunkSize = chunkSize;
        }

        @Override
        public void run() {
            final PercentileBlobsRecord anyRecord = records.iterator().next();
            final long startTimestamp = anyRecord.getStartTimestamp();
            final long period = anyRecord.getAggregationWindowLength();
            final long operationStart = clock.millis();
            try {
                for (PercentileBlobsRecord record : records) {
                    final Stopwatch recordRead = Stopwatch.createStarted();
                    if (record.getAggregationWindowLength() != period) {
                        sendError(String.format(
                                        "Percentile blob for '%s' is corrupted. Its chunk#%s has period '%s', initial chunk had period '%s'",
                                        record.getStartTimestamp(), record.getChunkIndex(),
                                        record.getAggregationWindowLength(), period));
                        return;
                    }
                    final byte[] sourceData = record.getData();
                    long subChunk = 0;
                    int recordProcessed = 0;
                    while (recordProcessed < sourceData.length) {
                        if (!serverObserver.isReady()) {
                            if (clock.millis() - grpcTimeoutMs > operationStart) {
                                final String message =
                                                String.format("Cannot read percentile data for start timestamp '%s' and period '%s' because of exceeding GRPC timeout '%s' ms",
                                                                startTimestamp, period,
                                                                grpcTimeoutMs);
                                sendError(message);
                                return;
                            }
                            Thread.sleep(timeToWaitNetworkReadinessMs);
                            continue;
                        }
                        final Builder percentileChunkBuilder = PercentileChunk.newBuilder();
                        percentileChunkBuilder.setPeriod(period);
                        percentileChunkBuilder.setStartTimestamp(startTimestamp);
                        final ByteString data = ByteString.readFrom(
                                        new ByteArrayInputStream(sourceData, recordProcessed,
                                                        chunkSize));
                        percentileChunkBuilder.setContent(data);
                        totalProcessed.getAndAdd(data.size());
                        recordProcessed += data.size();
                        subChunk++;
                        serverObserver.onNext(percentileChunkBuilder.build());
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace("Percentile record#{} sub-chunk#{} for {} processed({}/{})",
                                            record.getChunkIndex(),
                                            (int)Math.ceil(recordProcessed / (float)chunkSize),
                                            startTimestamp, recordProcessed, sourceData.length);
                        }
                    }
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Percentile record read '{}' bytes from database for '{}' timestamp in '{}' seconds from '{}' chunks",
                                        recordProcessed, startTimestamp,
                                        recordRead.elapsed(TimeUnit.SECONDS),
                                        subChunk);
                    }
                }
            } catch (IOException | InterruptedException ex) {
                LOGGER.error("Cannot split the data from table '{}' into chunks for start timestamp '{}' and period '{}'",
                                PERCENTILE_BLOBS_TABLE, startTimestamp, period, ex);
                serverObserver.onError(
                                Status.INTERNAL.withDescription(ex.getMessage()).asException());
            }
        }

        private void sendError(String message) {
            LOGGER.error(message);
            serverObserver.onError(Status.INTERNAL.withDescription(message).asException());
        }

    }
}

/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats.writers;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk;
import com.vmturbo.common.protobuf.stats.Stats.SetPercentileCountsResponse;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.tables.PercentileBlobs;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * {@link PercentileWriter} writes information about percentile into database.
 */
public class PercentileWriter implements StreamObserver<PercentileChunk> {
    private static final PercentileBlobs PERCENTILE_BLOBS_TABLE = PercentileBlobs.PERCENTILE_BLOBS;

    private final PipedOutputStream incomingData;
    private final PipedInputStream dataToWrite;
    private final StreamObserver<SetPercentileCountsResponse> responseObserver;
    private final Logger logger = LogManager.getLogger();
    private final HistorydbIO historydbIO;
    private final ExecutorService statsWriterExecutorService;
    private final AtomicLong processedChunks = new AtomicLong();
    private final AtomicLong totalBytesWritten = new AtomicLong();
    private final AtomicLong startTimestamp = new AtomicLong();

    private Connection connection;
    private DSLContext context;
    private PreparedStatement insertNewValues;
    private Future<?> dataWritingPromise;
    private DataMetricTimer dataMetricTimer;

    /**
     * Creates {@link PercentileWriter} instance.
     *
     * @param responseObserver provides information about errors to the client side
     *                 that called percentile writer instance.
     * @param historydbIO provides connection to database.
     * @param statsWriterExecutorService pool to do new values storing.
     */
    public PercentileWriter(@Nonnull StreamObserver<SetPercentileCountsResponse> responseObserver,
                    @Nonnull HistorydbIO historydbIO,
                    @Nonnull ExecutorService statsWriterExecutorService) {
        this.historydbIO = Objects.requireNonNull(historydbIO, "HistorydbIO should not be null");
        this.statsWriterExecutorService = Objects.requireNonNull(statsWriterExecutorService,
                        "StatsWriterExecutorService should not be null");
        this.dataToWrite = new PipedInputStream();
        this.incomingData = new PipedOutputStream();
        this.responseObserver = Objects.requireNonNull(responseObserver,
                        "Response observer should not be null");
    }

    @Override
    public void onNext(PercentileChunk percentileChunk) {
        try {
            if (connection == null) {
                initialChunk(percentileChunk);
            }
            final ByteString content = percentileChunk.getContent();
            totalBytesWritten.addAndGet(content.size());
            content.writeTo(incomingData);
            final long current = this.processedChunks.getAndIncrement();
            logger.trace("Chunk#{} of percentile data '{}' for '{}' timestamp and '{}' period has been processed.",
                            () -> current, content::size,
                            percentileChunk::getStartTimestamp, percentileChunk::getPeriod);
        } catch (IOException | SQLException ex) {
            handleException(ex, () -> String.format(
                            "Cannot execute update of percentile values for '%s' start timestamp",
                            startTimestamp.get()));
            try {
                connection.rollback();
            } catch (SQLException e) {
                logger.warn(() -> String
                                .format("rollback transaction which is updating '%s' table for '%s' start timestamp",
                                                PERCENTILE_BLOBS_TABLE, startTimestamp.get()), e);
            }
        } catch (VmtDbException ex) {
            handleException(ex, () -> String.format(
                            "Cannot get connection to SQL database to do update percentile  for '%s' timestamp",
                            startTimestamp.get()));
        }
    }

    private void initialChunk(PercentileChunk percentileChunk)
                    throws VmtDbException, IOException, SQLException {
        dataMetricTimer = SharedMetrics.PERCENTILE_WRITING.startTimer();
        connection = historydbIO.transConnection();
        incomingData.connect(dataToWrite);
        final long startTimestampMs = percentileChunk.getStartTimestamp();
        this.startTimestamp.set(startTimestampMs);
        final byte[] data = percentileChunk.getContent().toByteArray();
        context = historydbIO.JooqBuilder();
        try (PreparedStatement deleteExistingRecord = this.connection.prepareStatement(
                        context.deleteFrom(PERCENTILE_BLOBS_TABLE)
                                        .where(PERCENTILE_BLOBS_TABLE.START_TIMESTAMP.eq(startTimestampMs))
                                        .getSQL())) {
            deleteExistingRecord.setLong(1, startTimestampMs);
            deleteExistingRecord.execute();
        }
        insertNewValues = this.connection.prepareStatement(
                        context.insertInto(PERCENTILE_BLOBS_TABLE)
                                        .columns(PERCENTILE_BLOBS_TABLE.START_TIMESTAMP,
                                                        PERCENTILE_BLOBS_TABLE.AGGREGATION_WINDOW_LENGTH,
                                                        PERCENTILE_BLOBS_TABLE.DATA)
                                        .values(startTimestampMs, percentileChunk.getPeriod(), data).getSQL());
        insertNewValues.setLong(1, startTimestampMs);
        insertNewValues.setLong(2, percentileChunk.getPeriod());
        insertNewValues.setBinaryStream(3, dataToWrite);
        dataWritingPromise = statsWriterExecutorService.submit(() -> {
            insertNewValues.execute();
            connection.commit();
            return null;
        });
    }

    private void handleException(Throwable ex, Supplier<String> message) {
        logger.error(message.get(), ex);
        responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asException());
    }

    @Override
    public void onError(Throwable throwable) {
        closeResources(() -> handleException(throwable, () -> String.format(
                        "Cannot write percentile data for '%s' timestamp in '%s'",
                        startTimestamp.get(), dataMetricTimer.getTimeElapsedSecs())));
    }

    @Override
    public void onCompleted() {
        closeResources(() -> {
            try {
                dataWritingPromise.get();
                responseObserver.onNext(SetPercentileCountsResponse.newBuilder().build());
                logger.debug("Percentile data '{}' bytes in '{}' chunks for '{}' timestamp have been written successfully in '{}' seconds",
                                totalBytesWritten::get, processedChunks::get, startTimestamp::get,
                                dataMetricTimer::getTimeElapsedSecs);
                responseObserver.onCompleted();
            } catch (InterruptedException ex) {
                handleException(ex, () -> String.format(
                                "Attempt to update percentile data for '%s' was interrupted",
                                startTimestamp.get()));
            } catch (ExecutionException ex) {
                handleException(ex.getCause(), () -> String.format(
                                "Attempt to update percentile data for '%s' failed",
                                startTimestamp.get()));
            }
        });
    }

    private void closeResources(Runnable runnable) {
        IOUtils.closeQuietly(incomingData);
        if (connection != null) {
            try {
                runnable.run();
            } finally {
                dataMetricTimer.close();
                closeSilentlyIfInitialized(insertNewValues, PreparedStatement::close);
                closeSilentlyIfInitialized(context, DSLContext::close);
                closeSilentlyIfInitialized(connection, Connection::close);
            }
        }
        IOUtils.closeQuietly(dataToWrite);
    }

    private <T> void closeSilentlyIfInitialized(T item, SqlConsumer<T> method) {
        if (item != null) {
            try {
                method.accept(item);
            } catch (SQLException ex) {
                logger.debug("Cannot close {}", item.getClass().getSimpleName(), ex);
            }
        }
    }

    /**
     * {@link SqlConsumer} consumer that can throw {@link SQLException} instance while calling its
     * method.
     *
     * @param <T> type of the item which method will be called.
     */
    private interface SqlConsumer<T> {
        void accept(T item) throws SQLException;
    }

}

/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats.writers;

import java.io.IOException;
import java.io.InputStream;
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
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk;
import com.vmturbo.common.protobuf.stats.Stats.SetPercentileCountsResponse;
import com.vmturbo.components.common.utils.ThrowingConsumer;
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
                            () -> current, content::size, percentileChunk::getStartTimestamp,
                            percentileChunk::getPeriod);
        } catch (IOException | SQLException | VmtDbException ex) {
            closeResources(new CloseResourceHandler(ex));
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
                                        .where(PERCENTILE_BLOBS_TABLE.START_TIMESTAMP
                                                        .eq(startTimestampMs)).getSQL())) {
            deleteExistingRecord.setLong(1, startTimestampMs);
            deleteExistingRecord.execute();
        }
        insertNewValues = this.connection.prepareStatement(
                        context.insertInto(PERCENTILE_BLOBS_TABLE)
                                        .columns(PERCENTILE_BLOBS_TABLE.START_TIMESTAMP,
                                                        PERCENTILE_BLOBS_TABLE.AGGREGATION_WINDOW_LENGTH,
                                                        PERCENTILE_BLOBS_TABLE.DATA)
                                        .values(startTimestampMs, percentileChunk.getPeriod(), data)
                                        .getSQL());
        insertNewValues.setLong(1, startTimestampMs);
        insertNewValues.setLong(2, percentileChunk.getPeriod());
        insertNewValues.setBinaryStream(3, dataToWrite);
        dataWritingPromise = statsWriterExecutorService.submit(() -> {
            try {
                insertNewValues.execute();
                connection.commit();
            } catch (SQLException e) {
                doSilentlyIfInitialized(dataToWrite, InputStream::close);
                throw e;
            }
            return null;
        });
    }

    @Override
    public void onError(Throwable throwable) {
        closeResources(new CloseResourceHandler(throwable));
    }

    @Override
    public void onCompleted() {
        closeResources(new CloseResourceHandler(null));
    }

    private void closeResources(Runnable runnable) {
        IOUtils.closeQuietly(incomingData);
        try {
            runnable.run();
        } finally {
            dataMetricTimer.close();
            doSilentlyIfInitialized(insertNewValues, PreparedStatement::close);
            doSilentlyIfInitialized(context, DSLContext::close);
            doSilentlyIfInitialized(connection, Connection::close);
        }
        IOUtils.closeQuietly(dataToWrite);
    }

    private <T, E extends Exception> void doSilentlyIfInitialized(T item,
                    ThrowingConsumer<T, E> method) {
        doSilentlyIfInitialized(item, method, "close");
    }

    private <T, E extends Exception> void doSilentlyIfInitialized(T item,
                    ThrowingConsumer<T, E> method, String methodName) {
        if (item == null) {
            return;
        }
        try {
            method.accept(item);
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                logger.debug("Silent '{}' call for '{}' resource has been interrupted", methodName,
                                item.getClass().getSimpleName(), ex);
                return;
            }
            logger.debug("Cannot do '{}' silently for '{}'", methodName,
                            item.getClass().getSimpleName(), ex);
        }
    }

    /**
     * Closes resources occupied by percentile writer.
     */
    private class CloseResourceHandler implements Runnable {
        private final Throwable failure;

        private CloseResourceHandler(@Nullable Throwable failure) {
            this.failure = failure;
        }

        @Override
        public void run() {
            final Supplier<String> errorMsgSupplier = () -> String.format(
                            "Attempt to update percentile data for '%s' failed in '%s' seconds, because:",
                            startTimestamp.get(), dataMetricTimer.getTimeElapsedSecs());
            if (dataWritingPromise == null) {
                handleException(failure, errorMsgSupplier);
                return;
            }
            try {
                dataWritingPromise.get();
                responseObserver.onNext(SetPercentileCountsResponse.newBuilder().build());
                logger.debug("Percentile data '{}' bytes in '{}' chunks for '{}' timestamp have been written successfully in '{}' seconds",
                                totalBytesWritten::get, processedChunks::get, startTimestamp::get,
                                dataMetricTimer::getTimeElapsedSecs);
                responseObserver.onCompleted();
            } catch (InterruptedException ex) {
                handleException(ex, () -> String.format(
                                "Attempt to update percentile data for '%s' was interrupted in '%s' seconds",
                                startTimestamp.get(), dataMetricTimer.getTimeElapsedSecs()));
            } catch (ExecutionException ex) {
                handleException(combineFailureReasons(ex.getCause()), errorMsgSupplier);
            }
        }

        private void handleException(Throwable ex, Supplier<String> message) {
            doSilentlyIfInitialized(connection, Connection::rollback, "rollback");
            logger.error(message.get(), ex);
            responseObserver.onError(Status.INTERNAL.withCause(ex).asException());
        }

        @Nonnull
        private Throwable combineFailureReasons(@Nonnull Throwable cause) {
            if (failure != null && cause.getCause() == null) {
                final Throwable failureRootCause = ExceptionUtils.getRootCause(failure);
                final Throwable realFailureCause =
                                failureRootCause == null ? failure : failureRootCause;
                realFailureCause.initCause(cause);
                return realFailureCause;
            }
            return cause;
        }
    }
}

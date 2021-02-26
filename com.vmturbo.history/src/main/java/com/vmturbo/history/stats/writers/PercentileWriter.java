/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats.writers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

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

    private final StreamObserver<SetPercentileCountsResponse> responseObserver;
    private final Logger logger = LogManager.getLogger();
    private final HistorydbIO historydbIO;
    private final DataMetricTimer dataMetricTimer;

    private Connection connection;
    private DSLContext context;
    private int processedChunks;
    private long totalBytesWritten;
    private long startTimestamp;
    private boolean closed;

    /**
     * Creates {@link PercentileWriter} instance.
     *  @param responseObserver provides information about errors to the client side
     *                 that called percentile writer instance.
     * @param historydbIO provides connection to database.
     */
    public PercentileWriter(@Nonnull StreamObserver<SetPercentileCountsResponse> responseObserver,
                    @Nonnull HistorydbIO historydbIO) {
        this.historydbIO = Objects.requireNonNull(historydbIO, "HistorydbIO should not be null");
        this.responseObserver = Objects.requireNonNull(responseObserver,
                        "Response observer should not be null");
        dataMetricTimer = SharedMetrics.PERCENTILE_WRITING.startTimer();
    }

    @Override
    public void onNext(PercentileChunk chunk) {
        try {
            final long startTimestampMs = chunk.getStartTimestamp();
            final ByteString content = chunk.getContent();
            if (closed) {
                logger.warn("Closed '{}' received chunk for '{}' with '{}' data size",
                                PercentileWriter.class.getSimpleName(), startTimestampMs,
                                content.size());
                return;
            }
            if (connection == null) {
                connection = historydbIO.transConnection();
                startTimestamp = startTimestampMs;
                context = historydbIO.JooqBuilder();
                try (PreparedStatement deleteExistingRecords = this.connection.prepareStatement(
                                context.deleteFrom(PERCENTILE_BLOBS_TABLE)
                                                .where(PERCENTILE_BLOBS_TABLE.START_TIMESTAMP
                                                                .eq(startTimestampMs)).getSQL())) {
                    deleteExistingRecords.setLong(1, startTimestampMs);
                    deleteExistingRecords.execute();
                }
            }
            final byte[] bytes = content.toByteArray();
            try (PreparedStatement insertNewValues = this.connection.prepareStatement(
                            context.insertInto(PERCENTILE_BLOBS_TABLE)
                                            .columns(PERCENTILE_BLOBS_TABLE.START_TIMESTAMP,
                                                            PERCENTILE_BLOBS_TABLE.AGGREGATION_WINDOW_LENGTH,
                                                            PERCENTILE_BLOBS_TABLE.DATA,
                                                            PERCENTILE_BLOBS_TABLE.CHUNK_INDEX)
                                            .values(startTimestampMs, chunk.getPeriod(), bytes,
                                                            processedChunks).getSQL())) {
                try (ByteArrayInputStream data = new ByteArrayInputStream(bytes)) {
                    insertNewValues.setLong(1, startTimestampMs);
                    insertNewValues.setLong(2, chunk.getPeriod());
                    insertNewValues.setBinaryStream(3, data);
                    insertNewValues.setInt(4, processedChunks);
                    insertNewValues.execute();
                }
                logger.trace("Chunk#{} of percentile data '{}' for '{}' timestamp and '{}' period has been processed.",
                                () -> processedChunks, content::size, chunk::getStartTimestamp,
                                chunk::getPeriod);
            }
            totalBytesWritten += bytes.length;
            processedChunks++;
        } catch (IOException | SQLException | VmtDbException ex) {
            closeResources(new CloseResourceHandler(ex));
        }
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
        if (closed) {
            return;
        }
        closed = true;
        try {
            runnable.run();
        } finally {
            dataMetricTimer.close();
            doSilentlyIfInitialized(context, DSLContext::close);
            doSilentlyIfInitialized(connection, Connection::close);
        }
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
            if (failure != null) {
                handleException(failure, () -> String.format(
                                "Attempt to update percentile data for '%s' failed in '%s' seconds, because:",
                                startTimestamp, dataMetricTimer.getTimeElapsedSecs()));
                return;
            }
            try {
                connection.commit();
                responseObserver.onNext(SetPercentileCountsResponse.newBuilder().build());
                logger.debug("Percentile data '{}' bytes in '{}' chunks for '{}' timestamp have been written successfully in '{}' seconds",
                                () -> totalBytesWritten, () -> processedChunks,
                                () -> startTimestamp, dataMetricTimer::getTimeElapsedSecs);
                responseObserver.onCompleted();
            } catch (SQLException ex) {
                handleException(ex, () -> String.format(
                                "Failed to commit writing of '%s' chunks of '%s' blob which has '%s' bytes in '%s' seconds",
                                processedChunks, startTimestamp, totalBytesWritten,
                                dataMetricTimer.getTimeElapsedSecs()));
            }
        }

        private void handleException(Throwable ex, Supplier<String> message) {
            doSilentlyIfInitialized(connection, Connection::rollback, "rollback");
            logger.error(message.get(), ex);
            responseObserver.onError(Status.INTERNAL.withCause(ex).asException());
        }

    }

}

/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.history.stats.writers;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;

import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;

import com.vmturbo.components.common.utils.ThrowingConsumer;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * {@link AbstractBlobsWriter} writes information into blobs table in the database.  Such tables
 * include percentile table and moving stats table.
 *
 * @param <P> type of the API response to return to the write request.
 * @param <C> type of the record chunks from the write request.
 * @param <W> type of the records to write to the database.
 */
public abstract class AbstractBlobsWriter<P, C, W extends Record> implements StreamObserver<C> {
    private final StreamObserver<P> responseObserver;
    private final Logger logger = LogManager.getLogger();
    private final DataSource dataSource;
    private final DataMetricTimer dataMetricTimer;
    private final DataMetricSummary metric;
    private final Table<W> blobsTable;

    private Connection connection;
    private DSLContext context;
    private int processedChunks;
    private long totalBytesWritten;
    private long startTimestamp;
    private boolean closed;

    /**
     * Creates {@link AbstractBlobsWriter} instance.
     *
     * @param responseObserver provides information about errors to the client side that called
     *                          blobs writer instance.
     * @param dataSource provides connection to database.
     * @param metric the {@link DataMetricSummary} recording the performance.
     * @param blobsTable the database table storing the data blobs.
     */
    public AbstractBlobsWriter(@Nonnull StreamObserver<P> responseObserver,
            @Nonnull DataSource dataSource, @Nonnull DataMetricSummary metric,
            @Nonnull TableImpl<W> blobsTable) {
        this.dataSource = Objects.requireNonNull(dataSource, "DataSource should not be null");
        this.responseObserver = Objects.requireNonNull(responseObserver,
                        "Response observer should not be null");
        dataMetricTimer = metric.startTimer();
        this.metric = Objects.requireNonNull(metric, "DataMetricSummary cannot be null");
        this.blobsTable = Objects.requireNonNull(blobsTable, "Blobs table cannot be null");
    }

    @Override
    public void onNext(C chunk) {
        try {
            final long startTimestampMs = getStartTimestamp(chunk);
            final ByteString content = getContent(chunk);
            if (closed) {
                logger.warn("Closed '{}' received chunk for '{}' with '{}' data size",
                                this.getClass().getSimpleName(), startTimestampMs,
                                content.size());
                return;
            }
            if (connection == null) {
                connection = dataSource.getConnection();
                startTimestamp = startTimestampMs;
                context = DSL.using(connection, SQLDialect.MARIADB, new Settings().withRenderSchema(false));
                try (PreparedStatement deleteExistingRecords = this.connection.prepareStatement(
                        context.deleteFrom(blobsTable).where(getStartTimestampField().eq(startTimestampMs)).getSQL())) {
                    deleteExistingRecords.setLong(1, startTimestampMs);
                    deleteExistingRecords.execute();
                }
            }
            final byte[] bytes = writeChunk(connection, context, processedChunks, chunk);
            totalBytesWritten += bytes.length;
            processedChunks++;
        } catch (IOException | SQLException ex) {
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
     * Closes resources occupied by blobs table writer.
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
                                "Attempt to update %s data for '%s' failed in '%s' seconds, because:",
                                getRecordSimpleName(), startTimestamp, dataMetricTimer.getTimeElapsedSecs()));
                return;
            }
            try {
                connection.commit();
                responseObserver.onNext(newResponse());
                logger.debug("{} data '{}' bytes in '{}' chunks for '{}' timestamp have been written successfully in '{}' seconds",
                        () -> getRecordSimpleName(), () -> totalBytesWritten, () -> processedChunks,
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

    /**
     * Write the data of a chunk to the database.
     *
     * @param connection the database connection
     * @param context the DSL context
     * @param processedChunks the chunk number
     * @param chunk the chunk data to write to the database
     * @return the written byte array
     * @throws SQLException if an SQL exception is encountered
     * @throws IOException if an IO exception is encountered
     */
    protected abstract byte[] writeChunk(@Nonnull Connection connection,
            @Nonnull  DSLContext context, int processedChunks, @Nonnull C chunk)
            throws SQLException, IOException;

    /**
     * Gets the start timestamp of the chunk.
     *
     * @param chunk the chunk
     * @return the start timestamp of the chunk
     */
    protected abstract long getStartTimestamp(@Nonnull C chunk);

    /**
     * Gets the content of the chunk.
     *
     * @param chunk the chunk
     * @return the content of the chunk
     */
    protected abstract ByteString getContent(@Nonnull C chunk);

    /**
     * Gets the start timestamp field.
     *
     * @return the start timestamp
     */
    @Nonnull
    protected abstract TableField<W, Long> getStartTimestampField();

    /**
     * Gets the record's simple name.
     *
     * @return the record's simple name
     */
    protected abstract String getRecordSimpleName();

    /**
     * Returns a new response.
     *
     * @return the new response
     */
    protected abstract P newResponse();
}

package com.vmturbo.cloud.common.persistence;

import java.time.Duration;

import javax.annotation.Nonnull;

/**
 * Represents a queue for collecting data, up until a "full" batch of data exists in the queue such that it can be
 * flushed to a data sink.
 * @param <DataTypeT> The data type of this queue.
 * @param <DataStatsSummaryT> The data-specific statistics summary.
 */
public interface DataQueue<DataTypeT, DataStatsSummaryT> {

    /**
     * Adds {@code data} to the queue. If, after adding the data to the queue, the queue
     * size meets the criteria for a full batch, the data will be flushed to a configured
     * data sink.
     * @param data The data to add to the queue.
     */
    void addData(DataTypeT data);

    /**
     * Flushes any data currently in the queue to the data sink.
     */
    void flush();

    /**
     * Drains the queue, blocking any subsequent calls to {@link #addData(Object)} and closes the queue to further
     * inserts into the queue. Any underlying resources will be shut down upon completion of all in-process data
     * sink operations.
     *
     * <p>Depending on the configuration of the queue, any failed data sink operations that failed initially may
     * be retried as part of this method. prior to returning.
     * @param timeout The timeout to wait for in-progress data sink operations to complete, as well as the time allowed
     * for any retry attempts to complete.
     * @return Statistics about the data queue, including data=specific stats returned by the data sink.
     * @throws Exception An exception thrown by
     */
    @Nonnull
    DataQueueStats<DataStatsSummaryT> drainAndClose(@Nonnull Duration timeout) throws Exception;

    /**
     * Forces the queue to close, preventing any subsequent acceptance of new data and interrupting
     * any ongoing data sink operations.
     */
    void forceClose();
}

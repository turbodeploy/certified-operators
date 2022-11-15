package com.vmturbo.cloud.common.persistence;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.data.stats.DurationStatistics;
import com.vmturbo.cloud.common.persistence.ConcurrentDataQueue.DataOperation;
import com.vmturbo.cloud.common.persistence.DataQueueStats.DataSpecificStatsCollector;

/**
 * A journal responsible for logging data queue events and collecting data stats.
 * @param <DataStatsT> The data-type-specific stats.
 * @param <DataSummaryT> The summary of {@link DataStatsT}.
 */
class DataQueueJournal<DataStatsT, DataSummaryT> {

    private final Logger logger = LogManager.getLogger();

    private final DataSpecificStatsCollector<DataStatsT, DataSummaryT> dataStatsCollector;

    private final DurationStatistics.Collector runtimeCollector = DurationStatistics.collector();

    private final AtomicLong createdOperations = new AtomicLong();

    private final AtomicLong failedOperations = new AtomicLong();

    private final AtomicLong successfulOperations = new AtomicLong();

    /**
     * Constructs a new {@link DataQueueJournal} instance.
     * @param dataStatsCollector The data-types-specific stats collector.
     */
    DataQueueJournal(@Nonnull final DataSpecificStatsCollector<DataStatsT, DataSummaryT> dataStatsCollector) {
        this.dataStatsCollector = Objects.requireNonNull(dataStatsCollector);
    }

    /**
     * Creates a new {@link DataQueueJournal} instance.
     * @param dataStatsCollector The data-types-specific stats collector.
     * @param <DataStatsT> The data-types-specific stats.
     * @param <DataSummaryT> The summary type of {@link DataStatsT}.
     * @return the newly created {@link DataQueueJournal} instance.
     */
    @Nonnull
    public static <DataStatsT, DataSummaryT> DataQueueJournal<DataStatsT, DataSummaryT> create(
            @Nonnull final DataSpecificStatsCollector<DataStatsT, DataSummaryT> dataStatsCollector) {

        return new DataQueueJournal<>(dataStatsCollector);
    }

    /**
     * Records data sink operation creation.
     * @param dataOperation The data sink operation.
     */
    public void recordOperationCreation(@Nonnull final DataOperation dataOperation) {

        logger.debug("{} created", dataOperation::operationId);

        createdOperations.incrementAndGet();
    }

    /**
     * Records successful completion of a data sink operation.
     * @param dataOperation The data sink operation.
     */
    public void recordSuccessfulOperation(@Nonnull final ConcurrentDataQueue<?, DataStatsT, ?>.DataOperation dataOperation) {

        logger.debug("{} completed successfully", dataOperation::operationId);

        successfulOperations.incrementAndGet();
        dataOperation.jobStats().ifPresent(dataStatsCollector::collect);
        runtimeCollector.collect(dataOperation.runtime());
    }

    /**
     * Records a failed data sink operation.
     * @param dataOperation The data sink operation.
     * @param t The exception causing the operation failure.
     */
    public void recordFailedOperation(@Nonnull final ConcurrentDataQueue<?, DataStatsT, ?>.DataOperation dataOperation,
                                      @Nonnull Throwable t) {

        if (failedOperations.incrementAndGet() < 10) {
            logger.warn("{} completed exceptionally", dataOperation.operationId(), t);
        } else {
            logger.debug("{} completed exceptionally", dataOperation.operationId(), t);
        }
    }

    /**
     * The data queue stats.
     * @return The data queue stats.
     */
    @Nonnull
    public DataQueueStats<DataSummaryT> getQueueStats() {
        return DataQueueStats.<DataSummaryT>builder()
                .totalOperations(createdOperations.get())
                .successfulOperations(successfulOperations.get())
                .failedOperations(failedOperations.get())
                .jobRuntime(runtimeCollector.toStatistics())
                .dataStats(dataStatsCollector.toSummary())
                .build();
    }
}

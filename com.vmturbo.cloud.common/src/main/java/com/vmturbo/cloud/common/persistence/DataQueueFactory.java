package com.vmturbo.cloud.common.persistence;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.common.persistence.DataQueueStats.DataSpecificStatsCollector;

/**
 * A factory for producing {@link DataQueue} instances.
 */
public interface DataQueueFactory {

    /**
     * Creates a new data queue.
     * @param dataBatcher The data batcher.
     * @param dataSink The data sink.
     * @param dataStatsCollector The data-types-specific stats collector.
     * @param queueConfiguration The queue configuration.
     * @param <DataTypeT> The data type of the queue.
     * @param <DataStatsT> The data-type-specific stats.
     * @param <DataSummaryT> The {@link DataStatsT} summary.
     * @return The new {@link DataQueue} instance.
     */
    @Nonnull
    <DataTypeT, DataStatsT, DataSummaryT> DataQueue<DataTypeT, DataSummaryT> createQueue(
            @Nonnull DataBatcher<DataTypeT> dataBatcher,
            @Nonnull DataSink<DataTypeT, DataStatsT> dataSink,
            @Nonnull DataSpecificStatsCollector<DataStatsT, DataSummaryT> dataStatsCollector,
            @Nonnull DataQueueConfiguration queueConfiguration);

    /**
     * Default implementation of {@link DataQueueFactory}, producing instances of {@link ConcurrentDataQueue}.
     */
    class DefaultDataQueueFactory implements DataQueueFactory {

        @Override
        public <DataTypeT, DataStatsT, DataSummaryT> DataQueue<DataTypeT, DataSummaryT> createQueue(
                @Nonnull DataBatcher<DataTypeT> dataBatcher,
                @Nonnull DataSink<DataTypeT, DataStatsT> dataSink,
                @Nonnull DataSpecificStatsCollector<DataStatsT, DataSummaryT> dataStatsCollector,
                @Nonnull DataQueueConfiguration queueConfiguration) {

            final DataQueueJournal<DataStatsT, DataSummaryT> queueJournal =
                    DataQueueJournal.create(dataStatsCollector, queueConfiguration.failureErrorLogLimit());
            return new ConcurrentDataQueue<>(dataSink, dataBatcher, queueJournal, queueConfiguration);
        }
    }
}

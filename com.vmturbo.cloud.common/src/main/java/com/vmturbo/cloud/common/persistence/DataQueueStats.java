package com.vmturbo.cloud.common.persistence;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.data.stats.DurationStatistics;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * Statistics about a set of {@link DataQueue} operations.
 * @param <DataStatsSummaryT> The summary type for data-specific stats.
 */
@HiddenImmutableImplementation
@Immutable
public interface DataQueueStats<DataStatsSummaryT> {

    /**
     * Duration statistics on data sink operation runtime.
     * @return Duration statistics on data sink operation runtime.
     */
    @Nonnull
    DurationStatistics jobRuntime();

    /**
     * Total data sink operations created. This may be less than successful + failed operations, given
     * failed operations may be counted multiple times.
     * @return Total data sink operations created.
     */
    long totalOperations();

    /**
     * Count of total successful operations.
     * @return Count of total successful operations.
     */
    long successfulOperations();

    /**
     * Count of total failed operations.
     * @return Count of total failed operations.
     */
    long failedOperations();

    /**
     * Checks whether {@link #dataStats()} is null.
     * @return Whether {@link #dataStats()} is null.
     */
    default boolean hasDataStats() {
        return dataStats() != null;
    }

    /**
     * The data-type-specific stats summary.
     * @return The data-type-specific stats summary.
     */
    @Nullable
    DataStatsSummaryT dataStats();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @param <DataStatsSummaryT> The data stats summary type.
     * @return The newly constructed {@link Builder} instance.
     */
    @Nonnull
    static <DataStatsSummaryT> Builder<DataStatsSummaryT> builder() {
        return new Builder<>();
    }

    /**
     * A builder class for constructing immutable {@link DataQueueStats} instances.
     * @param <DataStatsSummaryT> The data stats summary type.
     */
    class Builder<DataStatsSummaryT> extends ImmutableDataQueueStats.Builder<DataStatsSummaryT> {}

    /**
     * A collector for data-type-specific stats, aggregating the collected stats into a summary.
     * @param <DataStatsT> The data-type-specific stats.
     * @param <DataStatsSummaryT> The summary of {@link DataStatsT}.
     */
    interface DataSpecificStatsCollector<DataStatsT, DataStatsSummaryT> {

        /**
         * Collects the provided {@code dataStats}.
         * @param dataStats The data stats to collect.
         */
        void collect(@Nonnull DataStatsT dataStats);

        /**
         * Converts the collected data stats to a summary.
         * @return The data stats summary.
         */
        @Nullable
        DataStatsSummaryT toSummary();
    }

    /**
     * A {@link DataSpecificStatsCollector} that does nothing with provided data stats and
     * produces a null data stats summary. Useful for testing or if data statistics are not
     * desired.
     */
    class NullStatsCollector implements DataSpecificStatsCollector<Void, Void> {

        @Override
        public void collect(@Nonnull Void dataStats) {
            // NOOP
        }

        @Override
        public Void toSummary() {
            return null;
        }

        /**
         * Creates a new {@link NullStatsCollector} instance.
         * @return The newly created {@link NullStatsCollector} instance.
         */
        @Nonnull
        public static NullStatsCollector create() {
            return new NullStatsCollector();
        }
    }
}

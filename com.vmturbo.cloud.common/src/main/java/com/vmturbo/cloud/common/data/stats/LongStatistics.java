package com.vmturbo.cloud.common.data.stats;

import java.util.LongSummaryStatistics;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * An immutable representation of long summary statistic values.
 */
@HiddenImmutableImplementation
@Immutable
public interface LongStatistics {

    /**
     * An empty stat with 0 for all attributes.
     */
    LongStatistics EMPTY_STATISTICS = LongStatistics.builder()
            .average(0)
            .count(0)
            .max(0)
            .min(0)
            .sum(0)
            .build();

    /**
     * The average of the collect stats.
     * @return The average of the collect stats.
     */
    double average();

    /**
     * The number of data points collected.
     * @return The number of data points collected.
     */
    long count();

    /**
     * The max value collected.
     * @return The max value collected.
     */
    long max();

    /**
     * The min value collected.
     * @return The min value collected.
     */
    long min();

    /**
     * The sum of all values collected.
     * @return The sum of all values collected.
     */
    long sum();

    /**
     * Checks whether this summary is empty.
     * @return Checks whether this summary is empty. Returns true if the count is zero.
     */
    default boolean isEmpty() {
        return this.count() == 0L;
    }

    /**
     * Constructs {@link LongStatistics} as an immutable version of the provided {@code summaryStats}.
     * @param summaryStats The summary stats.
     * @return The immutable {@link LongStatistics}.
     */
    @Nonnull
    static LongStatistics fromLongSummary(@Nonnull LongSummaryStatistics summaryStats) {

        Preconditions.checkNotNull(summaryStats);

        return LongStatistics.builder()
                .average(summaryStats.getAverage())
                .min(summaryStats.getMin())
                .max(summaryStats.getMax())
                .sum(summaryStats.getSum())
                .count(summaryStats.getCount())
                .build();
    }


    /**
     * Creates a new builder instance for {@link LongStatistics}.
     * @return A new builder instance for {@link LongStatistics}.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link LongStatistics}.
     */
    class Builder extends ImmutableLongStatistics.Builder {}
}

package com.vmturbo.cloud.commitment.analysis.runtime.data;

import java.util.DoubleSummaryStatistics;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

/**
 * An immutable static summary of {@link DoubleSummaryStatistics}.
 */
@Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Immutable
public interface DoubleStatistics {

    /**
     * An empty stat with 0 for all attributes.
     */
    DoubleStatistics EMPTY_STATISTICS = DoubleStatistics.builder()
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
    double max();

    /**
     * The min value collected.
     * @return The min value collected.
     */
    double min();

    /**
     * The sum of all values collected.
     * @return The sum of all values collected.
     */
    double sum();

    /**
     * Checks whether this summary is empty.
     * @return Checks whether this summary is empty. Returns true if the count is zero.
     */
    default boolean isEmpty() {
        return this.count() == 0L;
    }

    /**
     * Creates an immutable summary from {@link DoubleSummaryStatistics}.
     * @param doubleSummary The target mutable summary stats to copy.
     * @return A newly created {@link DoubleStatistics} instance.
     */
    @Nonnull
    static DoubleStatistics fromDoubleSummary(@Nonnull DoubleSummaryStatistics doubleSummary) {
        Preconditions.checkNotNull(doubleSummary);

        return DoubleStatistics.builder()
                .average(doubleSummary.getAverage())
                .count(doubleSummary.getCount())
                .max(doubleSummary.getMax())
                .min(doubleSummary.getMin())
                .sum(doubleSummary.getSum())
                .build();
    }

    /**
     * Creates a new builder instance for {@link DoubleStatistics}.
     * @return A new builder instance for {@link DoubleStatistics}.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link DoubleStatistics}.
     */
    class Builder extends ImmutableDoubleStatistics.Builder {}

}

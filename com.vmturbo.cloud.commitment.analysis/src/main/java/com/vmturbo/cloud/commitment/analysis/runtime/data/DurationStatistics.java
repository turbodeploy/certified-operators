package com.vmturbo.cloud.commitment.analysis.runtime.data;

import java.time.Duration;
import java.util.LongSummaryStatistics;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * This is a statistics summary of {@link Duration} data.
 */
@HiddenImmutableImplementation
@Immutable
public interface DurationStatistics {

    /**
     * The sum of all {@link Duration} entries collected.
     * @return The sum of all {@link Duration} entries collected.
     */
    @Nonnull
    Duration sum();

    /**
     * The average of all {@link Duration} entries collected. There may be some loss of precision at
     * the sub-nanosecond level.
     * @return The average of all {@link Duration} entries collected.
     */
    @Nonnull
    Duration average();

    /**
     * The minimum value of all {@link Duration} entries collected.
     * @return The minimum value of all {@link Duration} entries collected.
     */
    @Nonnull
    Duration min();

    /**
     * The maximum value of all {@link Duration} entries collected.
     * @return The maximum value of all {@link Duration} entries collected.
     */
    @Nonnull
    Duration max();

    /**
     * The count of entries collected.
     * @return The count of entries collected.
     */
    long count();

    /**
     * Creates and returns a new {@link Collector} instance, which can be used to collected {@link Duration}
     * instances.
     * @return The newly constructed collector instance.
     */
    @Nonnull
    static Collector collector() {
        return new Collector();
    }

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link DurationStatistics} instances.
     */
    class Builder extends ImmutableDurationStatistics.Builder {}

    /**
     * A collector of {@link Duration} instances, in order to create a {@link DurationStatistics}.
     */
    class Collector {

        private final LongSummaryStatistics nanosecondStats = new LongSummaryStatistics();

        private final ReadWriteLock statsLock = new ReentrantReadWriteLock();

        private Collector() {}

        /**
         * Collected statistics from the provided {@code duration}.
         * @param duration The {@link Duration} to collect.
         */
        public void collect(@Nonnull Duration duration) {

            Preconditions.checkNotNull(duration);

            statsLock.writeLock().lock();
            try {
                // long summary stats is not thread safe
                nanosecondStats.accept(duration.toNanos());
            } finally {
                statsLock.writeLock().unlock();
            }
        }

        /**
         * Converts the collected {@link Duration} statistics to a {@link DurationStatistics} instance.
         * @return The {@link DurationStatistics} instance.
         */
        @Nonnull
        public DurationStatistics toStatistics() {

            statsLock.readLock().lock();
            try {
                return DurationStatistics.builder()
                        .sum(Duration.ofNanos(nanosecondStats.getSum()))
                        .average(Duration.ofNanos((long)nanosecondStats.getAverage()))
                        .max(Duration.ofNanos(nanosecondStats.getMax()))
                        .min(Duration.ofNanos(nanosecondStats.getMin()))
                        .count(nanosecondStats.getCount())
                        .build();
            } finally {
                statsLock.readLock().unlock();
            }
        }
    }
}

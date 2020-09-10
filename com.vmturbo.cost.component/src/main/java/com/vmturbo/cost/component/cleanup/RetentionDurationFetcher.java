package com.vmturbo.cost.component.cleanup;

import java.time.temporal.TemporalUnit;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

/**
 * An interface for fetching a duration representing the retention period of some associated dataset.
 */
public interface RetentionDurationFetcher {

    /**
     * The retention duration, bounded to a single time unit.
     * @return The retention duration, bounded to a single time unit.
     */
    @Nonnull
    BoundedDuration getRetentionDuration();

    /**
     * A duration, bounded to a single time unit. The {@link #amount()} value is expected to be
     * relative to the {@link #unit()}.
     */
    @Immutable
    interface BoundedDuration {

        /**
         * The time amount of this duration.
         * @return The time amount of this duration.
         */
        long amount();

        /**
         * The time unit of this duration.
         * @return The time unit of this duration.
         */
        TemporalUnit unit();
    }

    /**
     * Creates a static {@link RetentionDurationFetcher}, which will always return a static {@link BoundedDuration}.
     * @param amount The static time amount.
     * @param unit The unit of the duration to be returned by the static fetcher.
     * @return A {@link RetentionDurationFetcher} that always returns a static {@link BoundedDuration}.
     */
    static RetentionDurationFetcher staticFetcher(long amount, @Nonnull TemporalUnit unit) {
        return () -> ImmutableBoundedDuration.builder()
                .amount(amount)
                .unit(unit)
                .build();
    }
}

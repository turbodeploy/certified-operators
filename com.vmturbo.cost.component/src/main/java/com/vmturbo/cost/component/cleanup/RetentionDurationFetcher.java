package com.vmturbo.cost.component.cleanup;

import java.time.temporal.TemporalUnit;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.common.data.BoundedDuration;

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
     * Creates a static {@link RetentionDurationFetcher}, which will always return a static {@link BoundedDuration}.
     * @param amount The static time amount.
     * @param unit The unit of the duration to be returned by the static fetcher.
     * @return A {@link RetentionDurationFetcher} that always returns a static {@link BoundedDuration}.
     */
    static RetentionDurationFetcher staticFetcher(long amount, @Nonnull TemporalUnit unit) {
        return () -> BoundedDuration.builder()
                .amount(amount)
                .unit(unit)
                .build();
    }
}

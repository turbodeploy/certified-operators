package com.vmturbo.cloud.commitment.analysis.demand;

import java.time.Duration;
import java.time.Instant;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import org.immutables.value.Value.Check;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

/**
 * A time interval, representing a (start time, end time) tuple.
 */
@Immutable
public abstract class TimeInterval {

    /**
     * The start time of this interval.
     * @return The start time of this interval.
     */
    @Nonnull
    public abstract Instant startTime();

    /**
     * The end time of this interval.
     * @return The end time of this interval.
     */
    @Nonnull
    public abstract Instant endTime();

    /**
     * The {@link Duration} of this time interval.
     * @return The {@link Duration} of this time interval.
     */
    @Nonnull
    @Derived
    public Duration duration() {

        return Duration.between(startTime(), endTime());
    }

    @Check
    protected void validate() {
        Preconditions.checkState(
                !startTime().isAfter(endTime()),
                "Start time must be before or equal time end time");
    }

}

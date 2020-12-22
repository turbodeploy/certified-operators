package com.vmturbo.cloud.common.data;

import java.time.Duration;
import java.time.Instant;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import org.immutables.value.Value.Check;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * A time interval, representing a (start time, end time) tuple.
 */
@HiddenImmutableImplementation
@Immutable(lazyhash = true)
public abstract class TimeInterval implements TimeSeriesData {

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

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public TimeInterval timeInterval() {
        return this;
    }

    /**
     * Converts this {@link TimeInterval} instance to a {@link Builder} instance.
     * @return The newly created builder instance.
     */
    public Builder toBuilder() {
        return TimeInterval.builder().from(this);
    }

    @Check
    protected void validate() {
        Preconditions.checkState(
                !startTime().isAfter(endTime()),
                "Start time must be before or equal time end time");
    }

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The new builder instance.
     */
    @Nonnull
    public static Builder builder() {
        return new Builder();
    }



    /**
     * A builder class for creating {@link TimeInterval} instances.
     */
    public static class Builder extends ImmutableTimeInterval.Builder {}
}

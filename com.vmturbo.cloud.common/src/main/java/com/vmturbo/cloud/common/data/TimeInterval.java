package com.vmturbo.cloud.common.data;

import java.time.Duration;
import java.time.Instant;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
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
     * A {@link TimeInterval} instance with both start and end time set to {@link Instant#EPOCH}.
     */
    public static final TimeInterval EPOCH = TimeInterval.builder()
            .startTime(Instant.EPOCH)
            .endTime(Instant.EPOCH)
            .build();

    /**
     * The start time of this interval (always inclusive).
     * @return The start time of this interval.
     */
    @Nonnull
    public abstract Instant startTime();

    /**
     * The end time of this interval (.
     * @return The end time of this interval.
     */
    @Nonnull
    public abstract Instant endTime();

    /**
     * Indicates whether end time is meant to be inclusive.
     * @return Whether end time is inclusive.
     */
    @Default
    public boolean isEndTimeInclusive() {
        return true;
    }

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
     * Checks whether this time interval contains the provided {@code instant}.
     * @param instant The target {@link Instant}.
     * @return True, if this time interval contains {@code instance}. False, otherwise.
     */
    public boolean contains(@Nonnull Instant instant) {

        return (instant.isAfter(startTime()) || instant.equals(startTime()))
            && (instant.isBefore(endTime()) || (isEndTimeInclusive() && instant.equals(endTime())));
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

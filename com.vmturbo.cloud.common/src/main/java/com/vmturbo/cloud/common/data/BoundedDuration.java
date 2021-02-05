package com.vmturbo.cloud.common.data;

import java.time.Duration;
import java.time.temporal.TemporalUnit;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Lazy;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * A duration limited to an amount of time to a single time unit.
 */
@HiddenImmutableImplementation
@Immutable
public interface BoundedDuration {

    /**
     * The duration amount.
     * @return The duration amount.
     */
    long amount();

    /**
     * The time unit of the {@link #duration()}.
     * @return The time unit of the {@link #duration()}.
     */
    @Nonnull
    TemporalUnit unit();

    /**
     * The duration, combining the {@link #amount()} and {@link #unit()}. This is expected to through
     * an exception if {@link #unit()} is not a supported {@link Duration} unit.
     * @return The duration.
     */
    @Lazy
    @Nonnull
    default Duration duration() {
        return Duration.of(amount(), unit());
    }

    /**
     * Constructs and returns a new builder instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link BoundedDuration} instances.
     */
    class Builder extends ImmutableBoundedDuration.Builder {}
}

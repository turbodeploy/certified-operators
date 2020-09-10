package com.vmturbo.cloud.commitment.analysis.demand;

import java.time.Duration;
import java.time.temporal.TemporalUnit;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

/**
 * A duration/period instance, restricted to a single time unit.
 */
@Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
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
     * The duration, combining the {@link #amount()} and {@link #unit()}.
     * @return The duration.
     */
    @Derived
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

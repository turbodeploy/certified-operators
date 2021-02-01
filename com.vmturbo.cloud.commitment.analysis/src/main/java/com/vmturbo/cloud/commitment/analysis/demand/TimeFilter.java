package com.vmturbo.cloud.commitment.analysis.demand;

import java.time.Instant;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * A filter for a time-based attribute.
 */
@HiddenImmutableImplementation
@Immutable
public interface TimeFilter {

    /**
     * A comparator for time-based values.
     */
    enum TimeComparator {
        BEFORE,
        BEFORE_OR_EQUAL_TO,
        EQUAL_TO,
        AFTER,
        AFTER_OR_EQUAL_TO
    }

    /**
     * The comparator for comparing some attribute to the target {@link #time()} value contained within
     * this filter. The comparator can be read as `target {@link #comparator()} {@link #time()} e.g.
     * if the comparator is {@link TimeComparator#AFTER_OR_EQUAL_TO}, it can be read as target >= {@link #time()}.
     *
     * @return The {@link TimeComparator} of this filter.
     */
    @Nonnull
    TimeComparator comparator();

    /**
     * This is the target time value to filter against.
     *
     * @return The target time to filter against.
     */
    @Nonnull
    Instant time();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed {@link Builder} instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing {@link TimeFilter} instances.
     */
    class Builder extends ImmutableTimeFilter.Builder {}
}

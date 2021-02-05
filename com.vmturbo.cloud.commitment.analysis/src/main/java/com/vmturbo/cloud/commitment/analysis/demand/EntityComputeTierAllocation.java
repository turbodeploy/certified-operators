package com.vmturbo.cloud.commitment.analysis.demand;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * A mapping representing an entity (expected to be a virtual machine) allocated on a compute tier.
 */
@HiddenImmutableImplementation
@Immutable
public interface EntityComputeTierAllocation extends EntityCloudTierMapping {

    /**
     * The compute tier demand of this allocation.
     * @return The compute tier demand of this allocation.
     */
    @Nonnull
    @Override
    ComputeTierDemand cloudTierDemand();

    /**
     * Converts this {@link EntityComputeTierAllocation} instance to a {@link Builder}.
     * @return A newly constructed {@link Builder} instance, populated from this
     * {@link EntityComputeTierAllocation} instance.
     */
    @Nonnull
    default Builder toBuilder() {
        return EntityComputeTierAllocation.builder().from(this);
    }

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The new builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for creating {@link TimeInterval} instances.
     */
    class Builder extends ImmutableEntityComputeTierAllocation.Builder {}
}

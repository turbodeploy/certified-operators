package com.vmturbo.cost.component.billed.cost;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * Statistics for persistence of billed cost data.
 */
@HiddenImmutableImplementation
@Immutable
public interface BilledCostPersistenceStats {

    /**
     * The number of tag groups.
     * @return The number of tag groups.
     */
    long tagGroupCount();

    /**
     * The number of billing buckets.
     * @return The number of billing buckets.
     */
    long billingBucketCount();

    /**
     * The number of unique cloud scopes.
     * @return The number of unique cloud scopes.
     */
    long cloudScopeCount();

    /**
     * The number of cloud cost items.
     * @return The number of cloud cost items.
     */
    long billingItemCount();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed {@link Builder} instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing immutable {@link BilledCostPersistenceStats} instances.
     */
    class Builder extends ImmutableBilledCostPersistenceStats.Builder {}
}

package com.vmturbo.cost.component.cca.configuration;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * This stores all CCA settings which can be toggled on and off via the config map.
 */
@HiddenImmutableImplementation
@Immutable
public interface CloudCommitmentAnalysisConfigurationHolder {

    /**
     * Indicates whether the historical demand selection will be scoped to match the purchasing scope.
     * If historical demand selection is not scoped, all recorded demand will be used in calculating
     * covered demand.
     * @return True, if historical demand selection should be scoped to match the purchase recommendation
     * scope.
     */
    boolean scopeHistoricalDemandSelection();

    /**
     * Setting to include/exclude suspended demand from analysis.
     *
     * @return True if we want to include suspended demand in analysis.
     */
    boolean allocationSuspended();

    /**
     * Setting to include/exclude flexible demand from analysis.
     *
     * @return True if we want to include flexible demand in analysis.
     */
    boolean allocationFlexible();

    /**
     * Minimum amount of time an entity must be powered on for a single allocation for the demand
     * to not be considered ephemeral. For example, if the value is 1 and we have an entity powered
     * on for 2 days, we will consider the demand in analysis.
     *
     * @return A long representing the time in days.
     */
    long minStabilityMillis();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing {@link CloudCommitmentAnalysisConfigurationHolder} instances.
     */
    class Builder extends ImmutableCloudCommitmentAnalysisConfigurationHolder.Builder{}
}

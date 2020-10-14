package com.vmturbo.cost.component.cca.configuration;

import org.immutables.value.Value.Immutable;

/**
 * This stores all CCA settings which can be toggled on and off via the config map.
 */
@Immutable
public interface CloudCommitmentAnalysisConfigurationHolder {


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
}

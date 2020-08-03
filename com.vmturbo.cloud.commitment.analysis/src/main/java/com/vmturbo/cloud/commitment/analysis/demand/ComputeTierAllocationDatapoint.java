package com.vmturbo.cloud.commitment.analysis.demand;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

/**
 * Represents allocated compute tier demand (OS, tenancy, cloud tier) for an entity for a
 * single point in time.
 */
@Immutable
public interface ComputeTierAllocationDatapoint extends EntityCloudTierDatapoint {

    /**
     * The {@link ComputeTierDemand} of this data point.
     * @return The {@link ComputeTierDemand} of this data point.
     */
    @Nonnull
    @Override
    ComputeTierDemand cloudTierDemand();
}

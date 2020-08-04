package com.vmturbo.cloud.commitment.analysis.demand;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Represents demand for a compute tier.
 */
@Immutable
public interface ComputeTierDemand extends CloudTierDemand {

    /**
     * The OS of the demand.
     *
     * @return The OS of the demand.
     */
    @Nonnull
    OSType osType();

    /**
     * The tenancy of the demand.
     *
     * @return The tenancy of the demand.
     */
    @Nonnull
    Tenancy tenancy();
}

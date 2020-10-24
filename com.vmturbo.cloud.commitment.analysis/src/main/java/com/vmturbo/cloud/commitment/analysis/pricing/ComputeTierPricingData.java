package com.vmturbo.cloud.commitment.analysis.pricing;

import java.util.OptionalDouble;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * Pricing data for a compute tier.
 */
@HiddenImmutableImplementation
@Immutable
public interface ComputeTierPricingData extends CloudTierPricingData {

    /**
     * The on-demand compute rate (does not include license).
     * @return The on-demand compute rate (does not include license).
     */
    double onDemandComputeRate();

    /**
     * The on-demand license rate.
     * @return The on-demand license rate.
     */
    double onDemandLicenseRate();

    /**
     * The reserved license rate for Azure when a workload is covered by an RI. This rate covers any
     * price adjustments applied to the account covering the demand.
     *
     * @return The reserved license rate
     */
    OptionalDouble reservedLicenseRate();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing {@link ComputeTierPricingData} instances.
     */
    class Builder extends ImmutableComputeTierPricingData.Builder {}
}

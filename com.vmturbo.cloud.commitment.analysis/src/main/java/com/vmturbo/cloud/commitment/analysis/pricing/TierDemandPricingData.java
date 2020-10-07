package com.vmturbo.cloud.commitment.analysis.pricing;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * An interface for encapsulating the on demand rate, the on demand license rate and the reserved license rate.
 */
@HiddenImmutableImplementation
@Immutable
public interface TierDemandPricingData {

    /**
     * If the tier demand pricing data can't be found, we return an object with 0 prices set.
     */
    TierDemandPricingData EMPTY_TIER_DEMAND_PRICING_DATA = TierDemandPricingData.builder()
            .onDemandRate(0f).reservedLicenseRate(0f).build();
    /**
     * The on-demand rate of the target compute tier. This includes any license costs associated with the
     * OS. This rate covers any price adjustments applied to the account covering the demand.
     *
     * @return The on-demand rate.
     */
    float onDemandRate();

    /**
     * The reserved license rate for Azure when a workload is covered by an RI. This rate covers any
     * price adjustments applied to the account covering the demand.
     *
     * @return The reserved license rate
     */
    float reservedLicenseRate();


    /**
     * Returns a builder of the TierDemandPricingData class.
     *
     * @return The builder.
     */
    static TierDemandPricingData.Builder builder() {
        return new TierDemandPricingData.Builder();
    }

    /**
     *  Static inner class for extending generated or yet to be generated builder.
     */
    class Builder extends ImmutableTierDemandPricingData.Builder {}
}

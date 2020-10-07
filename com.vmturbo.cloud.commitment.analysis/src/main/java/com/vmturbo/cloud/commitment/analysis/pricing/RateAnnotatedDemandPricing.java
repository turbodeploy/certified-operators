package com.vmturbo.cloud.commitment.analysis.pricing;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierDemand;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * This interface is used to represent the rate annotated demand pricing which is the pricing per
 * scoped cloud tier demand.
 */
@HiddenImmutableImplementation
@Immutable
public interface RateAnnotatedDemandPricing {

    /**
     * The scoped cloud tier demand which this rate annotated demand represents.
     *
     * @return The scoped cloud tier demand.
     */
    ScopedCloudTierDemand scopedCloudTierDemand();

    /**
     * The tier demand pricing data to encapsulate the on demand and reserved license rates.
     *
     * @return the tier demand pricing data.
     */
    TierDemandPricingData tierDemandPricingData();

    /**
     * Static inner class for extending generated or yet to be generated builder.
     */
    class Builder extends ImmutableRateAnnotatedDemandPricing.Builder {}

    /**
     * Returns a builder of the RateAnnotatedDemandPricing class.
     *
     * @return The builder.
     */
    static RateAnnotatedDemandPricing.Builder builder() {
        return new RateAnnotatedDemandPricing.Builder();
    }
}

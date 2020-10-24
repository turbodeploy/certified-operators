package com.vmturbo.cloud.commitment.analysis.pricing;

import org.immutables.value.Value.Derived;

/**
 * An interface for encapsulating the on demand rate, the on demand license rate and the reserved license rate.
 */
public interface CloudTierPricingData {

    /**
     * A {@link CloudTierPricingData} instance representing 0 cost.
     */
    CloudTierPricingData EMPTY_PRICING_DATA = new CloudTierPricingData() {};

    @Derived
    default boolean isEmpty() {
        return this.equals(EMPTY_PRICING_DATA);
    }
}

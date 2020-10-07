package com.vmturbo.cloud.commitment.analysis.pricing;

import java.util.Set;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * Interface  about tier specific pricing ie on demand rate, license and also cloud commitment specific pricing.
 * For example, the upfront and recurring pricing in case of reserved Instances.
 */
@HiddenImmutableImplementation
@Immutable
public interface RateAnnotatedCommitmentContext {

    /**
     * Set of Rate annotated demand pricing.
     *
     * @return A set of rate annotated demand pricing.
     */
    Set<RateAnnotatedDemandPricing> scopedDemandPricingData();

    /**
     * The cloud commitment pricing data which reflects the pricing of the cloud commitment spec data.
     *
     * @return The cloud commitment pricing data.
     */
    CloudCommitmentPricingData cloudCommitmentPricingData();

    /**
     *  Static inner class for extending generated or yet to be generated builder.
     */
    class Builder extends ImmutableRateAnnotatedCommitmentContext.Builder {}

    /**
     * Returns a builder of the RateAnnotatedCommitmentContext class.
     *
     * @return The builder.
     */
    static RateAnnotatedCommitmentContext.Builder builder() {
        return new RateAnnotatedCommitmentContext.Builder();
    }
}

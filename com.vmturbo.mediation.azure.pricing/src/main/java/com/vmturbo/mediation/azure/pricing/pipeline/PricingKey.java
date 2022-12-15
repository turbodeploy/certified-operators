package com.vmturbo.mediation.azure.pricing.pipeline;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier;

/**
 * Keys for different pricing in a DiscoveredPricing response. equals and hashCode
 * must be implemented sensibly for use as a map key. toString must be implemented to
 * return a reasonable identification string to the user.
 */
public interface PricingKey {
    /**
     * Convert the key to an equivalent list of price table identifiers.
     *
     * @return a list of price table identifiers equivalent to the key.
     */
    @Nonnull
    List<PricingIdentifier> getPricingIdentifiers();

    /**
     * Return a new Key with a modified plan ID.
     *
     * @param planId the planId for the new key.
     * @return a new key with the same values as this key, except for the new Plan ID.
     */
    @Nonnull
    PricingKey withPlanId(@Nonnull String planId);
}

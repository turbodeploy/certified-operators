package com.vmturbo.mediation.azure.pricing.controller;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.azure.pricing.pipeline.DiscoveredPricing;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.probe.ProxyAwareAccount;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Interface for pricing discovery controllers. A single probe may have multiple controllers
 * (eg MCA, Retail, or EA for Azure pricing).
 *
 * @param <A> The account type for the probe.
 */
public interface PricingDiscoveryController<A extends ProxyAwareAccount> {
    /**
     * Is this controller applicable to the specific account passed. Eg, for MCA
     * the MCA account values need to be populated, for retail they must not be, etc.
     *
     * @param account the account to check
     * @return true if this controller is the one that should handle discovery of this
     * account.
     */
    boolean appliesToAccount(@Nonnull A account);

    /**
     * Get an object which can be used as a key in a cache of discovered pricing.
     * It must implement equals and hashCode appropriately. Accounts which use the same
     * underlying pricing file should produce keys that compare and hash equal. Accounts which
     * do not share a file must have keys that do not compare equal and should have a low
     * probability of hashing equal. Also, toString should return something meaningful
     * for logging purposes.
     */
    @Nonnull
    DiscoveredPricing.Key getKey(@Nonnull A accountValues);

    /**
     * Validate the account values for this target.
     *
     * @param accountValues the account specifying the target to validate.
     * @return a Validation Response DTO
     */
    @Nonnull
    ValidationResponse validateTarget(@Nonnull A accountValues);

    /**
     * Discover the target.
     *
     * @param accountValues account values specifying the target to discover.
     * @return a Discovery Response DTO
     */
    @Nonnull
    DiscoveryResponse discoverTarget(@Nonnull A accountValues,
            @Nonnull IPropertyProvider propertyProvider);
}

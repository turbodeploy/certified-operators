package com.vmturbo.mediation.azure.pricing.fetcher;

import java.nio.file.Path;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.platform.sdk.probe.ProxyAwareAccount;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * An interface for obtaining a pricing data file for an account.
 *
 * @param <A> the type of account for which pricing can be obtained.
 */
public interface PricingFileFetcher<A extends ProxyAwareAccount> {
    /**
     * Get an object which can be used as a cache key in a cache of downloaded files.
     * It must implement equals and hashCode appropriately. Accounts which use the same
     * underlying pricing file should produce keys that compare and hash equal. Accounts which
     * do not share a file must have keys that do not compare equal and should have a low
     * probability of hashing equal. Also, toString should return something meaningful
     * for logging purposes.
     */
    @Nonnull
    Object getCacheKey(@Nonnull A account);

    /**
     * Given an account, return the path to a downloaded pricing file for that
     * account.
     *
     * @param account The account for which to obtain a pricing file.
     * @param propertyProvider properties configuring the discovery
     * @return a path to the downloaded pricing data file and a status string
     * @throws Exception any error that may occur while trying to download the file
     */
    @Nullable
    Pair<Path, String> fetchPricing(@Nonnull A account, @Nonnull IPropertyProvider propertyProvider)
        throws Exception;
}

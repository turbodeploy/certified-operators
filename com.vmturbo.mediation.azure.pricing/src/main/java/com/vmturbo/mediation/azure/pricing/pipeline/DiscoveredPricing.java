package com.vmturbo.mediation.azure.pricing.pipeline;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier;
import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable;

/**
 * The result of a live pricing discovery pipeline. It may contain more than one
 * price table, in the event that it's efficient to compute multiple price tables at
 * the same time than separately (eg, from a single pricing data file).
 */
public class DiscoveredPricing {
    private Map<Key, PriceTable.Builder> pricing;

    /**
     * Construct a discovered pricing result.
     */
    public DiscoveredPricing() {
        this.pricing = new HashMap<>();
    }

    @Nonnull
    public Map<Key, PriceTable.Builder> getPricingMap() {
        return pricing;
    }

    /**
     * Keys for different pricing in a DiscoveredPricing response. equals and hashCode
     * must be implemented sensibly for use as a map key. toString must be implemented to
     * return a reasonable identification string to the user.
     */
    public interface Key {
        /**
         * Convert the key to an equivalent list of price table identifiers.
         *
         * @return a list of price table identifiers quivalent to the key.
         */
        @Nonnull
        List<PricingIdentifier> getPricingIdentifiers();
    }
}

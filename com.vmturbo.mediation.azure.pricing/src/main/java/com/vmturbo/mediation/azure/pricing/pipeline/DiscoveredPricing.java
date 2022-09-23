package com.vmturbo.mediation.azure.pricing.pipeline;

import java.util.HashMap;

import com.vmturbo.platform.sdk.common.PricingDTO.PriceTable;

/**
 * The result of a live pricing discovery pipeline. It may contain more than one
 * price table, in the event that it's efficient to compute multiple price tables at
 * the same time than separately (eg, from a single pricing data file).
 */
public class DiscoveredPricing extends HashMap<PricingKey, PriceTable.Builder> {
}

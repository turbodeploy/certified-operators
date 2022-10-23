package com.vmturbo.mediation.azure.pricing.resolver;

import java.util.Map;
import java.util.TreeMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections.map.CaseInsensitiveMap;

import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor;

/**
 * Represents one or more AzureMeters that represent the same product. There can be multiple
 * AzureMeters both because of pricing for different plans, but also because there can be
 * different pricing at different quantities. The TreeMap represents a mapping from the
 * MINIMUM quantity needed to qualify for a price to the meter with that pricing. Note that
 * Turbo instead represents price breaks by the MAXIMUM quantity that qualifies for a price.
 */
public class ResolvedMeter {
    private final AzureMeterDescriptor descriptor;
    private final Map<String, TreeMap<Double, AzureMeter>> pricing;

    /**
     * Construct a ResolvedMeter.
     *
     * @param descriptor the descriptor that indicates the meaning of this pricing
     */
    public ResolvedMeter(@Nonnull AzureMeterDescriptor descriptor) {
        this.descriptor = descriptor;
        pricing = new CaseInsensitiveMap();
    }

    /**
     * Get the descriptor, which indicates the specific product(s) to which this pricing
     * applies.
     *
     * @return the descriptor for this pricing.
     */
    @Nonnull
    public AzureMeterDescriptor getDescriptor() {
        return descriptor;
    }

    /**
     * Get the map of pricing. The key of the outer map is the plan ID string
     * (which may not be the same as the ID for the plan used in the PricingIdentifier),
     * The key for the inner map is the minimum quantity at which the pricing applies.
     *
     * @return the pricing map
     */
    @Nonnull
    public Map<String, TreeMap<Double, AzureMeter>> getPricing() {
        return pricing;
    }

    /**
     * Get the pricing map for a particular plan.
     *
     * @param planId the plan ID for which to get pricing
     * @return the pricing by minimun quantity TreeMap for the plan, or null
     */
    @Nullable
    public TreeMap<Double, AzureMeter> getPricingByMinimumQuantity(@Nonnull String planId) {
        return pricing.get(planId);
    }

    /**
     * Put a meter into the pricing map.
     *
     * @param meter The meter to place into the pricing map
     */
    @Nullable
    public void putPricing(@Nonnull AzureMeter meter) {
        pricing.computeIfAbsent(meter.getPlanName(), k -> new TreeMap<>())
                .put(meter.getTierMinimumUnits(), meter);
    }

    // TODO
    // Method that also applies unit conversion and converts to keyed by maximum rather
    // than minimum quantity like:
    // public TreeMap<Long, CurrencyAmount> getNormalizedPricing(@Nonnull String planId,
    //     @Nonnull something desiredunits) ?
    //
    // or is this better to just go straight to the final form:
    // public List<Price> getPricing(@Nonnull String planId, @Nonnull something desiredUnits) ?
    //
    // Or maybe it should be a method in the processor base class instead of here?
}

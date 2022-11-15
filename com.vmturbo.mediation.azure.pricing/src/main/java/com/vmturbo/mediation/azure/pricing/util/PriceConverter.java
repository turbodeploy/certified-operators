package com.vmturbo.mediation.azure.pricing.util;

import static java.lang.Math.ceil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;

/**
 * Convert pricing from AzureMeter format to formats for use in meter processing stages.
 */
public class PriceConverter {
    /**
     * Given a meter, convert the pricing represented to a double in the desired units,
     * if the units are compatible.
     *
     * @return The price (currently in USD)
     * @throws PriceConversionException if the units are not compatible (eg convert
     * 1 GB/Month to Unit.MILLION_IOPS)
     */
    public double getPriceAmount(@Nonnull Unit unit, AzureMeter meter)
        throws PriceConversionException {

        // TODO: Needs to use getUnitOfMeasure, verify compatability with "unit", and scale
        // the value if necessary (eg, if unitOfMeasure is "1/Month" and unit is Unit.HOURS,
        // the price would need to be divided by 730 to scale to the desired units.

        return meter.getUnitPrice();
    }

    /**
     * Given a map of meters organized by minimum quantity needed to qualify for a price, return
     *  the price in the desired units. Since the method returns only a single value, it requires
     *  that the map contain only a single entry, with a minimum quantity of 0.
     *
     * @param unit The unit in which to report the price
     * @param pricing a map of pricing by minimum qualifying quantity
     * @return the price (currently in USD)
     * @throws PriceConversionException if the units are incompatible, or if the map doesn't
     *  contain a single entry with a minimum qualifying quantity of 0.
     */
    public double getPriceAmount(@Nonnull Unit unit, @Nonnull NavigableMap<Double, AzureMeter> pricing)
        throws PriceConversionException {

        if (pricing.size() != 1) {
            throw new PriceConversionException("Found " + pricing.size()
                + " price tiers, but tiered pricing is not supported in this context");
        } else if (pricing.firstKey() != 0.0D) {
            throw new PriceConversionException("Found minimum quantity needed of "
                + pricing.firstKey() + " but only 0 is supported in this context");
        } else {
            return getPriceAmount(unit, pricing.firstEntry().getValue());
        }
    }

    /**
     * Given a resolvedMeter containing a map of meters organized by minimum quantity needed to
     *  qualify for a price, return the price in the desired units. Since the method returns only a
     *  single value, it requires that the map contain only a single entry, with a minimum quantity
     *  of 0.
     *
     * @param unit The unit in which to report the price
     * @param resolvedMeter Contains a map of pricing by minimum qualifying quantity
     * @param planId the plan for which to convert pricing
     * @return the price (currently in USD)
     * @throws PriceConversionException if the units are incompatible, or if the map doesn't
     *  contain a single entry with a minimum qualifying quantity of 0.
     */
    public double getPriceAmount(@Nonnull Unit unit, @Nonnull ResolvedMeter resolvedMeter, @Nonnull String planId)
            throws PriceConversionException {

        NavigableMap<Double, AzureMeter> pricing = resolvedMeter.getPricing().get(planId);
        if (pricing != null) {
            return getPriceAmount(unit, pricing);
        } else {
            throw new PriceConversionException("No pricing found for plan " + planId);
        }
    }

    /**
     * Given a map of meters organized by minimum quantity needed to qualify for a price, return
     *  the Price in the desired units. Since the method returns only a single Price, it requires
     *  that the map contain only a single entry, with a minimum quantity of 0.
     *
     * @param unit The unit in which to report the price
     * @param pricing a map of pricing by minimum qualifying quantity
     * @return the price (currently in USD)
     * @throws PriceConversionException if the units are incompatible, or if the map doesn't
     *  contain a single entry with a minimum qualifying quantity of 0.
     */
    @Nonnull
    public Price getPrice(@Nonnull Unit unit, @Nonnull NavigableMap<Double, AzureMeter> pricing)
            throws PriceConversionException {
        return amountToPrice(unit, getPriceAmount(unit, pricing));
    }

    /**
     * Given a resolvedMeter containing a map of meters organized by minimum quantity needed to
     *  qualify for a price, return the price in the desired units. Since the method returns only a
     *  single value, it requires that the map contain only a single entry, with a minimum quantity
     *  of 0.
     *
     * @param unit The unit in which to report the price
     * @param resolvedMeter Contains a map of pricing by minimum qualifying quantity
     * @param planId the plan for which to convert pricing
     * @return the price (currently in USD)
     * @throws PriceConversionException if the units are incompatible, or if the map doesn't
     *  contain a single entry with a minimum qualifying quantity of 0.
     */
    @Nonnull
    public Price getPrice(@Nonnull Unit unit, @Nonnull ResolvedMeter resolvedMeter, @Nonnull String planId)
        throws PriceConversionException {
        return amountToPrice(unit, getPriceAmount(unit, resolvedMeter, planId));
    }

    /**
     * Given a map of meters organized by minimum quantity needed to qualify for a price, return a
     * list of prices in the desired units, organized by maximum qualifying quantity.
     *
     * @param unit The unit in which to report the prices
     * @param pricing A map of pricing by minimum qualifying quantity
     * @return the price (currently in USD)
     * @throws PriceConversionException if the units are incompatible, or if the plan is not found
     */
    @Nonnull
    public List<Price> getPrices(@Nonnull Unit unit, @Nonnull NavigableMap<Double, AzureMeter> pricing)
        throws PriceConversionException {
        //      getPriceAmount(@Nonnull Unit unit, AzureMeter meter)
        List<Price> prices = new ArrayList<>();
        Entry<Double, AzureMeter> thisEntry = pricing.firstEntry();
        Entry<Double, AzureMeter> nextEntry;

        if (thisEntry == null) {
            // This should not happen
            throw new PriceConversionException("No pricing found for resolved meter?");
        }

        while (thisEntry != null) {
            nextEntry = pricing.higherEntry(thisEntry.getKey());
            double amount = getPriceAmount(unit, thisEntry.getValue());

            if (nextEntry == null) {
                // This is the highest entry -- it's quantity range starts at thisEntry.getKey()
                // and has no upper limit.
                prices.add(amountToPrice(unit, amount));
            } else {
                // This entry goes from wherever the previous entry started (0, for the first)
                // up to just below the starting quantity for the next entry.

                prices.add(amountToPrice(unit, amount, (long)ceil(nextEntry.getKey() - 1.0)));
            }

            thisEntry = nextEntry;
        }

        return prices;
    }

    /**
     * Given a resolvedMeter containing a map of meters organized by minimum quantity needed to
     *  qualify for a price, return a list of prices in the desired units, organized by maximum
     *  qualifying quantity.
     *
     * @param unit The unit in which to report the prices
     * @param resolvedMeter Contains a map of pricing by minimum qualifying quantity
     * @param planId the plan for which to convert pricing
     * @return the price (currently in USD)
     * @throws PriceConversionException if the units are incompatible, or if the plan is not found
     */
    @Nonnull
    public List<Price> getPrices(@Nonnull Unit unit, @Nonnull ResolvedMeter resolvedMeter,
            @Nonnull String planId) throws PriceConversionException {
        NavigableMap<Double, AzureMeter> pricing = resolvedMeter.getPricing().get(planId);
        if (pricing != null) {
            return getPrices(unit, pricing);
        } else {
            throw new PriceConversionException("No pricing found for plan " + planId);
        }
    }

    /**
     * Create a Price DTO representing an amount of currency (in USD) per unit.
     *
     * @param unit the units in which the price is applied
     * @param amount the cost per unit
     * @return a Price DTO representing the given information
     */
    @Nonnull
    public Price amountToPrice(@Nonnull Unit unit, double amount) {
        return Price.newBuilder()
            .setUnit(unit)
            .setPriceAmount(CurrencyAmount.newBuilder()
                .setAmount(amount)
                .build())
            .build();
    }

    /**
     * Create a Price DTO representing an amount of currency (in USD) per unit.
     *
     * @param unit the units in which the price is applied
     * @param amount the cost per unit
     * @param endOfRange the maximum quantity for which this price applies
     * @return a Price DTO representing the given information
     */
    @Nonnull
    public Price amountToPrice(@Nonnull Unit unit, double amount, long endOfRange) {
        return Price.newBuilder()
            .setUnit(unit)
            .setEndRangeInUnits(endOfRange)
            .setPriceAmount(CurrencyAmount.newBuilder()
                .setAmount(amount)
                .build())
            .build();
    }
}

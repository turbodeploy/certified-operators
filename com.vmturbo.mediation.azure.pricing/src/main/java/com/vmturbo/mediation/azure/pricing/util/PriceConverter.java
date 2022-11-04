package com.vmturbo.mediation.azure.pricing.util;

import java.util.TreeMap;

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
    public double getPriceAmount(@Nonnull Unit unit, @Nonnull TreeMap<Double, AzureMeter> pricing)
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

        TreeMap<Double, AzureMeter> pricing = resolvedMeter.getPricing().get(planId);
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
    public Price getPrice(@Nonnull Unit unit, @Nonnull TreeMap<Double, AzureMeter> pricing)
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
    public Price getPrice(@Nonnull Unit unit, @Nonnull ResolvedMeter resolvedMeter, @Nonnull String planId)
        throws PriceConversionException {
        return amountToPrice(unit, getPriceAmount(unit, resolvedMeter, planId));
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

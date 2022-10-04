package com.vmturbo.mediation.azure.pricing;

import java.time.ZonedDateTime;

import javax.annotation.Nonnull;

/**
 * An interface for Azure pricing meters. This is intended to be the same across different
 * implementations for different underlying source data formats (eg MCA CSV vs retail API JSON).
 */
public interface AzureMeter {
    /**
     * Get the UUID of this record.
     *
     * @return a UUID
     */
    @Nonnull String getMeterId();

    /**
     * Get the name of the plan to which this record applies. This is the name in its
     * native form for the pricing source and will need mapping tp Turbo's stardard plan IDs
     * that are used in pricing identifiers. For example, from MCA "Azure plan" or Retail
     * "Consumption" to 0001, and from MCA "Azure plan for DevTest" and Retail "DevTestConsumption"
     * to 0002.
     *
     * @return a name identifying the plan to which this record applies
     */
    @Nonnull String getPlanName();

    /**
     * There may be multiple records for the same meter with different pricing based on the
     * quantity. This field specified the minimum quantity needed to qualify for the
     * price in the given record.
     *
     * @return the minimum quantity needed to qualify for the pricing in this record.
     */
    double getTierMinimumUnits();

    /**
     * The units of measure for billing for the item. This could be a scalar (eg "100"), or it could
     * be a number with a unit (eg "10 Hours"). The caller may need to apply a conversion to
     * get the price into the expected unit, eg, divide by 100 to get the price each, or by 10
     * to get price per hour in the examples above. Similarly for prices specified by
     * terabyte to gigabyte, etc.
     *
     * @return the unit of measure or quantity for the unit price.
     */
    @Nonnull String getUnitOfMeasure();

    /**
     * The price per unit.
     *
     * @return get the price per unit.
     */
    double getUnitPrice();

    /**
     * The Effective start date.
     *
     * @return get the Effective start date.
     */
    ZonedDateTime getEffectiveStartDate();

    /**
     * The Effective end date.
     *
     * @return get the Effective end date.
     */
    ZonedDateTime getEffectiveEndDate();
}

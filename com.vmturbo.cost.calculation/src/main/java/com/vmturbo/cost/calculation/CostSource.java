package com.vmturbo.cost.calculation;

/**
 * Interface representing the cost source. The cost source could be the on Demand rate, the RI discounted
 * rate, the buy RI discount or unknown.
 */
public enum CostSource {
    /**
     * The source is the on demand rate.
     */
    ON_DEMAND_RATE,
    /**
     * The source is the RI discounted rate.
     */
    RI_INVENTORY_DISCOUNT,
    /**
     * The source is the disocunt from a Buy RI action.
     */
    BUY_RI_DISCOUNT,
    /**
     * Unknown source.
     */
    UNKNOWN;
}

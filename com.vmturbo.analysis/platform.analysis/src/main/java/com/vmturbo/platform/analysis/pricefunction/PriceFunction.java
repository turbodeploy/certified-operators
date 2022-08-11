package com.vmturbo.platform.analysis.pricefunction;

import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.CommoditySold;

@FunctionalInterface
public interface PriceFunction {
    // Methods

    /**
     * The price of one unit of normalized utilization. When a trader wants to
     * buy say 30% utilization, it will be charged 0.3 of the unit price.
     * @param normalizedUtilization the utilization as a percentage of the utilization
     * admissible for the {@link CommoditySold}
     * @return the price that will be charged for 100% of the capacity
     */
    @Pure
    public double unitPrice(double normalizedUtilization);

    // Inner classes
    public static class Cache extends com.vmturbo.platform.analysis.pricefunction.Cache {/* workaround */};
}

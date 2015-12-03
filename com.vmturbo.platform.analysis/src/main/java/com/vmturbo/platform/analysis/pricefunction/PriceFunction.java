package com.vmturbo.platform.analysis.pricefunction;

import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.CommoditySold;

public interface PriceFunction {
    // Constants
    public static final double MAX_UNIT_PRICE = 1e22;

    // Methods

    @Pure
    public double apply(double normalizedUtilization);

    /**
     * The price of one unit of normalized utilization. When a trader wants to
     * buy say 30% utilization, it will be charged 0.3 of the unit price.
     * @param normalizedUtilization the utilization as a percentage of the utilization
     * admissible for the {@link CommoditySold}
     * @return the price that will be charged for 100% of the capacity
     */
    @Pure
    default double unitPrice(double normalizedUtilization) {
        if (normalizedUtilization < 0.0) {
            throw new IllegalArgumentException("Argument must be non-negative, was " + normalizedUtilization);
        }
        return normalizedUtilization < 1.0 ? apply(normalizedUtilization) : MAX_UNIT_PRICE;
    }

    /**
     * The unit price of the normalized utilization, which is utilization divided by
     * the utilization upper bound
     * @param utilization the actual utilization of the {@link CommoditySold}
     * @param utilUpperBound the upper bound admissible for the {@link CommoditySold}
     * @return unit price of the normalized utilization
     * @see #unitPrice(double)
     */
    @Pure
    default double unitPrice(double utilization, double utilUpperBound) {
        return unitPrice(utilization / utilUpperBound);
    }

    /**
     * The unit price of normalized excess utilization, which is calculated as
     * (peak utilization - utilization) / (1 - utilization) / utilization upper bound.
     * @param utilization
     * @param peakUtilization
     * @param utilUpperBound
     * @return unit price of the normalized excess utilization
     * @see #unitPrice(double)
     */
    @Pure
    default double unitPeakPrice(double utilization, double peakUtilization, double utilUpperBound){
        double normalizedExcessUtil = peakUtilization > utilization
            ? (peakUtilization - utilization) / (1.0f - utilization) / utilUpperBound
            : 0;
        return unitPrice(normalizedExcessUtil);
    }
}

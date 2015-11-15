package com.vmturbo.platform.analysis.pricefunction;

import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.CommoditySold;

public interface PriceFunction {
    /**
     * The unit price of the normalized utilization, which is utilization divided by
     * the utilization upper bound
     * @param utilization the actual utilization of the {@link CommoditySold}
     * @param utilUpperBound the upper bound admissible for the {@link CommoditySold}
     * @return unit price of the normalized utilization
     * @see #unitPrice(double)
     */
    @Pure
    public double unitPrice(double utilization, double utilUpperBound);

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
    double unitPeakPrice(double utilization, double peakUtilization, double utilUpperBound);
}

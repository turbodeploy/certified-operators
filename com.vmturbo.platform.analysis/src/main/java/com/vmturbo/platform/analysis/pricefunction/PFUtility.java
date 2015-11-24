package com.vmturbo.platform.analysis.pricefunction;

import java.util.concurrent.ConcurrentMap;
import java.util.function.UnaryOperator;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.collect.MapMaker;

/**
 * A factory for price functions
 */
public class PFUtility {

    public final static double MAX_UNIT_PRICE = 1e22;

    /**
     * Cache instances of {@link PriceFunction}. If a requested one
     * was already created then return the existing instance.
     * We don't care that this is a ConcurrentMap (because we access it
     * from synchronized methods), we only care about the weak values.
     */
    private static final ConcurrentMap<@NonNull String, @NonNull PriceFunction> pfMap =
            new MapMaker().weakValues().makeMap();

    /**
     * Similar to {@link #pfMap} for custom price functions.
     */
    private static final ConcurrentMap<@NonNull UnaryOperator<Double>, @NonNull PriceFunction> customPfMap =
            new MapMaker().weakValues().makeMap();

    private static final class WrapperPriceFunction implements PriceFunction {
        UnaryOperator<Double> wrapped;

        WrapperPriceFunction(@NonNull UnaryOperator<Double> wrapped) {
            super();
            this.wrapped = wrapped;
        }

        /**
         * The price of one unit of normalized utilization. When a trader wants to
         * buy say 30% utilization, it will be charged 0.3 of the unit price.
         * @param normalizedUtilization the utilization as a percentage of the utilization
         * admissible for the {@link CommoditySold}
         * @return the price that will be charged for 100% of the capacity
         */
        private double unitPrice(double normalizedUtilization) {
            if (normalizedUtilization < 0.0) {
                throw new IllegalArgumentException("Argument must be non-negative, was " + normalizedUtilization);
            }
            return normalizedUtilization < 1.0
                    ? wrapped.apply(normalizedUtilization)
                            : MAX_UNIT_PRICE;
        }

        @Override
        public double unitPrice(double utilization, double utilUpperBound) {
            return unitPrice(utilization / utilUpperBound);
        }

        @Override
        public double unitPeakPrice(double utilization, double peakUtilization, double utilUpperBound) {
            double normalizedExcessUtil = peakUtilization > utilization
                ? (peakUtilization - utilization) / (1.0f - utilization) / utilUpperBound
                : 0;
            return unitPrice(normalizedExcessUtil);
        }

    }

    /**
     * Wrap the provided price function in a class that implements all
     * {@link PriceFunction} methods.
     * @param uod a price function defined as a unary operator
     * @return a {@link PriceFunction} that implements all the required methods.
     */
    private static PriceFunction wrap(@NonNull UnaryOperator<Double> uod) {
        return new WrapperPriceFunction(uod);
    }

    public static synchronized PriceFunction createStandardWeightedPriceFunction(double weight) {
        String key = "SWPF-" + weight;
        // The reason it is implemented this way is that in the ternary expression,
        // the last argument is not evaluated when the condition is true. So if the map
        // contains the key, we will not unnecessarily create a new instance of
        // UnaryOperator.
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = wrap(u -> weight / ((1.0f - u) * (1.0f - u)));
            pfMap.put(key, pf);
        }
        return pf;
    }

    public static PriceFunction createConstantPriceFunction(double constant) {
        String key = "CPF-" + constant;
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = wrap(u -> constant);
            pfMap.put(key, pf);
        }
        return pf;
    }

    public static PriceFunction createStepPriceFunction(double stepAt, double priceBelow, double priceAbove) {
        String key = String.format("SPF-%.10f,%.10f,%.10f", stepAt, priceBelow, priceAbove);
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = wrap(u -> u < stepAt ? priceBelow : priceAbove);
            pfMap.put(key, pf);
        }
        return pf;
    }

    public static PriceFunction createPriceFunction(@NonNull UnaryOperator<Double> function) {
        PriceFunction pf = customPfMap.get(function);
        if (pf == null) {
            pf = wrap(function);
            customPfMap.put(function, pf);
        }
        return pf;
    }
}

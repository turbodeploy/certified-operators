package com.vmturbo.platform.analysis.pricefunction;

import java.util.concurrent.ConcurrentMap;
import java.util.function.UnaryOperator;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.collect.MapMaker;

/**
 * A factory for price functions
 */
public class PFUtility {

    public final static double MAX_UNIT_PRICE = Double.POSITIVE_INFINITY;

    private static final PFUtility pfu = new PFUtility();

    /**
     * Cache instances of {@link PriceFunction}. If a requested one
     * was already created then return the existing instance.
     * We don't care that this is a ConcurrentMap (because we access it
     * from synchronized methods), we only care about the weak values.
     */
    private static final ConcurrentMap<String, PriceFunction> pfMap =
            new MapMaker().weakValues().makeMap();

    /**
     * Similar to {@link #pfMap} for custom price functions.
     */
    private static final ConcurrentMap<UnaryOperator<Double>, PriceFunction> customPfMap =
            new MapMaker().weakValues().makeMap();

    private final class WrapperPriceFunction implements PriceFunction {
        UnaryOperator<Double> wrapped;

        WrapperPriceFunction(@NonNull UnaryOperator<Double> wrapped) {
            super();
            this.wrapped = wrapped;
        }

        @Override
        public double unitPrice(double normalizedUtilization) {
            if (normalizedUtilization < 0.0) {
                throw new IllegalArgumentException("Argument must be non-negative, was " + normalizedUtilization);
            }
            return normalizedUtilization < 1.0 ?
                    wrapped.apply(normalizedUtilization) :
                        MAX_UNIT_PRICE;
        }

        @Override
        public double unitPrice(double utilization, double utilThreshold) {
            return unitPrice(utilization / utilThreshold);
        }

        @Override
        public double unitPeakPrice(double utilization, double peakUtilization, double utilThreshold) {
            double excessUtil =
                    (peakUtilization - utilization) /
                    (1.0f - utilization) /
                    utilThreshold;
            return unitPrice(excessUtil);
        }
    }

    /**
     * Wrap the provided price function in a class that implements all
     * {@link PriceFunction} methods.
     * @param uod a price function defined as a unary operator
     * @return a {@link PriceFunction} that implements all the required methods.
     */
    private static PriceFunction wrap(@NonNull UnaryOperator<Double> uod) {
        return pfu.new WrapperPriceFunction(uod);
    }

    public static synchronized PriceFunction createStandardWeightedPriceFunction(double weight) {
        String key = "SWPF-" + weight;
        // The reason it is implemented this way is that in the ternary expression,
        // the last argument is not evaluated when the condition is true. So if the map
        // contains the key, we will not unnecessarily create a new instance of
        // UnaryOperator.
        PriceFunction pf = pfMap.containsKey(key) ?
                pfMap.get(key) :
                    wrap((u) -> weight / ((1.0f - u) * (1.0f - u)));
        pfMap.putIfAbsent(key, pf);
        return pf;
    }

    public static PriceFunction createConstantPriceFunction(double constant) {
        String key = "CPF-" + constant;
        PriceFunction pf = pfMap.containsKey(key) ?
                pfMap.get(key) :
                    wrap((u) -> constant);
        pfMap.putIfAbsent(key, pf);
        return pf;
    }

    public static PriceFunction createStepPriceFunction(double stepAt, double priceBelow, double priceAbove) {
        String key = String.format("SPF-%.10f,%.10f,%.10f", stepAt, priceBelow, priceAbove);
        PriceFunction pf = pfMap.containsKey(key) ?
                pfMap.get(key) :
                    wrap((u) -> u < stepAt ? priceBelow : priceAbove);
        pfMap.putIfAbsent(key, pf);
        return pf;
    }

    public static PriceFunction createPriceFunction(@NonNull UnaryOperator<Double> function) {
        PriceFunction pf = customPfMap.containsKey(function) ?
                customPfMap.get(function) :
                    wrap(function);
        customPfMap.putIfAbsent(function, pf);
        return pf;
    }
}

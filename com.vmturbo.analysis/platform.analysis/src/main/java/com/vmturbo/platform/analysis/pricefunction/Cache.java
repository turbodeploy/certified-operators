package com.vmturbo.platform.analysis.pricefunction;

import java.util.concurrent.ConcurrentMap;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.collect.MapMaker;

/**
 * A factory for price functions
 */
class Cache {
    // Constants
    public static final double MAX_UNIT_PRICE = 1e22;

    // Fields

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
    private static final ConcurrentMap<@NonNull PriceFunction, @NonNull PriceFunction> customPfMap =
            new MapMaker().weakValues().makeMap();

    // Methods

    public static synchronized PriceFunction createStandardWeightedPriceFunction(double weight) {
        String key = "SWPF-" + weight;
        // The reason it is implemented this way is that in the ternary expression,
        // the last argument is not evaluated when the condition is true. So if the map
        // contains the key, we will not unnecessarily create a new instance of
        // UnaryOperator.
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = u -> u > 1 ? Double.POSITIVE_INFINITY : Math.min(weight / ((1.0f - u) * (1.0f - u)), MAX_UNIT_PRICE);
            pfMap.put(key, pf);
        }
        return pf;
    }

    public static PriceFunction createConstantPriceFunction(double constant) {
        String key = "CPF-" + constant;
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = u -> u > 1 ? Double.POSITIVE_INFINITY : constant;
            pfMap.put(key, pf);
        }
        return pf;
    }

    public static PriceFunction createStepPriceFunction(double stepAt, double priceBelow, double priceAbove) {
        String key = String.format("SPF-%.10f,%.10f,%.10f", stepAt, priceBelow, priceAbove);
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = u -> u > 1 ? Double.POSITIVE_INFINITY : u < stepAt ? priceBelow : priceAbove;
            pfMap.put(key, pf);
        }
        return pf;
    }

    public static PriceFunction createPriceFunction(@NonNull PriceFunction function) {
        PriceFunction pf = customPfMap.get(function);
        if (pf == null) {
            pf = function;
            customPfMap.put(function, pf);
        }
        return pf;
    }
}

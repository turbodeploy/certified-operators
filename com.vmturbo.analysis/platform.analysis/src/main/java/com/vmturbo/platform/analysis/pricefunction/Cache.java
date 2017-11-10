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

    /*
     * The standard price function used by most commodities in the first incarnation of the market.
     * The formula is P(u) = min(w / (1-u)^2, MAX_UNIT_PRICE) for us < 1, and Double.POSITIVE_INFINITY for u > 1.
     */
    public static synchronized PriceFunction createStandardWeightedPriceFunction(double weight) {
        String key = "SWPF-" + weight;
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = (u, seller, commSold, e) ->
                u > 1 ? Double.POSITIVE_INFINITY : Math.min(weight / ((1.0f - u) * (1.0f - u)), MAX_UNIT_PRICE);
            pfMap.put(key, pf);
        }
        return pf;
    }

    /*
    * Same as standard price function but returns non-infinite value MAX_UNIT_PRICE for utilization > 1.
    */
   public static synchronized PriceFunction createNonInfiniteStandardWeightedPriceFunction(double weight) {
       String key = "NISWPF-" + weight;
       PriceFunction pf = pfMap.get(key);
       if (pf == null) {
           pf = (u, seller, commSold, e) ->
               u > 1 ? MAX_UNIT_PRICE : Math.min(weight / ((1.0f - u) * (1.0f - u)), MAX_UNIT_PRICE);
           pfMap.put(key, pf);
       }
       return pf;
   }

   /*
    * A constant function. The formula is P(u) = parameter if u <= 1
    * and Double.POSITIVE_INFINITY for u > 1.
    */
    public static PriceFunction createConstantPriceFunction(double constant) {
        String key = "CPF-" + constant;
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = (u, seller, commSold, e) ->  u > 1 ? Double.POSITIVE_INFINITY : constant;
            pfMap.put(key, pf);
        }
        return pf;
    }

    /*
     * A step function.
     * The formula is P(u) = if u < stepAt then priceBelow else priceAbove, if u <= 1
     * and Double.POSITIVE_INFINITY for u > 1.
     */
    public static PriceFunction createStepPriceFunction(double stepAt, double priceBelow, double priceAbove) {
        String key = String.format("SPF-%.10f,%.10f,%.10f", stepAt, priceBelow, priceAbove);
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = (u, seller, commSold, e) ->
                u > 1 ? Double.POSITIVE_INFINITY : u < stepAt ? priceBelow : priceAbove;
            pfMap.put(key, pf);
        }
        return pf;
    }

    public static synchronized PriceFunction createStepPriceFunctionForCloud() {
        // the weight here is the price at 70% utilization
        // TODO: use the per-commodity setting to drive resizes to a particular utilization
        // TODO: reconsider this approach to use shoppingList based pricing
        double weight = 11.11;
        String key = "SPFC-" + weight;
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = (u, seller, commSold, e) ->  u == 0 ? 0 : u > 1 ? Double.POSITIVE_INFINITY : weight;
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

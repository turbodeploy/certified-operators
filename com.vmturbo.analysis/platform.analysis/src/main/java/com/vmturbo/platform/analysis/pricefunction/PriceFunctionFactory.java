package com.vmturbo.platform.analysis.pricefunction;

import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.MapMaker;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Factory to create {@link PriceFunction}.
 */
public class PriceFunctionFactory {

    private PriceFunctionFactory() {}

    /**
     * Max unit price.
     */
    public static final double MAX_UNIT_PRICE = 1e22;

    /**
     * Cached instances of {@link PriceFunction}. If a requested one
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

    /**
     * Validation function for utilization.
     * @param utilization to validate.
     * @return false if utilization is NaN or greater than 1. True otherwise.
     */
    public static boolean isInvalid(double utilization) {
        return Double.isNaN(utilization) || utilization > 1;
    }

    /**
     * The standard price function with the utilization of the commodity scaled.
     * The formula is P(u) = min(w / (1-u')^2, MAX_UNIT_PRICE) for u' < 1, and
     * Double.POSITIVE_INFINITY for isInvalid(u') where  u' = u/scale.
     * @param weight weight associated with the commodity.
     * @param scale scaling factor for the utilization of the commodity  u' = u / scale
     * @return the scaled price function.
     */
    public static synchronized PriceFunction createScaledCapacityStandardWeightedPriceFunction(double weight, double scale) {
        String key = String.format("SCSWPF-%.10f,%.10f", weight, scale);
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = new ScaledCapacityStandardWeightedPriceFunction(weight, scale);
        }
        return pf;
    }

    /**
     * The standard price function used by most commodities in the first incarnation of the market.
     * The formula is P(u) = min(w / (1-u)^2, MAX_UNIT_PRICE) for u < 1, and
     * Double.POSITIVE_INFINITY for isInvalid(u).
     * @param weight weight associated with the commodity.
     * @return the price function.
     */
    public static synchronized PriceFunction createStandardWeightedPriceFunction(double weight) {
        String key = "SWPF-" + weight;
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = new StandardWeightedPriceFunction(weight);
            pfMap.put(key, pf);
        }
        return pf;
    }

    /**
     * Same as standard price function but returns finite value MAX_UNIT_PRICE for utilization > 1.
     * @param weight weight associated with the commodity.
     * @return the price function.
     */
    public static synchronized PriceFunction createFiniteStandardWeightedPriceFunction(double weight) {
        String key = "FSWPF-" + weight;
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = new FiniteStandardWeightedPriceFunction(weight);
            pfMap.put(key, pf);
        }
        return pf;
    }

    /**
     * A constant function. The formula is P(u) = parameter if u <= 1
     * and Double.POSITIVE_INFINITY for isInvalid(u).
     * @param constant returned for valid utilization.
     * @return the price function.
     */
    public static PriceFunction createConstantPriceFunction(double constant) {
        String key = "CPF-" + constant;
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = new ConstantPriceFunction(constant);
            pfMap.put(key, pf);
        }
        return pf;
    }

    /**
     * A constant function that remains constant for the income statement but returns infinite
     * price for SLs looking for price for placement.
     *
     * @return The price function.
     */
    public static PriceFunction createIgnoreUtilizationPriceFunction() {
        String key = "IUPF-0";
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = new IgnoreUtilizationPriceFunction();
            pfMap.put(key, pf);
        }
        return pf;
    }

    /**
     * A step function.
     * The formula is P(u) = if u < stepAt then priceBelow else priceAbove, if u <= 1
     * and Double.POSITIVE_INFINITY for isInvalid(u).
     *
     * @param stepAt is the junction point where price changes.
     * @param priceBelow is the price below step.
     * @param priceAbove is the price above step.
     * @return the price function.
     */
    public static PriceFunction createStepPriceFunction(double stepAt, double priceBelow, double priceAbove) {
        String key = String.format("SPF-%.10f,%.10f,%.10f", stepAt, priceBelow, priceAbove);
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = new StepPriceFunction(stepAt, priceBelow, priceAbove);
            pfMap.put(key, pf);
        }
        return pf;
    }

    /**
     * A step function for cloud commodities.
     * The formula is P(u) = 0 if u is 0, Double.POSITIVE_INFINITY for isInvalid(u), or weight otherwise.
     * @return the price function.
     */
    public static synchronized PriceFunction createStepPriceFunctionForCloud() {
        // the weight here is the price at 70% utilization
        // TODO: use the per-commodity setting to drive resizes to a particular utilization
        // TODO: reconsider this approach to use shoppingList based pricing
        double weight = 11.11;
        String key = "SPFC-" + weight;
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = new StepPriceFunctionForCloud(weight);
            pfMap.put(key, pf);
        }
        return pf;
    }

    /**
     * create a customPrice function.
     * @param function is the custom {@link PriceFunction}.
     * @return {@link PriceFunction}.
     */
    public static PriceFunction createPriceFunction(@NonNull PriceFunction function) {
        PriceFunction pf = customPfMap.get(function);
        if (pf == null) {
            pf = function;
            customPfMap.put(function, pf);
        }
        return pf;
    }

    /**
     * Create an external price function to get price from network interface
     * This function has implementation to call network interface only.
     *
     * @return Return the price function
     */
    public static PriceFunction createExternalPriceFunction() {
        // TODO: should use a type to create price function of relevance here
        // Create a key for external price function using count
        // This will ensure we can have multiple external functions
        String key = "NCMExternalPF";
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = new ExternalPriceFunction();
            pfMap.put(key, pf);
        }
        return pf;
    }

    /**
     * Consider current bought utilization instead of passed projected sold.
     * Return infinite when it exceeds capacity.
     * Prefer smaller excess capacity.
     *
     * @param weight multiplicator for the price.
     * @return price function
     */
    public static synchronized PriceFunction createSquaredReciprocalBoughtUtilizationPriceFunction(double weight) {
        String key = "SRBUPF-" + weight;
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = new SquaredReciprocalBoughtUtilizationPriceFunction(weight);
            pfMap.put(key, pf);
        }
        return pf;
    }
}

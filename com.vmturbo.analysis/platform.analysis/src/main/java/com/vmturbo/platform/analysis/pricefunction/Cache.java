package com.vmturbo.platform.analysis.pricefunction;

import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.collect.MapMaker;
import com.vmturbo.matrix.component.TheMatrix;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.platform.analysis.topology.Topology;

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

    /**
     * Validation function for utilization
     */
    private static boolean isInvalid (double utilization) {
        return Double.isNaN(utilization) || utilization > 1;
    }


    /**
     * The standard price function with the utilization of the commodity scaled.
     * The formula is P(u) = min(w / (1-u')^2, MAX_UNIT_PRICE) for u' < 1, and
     * Double.POSITIVE_INFINITY for isInvalid(u') where  u' = u/scale.
     * @param weight weight associated with the commodity.
     * @param scale scaling factor for the utilization of the commodity  u' = u / scale
     * @return the scaled price function
     */
    public static synchronized PriceFunction createScaledCapacityStandardWeightedPriceFunction(double weight, double scale) {
        String key = String.format("SCSWPF-%.10f,%.10f", weight, scale);
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = (u, sl, seller, commSold, e) ->
                    isInvalid((u / scale)) ? Double.POSITIVE_INFINITY : Math.min(weight / ((1.0f - (u / scale)) *
                            (1.0f - (u / scale))), MAX_UNIT_PRICE);
            pfMap.put(key, pf);
        }
        return pf;
    }


    /**
     * The standard price function used by most commodities in the first incarnation of the market.
     * The formula is P(u) = min(w / (1-u)^2, MAX_UNIT_PRICE) for u < 1, and
     * Double.POSITIVE_INFINITY for isInvalid(u).
     */
    public static synchronized PriceFunction createStandardWeightedPriceFunction(double weight) {
        String key = "SWPF-" + weight;
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = (u, sl, seller, commSold, e) ->
                isInvalid(u) ? Double.POSITIVE_INFINITY : Math.min(weight / ((1.0f - u) *
                        (1.0f - u)), MAX_UNIT_PRICE);
            pfMap.put(key, pf);
        }
        return pf;
    }

   /**
    * Same as standard price function but returns finite value MAX_UNIT_PRICE for utilization > 1.
    */
   public static synchronized PriceFunction createFiniteStandardWeightedPriceFunction(double weight) {
       String key = "FSWPF-" + weight;
       PriceFunction pf = pfMap.get(key);
       if (pf == null) {
           pf = (u, sl, seller, commSold, e) ->
               isInvalid(u) ? MAX_UNIT_PRICE : Math.min(weight / ((1.0f - u) * (1.0f - u)),
                       MAX_UNIT_PRICE);
           pfMap.put(key, pf);
       }
       return pf;
   }

   /**
    * A constant function. The formula is P(u) = parameter if u <= 1
    * and Double.POSITIVE_INFINITY for isInvalid(u).
    */
    public static PriceFunction createConstantPriceFunction(double constant) {
        String key = "CPF-" + constant;
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = (u, sl, seller, commSold, e) ->  isInvalid(u) ? Double.POSITIVE_INFINITY : constant;
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
             pf = (u, sl, seller, commSold, e) -> sl == null ? 0 : Double.POSITIVE_INFINITY;
             pfMap.put(key, pf);
         }
         return pf;
     }

    /**
     * A step function.
     * The formula is P(u) = if u < stepAt then priceBelow else priceAbove, if u <= 1
     * and Double.POSITIVE_INFINITY for isInvalid(u).
     */
    public static PriceFunction createStepPriceFunction(double stepAt, double priceBelow, double priceAbove) {
        String key = String.format("SPF-%.10f,%.10f,%.10f", stepAt, priceBelow, priceAbove);
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = (u, sl, seller, commSold, e) ->
                isInvalid(u) ? Double.POSITIVE_INFINITY : u < stepAt ? priceBelow : priceAbove;
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
            pf = (u, sl, seller, commSold, e) ->
                    u == 0 ? 0: isInvalid(u) ? Double.POSITIVE_INFINITY : weight;
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
        String key = "NCMExternalPf";
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = (normalizedUtilization, shoppingList, seller, cs, e) -> {
                double price = 0d;
                // Get the topology id from economy
                Topology topo = e.getTopology();
                // Price calculation happens from places which don't require
                // this information so skip calling external pf.
                // Buyer or seller clones will not be in matrix yet.
                // FIXME: Clone creation is not notified to matrix interface
                if (topo == null || shoppingList == null || seller.getCloneOf() != -1
                                || shoppingList.getBuyer().getCloneOf() != -1) {
                    return price;
                }
                // Use topology id to get matrix interface
                Optional<MatrixInterface> interfaceOptional = TheMatrix.instance(topo.getTopologyId());
                // If consumer passed to price function is null, just return 0 price, as flow
                // price only makes sense in the context of a specific consumer
                if (interfaceOptional.isPresent()) {
                    // Find OIds of traders and send them to calculate price
                    Long buyerOid = shoppingList.getBuyer().getOid();
                    Long sellerOid = seller.getOid();
                    price = interfaceOptional.get().calculatePrice(buyerOid, sellerOid);
                    // This multiplication done to negate the effect of dividing price by
                    // effective capacity during quote computation. EdeCommon.computeCommodityCost() on M2 side
                    price = price * cs.getEffectiveCapacity();
                }
                return price;
            };
            pfMap.put(key, pf);
        }
        return pf;
    }

    /**
     * Consider current bought utilization instead of passed projected sold.
     * Return infinite when it exceeds capacity.
     * Prefer smaller excess capacity.
     *
     * @param weight multiplicator for the price
     * @return price function
     */
    public static synchronized PriceFunction
                    createSquaredReciprocalBoughtUtilizationPriceFunction(double weight) {
        String key = "SRBU-" + weight;
        PriceFunction pf = pfMap.get(key);
        if (pf == null) {
            pf = (u, sl, seller, commSold, e) -> {
                // if shopping list is passed, disregard u
                // fetch the bought commodity by specification and consider it's utilization
                double util = u;
                if (sl != null && commSold.getCapacity() > 0) {
                    int soldIndex = seller.getCommoditiesSold().indexOf(commSold);
                    if (soldIndex >= 0) {
                        int boughtIndex = sl.getBasket().indexOf(seller.getBasketSold().get(soldIndex));
                        if (boughtIndex >= 0) {
                            util = sl.getQuantity(boughtIndex) / commSold.getCapacity();
                        }
                    }
                }
                if (isInvalid(util)) {
                    return Double.POSITIVE_INFINITY;
                } else if (util == 0) {
                    return MAX_UNIT_PRICE;
                } else {
                    return Math.min(weight / util / util, MAX_UNIT_PRICE);
                }
            };
            pfMap.put(key, pf);
        }
        return pf;
    }
}

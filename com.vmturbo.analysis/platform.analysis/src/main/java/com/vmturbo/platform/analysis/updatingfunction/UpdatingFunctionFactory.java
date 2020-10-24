package com.vmturbo.platform.analysis.updatingfunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapMaker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.matrix.component.TheMatrix;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Context;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderWithSettings;
import com.vmturbo.platform.analysis.ede.QuoteMinimizer;
import com.vmturbo.platform.analysis.pricefunction.QuoteFunctionFactory;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CbtpCostDTO;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO.MM1Commodity;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Create commodity updating functions.
 */
public final class UpdatingFunctionFactory {

    private static final Logger logger = LogManager.getLogger(UpdatingFunctionFactory.class);
    private static final String MM1_DF = "MM1-DF";

    private UpdatingFunctionFactory() {
    }

    /**
     * Cache instances of {@link UpdatingFunction} that should be cleared from the cache
     * between analysis cycles.
     */
    private static final ConcurrentMap<@NonNull String, @NonNull UpdatingFunction> mm1Cache =
            new MapMaker().weakValues().makeMap();

    /**
     * Creates {@link UpdatingFunction} for a given seller.
     *
     * @param costDTO          the DTO carries the cost information
     * @param updateFunctionTO contains the updateFunctionType
     * @return UpdatingFunction
     */
    public static UpdatingFunction createUpdatingFunction(CostDTO costDTO,
                                                          UpdatingFunctionTO updateFunctionTO) {
        switch (updateFunctionTO.getUpdatingFunctionTypeCase()) {
            case MAX:
                return MAX_COMM;
            case MIN:
                return MIN_COMM;
            case PROJECT_SECOND:
                return RETURN_BOUGHT_COMM;
            case DELTA:
                return ADD_COMM;
            case AVG_ADD:
                return AVG_COMMS;
            case IGNORE_CONSUMPTION:
                return IGNORE_CONSUMPTION;
            case EXTERNAL_UPDATE:
                return EXTERNAL_UPDATING_FUNCTION;
            case UPDATE_COUPON:
                return createCouponUpdatingFunction(costDTO);
            case STANDARD_DISTRIBUTION:
                return STANDARD_DISTRIBUTION;
            case MM1_DISTRIBUTION:
                return createMM1DistributionUpdatingFunction(updateFunctionTO
                        .getMm1Distribution()
                        .getDependentCommoditiesList());
            case UPDATINGFUNCTIONTYPE_NOT_SET:
            default:
                return null;
        }
    }

    /**
     * Creates coupon updating function for a given seller.
     *
     * @param costDTO          the DTO carries the cost information
     * @return UpdatingFunction
     */
    public static UpdatingFunction createCouponUpdatingFunction(CostDTO costDTO) {
        return (buyer, boughtIndex, commSold, seller, economy, take, overhead, currentSLs)
                        -> {
            Optional<Context> optionalContext = buyer.getBuyer().getSettings().getContext();
            long oid = seller.getOid();
            int couponCommBaseType = buyer.getBasket().get(boughtIndex).getBaseType();

            // Find the template matched with the buyer
            final Set<Entry<ShoppingList, Market>>
                    shoppingListsInMarket = economy.getMarketsAsBuyer(seller).entrySet();
            Market market = shoppingListsInMarket.iterator().next().getValue();
            List<Trader> sellers = market.getActiveSellers();
            List<Trader> mutableSellers = new ArrayList<Trader>();
            mutableSellers.addAll(sellers);
            mutableSellers.retainAll(economy.getMarket(buyer).getActiveSellers());
            // Get cheapest quote, that will be provided by the matching template
            final QuoteMinimizer minimizer = mutableSellers.stream().collect(
                    () -> new QuoteMinimizer(economy, buyer), QuoteMinimizer::accept,
                    QuoteMinimizer::combine);
            Trader matchingTP = minimizer.getBestSeller();

            double requestedCoupons;
            if (matchingTP != null) {
                // The capacity of coupon commodity sold by the matching tp holds the
                // number of coupons associated with the template. This is the number of
                // coupons consumed by a vm that got placed on a cbtp.

                // Determining the coupon quantity for buyers in consistent scaling
                // groups requires special handling when there is partial RI coverage.
                //
                // For example, if a m4.large needs 16 coupons and there are three VMs
                // in the scaling group. There is an m4.large CBTP that has 16 coupons.
                //
                // Since scaling actions are synthesized from a master buyer, we need to
                // spread the available coupons over all VMs in the scaling group in
                // order to be able to provide a consistent discounted cost for all
                // actions.  So, instead of one VM having 100% RI coverage and the other
                // two having 0% coverage, all VMs will have 33% coverage.
                int indexOfCouponCommByTp = matchingTP.getBasketSold().indexOfBaseType(couponCommBaseType);
                CommoditySold couponCommSoldByTp = matchingTP.getCommoditiesSold().get(indexOfCouponCommByTp);
                requestedCoupons = couponCommSoldByTp.getCapacity();
            } else {
                logger.warn("UPDATE_COUPON_COMM cannot find a best seller for:"
                        + buyer.getBuyer().getDebugInfoNeverUseInCode()
                        + " which moved to: "
                        + seller.getDebugInfoNeverUseInCode()
                        + " mutable sellers: "
                        + mutableSellers.stream()
                        .map(Trader::getDebugInfoNeverUseInCode)
                        .collect(Collectors.toList()));
                requestedCoupons = 0;
            }


            Map<Long, Double> totalCouponsToRelinquish = new HashMap<>();
            // if gf > 1 (its a group leader), reset coverage of the CSG and relinquish coupons to the CBTP
            // we do this irrespective of weather the buyer is moving in or out. If we did this only while moving out,
            // we could have a case where leader is not on a CBTP and we wouldnt relinquish
            if (buyer.getGroupFactor() > 0) {
                // consumer moving out of CBTP. Relinquish coupons and update the coupon bought
                // and the usage of sold coupon
                List<ShoppingList> peers = economy.getPeerShoppingLists(buyer);
                peers.retainAll(seller.getCustomers());
                double couponsBought = 0;
                for (ShoppingList peer : peers) {
                    int couponBoughtIndex = peer.getBasket().indexOfBaseType(couponCommBaseType);
                    // When moving out of a supplier, the buyer's supplier will be null. So we use
                    // peer shopping list's supplier here.
                    Long supplierOid = peer.getSupplier() == null ? null : peer.getSupplier().getOid();
                    Double couponsToRelinquish = totalCouponsToRelinquish.getOrDefault(supplierOid, new Double(0));
                    couponsBought = peer.getQuantity(couponBoughtIndex);
                    // the peers relinquish the excess
                    couponsToRelinquish += Math.max(0, couponsBought - requestedCoupons);
                    totalCouponsToRelinquish.put(supplierOid, couponsToRelinquish);
                    peer.setQuantity(couponBoughtIndex, Math.min(requestedCoupons, couponsBought));
                }
                Double relinquishedCouponsOnSeller = totalCouponsToRelinquish.getOrDefault(oid, new Double(0));
                couponsBought = buyer.getQuantity(boughtIndex);
                relinquishedCouponsOnSeller += Math.max(0, couponsBought - requestedCoupons);
                totalCouponsToRelinquish.put(oid, relinquishedCouponsOnSeller);
                buyer.setQuantity(boughtIndex, Math.min(requestedCoupons, couponsBought));
                // unplacing the buyer completely. Clearing up the context that contains complete
                // coverage information for the scalingGroup/individualVM
                if (optionalContext.isPresent()) {
                    for (Map.Entry<Long, Double> entry : totalCouponsToRelinquish.entrySet()) {
                        optionalContext.get().setTotalAllocatedCoupons(entry.getKey(),
                                optionalContext.get().getTotalAllocatedCoupons(entry.getKey())
                                .orElse(0.0) - entry.getValue());
                    }
                }
                // actual relinquishing of coupons to the seller
                double updatedUsage = commSold.getQuantity() - relinquishedCouponsOnSeller;
                if (updatedUsage < 0) {
                    logger.warn("coupon usage on consumer {} was greater than that on the seller {}",
                            buyer.getDebugInfoNeverUseInCode(), seller.getDebugInfoNeverUseInCode());
                    commSold.setQuantity(0);
                } else {
                    commSold.setQuantity(updatedUsage);
                }
            }
            if (!seller.getCustomers().contains(buyer)) {
                // reset usage while moving out
                double boughtQnty = buyer.getQuantity(boughtIndex);
                buyer.setQuantity(boughtIndex, 0);
                // relinquish coupons to the coverageTracked
                optionalContext.ifPresent(context -> context.setTotalAllocatedCoupons(oid,
                        context.getTotalAllocatedCoupons(oid).orElse(0.0) - boughtQnty));
                // for a consumer moving out of a CBTP, we have already updated the usage we just return the updated usage here
                return new double[] {Math.max(0.0, commSold.getQuantity() - boughtQnty), 0.0};
            } else {
                // consumer moving into CBTP. Use coupons and update the coupon bought
                // and the usage of sold coupon
                CbtpCostDTO cbtpResourceBundle = costDTO.getCbtpResourceBundle();
                // QuoteFunctionFactory.computeCost() already returns a cost that is
                // scaled by the group factor, so adjust for a single buyer.
                double templateCost = QuoteFunctionFactory.computeCost(buyer, matchingTP, false, economy)
                        .getQuoteValue();
                double availableCoupons = commSold.getCapacity() - commSold.getQuantity();
                // because we replay moves of every single VM into this CBTP, we dont have to worry about
                // existing coverage. If a VM already has a coverage of say 16 coupons and is scaling UP,
                // when we come here for that VM, the soldCommUsed is still 0 (NOT 16) because of that VM.
                // we then find out the right usage for that VM (HIGHER or LOWER) and add that as the VMs
                // contribution to the usage.
                double discountedCost = 0;
                double discountCoefficient = 0;
                double totalAllocatedCoupons = 0;
                if (buyer.getGroupFactor() > 0 && optionalContext.isPresent()) {
                    // group leader updates the coupon requested for the group
                    optionalContext.get().setTotalRequestedCoupons(oid, requestedCoupons * buyer.getGroupFactor());
                }
                if (availableCoupons > 0) {
                    totalAllocatedCoupons = Math.min(requestedCoupons, availableCoupons);
                    discountCoefficient = totalAllocatedCoupons / requestedCoupons;
                    // normalize total allocated coupons for a single buyer
                    buyer.setQuantity(boughtIndex, totalAllocatedCoupons);
                    // tier information is updated here indirectly through TotalRequestedCoupons update
                    if (optionalContext.isPresent()) {
                        optionalContext.get().setTotalAllocatedCoupons(oid,
                                optionalContext.get().getTotalAllocatedCoupons(oid)
                                        .orElse(0.0) + totalAllocatedCoupons);
                    }
                    discountedCost = ((1 - discountCoefficient) * templateCost) + (discountCoefficient
                            * ((1 - cbtpResourceBundle.getDiscountPercentage()) * templateCost));
                }
                // The cost of vm placed on a cbtp is the discounted cost
                buyer.setCost(discountedCost);
                if (logger.isDebugEnabled() || buyer.getBuyer().isDebugEnabled()) {
                    logger.info(buyer.getBuyer().getDebugInfoNeverUseInCode()
                            + " migrated to CBTP "
                            + seller.getDebugInfoNeverUseInCode()
                            + " offering a discount of "
                            + cbtpResourceBundle.getDiscountPercentage()
                            + " on TP " + matchingTP.getDebugInfoNeverUseInCode()
                            + " with a templateCost of " + templateCost
                            + " at a discountCoeff of " + discountCoefficient
                            + " with a final discount of " + discountedCost
                            + " requests " + requestedCoupons
                            + " coupons, allowed " + totalAllocatedCoupons
                            + " coupons");
                }
                /* Increase the used value of coupon commodity sold by cbtp accordingly.
                 * Increase the value by what was allocated to the buyer and not
                 * how much the buyer requested. This is important when we rollback the action.
                 * During rollback, we are only subtracting what buyer is buying (quantity).
                 * This was changed few lines above to what is allocated and not to what was
                 * requested by buyer.
                 */
                return new double[]
                        // the couponCovered is included in totalAllocatedCoupons so to avoid double counting,
                        // subtract couponCovered from the used on the commSold
                        {commSold.getQuantity() + totalAllocatedCoupons, 0};
            }
        };
    }

    /**
     * Add commodity updating function.
     */
    public static final UpdatingFunction ADD_COMM = (buyer, boughtIndex, commSold, seller, economy, take, overhead, currentSLs)
                    -> new double[]{buyer.getQuantities()[boughtIndex] + commSold.getQuantity(),
                                    buyer.getPeakQuantities()[boughtIndex] + commSold.getPeakQuantity()};
    /**
     * Subtract commodity updating function.
     */
    public static final UpdatingFunction SUB_COMM = (buyer, boughtIndex, commSold, seller, economy, take, overhead, currentSLs)
                    -> new double[]{Math.max(0, commSold.getQuantity() - buyer.getQuantities()[boughtIndex]),
                                    Math.max(0, commSold.getPeakQuantity() - buyer.getPeakQuantities()[boughtIndex])};

    /**
     * Ignore consumption updating function.
     * When taking the action, Return commSoldUsed. When not
     * taking the action, return 0 if the buyer fits or INFINITY otherwise.
     */
    public static final UpdatingFunction IGNORE_CONSUMPTION = (buyer, boughtIndex, commSold, seller,
                                                               economy, take, overhead, currentSLs)
                    -> {
                        if (take) {
                            return new double[]{commSold.getQuantity(), commSold.getPeakQuantity()};
                        } else {
                            return ((buyer.getQuantities()[boughtIndex] <= commSold.getCapacity())
                                    && (buyer.getPeakQuantities()[boughtIndex] <= commSold.getCapacity()))
                                    ? new double[]{0, 0} : new double[]{Double.POSITIVE_INFINITY,
                                    Double.POSITIVE_INFINITY};
                        }
                    };

    /**
     * Average commodity updating function.
     */
    public static final UpdatingFunction AVG_COMMS = (buyer, boughtIndex, commSold, seller, economy,
                                                      take, overhead, currentSLs)
                    -> {
                        // consider just the buyers that consume the commodity as customers
                        double numCustomers = commSold.getNumConsumers();
                        // if we take the move, we have already moved and we dont need to assume a new
                        // customer. If we are not taking the move, we want to update the used considering
                        // an incoming customer. In which case, we need to increase the custoemrCount by 1
                        if (take) {
                            return new double[]{
                                    (commSold.getQuantity() * numCustomers
                                            + buyer.getQuantities()[boughtIndex]) / numCustomers,
                                    (commSold.getPeakQuantity() * numCustomers
                                            + buyer.getPeakQuantities()[boughtIndex]) / numCustomers};
                        } else {
                            // This is done to prevent ping-pong occurring due to ""AVG_COMMS".
                            // Consider a commodity with quantity "1" on supplier1 and two consumers with
                            // quantity 12 and 10 on supplier2 (with avg 11).
                            // --Now consumer with quantity 12 goes shopping and will get cheaper quote at
                            // supplier1 as average is 6.5 .
                            // --Also, consumer with quantity 10 will also get cheaper quote at supplier1
                            // as average is ~7.66.
                            // --Consumer with quantity 1 will will get cheaper quote at supplier2 as its
                            // avg is 1 (because it was empty).
                            // Now in future placement iterations reverse will occur. To prevent this,
                            // when last consumer is there on current provider we return 0 (to provide
                            // best possible quote) so we don't recommend a move due to limitation of AVG_COMMS.
                            if (buyer.getSupplier() == seller && numCustomers == 1) {
                                return new double[]{0, 0};
                            }
                            return new double[]{Math.max(commSold.getQuantity(),
                                        (commSold.getQuantity() * numCustomers
                                        + buyer.getQuantities()[boughtIndex]) / (numCustomers + 1)),
                                        Math.max(commSold.getPeakQuantity(),
                                        (commSold.getPeakQuantity() * numCustomers
                                        + buyer.getPeakQuantities()[boughtIndex]) / (numCustomers + 1))};
                        }
                    };

    /**
     * Max commodity updating function.
     */
    public static final UpdatingFunction MAX_COMM = (buyer, boughtIndex, commSold, seller, economy, take, overhead, currentSLs)
                    -> new double[]{Math.max(buyer.getQuantities()[boughtIndex], commSold.getQuantity()),
                                    Math.max(buyer.getPeakQuantities()[boughtIndex], commSold.getPeakQuantity())};

    /**
     * Min commodity updating function.
     */
    public static final UpdatingFunction MIN_COMM = (buyer, boughtIndex, commSold, seller, economy, take, overhead, currentSLs)
                    -> new double[]{Math.min(buyer.getQuantities()[boughtIndex], commSold.getQuantity()),
                                    Math.min(buyer.getPeakQuantities()[boughtIndex], commSold.getPeakQuantity())};

    /**
     * Return bought commodity updating function.
     */
    public static final UpdatingFunction RETURN_BOUGHT_COMM = (buyer, boughtIndex, commSold, seller,
                                                               economy, take, overhead, currentSLs)
                    -> new double[]{buyer.getQuantities()[boughtIndex],
                                    buyer.getPeakQuantities()[boughtIndex]};

    /**
     * External updating function.
     */
    public static final UpdatingFunction EXTERNAL_UPDATING_FUNCTION =
            (buyer, boughtIndex, commSold, seller, economy, take, overhead, currentSLs) -> {
                        // If we are moving, external function needs to call place on matrix
                        // interface, else do nothing
                        if (take) {
                            Topology topology = economy.getTopology();
                            // check if topology or shopping list are null
                            // Make sure we do not call place for a clone of buyer or seller
                            if (topology == null || buyer == null || seller.getCloneOf() != -1
                                            || buyer.getBuyer().getCloneOf() != -1) {
                                return new double[] {0, 0};
                            }
                            Optional<MatrixInterface> interfaceOptional =
                                            TheMatrix.instance(topology.getTopologyId());
                            // Check if the matrix interface is present for this topology
                            if (interfaceOptional.isPresent()) {
                                long buyerOid = buyer.getBuyer().getOid();
                                long sellerOid = seller.getOid();
                                // Call Place method on interface to update matrix after placement
                                interfaceOptional.get().place(buyerOid, sellerOid);
                            }
                        }
                        return new double[] {0, 0};
                    };

    /**
     * Return a distribution function which distributes commodity values based on workload
     * conservation model.
     * Provision: projected = current x N/(N+1)
     * Suspension: projected = current x N/(N-1)
     */
    public static final UpdatingFunction STANDARD_DISTRIBUTION =
            (clonedSL, index, commSold, seller, economy, take, overhead, currentSLs) -> {
                    if (currentSLs == null) {
                        return new double[]{commSold.getQuantity(), commSold.getPeakQuantity()};
                    }
                    boolean isProvision = isProvision(clonedSL, currentSLs);
                    // Aggregate quantities across all shopping lists
                    final double quantitySum = currentSLs.stream()
                            .peek(sl -> {
                                if (logger.isTraceEnabled()) {
                                    logger.info("Standard distribution: {} to sum for {}: {}",
                                            sl.getBasket().get(index).getDebugInfoNeverUseInCode(),
                                            sl.getDebugInfoNeverUseInCode(), sl.getQuantity(index));
                                }
                            })
                            .mapToDouble(sl -> sl.getQuantity(index)).sum();
                    final double peakQuantitySum = currentSLs.stream()
                            .mapToDouble(sl -> sl.getPeakQuantity(index)).sum();
                    final int existingSize = currentSLs.size();
                    // Calculate projected quantities based on the workload conserving model
                    final int newSize = existingSize + (isProvision ? 1 : -1);
                    double projectedQuantity = quantitySum / newSize;
                    double projectedPeakQuantity = peakQuantitySum / newSize;
                    // Distribute the projected quantities
                    // Update existing shopping lists
                    distributeOnCurrent(currentSLs, index,
                            projectedQuantity, projectedPeakQuantity);
                    // Update new shopping list
                    if (isProvision) {
                        distributeOnClone(index, clonedSL,
                                projectedQuantity, projectedPeakQuantity);
                    }
                    return new double[]{projectedQuantity, projectedPeakQuantity};
                };

    /**
     * Create a distribution function which distributes commodity values based on M/M/1 queuing
     * model.
     * Provision, projected = current x (N x capacity - N x VCPUUtil)/((N+1) x capacity - N x VCPUUtil) ^ elasticity
     * Suspension, projected = current x (N x capacity - N x VCPUUtil)/((N-1) x capacity - N x VCPUUtil) ^ elasticity
     *
     * @param dependentCommodities a list dependent commodities required to compute MM1 distribution
     * @return {@link UpdatingFunction}
     */
    public static @Nonnull
    UpdatingFunction createMM1DistributionUpdatingFunction(List<MM1Commodity> dependentCommodities) {
        String key = String.format(
                "%s-%s",
                MM1_DF,
                dependentCommodities.stream()
                        .map(mm1Comm -> String.format(
                                "%.2f-%d",
                                mm1Comm.getElasticity(),
                                mm1Comm.getCommodityType()))
                        .collect(Collectors.joining("-")));
        return mm1Cache.computeIfAbsent(
                key,
                k -> new MM1Distribution()
                        .setDependentCommodities(dependentCommodities));
    }

    // when a user by mistake sets addComm as the updatingFn, we iterate over all the consumers and sum up the
    // usage for the comm across customers and set that as the usage on the soldComm. This is when consumers move in or out
    private static Set<UpdatingFunction> explicitCombinators = ImmutableSet.of(AVG_COMMS, MAX_COMM, MIN_COMM, ADD_COMM);

    public static Set<UpdatingFunction> getExplicitCombinatorsSet() {
        return explicitCombinators;
    }

    /**
     * A set of valid distribution functions.
     */
    private static Set<UpdatingFunction> distributionFunctions = ImmutableSet.of(STANDARD_DISTRIBUTION);

    /**
     * Check the validity of an updating function.
     *
     * @param distributionFunction the updating function to check
     * @return if the input {@link UpdatingFunction} is a valid updating function
     */
    public static boolean isValidDistributionFunction(UpdatingFunction distributionFunction) {
        return distributionFunctions.contains(distributionFunction)
                || (distributionFunction != null && mm1Cache.containsValue(distributionFunction));
    }

    /**
     * A helper function to check if the current distribution is for provision.
     *
     * @param sl         the shopping list of interest
     * @param currentSLs the existing shopping list
     * @return true if this is a distribution for provision, or a rollback of suspension
     */
    public static boolean isProvision(@Nonnull final ShoppingList sl,
                                      @Nonnull Set<ShoppingList> currentSLs) {
        final UUID uuid = sl.getShoppingListId();
        return currentSLs.stream()
                .map(ShoppingList::getShoppingListId)
                .noneMatch(uuid::equals);
    }

    /**
     * A helper function to update SLO commodity values on existing shopping lists.
     *
     * @param currentSLs         a set of shopping lists sponsored by a guaranteed buyer
     * @param boughtIndex         the index to the commodity specification in a basket
     * @param updatedQuantity     the quantity to update on the existing shopping lists
     * @param updatedPeakQuantity the peak quantity to update on the existing shopping lists
     */
    public static void distributeOnCurrent(Set<ShoppingList> currentSLs,
                                           final int boughtIndex,
                                           final double updatedQuantity,
                                           final double updatedPeakQuantity) {
        distributeOnCurrent(currentSLs, boughtIndex, updatedQuantity, updatedPeakQuantity, null);
    }

    /**
     * A helper function to update SLO commodity values on existing shopping lists.
     *
     * @param currentSLs          a set of shopping lists sponsored by a guaranteed buyer
     * @param boughtIndex          the index to the commodity specification in a basket
     * @param updatedQuantity      the quantity to update on the existing shopping lists
     * @param updatedPeakQuantity  the peak quantity to update on the existing shopping lists
     * @param dependentCommodities the dependent commodities to update
     */
    public static void distributeOnCurrent(Set<ShoppingList> currentSLs,
                                           final int boughtIndex,
                                           final double updatedQuantity,
                                           final double updatedPeakQuantity,
                                           List<Pair<Integer, Double>> dependentCommodities) {
        for (ShoppingList existingSL : currentSLs) {
            String buyerName = existingSL.getBuyer().getDebugInfoNeverUseInCode();
            CommoditySpecification commSpec = existingSL.getBasket().get(boughtIndex);
            String commName = commSpec.getDebugInfoNeverUseInCode();
            double originalBought = existingSL.getQuantity(boughtIndex);
            double originalBoughtPeak = existingSL.getPeakQuantity(boughtIndex);
            if (logger.isTraceEnabled()) {
                logger.info("Updating {} bought on existing buyer {}: {} -> {}",
                        commName, buyerName, originalBought, updatedQuantity);
            }
            existingSL.setQuantity(boughtIndex, updatedQuantity)
                    .setPeakQuantity(boughtIndex, updatedPeakQuantity);
            // update commSold that the shoppingList consume as a result of changing
            // quantity and peak quantity of shoppingList, the commSold is from existing
            // sellers
            Trader supplier = existingSL.getSupplier();
            if (supplier == null) {
                continue;
            }
            CommoditySold commSold = supplier.getCommoditySold(commSpec);
            if (commSold != null) {
                double originalSold = commSold.getQuantity();
                double originalSoldPeak = commSold.getPeakQuantity();
                double updatedSold = Math.max(0, originalSold - originalBought + updatedQuantity);
                commSold.setQuantity(updatedSold);
                commSold.setPeakQuantity(Math.max(originalSoldPeak - originalBoughtPeak
                        + updatedPeakQuantity, updatedSold));
                String supplierName = supplier.getDebugInfoNeverUseInCode();
                if (logger.isTraceEnabled()) {
                    logger.info("Updating {} sold on existing supplier {}: {} -> {} "
                                    + "[originalSold: {}, originalBought: {}, updatedBought: {}]",
                            commName, supplierName, originalSold, updatedSold,
                            originalSold, originalBought, updatedQuantity);
                }
            }
            updateDependentCommodities(supplier, dependentCommodities);
        }
    }

    /**
     * A helper function to set projected SLO commodity values on the cloned shopping list.
     *
     * @param index                 the index to the commodity specification in a basket
     * @param clonedSL              the cloned shopping list
     * @param projectedQuantity     the quantity to set on the cloned shopping list
     * @param projectedPeakQuantity the peak quantity to set on the cloned shopping list
     */
    public static void distributeOnClone(final int index,
                                         ShoppingList clonedSL,
                                         double projectedQuantity,
                                         double projectedPeakQuantity) {
        distributeOnClone(index, clonedSL, projectedQuantity, projectedPeakQuantity, null);
    }

    /**
     * A helper function to set projected SLO commodity values on the cloned shopping list.
     *
     * @param index                 the index to the commodity specification in a basket
     * @param clonedSL              the cloned shopping list
     * @param projectedQuantity     the quantity to set on the cloned shopping list
     * @param projectedPeakQuantity the peak quantity to set on the cloned shopping list
     * @param dependentCommodities  the dependent commodities to update
     */
    public static void distributeOnClone(final int index,
                                         ShoppingList clonedSL,
                                         double projectedQuantity,
                                         double projectedPeakQuantity,
                                         List<Pair<Integer, Double>> dependentCommodities) {
        if (clonedSL == null) {
            // Could be suspension or provision rollback
            return;
        }
        // The cloned shopping list could be a new shopping list from a new provision action, or
        // an existing shopping list from a rollback of a suspend action
        // Update the cloned shopping list
        clonedSL.setQuantity(index, projectedQuantity)
                .setPeakQuantity(index, projectedPeakQuantity);
        // Update the cloned seller
        Trader newSupplier = clonedSL.getSupplier();
        if (newSupplier == null) {
            // Should not happen
            return;
        }
        // Update quantity of the newly cloned seller to construct the buyer-seller relationship
        CommoditySold commSoldOnClone =
                newSupplier.getCommoditySold(clonedSL.getBasket().get(index));
        commSoldOnClone.setQuantity(clonedSL.getQuantity(index));
        commSoldOnClone.setPeakQuantity(clonedSL.getPeakQuantity(index));
        updateDependentCommodities(newSupplier, dependentCommodities);
    }

    /**
     * A helper function to update dependent commodities.
     *
     * @param trader               the trader to update
     * @param dependentCommodities the dependent commodities to update
     */
    private static void updateDependentCommodities(Trader trader,
                                                   List<Pair<Integer, Double>> dependentCommodities) {
        if (dependentCommodities == null) {
            return;
        }
        if (trader instanceof TraderWithSettings) {
            dependentCommodities.forEach(comm -> ((TraderWithSettings)trader)
                    .setBoughtQuantity(comm.getFirst(), comm.getSecond()));
        }
    }
}


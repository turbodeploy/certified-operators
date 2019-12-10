package com.vmturbo.platform.analysis.utilities;

import java.util.*;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.Context.BalanceAccount;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.ede.QuoteMinimizer;
import com.vmturbo.platform.analysis.pricefunction.QuoteFunctionFactory;
import com.vmturbo.platform.analysis.protobuf.CostDTOs;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CbtpCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO.ComputeResourceDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.DatabaseTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceCost;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceLimitation;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityCloudQuote;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityQuote;
import com.vmturbo.platform.analysis.utilities.Quote.InfiniteDependentComputeCommodityQuote;
import com.vmturbo.platform.analysis.utilities.Quote.InfiniteDependentResourcePairQuote;
import com.vmturbo.platform.analysis.utilities.Quote.LicenseUnavailableQuote;
import com.vmturbo.platform.analysis.utilities.Quote.MutableQuote;

/**
 * The factory class to construct cost function.
 */
public class CostFunctionFactory {

    private static final Logger logger = LogManager.getLogger();

    // a class to represent the minimum and maximum capacity of a commodity
    public static class CapacityLimitation {
        private double minCapacity_ = Double.POSITIVE_INFINITY;
        private double maxCapacity_ = Double.POSITIVE_INFINITY;

        public CapacityLimitation(double minCapacity, double maxCapacity) {
            minCapacity_ = minCapacity;
            maxCapacity_ = maxCapacity;
        }

        /**
         * Returns the minimal capacity limit of commodity
         */
        public double getMinCapacity() {
            return minCapacity_;
        }

        /**
         * Returns the maximal capacity limit of commodity
         */
        public double getMaxCapacity() {
            return maxCapacity_;
        }
    }

    /**
     * A class to represent the capacity constraint between two different commodities.
     * @author weiduan
     *
     */
    public static class DependentResourcePair {
        private CommoditySpecification baseCommodity_;
        private CommoditySpecification dependentCommodity_;
        private int maxRatio_;

        public DependentResourcePair(CommoditySpecification baseCommodity,
                        CommoditySpecification dependentCommodity, int maxRatio) {
            baseCommodity_ = baseCommodity;
            dependentCommodity_ = dependentCommodity;
            maxRatio_ = maxRatio;
        }

        /**
         * Returns the base commodity in the dependency pair
         */
        public CommoditySpecification getBaseCommodity() {
            return baseCommodity_;
        }

        /**
         * Returns the dependent commodity in the dependency pair
         */
        public CommoditySpecification getDependentCommodity() {
            return dependentCommodity_;
        }

        /**
         * Returns the maximal ratio of dependent commodity to base commodity
         */
        public int getMaxRatio() {
            return maxRatio_;
        }
    }

    /**
     * A class represents the price information of a commodity
     * NOTE: the PriceData comparator is overridden to make sure upperBound decides the order
     * @author weiduan
     *
     */
    @SuppressWarnings("rawtypes")
    public static class PriceData implements Comparable {
        private double upperBound_;
        private double price_;
        private boolean isUnitPrice_;
        private boolean isAccumulative_;
        private long regionId_;

        public PriceData(double upperBound, double price, boolean isUnitPrice,
                        boolean isAccumulative, long regionId) {
            upperBound_ = upperBound;
            price_ = price;
            isUnitPrice_ = isUnitPrice;
            isAccumulative_ = isAccumulative;
            regionId_ = regionId;
        }

        /**
         * Returns the upper bound limit of commodity
         */
        public double getUpperBound() {
            return upperBound_;
        }

        /**
         * Returns the price of commodity
         */
        public double getPrice() {
            return price_;
        }

        /**
         * Returns true if the price is a unit price
         */
        public boolean isUnitPrice() {
            return isUnitPrice_;
        }

        /**
         * Returns true if the cost should be accumulated
         */
        public boolean isAccumulative() {
            return isAccumulative_;
        }

        /**
         * Getter for the region id.
         *
         * @return the region id
         */
        public long getRegionId() {
            return  regionId_;
        }

        @Override
        public int compareTo(Object other) {
            return Double.compare(upperBound_, ((PriceData)other).getUpperBound());
        }
    }

    /**
     * A utility method to extract commodity capacity constraint from DTO and put it in a map.
     *
     * @param costDTO the DTO containing cost information
     * @return a map for CommoditySpecification to capacity constraint
     */
    public static Map<CommoditySpecification, CapacityLimitation>
                    translateResourceCapacityLimitation(StorageTierCostDTO costDTO) {
        Map<CommoditySpecification, CapacityLimitation> commCapacity = new HashMap<>();
        for (StorageResourceLimitation resourceLimit : costDTO.getStorageResourceLimitationList()) {
            if (!commCapacity.containsKey(resourceLimit.getResourceType())) {
                commCapacity.put(
                                ProtobufToAnalysis.commoditySpecification(
                                                resourceLimit.getResourceType()),
                                new CapacityLimitation(resourceLimit.getMinCapacity(),
                                                resourceLimit.getMaxCapacity()));
            } else {
                throw new IllegalArgumentException("Duplicate constraint on cpacity limitation");
            }
        }
        return commCapacity;
    }

    /**
     * A utility method to extract base and dependent commodities and their max ratio constraint.
     *
     * @param dependencyDTOs the DTO represents the base and dependent commodity relation
     * @return a list of {@link DependentResourcePair}
     */
    public static List<DependentResourcePair>
                    translateStorageResourceDependency(List<StorageResourceDependency> dependencyDTOs) {
        List<DependentResourcePair> dependencyList = new ArrayList<>();
        dependencyDTOs.forEach(dto -> dependencyList.add(new DependentResourcePair(
                        ProtobufToAnalysis.commoditySpecification(
                                        dto.getBaseResourceType()),
                        ProtobufToAnalysis.commoditySpecification(
                                        dto.getDependentResourceType()),
                        dto.getRatio())));
        return dependencyList;
    }

    /**
     * A utility method to extract base and dependent commodities.
     *
     * @param dependencyDTOs the DTO represents the base and dependent commodity relation
     * @return a mapping for commodities with dependency relation
     */
    public static Map<CommoditySpecification, CommoditySpecification>
                    translateComputeResourceDependency(List<ComputeResourceDependency> dependencyDTOs) {
        Map<CommoditySpecification, CommoditySpecification> dependencyMap = new HashMap<>();
        dependencyDTOs
                .forEach(dto -> dependencyMap
                        .put(ProtobufToAnalysis.commoditySpecification(dto.getBaseResourceType()),
                             ProtobufToAnalysis.commoditySpecification(dto.getDependentResourceType())));
        return dependencyMap;
    }

    /**
     * Validate if the requested amount comply with the limitation between base commodity
     * and dependency commodity
     *
     * @param sl the shopping list which requests resources
     * @param seller the seller which sells resources
     * @param dependencyList the dependency information between base and dependent commodities
     * @return A quote for the cost of the dependent resource pair. An infinite quote indicates the requested
     *         amount does not comply with the resource limitation.
     */
    private static MutableQuote getDependentResourcePairQuote(@NonNull ShoppingList sl,
                                                              @NonNull Trader seller,
                                                              @NonNull List<DependentResourcePair> dependencyList,
                                                              @Nonnull Map<CommoditySpecification, CapacityLimitation> commCapacity) {
        // check if the dependent commodity requested amount is more than the
        // max ratio * base commodity requested amount
        for (DependentResourcePair dependency : dependencyList) {
            int baseIndex = sl.getBasket().indexOf(dependency.getBaseCommodity());
            int depIndex = sl.getBasket().indexOf(dependency.getDependentCommodity());
            // we iterate over the DependentResourcePair list, which defines the dependency
            // between seller commodities. The sl is from the buyer, which may not buy all
            // resources sold by the seller thus it does not comply with the dependency constraint,
            // e.g: some VM does not request IOPS sold by IO1. The dependency
            // limitation check is irrelevant for such commodities, and we skip it.
            if (baseIndex == -1 || depIndex == -1) {
                continue;
            }
            double dependentCommodityLowerBoundCapacity =
                commCapacity.get(dependency.getDependentCommodity()).getMinCapacity();
            double baseQuantity = Math.max(sl.getQuantities()[baseIndex] * dependency.maxRatio_,
                dependentCommodityLowerBoundCapacity);
            if (baseQuantity < sl.getQuantities()[depIndex]) {
                return new InfiniteDependentResourcePairQuote(dependency, baseQuantity,
                    sl.getQuantities()[depIndex]);
            }
        }
        return CommodityQuote.zero(seller);
    }

    /**
     * Scan commodities in the shopping list to find any that the maximum capacities in the given map
     * of capacity limitations.
     *
     * @param sl the shopping list
     * @param commCapacity the information of a commodity and its minimum and maximum capacity
     * @return If all commodities fit within the max capacity, returns a number les than zero.
     *         If one or more commodities does not fit, returns the index of the first commodity encountered in
     *         the shopping list that does not fit.
     */
    private static MutableQuote insufficientCommodityWithinMaxCapacityQuote(ShoppingList sl, Trader seller,
                                               Map<CommoditySpecification, CapacityLimitation> commCapacity) {
        final CommodityQuote quote = new CommodityQuote(seller);
        // check if the commodities bought comply with capacity limitation on seller
        for (Entry<CommoditySpecification, CapacityLimitation> entry : commCapacity.entrySet()) {
            int index = sl.getBasket().indexOf(entry.getKey());
            if (index == -1) {
                // we iterate over the capacity limitation map, which defines the seller capacity
                // constraint. The sl is from the buyer, which may not buy all resources sold
                // by the seller, e.g: some VM does not request IOPS sold by IO1. The capacity
                // limitation check is irrelevant for such commodities, and we skip it.
                continue;
            }
            if (sl.getQuantities()[index] > entry.getValue().getMaxCapacity()) {
                logMessagesForCapacityLimitValidation(sl, index, entry);
                quote.addCostToQuote(Double.POSITIVE_INFINITY, entry.getValue().getMaxCapacity(), entry.getKey());
            }
        }

        return quote;
    }

    /**
     * Logs messages if the logger's trace is enabled or the seller/buyer of shopping list
     * have their debug enabled.
     *
     * @param sl the shopping list
     * @param index the index of the shopping list at which the quantity is not within range
     * @param entry the key value pair representing the commodity specification and the capacity
     *        limitation
     */
    private static void logMessagesForCapacityLimitValidation(ShoppingList sl, int index,
                    Entry<CommoditySpecification, CapacityLimitation> entry) {
        if (logger.isTraceEnabled() || sl.getBuyer().isDebugEnabled()) {
            logger.debug("{} requested amount {} of {} not within range {} to {}",
                            sl,
                            sl.getQuantities()[index],
                            sl.getBasket().get(index).getDebugInfoNeverUseInCode(),
                            entry.getValue().getMinCapacity(),
                            entry.getValue().getMaxCapacity());
        }
    }

    /**
     * Scan commodities in the shopping list to find any that exceed capacities in the seller.
     *
     * @param sl the shopping list
     * @param seller the templateProvider that supplies the resources
     * @return If all commodities in the shopping list fit within the seller, returns a number less than zero.
     *         If some commodity does not fit, returns the boughtIndex of the first commodity in that shopping list's
     *         basket that cannot fit within the seller.
     */
    private static MutableQuote insufficientCommodityWithinSellerCapacityQuote(ShoppingList sl, Trader seller, int couponCommodityBaseType) {
        // check if the commodities bought comply with capacity limitation on seller
        int boughtIndex = 0;
        Basket basket = sl.getBasket();
        final double[] quantities = sl.getQuantities();
        List<CommoditySold> commsSold = seller.getCommoditiesSold();
        final CommodityQuote quote = new CommodityQuote(seller);

        for (int soldIndex = 0; boughtIndex < basket.size();
                        boughtIndex++, soldIndex++) {
            CommoditySpecification basketCommSpec = basket.get(boughtIndex);

            // TODO Make this a list if we have more commodities to skip
            // Skip the coupon commodity
            if (basketCommSpec.getBaseType() == couponCommodityBaseType) {
                continue;
            }
            // Find corresponding commodity sold. Commodities sold are ordered the same way as the
            // basket commodities, so iterate once (O(N)) as opposed to searching each time (O(NLog(N))
            while (!basketCommSpec.isSatisfiedBy(seller.getBasketSold().get(soldIndex))) {
                soldIndex++;
            }
            double soldCapacity = commsSold.get(soldIndex).getCapacity();
            if (quantities[boughtIndex] > soldCapacity) {
                logMessagesForSellerCapacityValidation(sl, seller, quantities[boughtIndex],
                    basketCommSpec, soldCapacity);
                quote.addCostToQuote(Double.POSITIVE_INFINITY, soldCapacity, basketCommSpec);
            }
        }

        return quote;
    }

    /**
     * Logs messages if the logger's trace is enabled or the seller/buyer of shopping list
     * have their debug enabled.
     *
     * @param sl the shopping list whose capacity is being validated.
     * @param seller the seller
     * @param quantityBought the quantity of commodity requested
     * @param boughtCommSpec the commodity spec of the commodity requested
     * @param soldCapacity the capacity of the commodity of the seller
     */
    private static void logMessagesForSellerCapacityValidation(ShoppingList sl,
                    Trader seller, double quantityBought, CommoditySpecification boughtCommSpec,
                    double soldCapacity) {
        if (logger.isTraceEnabled() || seller.isDebugEnabled()
                        || sl.getBuyer().isDebugEnabled()) {
            logger.debug("{} requested amount {} of {} not within {} capacity ({})",
                            sl, quantityBought,
                            boughtCommSpec.getDebugInfoNeverUseInCode(),
                            seller, soldCapacity);
        }
    }

    /**
     * Validate if the sum of netTpUsed and ioTpUsed is within the netTpSold capacity
     *
     * @param sl the shopping list
     * @param seller the templateProvider that supplies the resources
     * @param comm1Type is the type of 1st commodity
     * @param comm2Type is the type of 2nd commodity
     * @return A quote for the dependent compute commodities. An infinite quote indicates that the
     *         dependent compute commodities do not comply with the maximum capacity.
     */
    private static MutableQuote getDependantComputeCommoditiesQuote(ShoppingList sl,
                                                                    Trader seller,
                                                                    int comm1Type,
                                                                    int comm2Type) {
        int comm1BoughtIndex = sl.getBasket().indexOf(comm1Type);
        int comm2BoughtIndex = sl.getBasket().indexOf(comm2Type);
        int comm1SoldIndex = seller.getBasketSold().indexOf(comm1Type);
        double comm1BoughtQuantity = comm1BoughtIndex != -1 ? sl.getQuantity(comm1BoughtIndex) : 0;
        double comm2BoughtQuantity = comm2BoughtIndex != -1 ? sl.getQuantity(comm2BoughtIndex) : 0;
        double boughtSum = comm1BoughtQuantity + comm2BoughtQuantity;

        double comm1SoldCapacity = seller.getCommoditiesSold().get(comm1SoldIndex).getCapacity();
        boolean isValid = boughtSum <= comm1SoldCapacity;
        logMessagesForDependentComputeCommValidation(isValid, seller, sl.getBuyer(), sl,
                        comm1BoughtIndex, comm2BoughtIndex, comm1BoughtQuantity, comm2BoughtQuantity,
                        comm1SoldIndex, comm1SoldCapacity);
        return isValid ? CommodityQuote.zero(seller) :
            new InfiniteDependentComputeCommodityQuote(comm1BoughtIndex, comm2BoughtIndex,
                comm1SoldCapacity, comm1BoughtQuantity, comm2BoughtQuantity);
    }

    /**
     * Logs messages if the logger's trace is enabled or the seller/buyer of shopping list
     * have their debug enabled.
     */
    private static void logMessagesForDependentComputeCommValidation(boolean isValid,
                    Trader seller, Trader buyer, ShoppingList sl, int comm1BoughtIndex,
                    int comm2BoughtIndex, double comm1BoughtQuantity, double comm2BoughtQuantity,
                    int comm1SoldIndex, double comm1SoldCapacity) {
        if (!isValid && (logger.isTraceEnabled() || seller.isDebugEnabled()
                        || buyer.isDebugEnabled())) {
            CommoditySpecification comm1Sold = seller.getBasketSold().get(comm1SoldIndex);
            logger.debug("{} bought ({}) + {} bought ({}) by {} is greater than {} capacity ({}) "
                            + "sold by {}", comm1BoughtIndex != -1
                                            ? sl.getBasket().get(comm1BoughtIndex)
                                                            .getDebugInfoNeverUseInCode()
                                            : "null",
                            comm1BoughtQuantity, comm2BoughtIndex != -1
                                            ? sl.getBasket().get(comm2BoughtIndex)
                                                            .getDebugInfoNeverUseInCode()
                                            : "null",
                            comm2BoughtQuantity, sl, comm1Sold != null
                                            ? comm1Sold.getDebugInfoNeverUseInCode()
                                            : "null",
                            comm1SoldCapacity, seller);
        }
    }

    /**
     * Calculate the discounted compute cost, based on available RIs discount on a cbtp.
     *
     * @param buyer {@link ShoppingList} associated with the vm that is requesting price
     * @param seller {@link Trader} the cbtp that the buyer asks a price from
     * @param cbtpResourceBundle {@link CbtpCostDTO} associated with the selling cbtp
     * @param economy {@link Economy}
     *
     * @return the cost given by {@link CostFunction}
     */
    public static MutableQuote calculateDiscountedComputeCost(ShoppingList buyer, Trader seller,
                                                              CbtpCostDTO cbtpResourceBundle,
                                                              UnmodifiableEconomy economy) {
        long groupFactor = buyer.getGroupFactor();

        final com.vmturbo.platform.analysis.economy.Context buyerContext = buyer.getBuyer()
                .getSettings().getContext();

        final CostTuple costTuple = cbtpResourceBundle.getCostTuple();
        boolean inLocationScope = isBuyerInLocationScope(costTuple, buyerContext);
        if (!inLocationScope) {
            if (logger.isTraceEnabled() || seller.isDebugEnabled()
                    || buyer.getBuyer().isDebugEnabled()) {
                logger.info("VM {} with context {} is not in scope of discounted tier {} with " +
                                "context {}", buyer.getDebugInfoNeverUseInCode(), buyerContext,
                        seller.getDebugInfoNeverUseInCode(), costTuple);
            }
            return new CommodityCloudQuote(seller, Double.POSITIVE_INFINITY, null, null);
        }
        // Match the vm with a template in order to:
        // 1) Estimate the number of coupons requested by the vm
        // 2) Determine the template cost the discount should apply to
        // Get the market for the cbtp. sellers in this market are Template Providers
        final @NonNull @ReadOnly Set<Entry<@NonNull ShoppingList, @NonNull Market>>
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

        // matchingTP is becoming null because minimizer has no best seller.
        // This can happen in 2 cases,
        // 1) when the budget of a business account is too low and then the quote in
        // budgetDepletionRiskBasedQuoteFunction is infinity
        // 2) when none of the mutableSellers can fit the buyer
        // so when it gets here, minimizer has no best seller.
        if (matchingTP == null) {
            return new CommodityCloudQuote(seller, Double.POSITIVE_INFINITY, null, null);
        }

        // Get the cost of the template matched with the vm. If this buyer represents a
        // consistent scaling group, the template cost will be for all members of the
        // group (the cost will be multiplied by the group factor)
        MutableQuote matchingTpQuote = QuoteFunctionFactory.computeCost(buyer, matchingTP, false, economy);
        Optional<EconomyDTOs.Context> context = matchingTpQuote.getContext();
        double templateCostForBuyer = matchingTpQuote.getQuoteValue();

        double costOnCurrentSupplier = QuoteFunctionFactory.computeCost(buyer, buyer.getSupplier(), false, economy)
                .getQuoteValue();

        // If we are sizing up to use a RI and we only allow resizes up to templates
        // that cost less than : (riCostFactor * cost at current supplier). If it is more, we prevent
        // such a resize to avoid cases like forcing small VMs to use large unused RIs.
        if (economy.getSettings().hasDiscountedComputeCostFactor()
                && templateCostForBuyer > economy.getSettings().getDiscountedComputeCostFactor() * costOnCurrentSupplier) {
            if (logger.isTraceEnabled() || seller.isDebugEnabled()
                    || buyer.getBuyer().isDebugEnabled()) {
                logger.info("Ignoring supplier : {} (cost : {}) for shopping list {} because "
                                + "it has cost {} times greater than current supplier {} (cost : {}).",
                                matchingTP.getDebugInfoNeverUseInCode(), templateCostForBuyer,
                                buyer.getDebugInfoNeverUseInCode(),
                                economy.getSettings().getDiscountedComputeCostFactor(),
                                buyer.getSupplier(), costOnCurrentSupplier);
            }
            return new CommodityCloudQuote(seller, Double.POSITIVE_INFINITY, null, null);
        }

        // The capacity of a coupon commodity sold by a template provider reflects the number of
        // coupons associated with the template it represents. This number is the requested amount
        // of coupons by a matching vm.
        int indexOfCouponCommByTp = matchingTP.getBasketSold()
                        .indexOfBaseType(cbtpResourceBundle.getCouponBaseType());
        CommoditySold couponCommSoldByTp =
                        matchingTP.getCommoditiesSold().get(indexOfCouponCommByTp);

        // If seller is the CBTP that the VM is currently on, check how many coupons are already covered.
        // TODO SS: check if we need to update the couponsCovered in the context
        // TODO SS: check if we want to use couponCount * groupFactor
        double currentCoverage = 0L;
//        if (seller == buyer.getSupplier()) {
        currentCoverage = buyer.getTotalAllocatedCoupons(economy, seller);
//        }
        // if the currentCouponCoverage satisfies the requestedAmount for the new template, request 0 coupons
        double requestedCoupons = Math.max(0, couponCommSoldByTp.getCapacity() * (groupFactor > 0 ? groupFactor : 1));

        // Calculate the number of available coupons from cbtp
        int indexOfCouponCommByCbtp = seller.getBasketSold()
                        .indexOfBaseType(cbtpResourceBundle.getCouponBaseType());
        CommoditySold couponCommSoldByCbtp =
                        seller.getCommoditiesSold().get(indexOfCouponCommByCbtp);

        // if the groupLeader is asking for cost, reqlinquish all coupons and provide quote for the group
        double availableCoupons =
                couponCommSoldByCbtp.getCapacity() - couponCommSoldByCbtp.getQuantity() + (groupFactor > 0 ? currentCoverage : 0);

        double singleVmTemplateCost = templateCostForBuyer / (groupFactor > 0 ? groupFactor : 1);

        double discountedCost = 0;
        double numCouponsToPayFor = 0;
        if (availableCoupons > 0) {
            if (couponCommSoldByTp.getCapacity() != 0) {
                double templateCostPerCoupon = singleVmTemplateCost / couponCommSoldByTp.getCapacity();
                // Assuming 100% discount for the portion of requested coupons satisfied by the CBTP
                numCouponsToPayFor = Math.max(0, (requestedCoupons - availableCoupons));
                if (costTuple != null && costTuple.getPrice() != 0) {
                    discountedCost = Math.max(costTuple.getPrice(),
                            numCouponsToPayFor * templateCostPerCoupon);
                } else {
                    discountedCost = numCouponsToPayFor * templateCostPerCoupon;
                }
            } else {
                logger.warn("Coupon commodity sold by {} has 0 capacity.",
                        matchingTP.getDebugInfoNeverUseInCode());
                discountedCost = Double.POSITIVE_INFINITY;
            }
        } else {
            // In case that there isn't discount available, avoid preferring a cbtp that provides
            // no discount on tp of the matching template.
            discountedCost = Double.POSITIVE_INFINITY;
        }
        // coverage in the moveContext is on the group level for the group leader
        if (buyer.getGroupFactor() > 0) {
            double totalCoverage = requestedCoupons + currentCoverage;
            return new CommodityCloudQuote(seller, discountedCost, context.get(),
                    totalCoverage, totalCoverage - numCouponsToPayFor);
        } else {
            return new CommodityCloudQuote(seller, discountedCost, context.get(),
                    requestedCoupons, requestedCoupons - numCouponsToPayFor);
        }
    }

    private static boolean isBuyerInLocationScope(final CostTuple costTuple,
                                                  final com.vmturbo.platform.analysis.economy
                                                          .Context buyerContext) {
        if (buyerContext != null) {
            return costTuple.hasZoneId() ? costTuple.getZoneId() == buyerContext.getZoneId()
                    : costTuple.getRegionId() == buyerContext.getRegionId();
        }
        return false;
    }

    /**
     * Calculates the cost of template that a shopping list has matched to
     *
     * @param seller {@link Trader} that the buyer matched to
     * @param sl is the {@link ShoppingList} that is requesting price
     * @param costTable Table that stores cost by Business Account, license type and Region.
     * @param licenseBaseType The base type of the license commodity the sl contains.
     * @return A quote for the cost given by {@link CostFunction}
     */
    private static MutableQuote calculateComputeAndDatabaseCostQuote(Trader seller, ShoppingList sl,
                                                                     CostTable costTable, final int licenseBaseType) {
        final int licenseCommBoughtIndex = sl.getBasket().indexOfBaseType(licenseBaseType);
        final long groupFactor = sl.getGroupFactor();
        final com.vmturbo.platform.analysis.economy.Context context = sl.getBuyer().getSettings().getContext();
        if (context == null) {
            logger.error("No context found for buyer {}. This may happen if the trader-business account relation" +
                    " has not been setup. Trader cannot be placed on this seller {}",
                    sl.getBuyer().getDebugInfoNeverUseInCode(), seller.getDebugInfoNeverUseInCode());
            return new CommodityQuote(seller, Double.POSITIVE_INFINITY);
        }
        final long regionIdBought = context.getRegionId();
        final BalanceAccount balanceAccount = context.getBalanceAccount();
        if (balanceAccount == null) {
            logger.warn("Business account is not found on seller: {}, for shopping list: {}, return " +
                            "infinity compute quote", seller.getDebugInfoNeverUseInCode(),
                    sl.getDebugInfoNeverUseInCode());
            return new CommodityQuote(seller, Double.POSITIVE_INFINITY);
        }
        final long accountId = costTable.hasAccountId(balanceAccount.getPriceId()) ?
                balanceAccount.getPriceId() : balanceAccount.getId();

        if (!costTable.hasAccountId(accountId)) {
            logger.warn("Business account id {} is not found on seller: {}, for shopping list: {}, "
                            + "return infinity compute quote", accountId,
                    seller.getDebugInfoNeverUseInCode(), sl.getDebugInfoNeverUseInCode());
            return new CommodityQuote(seller, Double.POSITIVE_INFINITY);
        }

        final int licenseTypeKey;
        // NOTE: CostTable.NO_VALUE (-1) is the no license commodity type
        if (licenseCommBoughtIndex == CostTable.NO_VALUE) {
            licenseTypeKey = licenseCommBoughtIndex;
            logger.error("License access commodity is missing in shopping list: {}, "
                    + "license base type: {}",
                    sl.getDebugInfoNeverUseInCode(), licenseBaseType);
        } else {
            licenseTypeKey = sl.getBasket().get(licenseCommBoughtIndex).getType();
        }

        CostTuple costTuple = costTable.getTuple(regionIdBought, accountId, licenseTypeKey);
        if (costTuple == null) {
            logger.error("Cannot find type {} and region {} in costMap, license base type: {}",
                    licenseTypeKey, regionIdBought, licenseBaseType);
            // NOTE: CostTable.NO_VALUE (-1) is the no license commodity type and the same for region.
            costTuple = costTable.getTuple(CostTable.NO_VALUE, accountId, CostTable.NO_VALUE);
        }

        final Long regionId = costTuple.getRegionId();
        final double cost = costTuple.getPrice();
        // NOTE: CostTable.NO_VALUE (-1) is the no license commodity type
        return Double.isInfinite(cost) && licenseCommBoughtIndex != CostTable.NO_VALUE ?
                new LicenseUnavailableQuote(seller, sl.getBasket().get(licenseCommBoughtIndex)) :
                new CommodityCloudQuote(seller, cost * (groupFactor > 0 ? groupFactor : 1), regionId, accountId);
    }

    /**
     * Creates {@link CostFunction} for a given seller.
     *
     * @param costDTO the DTO carries the cost information
     * @return CostFunction
     */
    public static @NonNull CostFunction createCostFunction(CostDTO costDTO) {
        switch (costDTO.getCostTypeCase()) {
            case STORAGE_TIER_COST:
                return createCostFunctionForStorageTier(costDTO.getStorageTierCost());
            case COMPUTE_TIER_COST:
                return createCostFunctionForComputeTier(costDTO.getComputeTierCost());
            case DATABASE_TIER_COST:
                return createCostFunctionForDatabaseTier(costDTO.getDatabaseTierCost());
            case CBTP_RESOURCE_BUNDLE:
                return createResourceBundleCostFunctionForCbtp(costDTO.getCbtpResourceBundle());
            default:
                throw new IllegalArgumentException("input = " + costDTO);
        }
    }


    /**
     * Create {@link CostFunction} by extracting data from {@link ComputeTierCostDTO}
     *
     * @param costDTO the DTO carries the data used to construct cost function
     * @return CostFunction
     */
    public static @NonNull CostFunction createCostFunctionForComputeTier(ComputeTierCostDTO costDTO) {
        final CostTable costTable = new CostTable(costDTO.getCostTupleListList());
        Map<CommoditySpecification, CommoditySpecification> dependencyMap =
                translateComputeResourceDependency(costDTO.getComputeResourceDepedencyList());
        final int licenseBaseType = costDTO.getLicenseCommodityBaseType();
        CostFunction costFunction = new CostFunction() {
            @Override
            public MutableQuote calculateCost(ShoppingList buyer, Trader seller, boolean validate,
                    UnmodifiableEconomy economy) {
                if (!validate) {
                    // seller is the currentSupplier. Just return cost
                    return calculateComputeAndDatabaseCostQuote(seller, buyer, costTable, licenseBaseType);
                }

                int couponCommodityBaseType = costDTO.getCouponBaseType();
                final MutableQuote capacityQuote =
                        insufficientCommodityWithinSellerCapacityQuote(buyer, seller, couponCommodityBaseType);
                if (capacityQuote.isInfinite()) {
                    return capacityQuote;
                }

                for (Entry<CommoditySpecification, CommoditySpecification> dependency
                                : dependencyMap.entrySet()) {
                    final MutableQuote dependantComputeCommoditiesQuote =
                            getDependantComputeCommoditiesQuote(buyer, seller, dependency.getKey().getType(),
                                                                dependency.getValue().getType());
                    if (dependantComputeCommoditiesQuote.isInfinite()) {
                        return dependantComputeCommoditiesQuote;
                    }
                }

                return calculateComputeAndDatabaseCostQuote(seller, buyer, costTable, licenseBaseType);
            }
        };
        return costFunction;
    }

    /**
     * Create {@link CostFunction} by extracting data from {@link CbtpCostDTO}
     *
     * @param cbtpResourceBundle the DTO carries the data used to construct cost function for cbtp
     * @return CostFunction
     */
    public static @NonNull CostFunction
                    createResourceBundleCostFunctionForCbtp(CbtpCostDTO cbtpResourceBundle) {

        CostFunction costFunction = new CostFunction() {
            @Override
            public MutableQuote calculateCost(ShoppingList buyer, Trader seller,
                                        boolean validate, UnmodifiableEconomy economy) {

                int couponCommodityBaseType = cbtpResourceBundle.getCouponBaseType();
                final MutableQuote quote =
                    insufficientCommodityWithinSellerCapacityQuote(buyer, seller,  couponCommodityBaseType);
                if (quote.isInfinite()) {
                    return quote;
                }
                if (!validate) {
                    // In case that a vm is already placed on a cbtp, We return the cost based on the best template it fits on
                    return calculateDiscountedComputeCostForCurrentSupplierOnCBTPForTier(buyer, seller,
                            cbtpResourceBundle, economy);
                }

                return calculateDiscountedComputeCost(buyer, seller, cbtpResourceBundle, economy);
            }
        };

        return costFunction;
    }

    public static MutableQuote calculateDiscountedComputeCostForCurrentSupplierOnCBTPForTier(ShoppingList buyer, Trader cbtp,
                                                CbtpCostDTO cbtpResourceBundle, UnmodifiableEconomy economy) {
        // Match the vm with a template in order to:
        // 1) Estimate the number of coupons requested by the vm
        // 2) Determine the template cost the discount should apply to
        // Get the market for the cbtp. sellers in this market are Template Providers
        final @NonNull @ReadOnly Set<Entry<@NonNull ShoppingList, @NonNull Market>>
                shoppingListsInMarket = economy.getMarketsAsBuyer(cbtp).entrySet();
        Market market = shoppingListsInMarket.iterator().next().getValue();
        long groupFactor = buyer.getGroupFactor();
        double discountedCost = 0;
        Optional<EconomyDTOs.Context> context = Optional.empty();
        if (groupFactor > 1) {
            // in order to make CSG VMs always shop
            discountedCost = Double.POSITIVE_INFINITY;
        } else {
            double totalRequestedCoupons = buyer.getTotalRequestedCoupons(economy, cbtp);
            if (totalRequestedCoupons != 0) {
                int indexOfCouponCommSoldByCbtp = cbtp.getBasketSold().indexOfBaseType(cbtpResourceBundle.getCouponBaseType());
                CommoditySpecification couponCommSpec = cbtp.getBasketSold().get(indexOfCouponCommSoldByCbtp);
                Optional<Trader> optionalTrader = market.getActiveSellers().stream().filter(t ->
                        t.getCommoditySold(couponCommSpec).getCapacity() == totalRequestedCoupons)
                        .findFirst();
                if (optionalTrader.isPresent()) {
                    Trader matchingTP = optionalTrader.get();
                    MutableQuote matchingTpQuote = QuoteFunctionFactory.computeCost(buyer, matchingTP, false, economy);
                    context = matchingTpQuote.getContext();
                    double currentTemplateCostForBuyer = matchingTpQuote.getQuoteValue();

                    // The capacity of a coupon commodity sold by a template provider reflects the number of
                    // coupons associated with the template it represents. This number is the requested amount
                    // of coupons by a matching vm.
                    int indexOfCouponCommByTp = matchingTP.getBasketSold()
                            .indexOfBaseType(cbtpResourceBundle.getCouponBaseType());
                    CommoditySold couponCommSoldByTp =
                            matchingTP.getCommoditiesSold().get(indexOfCouponCommByTp);
                    double requestedCoupons = couponCommSoldByTp.getCapacity() * groupFactor;

                    double singleVmTemplateCost = currentTemplateCostForBuyer / (groupFactor > 0 ? groupFactor : 1);
                    final CostTuple costTuple = cbtpResourceBundle.getCostTuple();
                    // TODO SS: Check if we need to consider groupFactor for couponsCovered
                    double couponsCovered = buyer.getTotalAllocatedCoupons(economy, matchingTP);
                    if (couponCommSoldByTp.getCapacity() != 0) {
                        double templateCostPerCoupon = singleVmTemplateCost / couponCommSoldByTp.getCapacity();
                        // Assuming 100% discount for the portion of requested coupons satisfied by the CBTP
                        double numCouponsToPayFor = Math.max(0, (requestedCoupons - couponsCovered));
                        if (costTuple != null && costTuple.getPrice() != 0) {
                            discountedCost = Math.max(costTuple.getPrice(),
                                    numCouponsToPayFor * templateCostPerCoupon);
                        } else {
                            discountedCost = numCouponsToPayFor * templateCostPerCoupon;
                        }
                    } else {
                        logger.warn("Coupon commodity sold by {} has 0 capacity.",
                                matchingTP.getDebugInfoNeverUseInCode());
                        discountedCost = Double.POSITIVE_INFINITY;
                    }
                } else {
                    // TODO SS: check if context needs to be updated here
                    discountedCost = Double.POSITIVE_INFINITY;
                }
            }
        }
        return new CommodityCloudQuote(cbtp, discountedCost, context.isPresent() ? context.get().getRegionId()
                : null, context.isPresent() ? context.get().getBalanceAccount().getId() : null);
    }

    /**
     * Create {@link CostFunction} by extracting data from {@link StorageTierCostDTO}
     *
     * @param costDTO the DTO carries the data used to construct cost function for storage
     * @return CostFunction
     */
    public static @NonNull CostFunction createCostFunctionForStorageTier(StorageTierCostDTO costDTO) {
        // the map to keep commodity to its min max capacity limitation
        Map<CommoditySpecification, CapacityLimitation> commCapacity =
                        translateResourceCapacityLimitation(costDTO);
        List<StorageResourceCost> resourceCost = costDTO.getStorageResourceCostList();
        // a map of map to keep the commodity to its price per business account
        Map<CommoditySpecification, Map<Long, List<PriceData>>> priceDataMap =
                        translateResourceCostForStorageTier(resourceCost);
        // the capacity constraint between commodities
        List<DependentResourcePair> dependencyList =
                        translateStorageResourceDependency(costDTO.getStorageResourceDependencyList());

        CostFunction costFunction = new CostFunction() {
            @Override
            public MutableQuote calculateCost(ShoppingList buyer, Trader seller,
                                        boolean validate, UnmodifiableEconomy economy) {
                if (seller == null) {
                    return CommodityQuote.zero(seller);
                }
                final MutableQuote maxCapacityQuote = insufficientCommodityWithinMaxCapacityQuote(
                    buyer, seller, commCapacity);
                if (maxCapacityQuote.isInfinite()) {
                    return maxCapacityQuote;
                }

                final MutableQuote dependentCommodityQuote =
                    getDependentResourcePairQuote(buyer, seller, dependencyList, commCapacity);
                if (dependentCommodityQuote.isInfinite()) {
                    return dependentCommodityQuote;
                }
                return calculateStorageTierCost(priceDataMap, commCapacity, buyer, seller);
            }
        };

        return costFunction;
    }

    /**
     * Create {@link CostFunction} by extracting data from {@link ComputeTierCostDTO}
     *
     * @param costDTO the DTO carries the data used to construct cost function
     * @return CostFunction
     */
    public static @NonNull CostFunction createCostFunctionForDatabaseTier(DatabaseTierCostDTO costDTO) {
        CostTable costTable = new CostTable(costDTO.getCostTupleListList());
        final int licenseBaseType = costDTO.getLicenseCommodityBaseType();

        CostFunction costFunction = new CostFunction() {
            @Override
            public MutableQuote calculateCost(ShoppingList buyer, Trader seller, boolean validate,
                                              UnmodifiableEconomy economy) {
                if (!validate) {
                    // seller is the currentSupplier. Just return cost
                    return calculateComputeAndDatabaseCostQuote(seller, buyer, costTable, licenseBaseType);
                }

                int couponCommodityBaseType = costDTO.getCouponBaseType();
                final MutableQuote capacityQuote =
                        insufficientCommodityWithinSellerCapacityQuote(buyer, seller, couponCommodityBaseType);
                if (capacityQuote.isInfinite()) {
                    return capacityQuote;
                }

                return calculateComputeAndDatabaseCostQuote(seller, buyer, costTable, licenseBaseType);
            }
        };
        return costFunction;
    }

    /**
     * A utility method to construct storage pricing information.
     *
     * @param resourceCostList a list of cost data
     * @return map The map has commodity specification as key and a mapping of business account to
     * price as value.
     */
    private static Map<CommoditySpecification, Map<Long, List<PriceData>>>
            translateResourceCostForStorageTier(List<StorageResourceCost> resourceCostList) {
        Map<CommoditySpecification, Map<Long, List<CostFunctionFactory.PriceData>>> priceDataMap =
                        new HashMap<>();
        for (StorageResourceCost resource : resourceCostList) {
            if (!priceDataMap.containsKey(resource.getResourceType())) {
                Map<Long, List<CostFunctionFactory.PriceData>> priceDataPerBusinessAccount = new HashMap<>();
                for (CostDTOs.CostDTO.StorageTierCostDTO.StorageTierPriceData priceData : resource.getStorageTierPriceDataList()) {
                    for (CostDTO.CostTuple costTuple : priceData.getCostTupleListList()) {
                        long businessAccountId = costTuple.getBusinessAccountId();
                        CostFunctionFactory.PriceData price = new PriceData(priceData.getUpperBound(),
                                costTuple.getPrice(),
                                priceData.getIsUnitPrice(),
                                priceData.getIsAccumulativeCost(),
                                costTuple.getRegionId());
                        if (priceDataPerBusinessAccount.containsKey(businessAccountId)) {
                            List<CostFunctionFactory.PriceData> currentPriceDataList = priceDataPerBusinessAccount
                                    .get(businessAccountId);
                            currentPriceDataList.add(price);
                        } else {
                            priceDataPerBusinessAccount.put(businessAccountId, new ArrayList<>(Arrays.asList(price)));
                        }

                    }
                }
                // make sure price list is ascending based on upper bound, because we need to get the
                // first resource range that can satisfy the requested amount, and the price
                // corresponds to that range will be used as price
                priceDataPerBusinessAccount.values().stream().forEach(list -> Collections.sort(list));
                priceDataMap.put(ProtobufToAnalysis.commoditySpecification(resource.getResourceType()),
                                 priceDataPerBusinessAccount);
            } else {
                throw new IllegalArgumentException("Duplicate entries for a commodity price");
            }
        }
        return priceDataMap;
    }

    /**
     * Calculates the total cost of all resources requested by a shopping list on a seller.
     *
     * @param priceDataMap the map containing commodity and its pricing information
     * on a seller
     * @param commCapacity the information of a commodity and its minimum and maximum capacity
     * @param sl the shopping list requests resources
     * @param seller the seller
     *
     * @return the Quote given by {@link CostFunction}
     */
    public static CommodityQuote calculateStorageTierCost(@NonNull Map<CommoditySpecification, Map<Long, List<PriceData>>> priceDataMap,
                                                               Map<CommoditySpecification, CapacityLimitation> commCapacity,
                                                               @NonNull ShoppingList sl, Trader seller) {
        // TODO: refactor the PriceData to improve performance for region and business account lookup
        double cost = 0;
        Long businessAccountChosenId = null;
        com.vmturbo.platform.analysis.economy.Context context = sl.getBuyer().getSettings().getContext();
        if (context == null) {
            logger.error("No context found for buyer {}. This may happen if the trader-business account relation" +
                    "has not been setup. Trader cannot be placed on this seller {}", sl.getBuyer().getDebugInfoNeverUseInCode(),
                    seller.getDebugInfoNeverUseInCode());
            return new CommodityQuote(seller, Double.POSITIVE_INFINITY);
        }
        final long regionId = context.getRegionId();
        for (Entry<CommoditySpecification, Map<Long, List<PriceData>>> commodityPrice : priceDataMap.entrySet()) {
            int i = sl.getBasket().indexOf(commodityPrice.getKey());
            if (i == -1) {
                // we iterate over the price data map, which defines the seller commodity price.
                // The sl is from the buyer, which may not buy all resources sold
                // by the seller, e.g: some VM does not request IOPS sold by IO1. We can skip it
                // when trying to compute cost.
                continue;
            }
            // calculate cost based on amount that is adjusted due to minimum capacity constraint
            double requestedAmount = sl.getQuantities()[i];
            if (commCapacity.containsKey(sl.getBasket().get(i))) {
                requestedAmount = Math.max(requestedAmount,
                                           commCapacity.get(sl.getBasket().get(i)).getMinCapacity());
            }
            double previousUpperBound = 0;
            final BalanceAccount balanceAccount = sl.getBuyer().getSettings().getContext().getBalanceAccount();
            if (balanceAccount == null) {
                logger.warn("Business account is not found for shopping list: {}, return infinity storage quote",
                        sl.getDebugInfoNeverUseInCode());

                return new CommodityCloudQuote(seller, Double.POSITIVE_INFINITY, regionId, null);
            }
            // priceMap may contain PriceData by price id. Price id is the identifier for a price
            // offering associated with a Balance Account. Different Balance Accounts (i.e.
            // Balance Accounts with different ids) may have the same price id, if they are
            // associated with the same price offering. If no entry is found in the priceMap for a
            // price id, then the Balance Account id is used to lookup the priceMap.
            final long priceId = balanceAccount.getPriceId();
            final long balanceAccountId = balanceAccount.getId();
            final Map<Long, List<PriceData>> priceMap = commodityPrice.getValue();
            businessAccountChosenId = priceMap.containsKey(priceId) ? priceId : balanceAccountId;
            final List<PriceData> priceDataList = priceMap.get(businessAccountChosenId);

            if (priceDataList == null) {
                if (logger.isTraceEnabled() || sl.getBuyer().isDebugEnabled()) {
                    logger.warn("No entry found in cost map for shopping list: {}, " +
                                    "using priceId: {}, balanceAccountId: {}, priceMap keys: {}",
                            sl.getDebugInfoNeverUseInCode(), priceId, businessAccountChosenId,
                            priceMap.keySet());
                }
                return new CommodityCloudQuote(seller, Double.POSITIVE_INFINITY, regionId, balanceAccountId);
            }

            List<PriceData> pricesScopedToregion = priceDataList.stream().filter(s -> s.getRegionId() == regionId).collect(Collectors.toList());
            for (PriceData priceData : pricesScopedToregion) {
                // the list of priceData is sorted based on upper bound
                double currentUpperBound = priceData.getUpperBound();
                if (priceData.isAccumulative()) {
                    // if the price is accumulative, we need to sum up all the cost where
                    // requested amount is more than upper bound till we find the exact range
                    cost += (priceData.isUnitPrice()
                                    ? priceData.getPrice() * Math.min(
                                                                      currentUpperBound - previousUpperBound,
                                                                      requestedAmount - previousUpperBound)
                                                    : priceData.getPrice());
                    // we find the exact range the requested amount falls
                    if (requestedAmount <= currentUpperBound) {
                        break;
                    }
                } else if (!priceData.isAccumulative() && requestedAmount > previousUpperBound
                                && requestedAmount <= currentUpperBound) {
                    // non accumulative cost only depends on the exact range where the requested
                    // amount falls
                    cost += (priceData.isUnitPrice() ? priceData.getPrice() *
                                    requestedAmount : priceData.getPrice());
                }
                previousUpperBound = currentUpperBound;
            }
        }
        return new CommodityCloudQuote(seller, cost, regionId, businessAccountChosenId);
    }
}

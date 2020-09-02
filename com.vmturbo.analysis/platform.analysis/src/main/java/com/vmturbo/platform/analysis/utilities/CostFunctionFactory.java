package com.vmturbo.platform.analysis.utilities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.commons.Pair;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Context;
import com.vmturbo.platform.analysis.economy.Context.BalanceAccount;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.ede.QuoteMinimizer;
import com.vmturbo.platform.analysis.pricefunction.QuoteFunctionFactory;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CbtpCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO.ComputeResourceDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.DatabaseTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.RangeTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceCost;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceLimitation;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceRangeDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceRatioDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageTierPriceData;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
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
    private static final double riCostDeprecationFactor = 0.00001;

    // a class to represent the minimum and maximum capacity of a commodity
    public static class CapacityLimitation {
        private double minCapacity_;
        private double maxCapacity_;

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
        private double maxRatio_;

        public DependentResourcePair(CommoditySpecification baseCommodity,
                        CommoditySpecification dependentCommodity, double maxRatio) {
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
        public double getMaxRatio() {
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
                throw new IllegalArgumentException("Duplicate constraint on capacity limitation");
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
                    translateStorageResourceDependency(List<StorageResourceRatioDependency> dependencyDTOs) {
        List<DependentResourcePair> dependencyList = new ArrayList<>();
        dependencyDTOs.forEach(dto -> dependencyList.add(new DependentResourcePair(
                        ProtobufToAnalysis.commoditySpecification(
                                        dto.getBaseResourceType()),
                        ProtobufToAnalysis.commoditySpecification(
                                        dto.getDependentResourceType()),
                        dto.getMaxRatio())));
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
    public static MutableQuote insufficientCommodityWithinSellerCapacityQuote(ShoppingList sl, Trader seller, int couponCommodityBaseType) {
        // check if the commodities bought comply with capacity limitation on seller
        if (!sl.getBasket().isSatisfiedBy(seller.getBasketSold())) {
            // buyer basket not satisfied by seller providing quote
            return new CommodityQuote(seller, Double.POSITIVE_INFINITY);
        }
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
            while (!basketCommSpec.equals(seller.getBasketSold().get(soldIndex))) {
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
     * @param costTable containing costTuples indexed by location id, account id and license
     *                  commodity type.
     *
     * @return the cost given by {@link CostFunction}
     */
    public static MutableQuote calculateDiscountedComputeCost(ShoppingList buyer, Trader seller,
                                                              CbtpCostDTO cbtpResourceBundle,
                                                              UnmodifiableEconomy economy,
                                                              CostTable costTable,
                                                              final int licenseBaseType) {
        long groupFactor = buyer.getGroupFactor();
        @Nullable final Context buyerContext = buyer.getBuyer().getSettings()
                .getContext().orElse(null);
        final int licenseCommBoughtIndex = buyer.getBasket().indexOfBaseType(licenseBaseType);
        if (costTable.getAccountIds().isEmpty()) {
            // empty cost table, return infinity to not place entity on this seller
            logger.warn("No cost information found for seller {}, return infinity quote",
                    seller.getDebugInfoNeverUseInCode());
            return new CommodityQuote(seller, Double.POSITIVE_INFINITY);
        }
        if (licenseCommBoughtIndex == -1) {
            // when there is no license for the shopping list, return infinity quote
            // NOTE: we assume that on prem entities have to contain LicenseAccessCommodity
            logger.warn("No license commodity found for buyer {}, return infinity quote",
                    buyer.getBuyer().getDebugInfoNeverUseInCode());
            return new CommodityQuote(seller, Double.POSITIVE_INFINITY);
        }
        final int licenseTypeKey = buyer.getBasket().get(licenseCommBoughtIndex).getType();
        final CostTuple costTuple = retrieveCbtpCostTuple(buyerContext, cbtpResourceBundle,
                costTable, licenseTypeKey);
        if (costTuple == null) {
            if (logger.isTraceEnabled() || seller.isDebugEnabled()
                    || buyer.getBuyer().isDebugEnabled()) {
                logger.info("VM {} with context {} is not in scope of discounted tier {} with " +
                                "context {}", buyer.getDebugInfoNeverUseInCode(), buyerContext,
                        seller.getDebugInfoNeverUseInCode(),
                        cbtpResourceBundle.getCostTupleListList());
            }
            return new CommodityCloudQuote(seller, Double.POSITIVE_INFINITY, null, null);
        }

        Trader destTP = findMatchingTpForCalculatingCost(economy, seller, buyer);

        // destTP is becoming null because minimizer has no best seller.
        // This can happen in 2 cases,
        // 1) when the budget of a business account is too low and then the quote in
        // budgetDepletionRiskBasedQuoteFunction is infinity
        // 2) when none of the mutableSellers can fit the buyer
        // so when it gets here, minimizer has no best seller.
        if (destTP == null) {
            return new CommodityCloudQuote(seller, Double.POSITIVE_INFINITY, null, null);
        }

        // Get the cost of the template matched with the vm. If this buyer represents a
        // consistent scaling group, the template cost will be for all members of the
        // group (the cost will be multiplied by the group factor)
        MutableQuote destTPQuote = QuoteFunctionFactory.computeCost(buyer, destTP, false, economy);
        Optional<EconomyDTOs.Context> context = destTPQuote.getContext();
        double templateCostForBuyer = destTPQuote.getQuoteValue();

        // for VMs placed on a CBTP, we need the price on the associated TP. We need this since we return infinite
        // price on the CBTP when it is the current supplier.
        Trader currentTP = findMatchingTpForCalculatingCost(economy, buyer.getSupplier(), buyer);
        double costOnCurrentSupplier = QuoteFunctionFactory.computeCost(buyer, currentTP, false, economy)
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
                                destTP.getDebugInfoNeverUseInCode(), templateCostForBuyer,
                                buyer.getDebugInfoNeverUseInCode(),
                                economy.getSettings().getDiscountedComputeCostFactor(),
                                buyer.getSupplier(), costOnCurrentSupplier);
            }
            return new CommodityCloudQuote(seller, Double.POSITIVE_INFINITY, null, null);
        }

        // The capacity of a coupon commodity sold by a template provider reflects the number of
        // coupons associated with the template it represents. This number is the requested amount
        // of coupons by a matching vm.
        int indexOfCouponCommByTp = destTP.getBasketSold()
                        .indexOfBaseType(cbtpResourceBundle.getCouponBaseType());
        CommoditySold couponCommSoldByTp =
                destTP.getCommoditiesSold().get(indexOfCouponCommByTp);

        // If seller is the CBTP that the VM is currently on, check how many coupons are already covered.
        double currentCoverage = buyer.getTotalAllocatedCoupons(economy, seller);
        // if the currentCouponCoverage satisfies the requestedAmount for the new template, request 0 coupons
        double requestedCoupons = Math.max(0, couponCommSoldByTp.getCapacity() * (groupFactor > 0 ? groupFactor : 1));

        // Calculate the number of available coupons from cbtp
        int indexOfCouponCommByCbtp = seller.getBasketSold()
                        .indexOfBaseType(cbtpResourceBundle.getCouponBaseType());
        int indexOfCouponCommBought = buyer.getBasket()
                .indexOfBaseType(cbtpResourceBundle.getCouponBaseType());
        CommoditySold couponCommSoldByCbtp =
                        seller.getCommoditiesSold().get(indexOfCouponCommByCbtp);

        // if the groupLeader is asking for cost, reqlinquish all coupons and provide quote for the group
        double availableCoupons =
                couponCommSoldByCbtp.getCapacity() - couponCommSoldByCbtp.getQuantity();
        if (groupFactor > 0) {
            // when leader shops, relinquish group's coverage
            availableCoupons += currentCoverage;
        } else if (seller == buyer.getSupplier()) {
            // when peer shops consider the coupons it used from the supplier. If we dont do that, VM might end up not
            // finding the coupon and might move out of CBTP. We did not do this before before peer always relinquished
            // before it shopped
            availableCoupons += buyer.getQuantity(indexOfCouponCommBought);
        }
        double singleVmTemplateCost = templateCostForBuyer / (groupFactor > 0 ? groupFactor : 1);

        double discountedCost = 0;
        double numCouponsToPayFor = 0;

        if (availableCoupons > 0) {

            if (couponCommSoldByTp.getCapacity() != 0) {
                final double matchingTpCouponCapacity = couponCommSoldByTp.getCapacity();
                final double templateCostPerCoupon = singleVmTemplateCost / matchingTpCouponCapacity;
                // Assuming 100% discount for the portion of requested coupons satisfied by the CBTP
                numCouponsToPayFor = Math.max(0, (requestedCoupons - availableCoupons));


                final double numCouponsCovered = requestedCoupons - numCouponsToPayFor;
                final double cbtpResellerRate = calculateCbtpResellerPrice(costTuple, destTP, singleVmTemplateCost);
                final double cbtpResellerCost = cbtpResellerRate * (numCouponsCovered / matchingTpCouponCapacity);

                discountedCost = numCouponsToPayFor * templateCostPerCoupon + cbtpResellerCost;

            } else {
                logger.warn("Coupon commodity sold by {} has 0 capacity.",
                        destTP.getDebugInfoNeverUseInCode());
                discountedCost = Double.POSITIVE_INFINITY;
            }
        } else {
            numCouponsToPayFor = requestedCoupons;
            // In case that there isn't discount available, avoid preferring a cbtp that provides
            // no discount on tp of the matching template.
            discountedCost = Double.POSITIVE_INFINITY;
        }
        // coverage in the moveContext is on the group level for the group leader
        if (buyer.getGroupFactor() > 0) {
            return new CommodityCloudQuote(seller, discountedCost, context.get(),
                    requestedCoupons, requestedCoupons - numCouponsToPayFor, economy);
        } else {
            return new CommodityCloudQuote(seller, discountedCost, context.get(),
                    buyer.getTotalRequestedCoupons(economy, seller),
                    currentCoverage + requestedCoupons - numCouponsToPayFor, economy);
        }
    }


    /**
     * Calculates the price for a CBTP, based on reselling TP commodities. The price calculation takes
     * into account a general reseller fee as part of the CBTP, which may represent the nominal fee
     * used to "size" CBTPs, as well as a core-based license fee based on the destination TP.
     *
     * @param costTuple The {@link CostTuple} of the CBTP
     * @param matchingTp The destination template provider the CBTP will resell
     * @param matchingTpPrice the on demand cost of the matching TP
     * @return The price/rate of the CBTP. This price does not take into account the coupons bought
     * on the CBTP and is therefore representative of the rate, not cost.
     */
    private static double calculateCbtpResellerPrice(@NonNull CostTuple costTuple,
                                                     @NonNull Trader matchingTp,
                                                     double matchingTpPrice) {

        double corePrice = 0.0;

        if (costTuple.hasCoreCommodityType() && costTuple.getCoreCommodityType() >= 0) {
            final int indexOfCoreCommodity =
                    matchingTp.getBasketSold().indexOf(costTuple.getCoreCommodityType());

            final CommoditySold coreCommSoldByMatchingTp = (indexOfCoreCommodity  >= 0) ?
                    matchingTp.getCommoditiesSold().get(indexOfCoreCommodity) : null;

            if (coreCommSoldByMatchingTp != null) {
                final long coreCapacity = Math.round(coreCommSoldByMatchingTp.getCapacity());
                corePrice = costTuple.getPriceByNumCoresMap().getOrDefault(coreCapacity, 0.0);
            }
        }

        // If there is no core price, then return a very small fraction of the on-demand price of
        // the matching TP.
        // The CBTP cost tuple itself will have a cost which corresponds with the cost of the
        // largest tier in that RI's family. But it does not make sense to look at that price since
        // this Shopping list could be going to some other tier in that family. So we should be
        // looking at the on demand price of that matching tier.
        // So we return a price which will be a fraction of the price of matching TP.
        if (corePrice == 0.0) {
            return matchingTpPrice * riCostDeprecationFactor;
        } else {
            return corePrice;
        }
    }

    /**
     * Find the TP that the trader best fits on.
     *
     * @param economy {@link Economy}
     * @param seller {@link Trader} the cbtp/tp that the buyer asks price from
     * @param buyer {@link ShoppingList} associated with the vm that is requesting price
     *
     * @return the Trader that the buyer best fits on.
     */
    private static Trader findMatchingTpForCalculatingCost(UnmodifiableEconomy economy, Trader seller, ShoppingList buyer) {
        if (seller == null) {
            return null;
        }
        // Match the vm with a template in order to:
        // 1) Estimate the number of coupons requested by the vm
        // 2) Determine the template cost the discount should apply to
        // Get the market for the cbtp. sellers in this market are Template Providers
        final @NonNull @ReadOnly Set<Entry<@NonNull ShoppingList, @NonNull Market>>
                shoppingListsInMarket = economy.getMarketsAsBuyer(seller).entrySet();
        if (shoppingListsInMarket.isEmpty()) {
            // if TP doesnt have any underlying markets where it shops, return TP as the seller
            return seller;
        }
        Market market = shoppingListsInMarket.iterator().next().getValue();
        List<Trader> sellers = market.getActiveSellers();
        List<Trader> mutableSellers = new ArrayList<Trader>();
        mutableSellers.addAll(sellers);
        mutableSellers.retainAll(economy.getMarket(buyer).getActiveSellers());
        if (mutableSellers.isEmpty()) {
            // seller is a TP
            return seller;
        }

        // else, seller is a CBTP so find the best TP
        // Get cheapest quote, that will be provided by the matching template
        final QuoteMinimizer minimizer = mutableSellers.stream().collect(
                () -> new QuoteMinimizer(economy, buyer), QuoteMinimizer::accept,
                QuoteMinimizer::combine);
        return minimizer.getBestSeller();
    }
    /**
     * Retrieves a CostTuple from the provided CostTable using cbtpScopeIds, region/zone and
     * license commodity index from the buyerContext.
     *
     * @param buyerContext for which the CostTuple is being retrieved.
     * @param cbtpCostDTO CBTP cost DTO.
     * @param costTable from which the CostTuple is being retrieved.
     * @return costTuple from the CostTable or {@code null} if not found.
     */
    @Nullable
    @VisibleForTesting
    protected static CostTuple retrieveCbtpCostTuple(
            final @Nullable Context buyerContext,
            final @Nonnull CbtpCostDTO cbtpCostDTO,
            final @Nonnull CostTable costTable,
            final int licenseTypeKey) {
        if (buyerContext == null) {
            // on prem entities has no context, iterating all ba and region to get cheapest cost
            return getCheapestTuple(costTable, licenseTypeKey);
        }

        final BalanceAccount balanceAccount = buyerContext.getBalanceAccount();
        final long priceId = balanceAccount.getPriceId();
        final long regionId = buyerContext.getRegionId();
        final long zoneId = buyerContext.getZoneId();

        // Match CbtpCostDTO based on billing family ID (if present) or business account ID
        final Set<Long> cbtpScopeIds = ImmutableSet.copyOf(cbtpCostDTO.getScopeIdsList());
        if (cbtpScopeIds.contains(balanceAccount.getParentId())
                || cbtpScopeIds.contains(balanceAccount.getId())) {
            // costTable will be indexed using zone id, if the CBTP is scoped to a zone.
            // Otherwise the costTable will be indexed using the region id.
            CostTuple costTuple = costTable.getTuple(regionId, priceId, licenseTypeKey);
            if (costTuple != null) {
                return costTuple;
            }
            costTuple = costTable.getTuple(zoneId, priceId, licenseTypeKey);
            if (costTuple != null) {
                return costTuple;
            }
        }

        return null;
    }

    /**
     * Returns the cheapest tuple in the CostTable.
     *
     * @param costTable the information for pricing
     * @param licenseTypeKey the type of license access commodity
     * @return the cheapest tuple in the CostTable
     */
    private static CostTuple getCheapestTuple(@Nonnull final CostTable costTable, final int licenseTypeKey) {
        double cheapestCost = Double.MAX_VALUE;
        CostTuple cheapestTuple = null;
        for (long id : costTable.getAccountIds()) {
            // The cheapest cost for a given license and ba in all region is kept in the costTable.
            // The key of that cheapest cost tuple is {NO_VALUE, businessAccountId, licenseCommodityType}
            CostTuple tuple = costTable.getTuple(CostTable.NO_VALUE, id, licenseTypeKey);
            if (tuple != null) {
                cheapestTuple = tuple.getPrice() < cheapestCost ? tuple : cheapestTuple;
            }
        }
        return cheapestTuple;
    }

    /**
     * Calculates the cost of template that a shopping list has matched to.
     *
     * @param seller {@link Trader} that the buyer matched to
     * @param sl is the {@link ShoppingList} that is requesting price
     * @param costTable Table that stores cost by Business Account, license type and Region.
     * @param licenseBaseType The base type of the license commodity the sl contains.
     * @return A quote for the cost given by {@link CostFunction}
     */
    @VisibleForTesting
    protected static MutableQuote calculateComputeAndDatabaseCostQuote(Trader seller, ShoppingList sl,
                                                                       CostTable costTable, final int licenseBaseType) {
        final int licenseCommBoughtIndex = sl.getBasket().indexOfBaseType(licenseBaseType);
        final long groupFactor = sl.getGroupFactor();
        final Optional<Context> optionalContext = sl.getBuyer().getSettings().getContext();
        if (!optionalContext.isPresent()) {
            return new CommodityQuote(seller, Double.POSITIVE_INFINITY);
        }
        final Context context = optionalContext.get();
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
        } else {
            licenseTypeKey = sl.getBasket().get(licenseCommBoughtIndex).getType();
        }

        CostTuple costTuple = costTable.getTuple(regionIdBought, accountId, licenseTypeKey);
        if (costTuple == null) {
            // If the cost tuple is null for the no license case, that means there is no pricing for
            // this region for any license for the template. In this case, we will return an infinite
            // quote rather than looking up the cheapest region as we don't want to support inter-region
            // moves.
            logger.debug("Cost for region {} and license key {} not found in seller {}. Returning infinite"
                    + " cost for this template.", regionIdBought, licenseTypeKey, sl.getDebugInfoNeverUseInCode());
            return new CommodityQuote(seller, Double.POSITIVE_INFINITY);
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
        final CostTable costTable = new CostTable(cbtpResourceBundle.getCostTupleListList());
        int licenseBaseType = cbtpResourceBundle.getLicenseCommodityBaseType();
        CostFunction costFunction = (CostFunction)(buyer, seller, validate, economy) -> {
            if (!validate) {
                // In case that a vm is already placed on a cbtp, We return INFINITE price forcing it to compare pricing on all sellers
                return new CommodityCloudQuote(seller, Double.POSITIVE_INFINITY, null, null);
            }
            int couponCommodityBaseType = cbtpResourceBundle.getCouponBaseType();
            final MutableQuote quote =
                insufficientCommodityWithinSellerCapacityQuote(buyer, seller,
                        couponCommodityBaseType);
            if (quote.isInfinite()) {
                return quote;
            }

            return calculateDiscountedComputeCost(buyer, seller, cbtpResourceBundle, economy,
                    costTable, licenseBaseType);
        };

        return costFunction;
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
                        translateStorageResourceDependency(costDTO.getStorageResourceRatioDependencyList());

        // Get a list of range dependencies.
        final List<RangeBasedResourceDependency> rangeDependencyList
                = translateStorageResourceRangeDependency(costDTO.getStorageResourceRangeDependencyList());

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
                // Return infinite quote if commodity value exceeds dependent max for consumer
                // in range dependency list.
                final MutableQuote exceedDependentCapacityQuote = exceedDependentCapacity(buyer,
                        rangeDependencyList, seller);
                if (exceedDependentCapacityQuote.isInfinite()) {
                    return exceedDependentCapacityQuote;
                }
                return calculateStorageTierCost(priceDataMap, commCapacity, rangeDependencyList, buyer, seller);
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
                for (StorageTierPriceData priceData : resource.getStorageTierPriceDataList()) {
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
     * @param priceDataMap the map containing commodity and its pricing information on a seller.
     * @param commCapacity the information of a commodity and its minimum and maximum capacity
     * @param rangeDependencyList range dependency list
     * @param sl the shopping list requests resources
     * @param seller the seller
     *
     * @return the Quote given by {@link CostFunction}
     */
    public static MutableQuote calculateStorageTierCost(@Nonnull Map<CommoditySpecification, Map<Long, List<PriceData>>> priceDataMap,
                                                        @Nonnull Map<CommoditySpecification, CapacityLimitation> commCapacity,
                                                        @Nonnull List<RangeBasedResourceDependency> rangeDependencyList,
                                                        @Nonnull ShoppingList sl, Trader seller) {
        // TODO: refactor the PriceData to improve performance for region and business account lookup
        Optional<Context> optionalContext = sl.getBuyer().getSettings().getContext();
        if (!optionalContext.isPresent()) {
            return new CommodityQuote(seller, Double.POSITIVE_INFINITY);
        }
        final Context context = optionalContext.get();
        final long regionId = context.getRegionId();
        final BalanceAccount balanceAccount = context.getBalanceAccount();
        final long priceId = balanceAccount.getPriceId();
        final long balanceAccountId = balanceAccount.getId();

        Pair<Double, Long> costAccountPair = getTotalCost(regionId, balanceAccountId, priceId,
                priceDataMap, rangeDependencyList, commCapacity, sl);
        Double cost = costAccountPair.first;
        Long chosenAccountId = costAccountPair.second;
        // If no pricing was found for commodities in basket, then return infinite quote for seller.
        if (cost == Double.MAX_VALUE) {
            logger.warn("No (cheapest) cost found for storage {} in region {}, account {}.",
                    seller.getDebugInfoNeverUseInCode(), regionId, balanceAccount);
            return new CommodityQuote(seller, Double.POSITIVE_INFINITY);
        }
        return new CommodityCloudQuote(seller, cost, regionId, chosenAccountId);
    }

    /**
     * Gets the cost for all commodities in the priceDataMap. Used to calculate cheapest cost
     * across all regions and accounts.
     *
     * @param regionId Region id for which cost needs to be obtained.
     * @param accountId Business account id.
     * @param priceId Pricing id for account.
     * @param priceDataMap Map containing all cost data.
     * @param rangeDependencyList Range dependency list.
     * @param commCapacity Capacity map to check how much min amount to get cost for.
     * @param shoppingList Shopping list with basket and quantities.
     * @return Pair with first value as the total cost for region, and second value as the chosen
     * business account (could be the price id if found). If there are no commodities in the pricing
     * map or if we could not get any costs, then Double.MAX_VALUE is returned for cost.
     */
    @Nonnull
    private static Pair<Double, Long> getTotalCost(
            long regionId, long accountId, @Nullable Long priceId,
            @Nonnull final Map<CommoditySpecification, Map<Long, List<PriceData>>> priceDataMap,
            @Nonnull final List<RangeBasedResourceDependency> rangeDependencyList,
            @Nonnull final Map<CommoditySpecification, CapacityLimitation> commCapacity,
            @Nonnull final ShoppingList shoppingList) {
        // Cost if we didn't get any valid commodity pricing, will be max, so that we don't
        // mistakenly think this region is cheapest because it is $0.
        double totalCost = Double.MAX_VALUE;
        Long businessAccountChosenId = null;

        // iterating the priceDataMap for each type of commodity resource
        for (Entry<CommoditySpecification, Map<Long, List<PriceData>>> commodityPrice
                : priceDataMap.entrySet()) {
            int i = shoppingList.getBasket().indexOf(commodityPrice.getKey());
            if (i == -1) {
                // we iterate over the price data map, which defines the seller commodity price.
                // The sl is from the buyer, which may not buy all resources sold
                // by the seller, e.g: some VM does not request IOPS sold by IO1. We can skip it
                // when trying to compute cost.
                continue;
            }
            // calculate cost based on amount that is adjusted due to minimum capacity constraint
            double requestedAmount = getRequestedAmount(shoppingList, i, rangeDependencyList);
            if (commCapacity.containsKey(shoppingList.getBasket().get(i))) {
                requestedAmount = Math.max(requestedAmount,
                        commCapacity.get(shoppingList.getBasket().get(i)).getMinCapacity());
            }
            final Map<Long, List<PriceData>> priceMap = commodityPrice.getValue();
            // priceMap may contain PriceData by price id. Price id is the identifier for a price
            // offering associated with a Balance Account. Different Balance Accounts (i.e.
            // Balance Accounts with different ids) may have the same price id, if they are
            // associated with the same price offering. If no entry is found in the priceMap for a
            // price id, then the Balance Account id is used to lookup the priceMap.
            businessAccountChosenId = priceId != null && priceMap.containsKey(priceId)
                    ? priceId : accountId;
            final List<PriceData> priceDataList = priceMap.get(businessAccountChosenId);
            if (priceDataList != null) {
                final List<PriceData> pricesScopedToRegion = priceDataList
                        .stream()
                        .filter(s -> s.getRegionId() == regionId)
                        .collect(Collectors.toList());
                double cost = getCostFromPriceDataList(requestedAmount, pricesScopedToRegion);
                if (cost != Double.MAX_VALUE) {
                    if (totalCost == Double.MAX_VALUE) {
                        // Initialize cost now that we are getting a valid cost.
                        totalCost = 0;
                    }
                    totalCost += cost;
                }
            }
        }
        if (businessAccountChosenId == null) {
            businessAccountChosenId = accountId;
        }
        return new Pair<>(totalCost, businessAccountChosenId);
    }

    /**
     * A helper method to calculate the cost from price data list.
     *
     * @param requestedAmount the requested amount
     * @param priceDataList pricing information
     * @return the cost for providing the given requested amount. Can be MAX_VALUE if no matching
     * cost was found (e.g requestedAmount out of bounds).
     */
    private static double getCostFromPriceDataList(final double requestedAmount,
                                                   @NonNull final List<PriceData> priceDataList){
        double cost = Double.MAX_VALUE;
        double previousUpperBound = 0;
        double eachCost;
        for (PriceData priceData : priceDataList) {
            // the list of priceData is sorted based on upper bound
            double currentUpperBound = priceData.getUpperBound();
            if (priceData.isAccumulative()) {
                // if the price is accumulative, we need to sum up all the cost where
                // requested amount is more than upper bound till we find the exact range
                eachCost = (priceData.isUnitPrice()
                                ? priceData.getPrice() * Math.min(
                                                                  currentUpperBound - previousUpperBound,
                                                                  requestedAmount - previousUpperBound)
                                                : priceData.getPrice());
                if (cost == Double.MAX_VALUE) {
                    cost = 0;
                }
                cost += eachCost;
                // we find the exact range the requested amount falls
                if (requestedAmount <= currentUpperBound) {
                    break;
                }
            } else if (!priceData.isAccumulative() && requestedAmount > previousUpperBound
                            && requestedAmount <= currentUpperBound) {
                // non accumulative cost only depends on the exact range where the requested
                // amount falls
                eachCost = (priceData.isUnitPrice() ? priceData.getPrice()
                        * requestedAmount : priceData.getPrice());
                if (cost == Double.MAX_VALUE) {
                    cost = 0;
                }
                cost += eachCost;
            }
            previousUpperBound = currentUpperBound;
        }
        return cost;
    }

    /**
     * Return an infinite quote if a commodity exceeds the maximum value allowed in a rangeDependencyList.
     *
     * @param shoppingList shopping list
     * @param rangeDependencyList range dependency list
     * @param seller seller
     * @return a quote; infinite quote if a commodity exceeds maximum allowed value.
     */
    private static MutableQuote exceedDependentCapacity(ShoppingList shoppingList,
            @Nonnull List<RangeBasedResourceDependency> rangeDependencyList, Trader seller) {
        final CommodityQuote quote = new CommodityQuote(seller);
        for (RangeBasedResourceDependency dependency : rangeDependencyList) {
            final CommoditySpecification baseType = dependency.getBaseCommodity(); // e.g. storage amount
            // Map dependent commodity type to a treeMap that maps dependent quantity to base quantity
            Map<CommoditySpecification, TreeMap<Double, Double>> dependentCommToMap =
                    dependency.getDependentCapacity2LeastBaseCapacityPairs();
            for (Entry<CommoditySpecification, TreeMap<Double, Double>> dependPairs : dependentCommToMap.entrySet()) {
                final CommoditySpecification dependentType = dependPairs.getKey(); // e.g. storage access
                final int dependentIndex = shoppingList.getBasket().indexOf(dependentType);
                if (dependentIndex == -1) {
                    // e.g. the map is for io-throughput. we don't have io-throughput commodity.
                    continue;
                }
                final double dependentQuantity = shoppingList.getQuantity(dependentIndex);
                TreeMap<Double, Double> dependentQuantityToBaseQuantityMap = dependPairs.getValue();
                Double dependentCapacity = dependentQuantityToBaseQuantityMap.lastKey();
                if (dependentCapacity != null && dependentQuantity > dependentCapacity) {
                    final int baseCommIndex = shoppingList.getBasket().indexOf(baseType);
                    double baseCommQuantity = 0;
                    if (baseCommIndex != -1) {
                        baseCommQuantity = shoppingList.getQuantity(baseCommIndex);
                    }
                    return new InfiniteRangeBasedResourceDependencyQuote(baseType,
                            dependentType, dependentCapacity, baseCommQuantity, dependentQuantity);
                }
            }
        }
        return quote;
    }

    /**
     * Get the requested amount, adjust it based on the rangeDependencyList if needed.
     *
     * @param shoppingList shopping list
     * @param commodityIndex the index of the commodity on the shopping list
     * @param rangeDependencyList the range dependency list
     * @return the requested amount of the commodity
     */
    private static double getRequestedAmount(@Nonnull final ShoppingList shoppingList,
            int commodityIndex,
            @Nonnull List<RangeBasedResourceDependency> rangeDependencyList) {
        double baseCommQuantity = shoppingList.getQuantity(commodityIndex);
        for (RangeBasedResourceDependency dependency : rangeDependencyList) {
            final CommoditySpecification baseType = dependency.getBaseCommodity();
            if (baseType != null && !baseType.equals(shoppingList.getBasket().get(commodityIndex))) {
                continue;
            }

            // Map dependent commodity type to a treeMap that maps dependent quantity to base quantity
            // (e.g. storage access -> storage amount)
            Map<CommoditySpecification, TreeMap<Double, Double>> dependentCommToMap =
                    dependency.getDependentCapacity2LeastBaseCapacityPairs();

            for (Entry<CommoditySpecification, TreeMap<Double, Double>> dependPairs : dependentCommToMap.entrySet()) {
                final CommoditySpecification dependentType = dependPairs.getKey();
                final int dependentIndex = shoppingList.getBasket().indexOf(dependentType);
                if (dependentIndex == -1) {
                    // e.g. the map is for io-throughput. we don't have io-throughput commodity.
                    continue;
                }
                final Double dependentQuantity = shoppingList.getQuantity(dependentIndex);
                TreeMap<Double, Double> dependentQuantityToBaseQuantityMap = dependPairs.getValue();
                Entry<Double, Double> reverseMapEntry = dependentQuantityToBaseQuantityMap.ceilingEntry(dependentQuantity);
                if (reverseMapEntry != null) {
                    final Double newBaseCommQuantity = reverseMapEntry.getValue();
                    // Increase the requested amount if the base commodity quantity that corresponds
                    // to the dependent quantity (e.g. IOPS) is larger than the original requested
                    // quantity.
                    if (newBaseCommQuantity != null && newBaseCommQuantity > baseCommQuantity) {
                        baseCommQuantity = newBaseCommQuantity;
                    }
                }
            }
        }
        return baseCommQuantity;
    }

    /**
     * A utility method to extract base and dependent commodities and their range constraint.
     *
     * @param dependencyDTOs the DTO represents the base and dependent commodity range relation.
     * @return a list of {@link RangeBasedResourceDependency}
     */
    @Nonnull
    public static List<RangeBasedResourceDependency>
    translateStorageResourceRangeDependency(List<StorageResourceRangeDependency> dependencyDTOs) {
        Map<CommoditySpecification, List<StorageResourceRangeDependency>> dtosPerBaseType =
                dependencyDTOs.stream().collect(Collectors.groupingBy(dto -> ProtobufToAnalysis.commoditySpecification(dto.getBaseResourceType())));
        final List<RangeBasedResourceDependency> dependencyList = new ArrayList<>();
        dtosPerBaseType.forEach((baseType, dependencyDTOList) -> {
            Map<CommoditySpecification, List<RangeTuple>> tuplesPerDependentType = dependencyDTOList.stream()
                    .collect(Collectors.toMap(d -> ProtobufToAnalysis.commoditySpecification(d.getDependentResourceType()),
                            StorageResourceRangeDependency::getRangeTupleList,
                            (l1, l2) -> {
                                List<RangeTuple> res = new ArrayList<>();
                                res.addAll(l1);
                                res.addAll(l2);
                                return res;
                            }));
            dependencyList.add(new RangeBasedResourceDependency(baseType, tuplesPerDependentType));
        });
        return dependencyList;
    }

    /**
     * A class to represent the capacity range dependency for one or more commodities on base commodity.
     */
    static class RangeBasedResourceDependency {
        private CommoditySpecification baseCommodity_;
        // Mapping from dependent commodity type to a treeMap, where treeMap records to achieve each dependent capacity,
        // the minimal quantity of base commodity.
        private final Map<CommoditySpecification, TreeMap<Double, Double>> dependentCapacity2LeastBaseCapacityPairs_ = new HashMap<>();

        /**
         * Initialize RangeBasedResourceDependency object representing commodity capacity range constraints.
         *
         * @param baseCommodity base commodity type.
         * @param tuplesPerDependentType mapping from dependent commodity type to dependent commodity tuples.
         */
        RangeBasedResourceDependency(CommoditySpecification baseCommodity,
                                            Map<CommoditySpecification, List<RangeTuple>> tuplesPerDependentType) {
            baseCommodity_ = baseCommodity;
            initializeRangePairs(tuplesPerDependentType);
        }

        private void initializeRangePairs(@Nonnull Map<CommoditySpecification, List<RangeTuple>> tuplesPerDependentType) {
            tuplesPerDependentType.forEach((dependentType, tuplesList) -> {
                for (RangeTuple tuple : tuplesList) {
                    final double baseCap = tuple.getBaseMaxCapacity();
                    final double dependentCap = tuple.getDependentMaxCapacity();
                    TreeMap<Double, Double> dependPairs =
                            dependentCapacity2LeastBaseCapacityPairs_.computeIfAbsent(dependentType, h -> new TreeMap<>());
                    Double tmpVal = dependPairs.get(dependentCap);
                    if (tmpVal == null || tmpVal > baseCap) {
                        dependPairs.put(dependentCap, baseCap);
                    }
                }
            });
        }

        /**
         * Returns the base commodity in the dependency pair.
         *
         * @return base commodity.
         */
        public CommoditySpecification getBaseCommodity() {
            return baseCommodity_;
        }

        /**
         * Returns a map that maps dependent commodity type to a treeMap that maps dependent quantity to base quantity.
         *
         * @return a map that maps dependent commodity type to a treeMap that maps dependent quantity to base quantity.
         */
        public Map<CommoditySpecification, TreeMap<Double, Double>> getDependentCapacity2LeastBaseCapacityPairs() {
            return dependentCapacity2LeastBaseCapacityPairs_;
        }
    }

    /**
     * Create a new {@link InfiniteRangeBasedResourceDependencyQuote} which describes an infinite {@link Quote}
     * due to a range based dependent resource requirement that cannot be met.
     */
    public static class InfiniteRangeBasedResourceDependencyQuote extends MutableQuote {
        private final CommoditySpecification baseCommodityType;
        private final CommoditySpecification dependentCommodityType;
        private final double dependentCommodityCapacity;
        private final double baseCommodityQuantity;
        private final double dependentCommodityQuantity;

        /**
         * Create a new {@link InfiniteRangeBasedResourceDependencyQuote}.
         *
         * @param baseCommodityType base commodity type.
         * @param dependentCommodityType dependent commodity type.
         * @param dependentCommodityCapacity The capacity of the dependent commodity in the dependent pair.
         * @param baseCommodityQuantity The quantity of the base commodity in the pair.
         * @param dependentCommodityQuantity The quantity of the dependent commodity in the pair.
         */
        public InfiniteRangeBasedResourceDependencyQuote(@Nonnull final CommoditySpecification baseCommodityType,
                @Nonnull final CommoditySpecification dependentCommodityType,
                double dependentCommodityCapacity,
                double baseCommodityQuantity,
                double dependentCommodityQuantity) {
            super(null, Double.POSITIVE_INFINITY);

            this.baseCommodityType = Objects.requireNonNull(baseCommodityType);
            this.dependentCommodityType = Objects.requireNonNull(dependentCommodityType);
            this.dependentCommodityCapacity = dependentCommodityCapacity;
            this.baseCommodityQuantity = baseCommodityQuantity;
            this.dependentCommodityQuantity = dependentCommodityQuantity;
        }

        @Override
        public int getRank() {
            return (int)(Math.abs(dependentCommodityQuantity
                    - dependentCommodityCapacity) / dependentCommodityQuantity) * MAX_RANK;
        }

        @Override
        public String getExplanation(@Nonnull final ShoppingList shoppingList) {
            return "Dependent commodity " + dependentCommodityType.getDebugInfoNeverUseInCode() + " ("
                    + dependentCommodityQuantity + ") exceeds ranged capacity (" + dependentCommodityCapacity
                    + "), with base commodity " + baseCommodityType.getDebugInfoNeverUseInCode()
                    + " (" + baseCommodityQuantity + ").";
        }
    }
}

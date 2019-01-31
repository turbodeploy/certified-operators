package com.vmturbo.platform.analysis.utilities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.Basket;
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
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceCost;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceLimitation;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;
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
         * @return
         */
        public double getMinCapacity() {
            return minCapacity_;
        }

        /**
         * Returns the maximal capacity limit of commodity
         * @return
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
         * @return
         */
        public CommoditySpecification getBaseCommodity() {
            return baseCommodity_;
        }

        /**
         * Returns the dependent commodity in the dependency pair
         * @return
         */
        public CommoditySpecification getDependentCommodity() {
            return dependentCommodity_;
        }

        /**
         * Returns the maximal ratio of dependent commodity to base commodity
         * @return
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

        public PriceData(double upperBound, double price, boolean isUnitPrice,
                        boolean isAccumulative) {
            upperBound_ = upperBound;
            price_ = price;
            isUnitPrice_ = isUnitPrice;
            isAccumulative_ = isAccumulative;
        }

        /**
         * Returns the upper bound limit of commodity
         * @return
         */
        public double getUpperBound() {
            return upperBound_;
        }

        /**
         * Returns the price of commodity
         * @return
         */
        public double getPrice() {
            return price_;
        }

        /**
         * Returns true if the price is a unit price
         * @return
         */
        public boolean isUnitPrice() {
            return isUnitPrice_;
        }

        /**
         * Returns true if the cost should be accumulated
         * @return
         */
        public boolean isAccumulative() {
            return isAccumulative_;
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
     * A utility method to extract commodity's price information from DTO.
     *
     * @param resourceCost a list of DTO represents price information
     * @return a map which keeps commodity and its price information
     */
    @SuppressWarnings("unchecked")
    public static @NonNull Map<CommoditySpecification, List<PriceData>>
                    translatePriceCost(List<StorageResourceCost> resourceCost) {
        Map<CommoditySpecification, List<CostFunctionFactory.PriceData>> priceDataMap =
                        new HashMap<>();
        for (StorageResourceCost cost : resourceCost) {
            if (!priceDataMap.containsKey(cost.getResourceType())) {
                List<CostFunctionFactory.PriceData> priceDataList = new ArrayList<>();
                for (CostDTOs.CostDTO.StorageTierCostDTO.StorageTierPriceData priceData : cost.getStorageTierPriceDataList()) {
                    CostFunctionFactory.PriceData newEntry = new PriceData(
                                    priceData.getUpperBound(), priceData.getPrice(),
                                    priceData.getIsUnitPrice(), priceData.getIsAccumulativeCost());
                    priceDataList.add(newEntry);
                }
                // make sure list is ascending based on upperbound, because we need to get the
                // first resource range that can satisfy the requested amount, and the price
                // corresponds to that range will be used as price
                Collections.sort(priceDataList);
                priceDataMap.put(ProtobufToAnalysis.commoditySpecification(cost.getResourceType()),
                                priceDataList);
            } else {
                throw new IllegalArgumentException("Duplicate entries for a commodity price");
            }
        }
        return priceDataMap;
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
     * Calculates the cost of template that a shopping list has matched to
     *
     * @param seller {@link Trader} that the buyer matched to
     * @param sl is the {@link ShoppingList} that is requesting price
     * @param costDTO is the resourceBundle associated with this templateProvider
     * @param costMap is a map of map which stores cost by business account and license type
     *
     * @return A quote for the cost given by {@link CostFunction}
     */
    public static MutableQuote calculateComputeCostQuote(Trader seller, ShoppingList sl,
                                                         ComputeTierCostDTO costDTO,
                                                         Map<Long, Map<Integer, Double>> costMap) {
        final int licenseBaseType = costDTO.getLicenseCommodityBaseType();
        final int licenseCommBoughtIndex = sl.getBasket().indexOfBaseType(licenseBaseType);
        final long groupFactor = sl.getGroupFactor();
        if (sl.getBuyer().getSettings() == null
                        || sl.getBuyer().getSettings().getBalanceAccount() == null
                        || costMap.get(sl.getBuyer().getSettings().getBalanceAccount().getId()) == null) {
            logger.warn("Business account is not found on seller, return infinity quote");
            return new CommodityQuote(seller, Double.POSITIVE_INFINITY);
        }
        Map<Integer, Double> costByLicense = costMap.get(sl.getBuyer().getSettings().getBalanceAccount().getId());
        if (licenseCommBoughtIndex == -1) {
            // NOTE: -1 is the no license commodity type
            return new CommodityQuote(seller, costByLicense.get(licenseCommBoughtIndex) * groupFactor);
        }

        // if the license commodity exists in the basketBought, get the type of that commodity
        // and lookup the costMap for that license type
        final Integer type = sl.getBasket().get(licenseCommBoughtIndex).getType();
        final Double cost = costByLicense.get(type);
        if (cost == null) {
            logger.error("Cannot find type {} in costMap, license base type: {}", type,
                    licenseBaseType);
            // NOTE: -1 is the no license commodity type
            return new CommodityQuote(seller, costByLicense.get(-1));
        }

        return Double.isInfinite(cost) ?
            new LicenseUnavailableQuote(seller, sl.getBasket().get(licenseCommBoughtIndex)) :
            new CommodityQuote(seller, cost * groupFactor);
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
    public static double calculateDiscountedComputeCost(ShoppingList buyer, Trader seller,
                    CbtpCostDTO cbtpResourceBundle, UnmodifiableEconomy economy) {
        long groupFactor = buyer.getGroupFactor();
        // If the buyer is already placed on a CBTP return 0
        if (buyer.getSupplier() == seller) {
            return 0;
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
            return Double.POSITIVE_INFINITY;
        }

        // The capacity of a coupon commodity sold by a template provider reflects the number of
        // coupons associated with the template it represents. This number is the requested amount
        // of coupons by a matching vm.
        int indexOfCouponCommByTp = matchingTP.getBasketSold()
                        .indexOfBaseType(cbtpResourceBundle.getCouponBaseType());
        CommoditySold couponCommSoldByTp =
                        matchingTP.getCommoditiesSold().get(indexOfCouponCommByTp);
        double requestedCoupons = couponCommSoldByTp.getCapacity() * groupFactor;

        // Calculate the number of available coupons from cbtp
        int indexOfCouponCommByCbtp = seller.getBasketSold()
                        .indexOfBaseType(cbtpResourceBundle.getCouponBaseType());
        CommoditySold couponCommSoldByCbtp =
                        seller.getCommoditiesSold().get(indexOfCouponCommByCbtp);
        double availableCoupons =
                        couponCommSoldByCbtp.getCapacity() - couponCommSoldByCbtp.getQuantity();

        // Get the cost of the template matched with the vm. If this buyer represents a
        // consistent scaling group, the template cost will be for all members of the
        // group (the cost will be multiplied by the group factor)
        double templateCostForBuyer = QuoteFunctionFactory
                .computeCost(buyer, matchingTP, false, economy)
                .getQuoteValue();
        double singleVmTemplateCost = templateCostForBuyer / (groupFactor > 0 ? groupFactor : 1);

        double discountedCost = 0;
        if (availableCoupons > 0) {
            if (couponCommSoldByTp.getCapacity() != 0) {
                double templateCostPerCoupon = singleVmTemplateCost / couponCommSoldByTp.getCapacity();
                // Assuming 100% discount for the portion of requested coupons satisfied by the CBTP
                double numCouponsToPayFor = Math.max(0, (requestedCoupons - availableCoupons));
                if (cbtpResourceBundle.getPrice() != 0) {
                    discountedCost = Math.max(cbtpResourceBundle.getPrice(),
                            numCouponsToPayFor * templateCostPerCoupon);
                } else {
                    discountedCost = numCouponsToPayFor * templateCostPerCoupon;
                }
            }
        } else {
            // In case that there isn't discount available, avoid preferring a cbtp that provides
            // no discount on tp of the matching template.
            discountedCost = Double.POSITIVE_INFINITY;
        }
        return discountedCost;
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
            case CBTP_RESOURCE_BUNDLE:
                return createResourceBundleCostFunctionForCbtp(costDTO.getCbtpResourceBundle());
            default:
                throw new IllegalArgumentException("input = " + costDTO);
        }
    }

    /**
     * A utility method to construct the pricing information map.
     *
     * @param costTupleList a list of cost data
     * @return map The map has business account id as key and a mapping of license to price as value.
     */
    private static Map<Long, Map<Integer, Double>>
            translateResourceCostForTier(List<CostTuple> costTupleList) {
        Map<Long, Map<Integer, Double>> costMap = new HashMap<>();
        for (CostTuple costTuple: costTupleList) {
            long baId = costTuple.getBusinessAccountId();
            if (costMap.containsKey(baId)) {
                costMap.get(baId).put(costTuple.getLicenseCommodityType(), costTuple.getPrice());
            } else {
                Map<Integer, Double> priceByLicense = new HashMap<>();
                priceByLicense.put(costTuple.getLicenseCommodityType(), costTuple.getPrice());
                costMap.put(baId, priceByLicense);
            }
        }
        return costMap;
    }

    /**
     * Create {@link CostFunction} by extracting data from {@link ComputeTierCostDTO}
     *
     * @param costDTO the DTO carries the data used to construct cost function
     * @return CostFunction
     */
    public static @NonNull CostFunction createCostFunctionForComputeTier(ComputeTierCostDTO costDTO) {
        Map<Long, Map<Integer, Double>> costMap =
                        translateResourceCostForTier(costDTO.getCostTupleListList());
        Map<CommoditySpecification, CommoditySpecification> dependencyMap =
                translateComputeResourceDependency(costDTO.getComputeResourceDepedencyList());

        CostFunction costFunction = new CostFunction() {
            @Override
            public MutableQuote calculateCost(ShoppingList buyer, Trader seller, boolean validate,
                    UnmodifiableEconomy economy) {
                if (!validate) {
                    // seller is the currentSupplier. Just return cost
                    return calculateComputeCostQuote(seller, buyer, costDTO, costMap);
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

                return calculateComputeCostQuote(seller, buyer, costDTO, costMap);
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
                if (!validate) {
                    // In case that a vm is already placed on a cbtp, we need to avoid
                    // calculating it's discounted cost in order to prevent a recursive
                    // call to calculateDiscountedComputeCost. The recursive call will
                    // be triggered because of the fact the cbtp is also a seller in a
                    // market associated with the vm, and the quote minimizer will try
                    // to calculate it's cost, that is a discounted cost.
                    // By returning 0 we make sure that once a vm got placed on a cbtp
                    // it will not move to another location. This assuming that the
                    // cbtp provides the cheapest cost.
                    return CommodityQuote.zero(seller);
                }

                int couponCommodityBaseType = cbtpResourceBundle.getCouponBaseType();
                final MutableQuote quote =
                    insufficientCommodityWithinSellerCapacityQuote(buyer, seller,  couponCommodityBaseType);
                if (quote.isInfinite()) {
                    return quote;
                }

                return new CommodityQuote(seller,
                    calculateDiscountedComputeCost(buyer, seller, cbtpResourceBundle, economy));
            }
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
                return new CommodityQuote(seller, calculateStorageTierCost(priceDataMap, commCapacity, buyer));
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
                    long businessAccountId = priceData.getBusinessAccountId();
                    CostFunctionFactory.PriceData price = new PriceData(priceData.getUpperBound(),
                                                                           priceData.getPrice(),
                                                                           priceData.getIsUnitPrice(),
                                                                           priceData.getIsAccumulativeCost());
                    if (priceDataPerBusinessAccount.containsKey(businessAccountId)) {
                        List<CostFunctionFactory.PriceData> currentPriceDataList = priceDataPerBusinessAccount
                            .get(businessAccountId);
                        currentPriceDataList.add(price);
                    } else {
                        priceDataPerBusinessAccount.put(businessAccountId, new ArrayList<>(Arrays.asList(price)));
                    }

                }
                // make sure price list is ascending based on upperbound, because we need to get the
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
     *
     * @return the cost given by {@link CostFunction}
     */
    public static double calculateStorageTierCost(@NonNull Map<CommoditySpecification, Map<Long, List<PriceData>>> priceDataMap,
                                              Map<CommoditySpecification, CapacityLimitation> commCapacity,
                                              @NonNull ShoppingList sl) {
        double cost = 0;
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
            if (sl.getBuyer().getSettings() == null
                    || sl.getBuyer().getSettings().getBalanceAccount() == null
                    || !commodityPrice.getValue().containsKey(sl.getBuyer().getSettings().getBalanceAccount().getId())) {
                logger.warn("Business account is not found on seller, return infinity quote");
                return Double.POSITIVE_INFINITY;
            }
            for (PriceData priceData : commodityPrice.getValue().get(sl.getBuyer().getSettings().getBalanceAccount().getId())) {
                // the list of priceData is sorted based on upperbound
                double currentUpperBound = priceData.getUpperBound();
                if (priceData.isAccumulative()) {
                    // if the price is accumulative, we need to sum up all the cost where
                    // requested amount is more than upperbound till we find the exact range
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
        return cost;
    }

}

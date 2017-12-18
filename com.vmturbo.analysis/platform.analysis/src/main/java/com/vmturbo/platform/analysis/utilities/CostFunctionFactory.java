package com.vmturbo.platform.analysis.utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.BalanceAccount;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.protobuf.CostDTOs;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeResourceBundleCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageResourceBundleCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageResourceBundleCostDTO.ResourceCost;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageResourceBundleCostDTO.ResourceDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageResourceBundleCostDTO.ResourceLimitation;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;

/**
 * The factory class to construct cost function.
 */
public class CostFunctionFactory {

    // a class to represent the minimum and maximum capacity of a commodity
    public static class CapacityLimitation {
        private double minCapacity_ = Double.POSITIVE_INFINITY;
        private double maxCapacity_ = Double.POSITIVE_INFINITY;

        public CapacityLimitation(double minCapacity, double maxCapacity) {
            minCapacity_ = minCapacity;
            maxCapacity_ = maxCapacity;
        }

        public double getMinCapacity() {
            return minCapacity_;
        }

        public double getMaxCapacity() {
            return maxCapacity_;
        }
    }

    // a class to represent the capacity constraint between two different commodities.
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

        public CommoditySpecification getBaseCommodity() {
            return baseCommodity_;
        }

        public CommoditySpecification getDependentCommodity() {
            return dependentCommodity_;
        }

        public int getMaxRatio() {
            return maxRatio_;
        }
    }

    // a class represents the price information of a commodity
    // NOTE: the PriceData comparator is overridden to make sure upperBound decides the order
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

        public double getUpperBound() {
            return upperBound_;
        }

        public double getPrice() {
            return price_;
        }

        public boolean isUnitPrice() {
            return isUnitPrice_;
        }

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
                    translateResourceCapacityLimitation(StorageResourceBundleCostDTO costDTO) {
        Map<CommoditySpecification, CapacityLimitation> commCapacity = new HashMap<>();
        for (ResourceLimitation resourceLimit : costDTO.getResourceLimitationList()) {
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
                    translatePriceCost(List<ResourceCost> resourceCost) {
        Map<CommoditySpecification, List<CostFunctionFactory.PriceData>> priceDataMap =
                        new HashMap<>();
        for (ResourceCost cost : resourceCost) {
            if (!priceDataMap.containsKey(cost.getResourceType())) {
                List<CostFunctionFactory.PriceData> priceDataList = new ArrayList<>();
                for (CostDTOs.CostDTO.StorageResourceBundleCostDTO.PriceData priceData : cost.getPriceDataList()) {
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
     * @param dependencyDTO the DTO represents the base and dependent commodity relation
     * @return a list of {@link DependentResourcePair}
     */
    public static List<DependentResourcePair>
                    translateResourceDependency(List<ResourceDependency> dependencyDTOs) {
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
     * Validate if the requested amount comply with the limitation between base commodity
     * and dependency commodity
     *
     * @param sl the shopping list which requests resources
     * @param seller the seller which sells resources
     * @param dependentResourcePair the dependency information between base and dependent commodities
     * @return true if the validation passes, otherwise false
     */
    private static boolean validateDependentCommodityAmount(@NonNull ShoppingList sl,
                    @NonNull Trader seller, @NonNull List<DependentResourcePair> dependencyList) {
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
            if (sl.getQuantities()[baseIndex]
                            * dependency.maxRatio_ < sl.getQuantities()[depIndex]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Validate if the requested amount of commodity is within the minimum and maximum range.
     *
     * @param sl the shopping list
     * @param commCapacity the information of a commodity and its minimum and maximum capacity
     * @return true if validation passes, otherwise false
     */
    private static boolean validateRequestedAmountWithinCapacity(ShoppingList sl,
                    Map<CommoditySpecification, CapacityLimitation> commCapacity) {
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
            if (sl.getQuantities()[index] < entry.getValue().getMinCapacity()
                            || sl.getQuantities()[index] > entry.getValue().getMaxCapacity()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Validate if the shoppingList can fit in the seller
     *
     * @param sl the shopping list
     * @param seller the templateProvider that supplies the resources
     * @return true if validation passes, otherwise false
     */
    private static boolean validateRequestedAmountWithinSellerCapacity(ShoppingList sl,
                    Trader seller) {
        // check if the commodities bought comply with capacity limitation on seller
        int boughtIndex = 0;
        Basket basket = sl.getBasket();
        final double[] quantities = sl.getQuantities();
        final double[] peakQuantities = sl.getPeakQuantities();
        List<CommoditySold> commsSold = seller.getCommoditiesSold();
        for (int soldIndex = 0; boughtIndex < basket.size();
                        boughtIndex++, soldIndex++) {
            CommoditySpecification basketCommSpec = basket.get(boughtIndex);
            // Find corresponding commodity sold. Commodities sold are ordered the same way as the
            // basket commodities, so iterate once (O(N)) as opposed to searching each time (O(NLog(N))
            while (!basketCommSpec.isSatisfiedBy(seller.getBasketSold().get(soldIndex))) {
                soldIndex++;
            }

            if (quantities[boughtIndex] > commsSold.get(soldIndex).getEffectiveCapacity() ||
                            peakQuantities[boughtIndex] > commsSold.get(soldIndex).getEffectiveCapacity()) {
                return false;
            }
        }
        return true;
    }

    public static double calculateComputeCost(Trader seller, ShoppingList buyer) {
        double currentCost = (buyer.getSupplier() == null || buyer.getSupplier().getSettings().getCostFunction() == null) ? 0 :
                                    buyer.getSupplier().getSettings().getCostFunction().calculateCost(null, null);
        double newCost = seller.getSettings().getCostFunction().calculateCost(null, null);
        PriceFunction pf = PriceFunction.Cache.createStandardWeightedPriceFunction(1);
        BalanceAccount ba = seller.getSettings().getBalanceAccount();
        double util = (ba.getSpent() - currentCost + newCost) / ba.getBudget();
        return pf.unitPrice(util, seller, null, null);
    }

    /**
     * Calculates the total cost of all resources requested by a shopping list on a seller.
     *
     * @param commodityPriceDataMap the map containing commodity and its pricing information
     * on a seller
     * @param sl the shopping list requests resources
     * @return the cost given by {@link CostFunction}
     */
    public static double calculateStorageCost(
                    @NonNull Map<CommoditySpecification, List<PriceData>> commodityPriceDataMap,
                    @NonNull ShoppingList sl) {
        double cost = 0;
        for (Entry<CommoditySpecification, List<PriceData>> commodityPrice : commodityPriceDataMap
                        .entrySet()) {
            int i = sl.getBasket().indexOf(commodityPrice.getKey());
            if (i == -1) {
                // we iterate over the price data map, which defines the seller commodity price.
                // The sl is from the buyer, which may not buy all resources sold
                // by the seller, e.g: some VM does not request IOPS sold by IO1. We can skip it
                // when trying to compute cost.
                continue;
            }
            double requestedAmount = sl.getQuantities()[i];
            double previousUpperBound = 0;
            for (PriceData priceData : commodityPrice.getValue()) {
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
                    cost += (priceData.isUnitPrice() ? priceData.getPrice() * requestedAmount
                                    : priceData.getPrice());
                }
                previousUpperBound = currentUpperBound;
            }
        }
        return cost;
    }

    /**
     * Creates {@link CostFunction} for a given seller.
     *
     * @param costDTO the DTO carries the cost information
     * @return CostFunction
     */
    public static @NonNull CostFunction createCostFunction(CostDTO costDTO) {
        switch (costDTO.getCostTypeCase()) {
            case DS_RESOURCE_BUNDLE_COST:
                return createResourceBundleCostFunctionForStorage(costDTO.getDsResourceBundleCost());
            case PM_RESOURCE_BUNDLE_COST:
                return createResourceBundleCostFunctionForCompute(costDTO.getPmResourceBundleCost());
            default:
                throw new IllegalArgumentException("input = " + costDTO);
        }
    }

    /**
     * Create {@link CostFunction} by extracting data from {@link StorageResourceBundleCostDTO}
     *
     * @param costDTO the DTO carries the data used to construct cost function
     * @return CostFunction
     */
    public static @NonNull CostFunction createResourceBundleCostFunctionForStorage(StorageResourceBundleCostDTO costDTO) {
        // the map to keep commodity to its min max capacity limitation
        Map<CommoditySpecification, CapacityLimitation> commCapacity =
                        translateResourceCapacityLimitation(costDTO);
        List<ResourceCost> resourceCost = costDTO.getResourceCostList();
        // the map to keep the commodity to its unit price
        Map<CommoditySpecification, List<PriceData>> priceDataMap =
                        translatePriceCost(resourceCost);
        // the capacity constraint between commodities
        List<DependentResourcePair> dependencyList =
                        translateResourceDependency(costDTO.getResourceDependencyList());
        CostFunction costFunction = (buyer, seller)
                        -> {
                            if (seller == null) {
                                return 0;
                            }
                            if (!validateRequestedAmountWithinCapacity(buyer, commCapacity)) {
                                return Double.POSITIVE_INFINITY;
                            }
                            if (!validateDependentCommodityAmount(buyer, seller, dependencyList)) {
                                return Double.POSITIVE_INFINITY;
                            }
                            return calculateStorageCost(priceDataMap, buyer);
                        };
        return costFunction;
    }

    /**
     * Create {@link CostFunction} by extracting data from {@link ComputeResourceBundleCostDTO}
     *
     * @param costDTO the DTO carries the data used to construct cost function
     * @return CostFunction
     */
    public static @NonNull CostFunction createResourceBundleCostFunctionForCompute(ComputeResourceBundleCostDTO costDTO) {
        double templatePrice = costDTO.getCostWithoutLicense();
        CostFunction costFunction = (buyer, seller)
                        -> {
                            Trader currentSupplier = buyer.getSupplier();
                            if (currentSupplier == seller) {
                                // seller is the currentSupplier. Just return cost
                                return templatePrice;
                            }
                            if (!validateRequestedAmountWithinSellerCapacity(buyer, seller)) {
                                return Double.POSITIVE_INFINITY;
                            }
                            return templatePrice;
                        };
        return costFunction;
    }
}

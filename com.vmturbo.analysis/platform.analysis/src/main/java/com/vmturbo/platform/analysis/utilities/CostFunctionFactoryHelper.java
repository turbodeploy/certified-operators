package com.vmturbo.platform.analysis.utilities;

import static java.util.stream.Collectors.groupingBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.commons.Pair;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Context;
import com.vmturbo.platform.analysis.economy.Context.BalanceAccount;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.RangeTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceCost;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceLimitation;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceRangeDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceRatioDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageTierPriceData;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactory.PriceData;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityCloudQuote;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityContext;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityQuote;
import com.vmturbo.platform.analysis.utilities.Quote.InfiniteBelowMinAboveMaxCapacityLimitationQuote;
import com.vmturbo.platform.analysis.utilities.Quote.InfiniteRangeBasedResourceDependencyQuote;
import com.vmturbo.platform.analysis.utilities.Quote.InfiniteRatioBasedResourceDependencyQuote;
import com.vmturbo.platform.analysis.utilities.Quote.MutableQuote;

/**
 * Utility classes and methods to create CostFunctions.
 */
public class CostFunctionFactoryHelper {

    private static final Logger logger = LogManager.getLogger();

    private CostFunctionFactoryHelper() {}

    /**
     * If a cloud volume in Reversibility mode gets infinite quote from a seller, while can get finite
     * quote if it can be in Savings mode, the Savings mode quote value need to additionally add a
     * big enough penalty.
     */
    public static final double REVERSIBILITY_PENALTY_COST = 1000d;

    /**
     * a class to represent the minimum and maximum capacity of a commodity.
     */
    public static class CapacityLimitation {
        private double minCapacity_;
        private double maxCapacity_;
        private boolean checkMinCapacity_;

        /**
         * Initialize CapacityLimitation object representing commodity min/max capacity limitation.
         *
         * @param minCapacity commodity min capacity
         * @param maxCapacity commodity max capacity
         * @param checkMinCapacity whether min capacity is a strict constraint.
         *                         True when consumer must consume amount above min.
         */
        public CapacityLimitation(double minCapacity, double maxCapacity, boolean checkMinCapacity) {
            minCapacity_ = minCapacity;
            maxCapacity_ = maxCapacity;
            checkMinCapacity_ = checkMinCapacity;
        }

        /**
         * Get min capacity limitation.
         *
         * @return the minimal capacity limit of commodity.
         */
        public double getMinCapacity() {
            return minCapacity_;
        }

        /**
         * Get max capacity limitation.
         *
         * @return the maximal capacity limit of commodity.
         */
        public double getMaxCapacity() {
            return maxCapacity_;
        }

        /**
         * Whether min capacity is a strict constraint.
         *
         * @return true if consumer quantity needs to exceed min.
         */
        public boolean getCheckMinCapacity() {
            return checkMinCapacity_;
        }
    }

    /**
     * A class to represent the capacity ratio constraint between two different commodities.
     */
    public static class RatioBasedResourceDependency {
        private CommoditySpecification baseCommodity_;
        private CommoditySpecification dependentCommodity_;
        private double maxRatio_;
        private double minRatio_;
        private boolean hasMinRatio_;
        private boolean increaseBaseDefaultSupported_;

        /**
         * Initialize RatioBasedResourceDependency object representing commodity capacity ratio constraint.
         *
         * @param baseCommodity base commodity type
         * @param dependentCommodity dependent commodity type
         * @param maxRatio max ratio of dependent commodity capacity on base commodity capacity
         * @param hasMinRatio whether there is min ratio between base and dependent commodities
         * @param minRatio min ratio of dependent commodity capacity on base commodity capacity
         * @param increaseBaseDefaultSupported whether can directly increase base commodity
         *                                     capacity to achieve the maxRatio.
         */
        public RatioBasedResourceDependency(CommoditySpecification baseCommodity,
                                            CommoditySpecification dependentCommodity,
                                            double maxRatio, boolean hasMinRatio,
                                            double minRatio, boolean increaseBaseDefaultSupported) {
            baseCommodity_ = baseCommodity;
            dependentCommodity_ = dependentCommodity;
            maxRatio_ = maxRatio;
            minRatio_ = minRatio;
            hasMinRatio_ = hasMinRatio;
            increaseBaseDefaultSupported_ = increaseBaseDefaultSupported;
        }

        /**
         * Base commodity type in the ratio dependency.
         *
         * @return the base commodity in the dependency pair.
         */
        public CommoditySpecification getBaseCommodity() {
            return baseCommodity_;
        }

        /**
         * Dependent commodity type in the ratio dependency.
         *
         * @return the dependent commodity in the dependency pair.
         */
        public CommoditySpecification getDependentCommodity() {
            return dependentCommodity_;
        }

        /**
         * Get the maximal ratio of dependent commodity on base commodity.
         *
         * @return the maximal ratio.
         */
        public double getMaxRatio() {
            return maxRatio_;
        }

        /**
         * Whether there is min ratio between base and dependent commodities.
         *
         * @return Whether the dependency has minRatio.
         */
        public boolean hasMinRatio() {
            return hasMinRatio_;
        }

        /**
         * Get the minimal ratio of dependent commodity on base commodity.
         *
         * @return the minimal ratio.
         */
        public double getMinRatio() {
            return minRatio_;
        }

        /**
         * When dependent commodity capacity is larger than (base commodity capacity * maxRatio),
         * if can directly increase base commodity capacity to achieve the maxRatio.
         *
         * @return true if increasing base capacity to achieve maxRatio is default supported.
         */
        public boolean getIncreaseBaseDefaultSupported() {
            return increaseBaseDefaultSupported_;
        }
    }

    /**
     * A class to represent the capacity range dependency for one or more commodities on base commodity.
     */
    public static class RangeBasedResourceDependency {
        private CommoditySpecification baseCommodity_;
        // Mapping from base commodity capacity to dependent commodities' capacities.
        private final TreeMap<Double, Map<CommoditySpecification, Double>> rangedCapacityPairs_ = new TreeMap<>();
        // Mapping from dependent commodity type to a treeMap, where treeMap records to achieve each dependent capacity,
        // the minimal quantity of base commodity.
        private final Map<CommoditySpecification, TreeMap<Double, Double>> dependentCapacity2LeastBaseCapacityPairs_ = new HashMap<>();

        /**
         * Initialize RangeBasedResourceDependency object representing commodity capacity range constraints.
         *
         * @param baseCommodity base commodity type.
         * @param tuplesPerDependentType mapping from dependent commodity type to dependent commodity tuples.
         */
        public RangeBasedResourceDependency(CommoditySpecification baseCommodity,
                                            Map<CommoditySpecification, List<RangeTuple>> tuplesPerDependentType) {
            baseCommodity_ = baseCommodity;
            initializeRangePairs(tuplesPerDependentType);
        }

        private void initializeRangePairs(@Nonnull Map<CommoditySpecification, List<RangeTuple>> tuplesPerDependentType) {
            tuplesPerDependentType.forEach((dependentType, tuplesList) -> {
                for (RangeTuple tuple : tuplesList) {
                    final double baseCap = tuple.getBaseMaxCapacity();
                    final double dependentCap = tuple.getDependentMaxCapacity();
                    rangedCapacityPairs_.computeIfAbsent(baseCap, m -> new HashMap<>()).put(dependentType, dependentCap);
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
         * Returns dependent commodities' capacities, given base commodity quantity.
         *
         * @param baseCommodityQuantity base commodity quantity.
         * @return dependent commodities' capacities.
         */
        @Nonnull
        public Map<CommoditySpecification, Double> getDependentCommodityCapacity(Double baseCommodityQuantity) {
            Entry<Double, Map<CommoditySpecification, Double>> pair = rangedCapacityPairs_.ceilingEntry(baseCommodityQuantity);
            return pair == null ? Collections.emptyMap() : pair.getValue();
        }

        /**
         * To achieve a certain quantity for given dependent commodity type, calculate least quantity
         * for base commodity quantity.
         *
         * @param dependentCommType commodity type.
         * @param dependentCommodityQuantity dependent commodity quantity.
         * @return base commodity quantity.
         */
        @Nullable
        public Double getLeastBaseCommodityCapacity(CommoditySpecification dependentCommType, double dependentCommodityQuantity) {
            TreeMap<Double, Double> pairs = dependentCapacity2LeastBaseCapacityPairs_.get(dependentCommType);
            if (pairs == null) {
                return null;
            }
            Entry<Double, Double> entry = pairs.ceilingEntry(dependentCommodityQuantity);
            return entry == null ? null : entry.getValue();
        }
    }

    /**
     * A utility method to construct storage pricing information.
     *
     * @param resourceCostList a list of cost data
     * @return map The map has commodity specification as key and a mapping of business account to
     * price as value.
     */
    public static Map<CommoditySpecification, Table<Long, Long, List<PriceData>>>
    translateResourceCostForStorageTier(List<StorageResourceCost> resourceCostList) {
        Map<CommoditySpecification, Table<Long, Long, List<PriceData>>> priceDataMap = new HashMap<>();
        for (StorageResourceCost resource : resourceCostList) {
            final CommoditySpecification commoditySpecification
                    = ProtobufToAnalysis.commoditySpecification(resource.getResourceType());
            final Table<Long, Long, List<PriceData>> accountRegionPriceDataTable
                    = priceDataMap.computeIfAbsent(commoditySpecification, t -> HashBasedTable.create());
            for (StorageTierPriceData priceData : resource.getStorageTierPriceDataList()) {
                for (CostDTO.CostTuple costTuple : priceData.getCostTupleListList()) {
                    final long businessAccountId = costTuple.getBusinessAccountId();
                    final long regionId = costTuple.getRegionId();
                    PriceData price = new PriceData(priceData.getUpperBound(), costTuple.getPrice(),
                            priceData.getIsUnitPrice(), priceData.getIsAccumulativeCost(), regionId);
                    List<PriceData> priceDataList = accountRegionPriceDataTable.get(businessAccountId, regionId);
                    if (priceDataList == null) {
                        priceDataList = new ArrayList<>();
                        accountRegionPriceDataTable.put(businessAccountId, regionId, priceDataList);
                    }
                    priceDataList.add(price);
                }
            }
        }
        // make sure price list is ascending based on upper bound, because we need to get the
        // first resource range that can satisfy the requested amount, and the price
        // corresponds to that range will be used as price
        priceDataMap.values().stream().flatMap(table -> table.values().stream()).forEach(list -> Collections.sort(list));
        return priceDataMap;
    }

    /**
     * A utility method to extract commodity capacity constraint from DTO and put it in a map.
     *
     * @param storageResourceLimitations the DTO representing commodity min/max constraint.
     * @param commTypesWithConstraints the set to keep commodity types that are related to tier constraints.
     * @return a map for CommoditySpecification to capacity constraint.
     */
    @Nonnull
    public static Map<CommoditySpecification, CapacityLimitation>
    translateResourceCapacityLimitation(List<StorageResourceLimitation> storageResourceLimitations,
                                        Set<CommoditySpecification> commTypesWithConstraints) {
        final Map<CommoditySpecification, CapacityLimitation> commCapacity = new HashMap<>();
        for (StorageResourceLimitation resourceLimit : storageResourceLimitations) {
            if (!commCapacity.containsKey(resourceLimit.getResourceType())) {
                commCapacity.put(
                        ProtobufToAnalysis.commoditySpecification(
                                resourceLimit.getResourceType()),
                        new CapacityLimitation(resourceLimit.getMinCapacity(),
                                resourceLimit.getMaxCapacity(), resourceLimit.getCheckMinCapacity()));
            } else {
                throw new IllegalArgumentException("Duplicate constraint on capacity limitation");
            }
        }
        commTypesWithConstraints.addAll(commCapacity.keySet());
        return commCapacity;
    }

    /**
     * A utility method to extract base and dependent commodities and their ratio constraint.
     *
     * @param dependencyDTOs the DTO represents the base and dependent commodity ratio relation.
     * @param commTypesWithConstraints the set to keep commodity types that are related to tier constraints.
     * @return a list of {@link RatioBasedResourceDependency}
     */
    @Nonnull
    public static List<RatioBasedResourceDependency>
    translateStorageResourceRatioDependency(List<StorageResourceRatioDependency> dependencyDTOs,
                                            Set<CommoditySpecification> commTypesWithConstraints) {
        final List<RatioBasedResourceDependency> dependencyList = new ArrayList<>();
        dependencyDTOs.forEach(dto -> {
            CommoditySpecification baseType = ProtobufToAnalysis.commoditySpecification(dto.getBaseResourceType());
            CommoditySpecification dependentType = ProtobufToAnalysis.commoditySpecification(dto.getDependentResourceType());
            final boolean hasMinRatio = dto.hasMinRatio();
            dependencyList.add(new RatioBasedResourceDependency(baseType, dependentType,
                    dto.getMaxRatio(), hasMinRatio, hasMinRatio ? dto.getMinRatio() : 0d,
                    dto.getIncreaseBaseDefaultSupported()));
            commTypesWithConstraints.add(baseType);
            commTypesWithConstraints.add(dependentType);
        });
        return dependencyList;
    }

    /**
     * A utility method to extract base and dependent commodities and their range constraint.
     *
     * @param dependencyDTOs the DTO represents the base and dependent commodity range relation.
     * @param commTypesWithConstraints the set to keep commodity types that are related to tier constraints.
     * @return a list of {@link RangeBasedResourceDependency}
     */
    @Nonnull
    public static List<RangeBasedResourceDependency>
    translateStorageResourceRangeDependency(List<StorageResourceRangeDependency> dependencyDTOs,
                                            Set<CommoditySpecification> commTypesWithConstraints) {
        Map<CommoditySpecification, List<StorageResourceRangeDependency>> dtosPerBaseType =
                dependencyDTOs.stream().collect(groupingBy(dto -> ProtobufToAnalysis.commoditySpecification(dto.getBaseResourceType())));
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
            commTypesWithConstraints.add(baseType);
            commTypesWithConstraints.addAll(tuplesPerDependentType.keySet());

        });
        return dependencyList;
    }

    /**
     * Get Quote from a StorageTier for a given volume shoppingList.
     *
     * @param sl is the {@link ShoppingList} that is requesting price.
     * @param seller {@link Trader} that the buyer matched to.
     * @param commQuantityMap commQuantityMap used for CapacityLimitation check.
     * @param priceDataMap a map of map to keep the commodity to its price per business account.
     * @param commCapacityLimitation the map to keep commodity to its min max capacity limitation.
     * @param ratioDependencyList ratio capacity constraint between commodities.
     * @param rangeDependencyList range capacity constraint between commodities.
     * @param isSavingsMode if volume is in Savings mode.
     * @param appendPenalty if penalty cost should be counted into quote cost.
     * @return quote for given volume shoppingList on given StorageTier seller.
     */
    public static MutableQuote calculateStorageTierQuote(ShoppingList sl, Trader seller,
                                                         Map<CommoditySpecification, Double> commQuantityMap,
                                                         Map<CommoditySpecification, Table<Long, Long, List<PriceData>>> priceDataMap,
                                                         Map<CommoditySpecification, CapacityLimitation> commCapacityLimitation,
                                                         List<RatioBasedResourceDependency> ratioDependencyList,
                                                         List<RangeBasedResourceDependency> rangeDependencyList,
                                                         boolean isSavingsMode, boolean appendPenalty) {
        // Differently from commQuantityMap, which is used to preserve sl's commodity demand,
        // commCapacityMap is used to preserve commodities' capacity for commTypesWithConstraints.
        // commCapacityMap is updated during constraint check, and finally reflect projected sl's state.
        Map<CommoditySpecification, Double> commCapacityMap = new HashMap<>();

        final MutableQuote capacityLimitationQuote = getCommodityWithinCapacityLimitationQuote(
                sl, seller, commQuantityMap, commCapacityMap, commCapacityLimitation, isSavingsMode);
        if (capacityLimitationQuote != null) {
            return capacityLimitationQuote;
        }

        final MutableQuote ratioBasedResourceDependencyQuote = getRatioBasedResourceDependencyQuote(
                sl, seller, commQuantityMap, commCapacityMap, commCapacityLimitation, ratioDependencyList, isSavingsMode);
        if (ratioBasedResourceDependencyQuote != null) {
            return ratioBasedResourceDependencyQuote;
        }

        final MutableQuote rangeBasedResourceDependencyQuote = getRangeBasedResourceDependencyQuote(
                sl, seller, commQuantityMap, commCapacityMap, rangeDependencyList, isSavingsMode);
        if (rangeBasedResourceDependencyQuote != null) {
            return rangeBasedResourceDependencyQuote;
        }

        // Commodity types with price, which are decisive commodities on the tier.
        Set<CommoditySpecification> decisiveComms = priceDataMap.keySet();
        // For decisive commodities, new capacity should come from commQuantityMap.
        decisiveComms.stream().forEach(c -> {
            final Double val = commQuantityMap.get(c);
            if (val != null) {
                commCapacityMap.put(c, val);
            }
        });
        // Create CommodityContexts based on commCapacityMap.
        // CommodityContext is preserved in the Quote to reflect commodity's projected state
        // for the sl to stay on the tier.
        List<CommodityContext> commContexts = new ArrayList<>();
        commCapacityMap.forEach((comm, capacity) ->
                commContexts.add(new CommodityContext(comm, capacity, decisiveComms.contains(comm))));

        return calculateStorageTierCost(commQuantityMap, sl, seller, commContexts, priceDataMap, appendPenalty);
    }

    /**
     * For commodities that have CapacityLimitation, check max/min limitations, and update commQuantityMap, commCapacityMap.
     * Return null if all checks pass, otherwise return InfiniteBelowMinAboveMaxCapacityLimitationQuote.
     *
     * @param sl is the {@link ShoppingList} that is requesting price.
     * @param seller {@link Trader} that the buyer matched to.
     * @param commQuantityMap commQuantityMap used for CapacityLimitation check.
     * @param commCapacityMap commCapacityMap used to record commodity capacities.
     * @param commCapacityLimitation the map to keep commodity to its min max capacity limitation.
     * @param isSavingsMode if volume is in Savings mode.
     * @return If all commodity quantities fit within CapacityLimitations, returns null.
     *         If one or more commodity quantities does not fit, return InfiniteBelowMinAboveMaxCapacityLimitationQuote.
     */
    @Nullable
    private static MutableQuote getCommodityWithinCapacityLimitationQuote(@Nonnull ShoppingList sl, @Nonnull Trader seller,
                                                                          @Nonnull Map<CommoditySpecification, Double> commQuantityMap,
                                                                          @Nonnull Map<CommoditySpecification, Double> commCapacityMap,
                                                                          @Nonnull Map<CommoditySpecification, CapacityLimitation> commCapacityLimitation,
                                                                          boolean isSavingsMode) {
        // check if the commodities bought comply with capacity limitation on seller
        for (Entry<CommoditySpecification, CapacityLimitation> entry : commCapacityLimitation.entrySet()) {
            final CommoditySpecification commoditySpecification = entry.getKey();
            final Double commodityQuantity = commQuantityMap.get(commoditySpecification);
            if (commodityQuantity == null) {
                continue;
            }
            final CapacityLimitation capacityLimitation = entry.getValue();
            final double maxCapacity = capacityLimitation.getMaxCapacity();
            // Check max capacity
            if (commodityQuantity > maxCapacity) {
                return new InfiniteBelowMinAboveMaxCapacityLimitationQuote(
                        commoditySpecification, capacityLimitation, commodityQuantity);
            }
            // Check min capacity
            if (commodityQuantity < capacityLimitation.getMinCapacity()) {
                // If no need to check capacity min limitation for the commodity, update commQuantityMap.
                // If need to check capacity min limitation(StorageAmount commodity), and isSavingsMode, update commQuantityMap.
                if (!capacityLimitation.getCheckMinCapacity() || isSavingsMode) {
                    commQuantityMap.put(commoditySpecification, capacityLimitation.getMinCapacity());
                    logger.debug("Adjusting shoppingList {} commodity {} quantity from {} to {},"
                                    + " because old quantity is below tier {} min capacity",
                            sl.getDebugInfoNeverUseInCode(), commoditySpecification.getDebugInfoNeverUseInCode(),
                            commodityQuantity, capacityLimitation.getMinCapacity(), seller.getDebugInfoNeverUseInCode());
                } else {
                    // If need to check capacity min limitation for StorageAmount commodity, and not in Savings mode.
                    return new InfiniteBelowMinAboveMaxCapacityLimitationQuote(commoditySpecification, capacityLimitation, commodityQuantity);
                }
            }
            // Update commodity capacity in commCapacityMap if needed
            if (maxCapacity < commCapacityMap.computeIfAbsent(commoditySpecification, d -> maxCapacity)) {
                commCapacityMap.put(commoditySpecification, maxCapacity);
            }
        }
        return null;
    }

    /**
     * Validate if the requested quantity comply with the ratio limitation between base commodity
     * and dependency commodity, and update commQuantityMap, commCapacityMap.
     * Return null if all checks pass, otherwise return InfiniteRatioBasedResourceDependencyQuote.
     *
     * @param sl is the {@link ShoppingList} that is requesting price.
     * @param seller {@link Trader} that the buyer matched to.
     * @param commQuantityMap commQuantityMap used for ratio dependencies check.
     * @param commCapacityMap commCapacityMap used to record commodity capacities.
     * @param commCapacityLimitation the map to keep commodity to its min max capacity limitation.
     * @param ratioDependencyList ratio capacity constraint between commodities.
     * @param isSavingsMode if volume is in Savings mode.
     * @return If all commodity quantities fit within RatioBasedResourceDependencies, returns null.
     *         If one or more commodity quantities does not fit, return InfiniteRatioBasedResourceDependencyQuote.
     */
    @Nullable
    private static MutableQuote getRatioBasedResourceDependencyQuote(@Nonnull ShoppingList sl, @Nonnull Trader seller,
                                                                     @NonNull Map<CommoditySpecification, Double> commQuantityMap,
                                                                     @Nonnull Map<CommoditySpecification, Double> commCapacityMap,
                                                                     @Nonnull Map<CommoditySpecification, CapacityLimitation> commCapacityLimitation,
                                                                     @Nonnull List<RatioBasedResourceDependency> ratioDependencyList,
                                                                     boolean isSavingsMode) {
        for (RatioBasedResourceDependency dependency : ratioDependencyList) {
            final CommoditySpecification baseType = dependency.getBaseCommodity();
            final CommoditySpecification dependentType = dependency.getDependentCommodity();
            final Double baseCommQuantity = commQuantityMap.get(baseType);
            final Double dependentCommQuantity = commQuantityMap.get(dependentType);
            if (baseCommQuantity == null || dependentCommQuantity == null) {
                continue;
            }
            // Check minRatio if exists.
            if (dependency.hasMinRatio()) {
                final double minDependentQuantity = dependency.getMinRatio() * baseCommQuantity;
                if (dependentCommQuantity < minDependentQuantity) {
                    // increase dependentCommQuantity because of minRatio
                    commQuantityMap.put(dependentType, minDependentQuantity);
                    logger.debug("Adjusting shoppingList {} commodity {} quantity from {} to {},"
                                    + " because old quantity is below tier {} min ratio {} with base commodity {} quantity {}",
                            sl.getDebugInfoNeverUseInCode(), dependentType.getDebugInfoNeverUseInCode(),
                            dependentCommQuantity, minDependentQuantity, seller.getDebugInfoNeverUseInCode(),
                            dependency.getMinRatio(), baseType.getDebugInfoNeverUseInCode(), baseCommQuantity);
                    return getRatioBasedResourceDependencyQuote(
                            sl, seller, commQuantityMap, commCapacityMap, commCapacityLimitation, ratioDependencyList, isSavingsMode);
                }
            }
            // Check maxRatio.
            final double maxRatio = dependency.getMaxRatio();
            double maxAllowedQuantity = baseCommQuantity * maxRatio;
            if (commCapacityLimitation.containsKey(dependentType)) {
                maxAllowedQuantity = Math.max(maxAllowedQuantity, commCapacityLimitation.get(dependentType).getMinCapacity());
            }
            if (dependentCommQuantity > maxAllowedQuantity) {
                if (dependency.getIncreaseBaseDefaultSupported() || isSavingsMode) {
                    // Increase baseCommQuantity to achieve maxRatio.
                    // newBaseCommQuantity will be ceiling integer of (dependentCommQuantity / maxRatio).
                    final double newBaseCommQuantity = Math.ceil(dependentCommQuantity / maxRatio);
                    commQuantityMap.put(baseType, newBaseCommQuantity);
                    logger.debug("Adjusting shoppingList {} commodity {} quantity from {} to {},"
                                    + " because old quantity is below tier {} max ratio {} with dependent commodity {} quantity {}",
                            sl.getDebugInfoNeverUseInCode(), baseType.getDebugInfoNeverUseInCode(),
                            baseCommQuantity, newBaseCommQuantity, seller.getDebugInfoNeverUseInCode(),
                            maxRatio, dependentType.getDebugInfoNeverUseInCode(), dependentCommQuantity);
                    return getRatioBasedResourceDependencyQuote(
                            sl, seller, commQuantityMap, commCapacityMap, commCapacityLimitation, ratioDependencyList, isSavingsMode);
                } else {
                    return new InfiniteRatioBasedResourceDependencyQuote(dependency, baseCommQuantity, dependentCommQuantity);
                }
            } else {
                // MaxRatio constraint is met.
                // Update commodity capacity in commCapacityMap if needed.
                final Double oldCap = commCapacityMap.get(dependentType);
                if (oldCap == null || maxAllowedQuantity < oldCap) {
                    commCapacityMap.put(dependentType, maxAllowedQuantity);
                }
            }
        }
        return null;
    }

    /**
     * Validate if the requested amount comply with the range dependencies between base commodity
     * and dependency commodity, and update commQuantityMap, commCapacityMap.
     * Return null if all checks pass, otherwise return InfiniteRangeBasedResourceDependencyQuote.
     *
     * @param sl is the {@link ShoppingList} that is requesting price.
     * @param seller {@link Trader} that the buyer matched to.
     * @param commQuantityMap commQuantityMap used for range dependencies check.
     * @param commCapacityMap commCapacityMap used to record commodity capacities.
     * @param rangeDependencyList range capacity constraint between commodities.
     * @param isSavingsMode if volume is in Savings mode.
     * @return If all commodity quantities fit within RangeBasedResourceDependencies, returns null.
     *         If one or more commodity quantities does not fit, return InfiniteRangeBasedResourceDependencyQuote.
     */
    @Nullable
    private static MutableQuote getRangeBasedResourceDependencyQuote(@Nonnull ShoppingList sl, @Nonnull Trader seller,
                                                                     @NonNull Map<CommoditySpecification, Double> commQuantityMap,
                                                                     @Nonnull Map<CommoditySpecification, Double> commCapacityMap,
                                                                     @Nonnull List<RangeBasedResourceDependency> rangeDependencyList,
                                                                     boolean isSavingsMode) {
        for (RangeBasedResourceDependency dependency : rangeDependencyList) {
            final CommoditySpecification baseType = dependency.getBaseCommodity();
            final Double baseCommQuantity = commQuantityMap.get(baseType);
            if (baseCommQuantity == null) {
                continue;
            }
            for (Entry<CommoditySpecification, Double> dependPairs : dependency.getDependentCommodityCapacity(baseCommQuantity).entrySet()) {
                final CommoditySpecification dependentType = dependPairs.getKey();
                final Double dependentCapacity = dependPairs.getValue();
                final Double dependentCommQuantity = commQuantityMap.get(dependentType);
                if (dependentCommQuantity == null) {
                    continue;
                } else if (dependentCapacity >= dependentCommQuantity) {
                    // Update commodity capacity in commCapacityMap if needed.
                    if (dependentCapacity < commCapacityMap.computeIfAbsent(dependentType, d -> dependentCapacity)) {
                        commCapacityMap.put(dependentType, dependentCapacity);
                    }
                    continue;
                }
                // dependentCommQuantity exceeds dependentCapacity.
                if (!isSavingsMode) {
                    return new InfiniteRangeBasedResourceDependencyQuote(baseType,
                            dependentType, dependentCapacity, baseCommQuantity, dependentCommQuantity);
                } else {
                    // For Savings mode, get new base commodity quantity that can achieve dependentCommQuantity.
                    final Double newBaseCommQuantity = dependency.getLeastBaseCommodityCapacity(dependentType, dependentCommQuantity);
                    if (newBaseCommQuantity == null) {
                        return new InfiniteRangeBasedResourceDependencyQuote(baseType,
                                dependentType, dependentCapacity, baseCommQuantity, dependentCommQuantity);
                    }
                    // Update commQuantityMap for baseType.
                    commQuantityMap.put(baseType, Math.max(baseCommQuantity, newBaseCommQuantity));
                    logger.debug("Adjusting shoppingList {} commodity {} quantity from {} to {},"
                                    + " because old quantity is below tier {} requirement to achieve dependent commodity {} quantity {}",
                            sl.getDebugInfoNeverUseInCode(), baseType.getDebugInfoNeverUseInCode(),
                            baseCommQuantity, newBaseCommQuantity, seller.getDebugInfoNeverUseInCode(),
                            dependentType.getDebugInfoNeverUseInCode(), dependentCommQuantity);
                    // With updated commQuantityMap, check range dependencies again.
                    return getRangeBasedResourceDependencyQuote(
                            sl, seller, commQuantityMap, commCapacityMap, rangeDependencyList, isSavingsMode);
                }
            }
        }
        return null;
    }

    /**
     * Calculates the total cost of all resources requested by commCapacityMap on a seller.
     *
     * @param commQuantityMap the map containing commodity quantity used for price calculation.
     * @param sl the shopping list requests resources.
     * @param seller the seller.
     * @param commodityContext CommodityContext to be set on the quote.
     * @param priceDataMap a map of map to keep the commodity to its price per business account.
     * @param appendPenalty if penalty cost should be counted into quote cost.
     * @return the Quote given by {@link CostFunction}
     */
    private static CommodityQuote calculateStorageTierCost(@Nonnull Map<CommoditySpecification, Double> commQuantityMap,
                                                           @Nonnull ShoppingList sl, @Nonnull Trader seller,
                                                           @Nonnull List<CommodityContext> commodityContext,
                                                           @Nonnull Map<CommoditySpecification, Table<Long, Long, List<PriceData>>> priceDataMap,
                                                           boolean appendPenalty) {
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
                commQuantityMap, priceDataMap, appendPenalty);
        Double cost = costAccountPair.first;
        Long chosenAccountId = costAccountPair.second;
        // If no pricing was found for commodities in basket, then return infinite quote for seller.
        if (cost == Double.MAX_VALUE) {
            logger.warn("No (cheapest) cost found for storage {} in region {}, account {}.",
                    seller.getDebugInfoNeverUseInCode(), regionId, balanceAccount);
            return new CommodityQuote(seller, Double.POSITIVE_INFINITY);
        }
        return new CommodityCloudQuote(seller, cost, regionId, chosenAccountId, commodityContext);
    }

    /**
     * Gets the cost for all commodities in the priceDataMap. Used to calculate cheapest cost
     * across all regions and accounts.
     *
     * @param regionId Region id for which cost needs to be obtained.
     * @param accountId Business account id.
     * @param priceId Pricing id for account.
     * @param commQuantityMap the map containing commodity quantity used for price calculation.
     * @param priceDataMap Map containing all cost data.
     * @param appendPenalty if penalty cost should be counted into quote cost.
     * @return Pair with first value as the total cost for region, and second value as the chosen
     * business account (could be the price id if found). If there are no commodities in the pricing
     * map or if we could not get any costs, then Double.MAX_VALUE is returned for cost.
     */
    @Nonnull
    private static Pair<Double, Long> getTotalCost(
            long regionId, long accountId, @Nullable Long priceId,
            @Nonnull Map<CommoditySpecification, Double> commQuantityMap,
            @Nonnull Map<CommoditySpecification, Table<Long, Long, List<PriceData>>> priceDataMap,
            boolean appendPenalty) {
        // Cost if we didn't get any valid commodity pricing, will be max, so that we don't
        // mistakenly think this region is cheapest because it is $0.
        double totalCost = Double.MAX_VALUE;
        Long businessAccountChosenId = null;

        // iterating the priceDataMap for each type of commodity resource
        for (Entry<CommoditySpecification, Table<Long, Long, List<PriceData>>> commodityPrice
                : priceDataMap.entrySet()) {
            final Double requestedAmount = commQuantityMap.get(commodityPrice.getKey());
            if (requestedAmount == null) {
                // The commQuantityMap is from the buyer, which may not buy all resources sold
                // by the seller, e.g: some VM does not request IOPS sold by IO1. We can skip it
                // when trying to compute cost.
                continue;
            }
            final Table<Long, Long, List<PriceData>> priceTable = commodityPrice.getValue();
            // priceMap may contain PriceData by price id. Price id is the identifier for a price
            // offering associated with a Balance Account. Different Balance Accounts (i.e.
            // Balance Accounts with different ids) may have the same price id, if they are
            // associated with the same price offering. If no entry is found in the priceMap for a
            // price id, then the Balance Account id is used to lookup the priceMap.
            businessAccountChosenId = priceId != null && priceTable.containsRow(priceId)
                    ? priceId : accountId;
            final List<PriceData> priceDataList = priceTable.get(businessAccountChosenId, regionId);
            if (priceDataList != null) {
                double cost = getCostFromPriceDataList(requestedAmount, priceDataList);
                if (cost != Double.MAX_VALUE) {
                    if (totalCost == Double.MAX_VALUE) {
                        // Initialize cost now that we are getting a valid cost.
                        totalCost = appendPenalty ? REVERSIBILITY_PENALTY_COST : 0;
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
     * @return the cost for providing the given requested amount
     */
    private static double getCostFromPriceDataList(final double requestedAmount,
                                                   @NonNull final List<PriceData> priceDataList) {
        for (PriceData priceData : priceDataList) {
            // the list of priceData is sorted based on upper bound
            if (requestedAmount > priceData.getUpperBound()) {
                continue;
            }
            if (priceData.isAccumulative()) {
                return priceData.getPrice();
            } else if (priceData.isUnitPrice()) {
                return priceData.getPrice() * requestedAmount;
            }
        }
        return Double.MAX_VALUE;
    }
}

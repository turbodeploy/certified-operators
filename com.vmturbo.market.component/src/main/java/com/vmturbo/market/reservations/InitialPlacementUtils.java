package com.vmturbo.market.reservations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer.InitialPlacementCommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.market.reservations.InitialPlacementFinderResult.FailureInfo;
import com.vmturbo.market.topology.conversions.MarketAnalysisUtils;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.CommoditySoldWithSettings;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyCacheDTOs.EconomyCacheDTO;
import com.vmturbo.platform.analysis.protobuf.EconomyCacheDTOs.EconomyCacheDTO.CommTypeEntry;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO.SumOfCommodity;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;
import com.vmturbo.platform.analysis.utilities.InfiniteQuoteExplanation;
import com.vmturbo.platform.analysis.utilities.InfiniteQuoteExplanation.CommodityBundle;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A class with utility methods for initial placement.
 */
public final class InitialPlacementUtils {
    /**
     * Logger.
     */
    private static final Logger logger = LogManager.getLogger();

    /**
     * prefix for initial placement log messages.
     */
    private static final String logPrefix = "FindInitialPlacement: ";

    /**
     * The clone prefix name.
     */
    public static final String PLACEMENT_CLONE_SUFFIX = "_PLACEMENT_CLONE";

    /**
     * The provider types to be included in economy cache.
     */
    public static final Set<Integer> PROVIDER_ENTITY_TYPES = ImmutableSet.of(EntityType.PHYSICAL_MACHINE_VALUE,
            EntityType.STORAGE_VALUE);

    /**
     * Constructor.
     */
    private InitialPlacementUtils() {}

    /**
     * A wrapper class to carry the commodity specification and commodity sold information.
     */
    private static class UpdateCommodityWrapper {
        CommoditySpecification commSpec;
        CommoditySoldWithSettings commSold;

        UpdateCommodityWrapper(@Nonnull final CommoditySpecification commSpec,
                @Nonnull final CommoditySoldWithSettings commSold) {
            this.commSpec = commSpec;
            this.commSold = commSold;
        }
    }

    /**
     * Clones the economy which contains the latest broadcast entities. Only physical machine,
     * storage traders and existing reservation entities are kept in the cloned economy.
     *
     * @param originalEconomy the economy to be cloned
     * @param cloneForDiags if true clone all traders since the economy is coming from diagnostics.
     * @return a copy of original economy which contains only PM and DS.
     */
    public static Economy cloneEconomy(@Nonnull final UnmodifiableEconomy originalEconomy,
                                       boolean cloneForDiags) {
        Topology t = new Topology();
        Economy cloneEconomy = t.getEconomyForTesting();
        cloneEconomy.setTopology(t);
        Map<Long, Trader> cloneTraderToOidMap = t.getModifiableTraderOids();
        originalEconomy.getTraders().stream()
                .filter(trader -> PROVIDER_ENTITY_TYPES.contains(trader.getType()) || cloneForDiags)
                .forEach(trader -> {
                    Trader cloneTrader = cloneTraderForInitialPlacement(trader, cloneEconomy);
                    cloneTraderToOidMap.put(trader.getOid(), cloneTrader);
                });
        return cloneEconomy;
    }

    /**
     * Clone a trader into economy.
     *
     * @param trader source trader
     * @param into economy to create clone in
     * @return new trader
     */
    public static Trader cloneTraderForInitialPlacement(Trader trader, Economy into) {
        Trader cloneTrader = into.addTrader(trader.getType(), trader.getState(),
                        new Basket(trader.getBasketSold()), trader.getCliques());
        cloneTrader.setOid(trader.getOid());

        // Copy bare minimum trader properties
        cloneTrader.setDebugInfoNeverUseInCode(
                trader.getDebugInfoNeverUseInCode());
        cloneTrader.getSettings().setQuoteFunction(
                trader.getSettings().getQuoteFunction());
        cloneTrader.getSettings().setCanAcceptNewCustomers(true);
        cloneTrader.getSettings().setIsShopTogether(true);
        cloneCommoditiesSold(trader, cloneTrader);
        return cloneTrader;
    }

    /**
     * Clones the commodities sold from one trader (the original) to its clone.
     * @param trader the original trader
     * @param cloneTrader the clone of the original trader
     */
    private static void cloneCommoditiesSold(Trader trader, Trader cloneTrader) {
        List<CommoditySold> commoditiesSold = trader.getCommoditiesSold();
        List<CommoditySold> cloneCommoditiesSold = cloneTrader.getCommoditiesSold();
        for (int commIndex = 0; commIndex < commoditiesSold.size(); commIndex++) {
            CommoditySold commSold = commoditiesSold.get(commIndex);
            CommoditySold cloneCommSold = cloneCommoditiesSold.get(commIndex);
            cloneCommoditiesSoldAttributes(commSold, cloneCommSold);
        }
    }

    /**
     * Clones each attribute from a model commoditySold to a clone commoditySold.
     *
     * @param modelCommSold the commSold serves as a model.
     * @param cloneCommSold the newly cloned commSold
     */
    private static void cloneCommoditiesSoldAttributes(@Nonnull CommoditySold modelCommSold,
            @Nonnull CommoditySold cloneCommSold) {
        cloneCommSold.setCapacity(modelCommSold.getCapacity());
        cloneCommSold.setQuantity(modelCommSold.getQuantity());
        cloneCommSold.setPeakQuantity(modelCommSold.getPeakQuantity());
        CommoditySoldSettings commSoldSettings = modelCommSold.getSettings();
        CommoditySoldSettings cloneCommSoldSettings = cloneCommSold.getSettings();
        cloneCommSoldSettings.setPriceFunction(commSoldSettings.getPriceFunction())
                .setUpdatingFunction(commSoldSettings.getUpdatingFunction())
                .setUtilizationUpperBound(commSoldSettings.getUtilizationUpperBound());
    }

    /**
     * Create traderTOs based on the given InitialPlacementBuyer list.
     *
     * @param buyer the given reservation buyer information.
     * @param commTypeToSpecMap topology dto commodity type to trader commodity specification map.
     * @param clusterCommPerSl the shopping list oid to boundary commodity type mapping.
     * @return a list of traderTOs.
     */
    public static Optional<TraderTO> constructTraderTO(@Nonnull final InitialPlacementBuyer buyer,
            @Nonnull final BiMap<CommodityType, Integer> commTypeToSpecMap,
            @Nonnull final Map<Long, CommodityType> clusterCommPerSl) {
        try {
            TraderTO.Builder traderTO = TraderTO.newBuilder();
            for (InitialPlacementCommoditiesBoughtFromProvider sl
                    : buyer.getInitialPlacementCommoditiesBoughtFromProviderList()) {
                long slOid = sl.getCommoditiesBoughtFromProviderId();
                CommodityType boundaryCommType = clusterCommPerSl.get(slOid);
                List<CommodityBoughtTO> commBoughtTOs = constructCommBoughtTO(
                        sl.getCommoditiesBoughtFromProvider().getCommodityBoughtList(),
                        commTypeToSpecMap, boundaryCommType);
                if (commBoughtTOs.isEmpty()) {
                    logger.warn(logPrefix + "Cannot create trader {} due to invalid constraint, skipping"
                            + " reservation for it", buyer.getBuyerId());
                    return Optional.empty();
                }
                // NOTE: InitialPlacementShoppingList has a provider type attribute, but sl doesnt
                // have a counterpart representing it.
                ShoppingListTO slTO = ShoppingListTO.newBuilder()
                        .setOid(sl.getCommoditiesBoughtFromProviderId())
                        .setMovable(true)
                        .addAllCommoditiesBought(commBoughtTOs)
                        .build();
                traderTO.addShoppingLists(slTO);
            }
            traderTO.setOid(buyer.getBuyerId()).setDebugInfoNeverUseInCode(
                        buyer.getBuyerId() + PLACEMENT_CLONE_SUFFIX)
                        .setState(TraderStateTO.ACTIVE)
                        .setSettings(TraderSettingsTO.newBuilder()
                                .setIsShopTogether(true)
                                .setQuoteFunction(QuoteFunctionDTO.newBuilder()
                                        .setSumOfCommodity(SumOfCommodity.newBuilder().build())));
            return Optional.of(traderTO.build());

        } catch (Exception e) {
            logger.error(logPrefix + "Cannot create trader with ID {} because of exception {}",
                    buyer.getBuyerId(), e);
            return Optional.empty();
        }
    }

    /**
     * Create commodityBoughtTOs based on the list of InitialPlacementCommodityBoughtDTO.
     *
     * @param commBoughtList the given reservation commodityBoughtDTO of each entity.
     * @param commTypeToSpecMap topology dto commodity type to trader commodity specification map.
     * @param boundaryCommType the cluster or storage cluster commodity type.
     * @return a list of commodityBoughtTO in traderTO.
     */
    private static List<CommodityBoughtTO> constructCommBoughtTO(
            @Nonnull final List<TopologyDTO.CommodityBoughtDTO> commBoughtList,
            @Nonnull final BiMap<CommodityType, Integer> commTypeToSpecMap,
            @Nullable final CommodityType boundaryCommType) {
        List<CommodityBoughtTO> commBoughtTOs = new ArrayList<>();
        // In case the cluster or storage cluster commodity already in the commBoughtList,
        // For example, the cluster maybe provided by user thus it is part of commBoughtList
        boolean needsExtraBoundaryCommBought = true;
        if (boundaryCommType == null || (commBoughtList.stream().anyMatch(c ->
                c.getCommodityType().getType() == boundaryCommType.getType()
                && c.getCommodityType().getKey() == boundaryCommType.getKey()))) {
            needsExtraBoundaryCommBought = false;
        }
        for (TopologyDTO.CommodityBoughtDTO commBought : commBoughtList) {
            Integer commSpecType = commTypeToSpecMap.get(commBought.getCommodityType());
            if (commSpecType == null) {
                logger.warn(logPrefix + "The reservation is given a commodity type {} key {} which may be just"
                                + " created in system, please wait one round of analysis and try this reservation again",
                        commBought.getCommodityType().getType(), commBought.getCommodityType().getKey());
                return new ArrayList<>();
            }
            CommodityBoughtTO commBoughtTO = CommodityBoughtTO.newBuilder()
                    .setSpecification(CommoditySpecificationTO.newBuilder()
                            .setBaseType(commBought.getCommodityType().getType())
                            // NOTE: The type is not consistent between market cycles
                            .setType(commSpecType)
                            .build())
                    .setQuantity((float)commBought.getUsed())
                    .setPeakQuantity((float)commBought.getUsed()).build();
            commBoughtTOs.add(commBoughtTO);
        }
        if (needsExtraBoundaryCommBought) {
            Integer type = commTypeToSpecMap.get(boundaryCommType);
            if (type != null) {
                CommodityBoughtTO clusterCommBoughtTO = CommodityBoughtTO.newBuilder()
                        .setSpecification(CommoditySpecificationTO.newBuilder().setBaseType(
                                boundaryCommType.getType())
                                // NOTE: The type is not consistent between market cycles
                                .setType(type).build())
                        // set very small quantity on the boundary comm bought
                        .setQuantity(0.001f)
                        .setPeakQuantity(0.001f)
                        .build();
                commBoughtTOs.add(clusterCommBoughtTO);
            }

        }
        return commBoughtTOs;
    }

    /**
     * Construct {@link TraderTO}s for all reservation buyers while preserving the reservation order.
     *
     * @param economy the economy to construct buyers.
     * @param commTypeToSpecMap the commodity type to commodity specification mapping.
     * @param buyerOidToPlacement the reservation buyers and their placement decisions.
     * @param existingReservations  map of existing reservations by oid.
     * @param includeDeployed if true add the reservations with deployed = true into economy cache.
     * @return a list of {@link TraderTO} lists. The order of reservation buyers follows the order
     * they were added.
     */
    @Nonnull
    public static Map<Long, List<TraderTO>> constructTraderTOListWithBoundary(
            @Nonnull final Economy economy,
            @Nonnull final BiMap<CommodityType, Integer> commTypeToSpecMap,
            @Nonnull final Map<Long, List<InitialPlacementDecision>> buyerOidToPlacement,
            @Nonnull final Map<Long, InitialPlacementDTO> existingReservations,
            boolean includeDeployed) {
        Map<Long, List<TraderTO>> placedBuyersPerRes = new LinkedHashMap<>();
        // Create the reservations one by one following the order they were added
        existingReservations.forEach((resId, findInitialPlacement) -> {
            List<TraderTO> placedBuyers = new ArrayList<>();
            findInitialPlacement.getInitialPlacementBuyerList().forEach(buyer -> {
                if (includeDeployed || !buyer.getDeployed()) {
                    List<InitialPlacementDecision> placementPerBuyer =
                            buyerOidToPlacement.get(buyer.getBuyerId());
                    if (placementPerBuyer != null && placementPerBuyer.stream()
                            .allMatch(r -> r.supplier.isPresent())) {
                        // When all shopping lists of a buyer have a supplier, figure out the cluster
                        // boundaries from the suppliers.
                        Optional<TraderTO> traderTO = constructTraderTOWithBoundary(economy,
                            commTypeToSpecMap, placementPerBuyer, buyer);
                        if (traderTO.isPresent()) {
                            placedBuyers.add(traderTO.get());
                        }
                    }
                }
            });
            placedBuyersPerRes.put(resId, placedBuyers);
        });
        return placedBuyersPerRes;
    }

    /**
     * Construct {@link TraderTO} for a given reservation buyer.
     *
     * @param economy the economy to construct buyers.
     * @param commTypeToSpecMap the commodity type to commodity specification mapping.
     * @param decisions the placement decisions for all shopping list of a buyer.
     * @param buyer the given reservation buyer.
     * @return {@link TraderTO} representing the buyer.
     */
    public static Optional<TraderTO> constructTraderTOWithBoundary(
            @Nonnull final Economy economy,
            @Nonnull final BiMap<CommodityType, Integer> commTypeToSpecMap,
            @Nonnull final List<InitialPlacementDecision> decisions,
            @Nonnull final InitialPlacementBuyer buyer) {
        Map<Long, CommodityType> clusterCommPerSl = new HashMap<>();
        try {
            for (InitialPlacementDecision placement : decisions) {
                if (placement.supplier.isPresent()) {
                    Trader supplier = economy.getTopology().getTradersByOid().get(placement.supplier.get());
                    if (supplier == null) {
                        logger.error(logPrefix + "Cannot create trader with ID {} because of invalid supplier",
                                buyer.getBuyerId());
                        return Optional.empty();
                    }
                    Optional<CommodityType> commType = findBoundaryComm(Optional.of(buyer),
                            placement.slOid, supplier, commTypeToSpecMap);
                    if (commType.isPresent()) {
                        clusterCommPerSl.put(placement.slOid, commType.get());
                    }
                }
            }
        } catch (Exception e) {
            logger.error(logPrefix + "Cannot create trader with ID {} because of exception {}",
                    buyer.getBuyerId(), e);
            return Optional.empty();
        }
            //construct traderTO from InitialPlacementBuyer including cluster boundary
        return InitialPlacementUtils.constructTraderTO(buyer, commTypeToSpecMap,
                    clusterCommPerSl);
    }

    /**
     * Figure out the cluster or storage cluster commodity for a given trader.
     *
     * @param originalBuyer the original {@link InitialPlacementBuyer}.
     * @param slOid the given shopping list oid.
     * @param provider a given trader which is a supplier.
     * @param commTypeToSpecMap the commodity type to commodity specification mapping.
     * @return CommodityType corresponding to cluster boundary.
     */
    public static Optional<TopologyDTO.CommodityType> findBoundaryComm(
            @Nonnull final Optional<InitialPlacementBuyer> originalBuyer,
            final long slOid, @Nonnull final Trader provider,
            @Nonnull final BiMap<CommodityType, Integer> commTypeToSpecMap) {
        // Assuming  pm has at most 1 cluster commodity
        Optional<CommoditySpecification> clusterCommSpec = provider.getBasketSold().stream()
                .filter(c -> (provider.getType() == EntityType.PHYSICAL_MACHINE_VALUE
                        && c.getBaseType() == CommodityDTO.CommodityType.CLUSTER_VALUE)).findFirst();
        // Storage may sell multiple storage cluster commoities.
        List<CommoditySpecification> stClusterComms = provider.getBasketSold().stream()
                .filter(c -> provider.getType() == EntityType.STORAGE_VALUE
                && c.getBaseType() == CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE).collect(
                        Collectors.toList());
        if (provider.getType() == EntityType.PHYSICAL_MACHINE_VALUE && clusterCommSpec.isPresent()) {
            return Optional.ofNullable(commTypeToSpecMap.inverse()
                    .get(clusterCommSpec.get().getType()));
        } else if (provider.getType() == EntityType.PHYSICAL_MACHINE_VALUE) {
            return Optional.empty();
        }
        if (!stClusterComms.isEmpty()) {
            // In case there is 1 storage cluster comm sold, choose it as the constraint boundary.
            if (stClusterComms.size() == 1) {
                return Optional.ofNullable(commTypeToSpecMap.inverse()
                        .get(stClusterComms.get(0).getType()));
            } else if (originalBuyer.isPresent()) {
                // For provider that sells serveral st cluster comm, choose the st cluster based on
                // InitialPlacementBuyer. If no st cluster comm found on buyer, then choose a random one.
                Set<TopologyDTO.CommodityType> stClusterCommBought = originalBuyer.get()
                        .getInitialPlacementCommoditiesBoughtFromProviderList().stream()
                        .filter(sl -> sl.getCommoditiesBoughtFromProviderId() == slOid)
                        .map(sl -> sl.getCommoditiesBoughtFromProvider().getCommodityBoughtList())
                        .flatMap(List::stream)
                        .filter(commBought -> commBought.getCommodityType().getType()
                                == CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE)
                        .map(commBought -> commBought.getCommodityType())
                        .collect(Collectors.toSet());
                Optional<CommodityType> stClusterCommType = stClusterComms.stream()
                        .map(c -> commTypeToSpecMap.inverse().get(c.getType()))
                        .filter(c -> stClusterCommBought.contains(c)).findFirst();
                if (stClusterCommType.isPresent()) {
                    return stClusterCommType;
                } else {
                    return Optional.ofNullable(commTypeToSpecMap.inverse().get(stClusterComms.get(0)));
                }
            }
        }
        return Optional.empty();

    }

    /**
     * A utility method to populate the sellers for each market in economy. Get economy ready
     * to run placement.
     *
     * @param economy the given economy.
     */
    public static void getEconomyReady(@Nonnull final Economy economy) {
        economy.resetMarketsPopulatedFlag();
        economy.composeMarketSubsetForPlacement();
        economy.clearSellersFromMarkets();
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
    }

    /**
     * A helper method to find the cluster or storage cluster commodity the closest seller bounded with.
     * When there is no cluster or storage cluster commodity sold by the closest seller, return an empty
     * string.
     *
     * @param closestSeller a closest seller.
     * @param commTypeToSpecMap the commodity type to commodity specification mapping.
     * @param clusterCommType cluster commodity type or storage cluster commodity type.
     * @return the key of the cluster or storage cluster commodity the closest seller bounded with.
     */
    private static String findClosestSellerClusterCommodity(@Nonnull final Trader closestSeller,
            @Nonnull final BiMap<CommodityType, Integer> commTypeToSpecMap,
            @Nonnull final int clusterCommType) {
        for (CommoditySpecification cs : closestSeller.getBasketSold()) {
            if (commTypeToSpecMap.inverse().get(cs.getType()).getType() == clusterCommType) {
                return commTypeToSpecMap.inverse().get(cs.getType()).getKey();
            }
        }
        return "";
    }

    /**
     * Populate {@link FailureInfo}s based on {@link InfiniteQuoteExplanation}s.
     *
     * @param exp the {@link InfiniteQuoteExplanation} for unplacement.
     * @param commTypeToSpecMap the commodity type to commodity specification mapping.
     * @param isRealtimeCache true if the failure occurs in the real time cache, false in the historical cache.
     * @return a list of {@link FailureInfo}s.
     */
    public static List<FailureInfo> populateFailureInfos(@Nonnull final InfiniteQuoteExplanation exp,
            @Nonnull final BiMap<CommodityType, Integer> commTypeToSpecMap, final boolean isRealtimeCache) {
        List<FailureInfo> failureInfos = new ArrayList<>();
        for (CommodityBundle bundle : exp.commBundle) {
            CommodityType commType = commTypeToSpecMap.inverse().get(bundle.commSpec.getType());
            double maxQuantity = MarketAnalysisUtils.getMaxAvailableForUnplacementReason(commType, bundle);
            if (exp.seller.isPresent()) {
                Trader closestSeller = exp.seller.get();
                String clusterKey = "";
                if (closestSeller.getType() == EntityType.STORAGE_VALUE) {
                    // When the closest seller is a storage, the reservation buyer's shopping list is
                    // first used to pick up the constraining cluster.
                    List<CommoditySpecification> storageClusterCommSpec = exp.shoppingList.getBasket()
                                    .stream()
                                    .filter(commbought -> commTypeToSpecMap.inverse().get(commbought.getType()).getType()
                                            == CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE)
                                    .collect(Collectors.toList());
                    if (!storageClusterCommSpec.isEmpty()) {
                        // Populate the storage cluster constraint enforced by the reservation shopping list.
                        clusterKey = commTypeToSpecMap.inverse().get(storageClusterCommSpec.get(0).getType()).getKey();
                    } else {
                        // If the reservation buyer does not have a storage cluster constraint, pick up any storage
                        // cluster among the commodities sold by the closest seller. When the closest storage seller
                        // has no storage cluster commsold, return an empty string.
                        clusterKey = findClosestSellerClusterCommodity(closestSeller, commTypeToSpecMap,
                                CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE);
                    }
                } else if (closestSeller.getType() == EntityType.PHYSICAL_MACHINE_VALUE) {
                    // Iterate the commodities sold by closest seller and collect cluster or storage cluster
                    // commodities, populate them in the failureInfos.
                    clusterKey = findClosestSellerClusterCommodity(closestSeller, commTypeToSpecMap,
                            CommodityDTO.CommodityType.CLUSTER_VALUE);
                }
                failureInfos.add(new FailureInfo(commType, closestSeller.getOid(), maxQuantity,
                        bundle.requestedAmount, clusterKey, isRealtimeCache));
            } else {
                // no closest seller
                failureInfos.add(new FailureInfo(commType, 0, maxQuantity, bundle.requestedAmount,
                        "", isRealtimeCache));
            }
        }
        return failureInfos;
    }

    /**
     * Extract cluster boundary commodity type from the {@link InitialPlacementDecision}s.
     *
     * @param economy the economy which generated the {@link InitialPlacementDecision}s.
     * @param commTypeToSpecMap the commodity type to commodity specification mapping.
     * @param placements a map of buyer oid to its list of {@link InitialPlacementDecision}s.
     * @param originalBuyers a list of original {@link InitialPlacementBuyer}s.
     * @param buyerFailed a set of failure buyer oids.
     * @return a map of shopping list oid to its supplier's cluster commodity type.
     */
    public static Map<Long, CommodityType> extractClusterBoundary(
            @Nonnull final Economy economy,
            @Nonnull final BiMap<CommodityType, Integer> commTypeToSpecMap,
            @Nonnull final Map<Long, List<InitialPlacementDecision>> placements,
            @Nonnull final List<InitialPlacementBuyer> originalBuyers,
            @Nonnull final Set<Long> buyerFailed) {
        Map<Long, CommodityType> clusterCommPerSl = new HashMap<>();
        for (Map.Entry<Long, List<InitialPlacementDecision>> entry : placements.entrySet()) {
            Long buyerOid = entry.getKey();
            for (InitialPlacementDecision placement : entry.getValue()) {
                if (!placement.supplier.isPresent()) {
                    // At least one buyer failed to get a supplier, return empty map
                    buyerFailed.add(buyerOid);
                } else {
                    Trader supplier = economy.getTopology().getTradersByOid()
                            .get(placement.supplier.get());
                    Optional<InitialPlacementBuyer> originalBuyer = originalBuyers.stream()
                            .filter(b -> b.getBuyerId() == buyerOid).findFirst();
                    if (supplier != null) {
                        Optional<CommodityType> commType = InitialPlacementUtils
                                .findBoundaryComm(originalBuyer, placement.slOid, supplier, commTypeToSpecMap);
                        if (commType.isPresent()) {
                            clusterCommPerSl.put(placement.slOid, commType.get());
                        }
                    }
                }
            }
        }
        return clusterCommPerSl;
    }

    /**
     * Prints the buyer's {@link InitialPlacementDecision}s.
     *
     * @param placements a map of buyer oid to its list of {@link InitialPlacementDecision}s.
     */
    public static void printPlacementDecisions(Map<Long, List<InitialPlacementDecision>> placements) {
        StringBuilder sb = new StringBuilder();
        placements.entrySet().forEach(e -> {
            sb.append(" buyer oid ").append(e.getKey());
            e.getValue().forEach( pl -> {
                sb.append(" with shopping list oid ").append(pl.slOid);
                if (pl.supplier.isPresent()) {
                    sb.append(" placed on ").append(pl.supplier.get());
                } else {
                    sb.append(" can not be placed");
                }
            });
        });
        logger.info(logPrefix + "Placement decision is {} ", sb.toString());
    }


    /**
     * Make physical machine sellers that sell the given cluster commodities not eligible for placement.
     *
     * @param economy the economy.
     * @param commTypeMap the commodity type to commodity specification mapping.
     * @param clusterCommPerSl buyer's shopping list oid to the cluster that it failed to be placed.
     * @return an oid set of sellers that are not eligible for placement.
     */
    public static Set<Long> setSellersNotAcceptCustomers(@Nonnull final Economy economy,
            @Nonnull final BiMap<CommodityType, Integer> commTypeMap,
            @Nonnull final Map<Long, CommodityType> clusterCommPerSl) {
        economy.clearSellersFromMarkets();
        Set<Integer> clusterCommSpecSet = clusterCommPerSl.values().stream()
                // only retain the cluster commodity not the storage cluster commodity
                .filter(c -> c.getType() == CommodityDTO.CommodityType.CLUSTER_VALUE)
                .map(c -> commTypeMap.get(c)).collect(Collectors.toSet());
        Set<Long> ineligibleSellers = new HashSet<>();
        economy.getTraders().stream().forEach(t -> {
            if (t.getBasketSold().stream().anyMatch(cs -> clusterCommSpecSet.contains(cs.getType()))) {
                // make all sellers that sell this cluster comm as CanAcceptNewCustomer false
                if (t.getSettings().canAcceptNewCustomers()) {
                    t.getSettings().setCanAcceptNewCustomers(false);
                    ineligibleSellers.add(t.getOid());
                }
            }
        });
        return ineligibleSellers;
    }

    /**
     * Reset previously marked sellers back to be able to accept customers.
     *
     * @param economy the economy.
     * @param ineligibleSellers a set of previously marked sellers.
     */
    public static void restoreCanNotAcceptNewCustomerSellers(@Nonnull final Economy economy,
            @Nonnull final Set<Long> ineligibleSellers) {
        setCanAcceptNewCustomerSellers(economy, ineligibleSellers, true);
    }

    /**
     * Set sellers can accept new customers value.
     *
     * @param economy the economy.
     * @param sellers the sellers.
     * @param canAccept the value.
     */
    public static void setCanAcceptNewCustomerSellers(@Nonnull final Economy economy,
            @Nonnull final Set<Long> sellers, boolean canAccept) {
        economy.clearSellersFromMarkets();
        economy.getTraders().stream().forEach(t -> {
            if (sellers.contains(t.getOid())) {
                t.getSettings().setCanAcceptNewCustomers(canAccept);
            }
        });
    }

    /**
     * Updates the non access commodities for the newly added traders.
     * Since the newly added traders are added to historical economy from the real time economy, its possible for the
     * commodities on the newly added traders to have a comm-spec number different from the comm-spec number for the
     * same commodity in the historical cache.
     * So this method removes such non access commodities on the newly added traders, and then the correct
     * comm-spec numbers are looked up in the historicalCachedCommTypeMap and added to the newly added traders.
     *
     * @param realtimeCachedEconomy real time cached economy
     * @param realtimeCachedCommTypeMap real time comm type to comm spec map
     * @param historicalCachedEconomy historical cached economy
     * @param historicalCachedCommTypeMap historical comm type to comm spec map
     * @param newlyAddedTraders newly added traders
     */
    public static void updateNonAccessCommodities(@Nonnull final Economy realtimeCachedEconomy,
            @Nonnull final BiMap<CommodityType, Integer> realtimeCachedCommTypeMap,
            @Nonnull final Economy historicalCachedEconomy,
            @Nonnull final BiMap<CommodityType, Integer> historicalCachedCommTypeMap,
            List<Trader> newlyAddedTraders) {

        // 1. get nonAccessCommoditiesByTraderOid in historicalCachedEconomy/realtimeCachedEconomy. Both have same commodities.
        // using historicalCachedCommTypeMap.. If historicalCachedCommTypeMap does not have an entry
        // get the entry in realtimeCachedCommTypeMap add it in the
        // historicalCachedCommTypeMap.
        Map<Long, List<UpdateCommodityWrapper>> nonAccessCommoditiesByTraderOid =
                findNonAccessCommByTrader(newlyAddedTraders, realtimeCachedCommTypeMap, historicalCachedCommTypeMap);
        // 1. delete existing commodities..
        for (Trader t : newlyAddedTraders) {
            Set<CommoditySpecification> removeCommSpecs = new HashSet<>();
            for (CommoditySpecification cs : t.getBasketSold()) {
                // the trader currently is selling commodity based on realtimeCachedCommTypeMap
                CommodityType realTimeCommType = realtimeCachedCommTypeMap.inverse()
                        .get(cs.getType());
                if (realTimeCommType != null && !realTimeCommType.hasKey()) {
                    removeCommSpecs.add(cs);
                }
            }
            removeCommSpecs.stream().forEach(cs -> {
                // This will remove the commSpec from the trader's basket and the commoditySold
                // from the corresponding index
                t.removeCommoditySold(cs);
            });
        }
        attachNewCommodities(nonAccessCommoditiesByTraderOid, historicalCachedEconomy, realtimeCachedCommTypeMap,
                historicalCachedCommTypeMap, false);
    }

    /**
     * Update historical economy traders' access commodities that can be changed in real time.
     *
     * @param realtimeCachedEconomy real time economy.
     * @param realtimeCachedCommTypeMap the real time commodity type to commoditySpecification's type mapping.
     * @param historicalCachedEconomy historical economy.
     * @param historicalCachedCommTypeMap the historical commodity type to commoditySpecification's type mapping.
     */
    public static void updateAccessCommodities(@Nonnull final Economy realtimeCachedEconomy,
            @Nonnull final BiMap<CommodityType, Integer> realtimeCachedCommTypeMap,
            @Nonnull final Economy historicalCachedEconomy,
            @Nonnull final BiMap<CommodityType, Integer> historicalCachedCommTypeMap) {
        // Remove all old access commodities in historical economy cache and historicalCachedCommTypeMap.
        Set<CommoditySpecification> oldAccessComm = new HashSet<>();
        for (Trader t : historicalCachedEconomy.getTraders()) {
            Set<CommoditySpecification> removeCommSpecs = new HashSet<>();
            for (int i = 0; i < t.getBasketSold().size(); i++) {
                CommodityType historicalComm = historicalCachedCommTypeMap.inverse()
                        .get(t.getBasketSold().get(i).getType());
                if (historicalComm != null && historicalComm.hasKey()) {
                    removeCommSpecs.add(t.getBasketSold().get(i));
                }
            }
            oldAccessComm.addAll(removeCommSpecs);
            removeCommSpecs.stream().forEach(cs -> {
                // This will remove the commSpec from the trader's basket and the commoditySold from the corresponding index
                t.removeCommoditySold(cs);
            });
        }
        oldAccessComm.stream().forEach(c -> historicalCachedCommTypeMap.inverse().remove(c.getType()));
        // A map keeps the latest access comm based on real time economy.
        Map<Long, List<UpdateCommodityWrapper>> accessCommoditiesByTraderOid =
                findAccessCommByTrader(realtimeCachedEconomy.getTraders(), realtimeCachedCommTypeMap);
        // Add all new access commodities based on realtime in historical economy cache and historicalCachedCommTypeMap.
        attachNewCommodities(accessCommoditiesByTraderOid, historicalCachedEconomy, realtimeCachedCommTypeMap,
                historicalCachedCommTypeMap, true);
        logger.info(logPrefix + " historical economy cache finished commodity update on "
                + accessCommoditiesByTraderOid.size() + " entities");
    }

    /**
     * Add access commodities on each trader.
     *
     * @param commoditiesByTraderOid a map of commodities for each trader.
     * @param economy the economy contains the traders.
     * @param realtimeCachedCommTypeMap the real time commodity type to commoditySpecification's type mapping.
     * @param historicalCachedCommTypeMap the historical commodity type to commoditySpecification's type mapping.
     * @param updateSpecMap boolean to specify if the spec map has to be updated.
     */
    public static void attachNewCommodities(
            @Nonnull final Map<Long, List<UpdateCommodityWrapper>> commoditiesByTraderOid,
            @Nonnull final Economy economy,
            @Nonnull final BiMap<CommodityType, Integer> realtimeCachedCommTypeMap,
            @Nonnull final BiMap<CommodityType, Integer> historicalCachedCommTypeMap,
            boolean updateSpecMap) {
        for (Map.Entry<Long, List<UpdateCommodityWrapper>> entry
                : commoditiesByTraderOid.entrySet()) {
            Trader trader = economy.getTopology().getTradersByOid().get(entry.getKey());
            if (trader == null) {
                logger.warn("Trader with oid {} in realtime does not exist in historical economy.",
                        entry.getKey());
                continue;
            }
            for (UpdateCommodityWrapper comm : entry.getValue()) {
                CommoditySoldWithSettings newCommSold = (CommoditySoldWithSettings)trader
                        .addCommoditySold(comm.commSpec);
                cloneCommoditiesSoldAttributes(comm.commSold, newCommSold);
                if (updateSpecMap) {
                    // Add the new commodity into the historicalCachedCommTypeMap immediately.
                    CommodityType realtimeCommType = realtimeCachedCommTypeMap.inverse().get(comm.commSpec.getType());
                    if (historicalCachedCommTypeMap.inverse().containsKey(comm.commSpec.getType())) {
                        historicalCachedCommTypeMap.inverse().remove(comm.commSpec.getType());
                    }
                    historicalCachedCommTypeMap.put(realtimeCommType, comm.commSpec.getType());
                }
            }
        }
    }

    /**
     * Find non access commodities of each trader.
     *
     * @param traders a list of traders.
     * @param realtimeCachedCommTypeMap real time commodity type to commoditySpecification's type mapping.
     * @param historicalCachedCommTypeMap historical commodity type to commoditySpecification's type mapping.
     * @return a map of access commodities for each trader.
     */
    public static Map<Long, List<UpdateCommodityWrapper>> findNonAccessCommByTrader(
            @Nonnull final List<Trader> traders,
            @Nonnull final BiMap<CommodityType, Integer> realtimeCachedCommTypeMap,
            @Nonnull final BiMap<CommodityType, Integer> historicalCachedCommTypeMap) {

        // 1. get nonAccessCommoditiesByTraderOid in historicalCachedEconomy/realtimeCachedEconomy. Both have same commodities.
        // using historicalCachedCommTypeMap.. If historicalCachedCommTypeMap does not have an entry
        // get the entry in realtimeCachedCommTypeMap add it in the
        // historicalCachedCommTypeMap with 50000 + value.

        Map<Long, List<UpdateCommodityWrapper>> nonAccessCommoditiesByTraderOid = new HashMap<>();
        for (Trader t : traders) {
            for (int i = 0; i < t.getBasketSold().size(); i++) {
                CommoditySpecification realTimeCommSpec = t.getBasketSold().get(i);
                CommodityType realTimeCommType = realtimeCachedCommTypeMap.inverse().get(realTimeCommSpec.getType());
                if (realTimeCommType != null && !realTimeCommType.hasKey()) {
                    // a commodity with no key is considered as a non access comm
                    Integer historicalCommSpecType = historicalCachedCommTypeMap.get(realTimeCommType);
                    if (historicalCommSpecType != null) {
                        CommoditySpecification histCommSpec = new CommoditySpecification(historicalCommSpecType,
                                realTimeCommType.getType());
                        histCommSpec.setDebugInfoNeverUseInCode(realTimeCommSpec.getDebugInfoNeverUseInCode());
                        nonAccessCommoditiesByTraderOid.computeIfAbsent(t.getOid(), oid -> new ArrayList<>())
                                .add(new UpdateCommodityWrapper(histCommSpec,
                                        (CommoditySoldWithSettings)t.getCommoditiesSold().get(i)));
                    }
                }
            }
        }
        return nonAccessCommoditiesByTraderOid;
    }

    /**
     * Find access commodities of each trader.
     *
     * @param traders a list of traders.
     * @param commTypeMap a commodity type to commoditySpecification's type mapping.
     * @return a map of access commodities for each trader.
     */
    public static Map<Long, List<UpdateCommodityWrapper>> findAccessCommByTrader(
            @Nonnull final List<Trader> traders,
            @Nonnull final BiMap<CommodityType, Integer> commTypeMap) {
        Map<Long, List<UpdateCommodityWrapper>> accessCommoditiesByTraderOid = new HashMap<>();
        for (Trader t : traders) {
            for (int i = 0; i < t.getBasketSold().size(); i++) {
                CommodityType realtimeComm = commTypeMap.inverse()
                        .get(t.getBasketSold().get(i).getType());
                if (realtimeComm != null && realtimeComm.hasKey()) {
                    // a commodity has key is considered as an access comm
                    List<UpdateCommodityWrapper> placementCommList = accessCommoditiesByTraderOid.get(t.getOid());
                    if (placementCommList == null) {
                        List<UpdateCommodityWrapper> newPlacementComm = new ArrayList<>();
                        accessCommoditiesByTraderOid.put(t.getOid(), newPlacementComm);
                        newPlacementComm.add(new UpdateCommodityWrapper(t.getBasketSold().get(i),
                                (CommoditySoldWithSettings)t.getCommoditiesSold().get(i)));
                    } else {
                        placementCommList.add(new UpdateCommodityWrapper(t.getBasketSold().get(i),
                                (CommoditySoldWithSettings)t.getCommoditiesSold().get(i)));
                    }
                }
            }
        }
        return accessCommoditiesByTraderOid;
    }

    /**
     * Construct the economy based on {@link EconomyCacheDTO}.
     *
     * @param economyCacheDTO the serialized economy cache object loaded from table.
     * @return economy the economy reconstructed.
     */
    public static Optional<Economy> reconstructEconomyCache(Optional<EconomyCacheDTO> economyCacheDTO) {
        // The table does not have a DTO persisted.
        if (!economyCacheDTO.isPresent()) {
            return Optional.empty();
        }
        // Build economy by adding traderTOs from economyCacheDTO.
        Topology topology = new Topology();
        for (TraderTO traderTO : economyCacheDTO.get().getTradersList()) {
            ProtobufToAnalysis.addTrader(topology, traderTO);
        }
        // EconomyForTesting returns the modifiable economy instead of unmodifiable economy.
        return Optional.of(topology.getEconomyForTesting());
    }

    /**
     * Construct the commodity type to specification map based on {@link EconomyCacheDTO}.
     *
     * @param economyCacheDTO the serialized economy cache object loaded from table.
     * @return the commodity type to commodity specification type mapping
     */
    public static Optional<BiMap<CommodityType, Integer>> reconstructCommTypeMap(
            Optional<EconomyCacheDTO> economyCacheDTO) {
        // The table does not have a DTO persisted.
        if (!economyCacheDTO.isPresent()) {
            return Optional.empty();
        }
        // Populate commodity type to commodity specification mapping.
        BiMap<CommodityType, Integer> commTypeBiMap = HashBiMap.create();
        for (CommTypeEntry entry : economyCacheDTO.get().getCommTypeEntryList()) {
            CommodityType commodityType;
            if (entry.hasKey()) {
                commodityType = CommodityType.newBuilder().setType(entry.getCommType())
                        .setKey(entry.getKey()).build();
            } else {
                commodityType = CommodityType.newBuilder().setType(entry.getCommType()).build();
            }
            commTypeBiMap.put(commodityType, entry.getCommSpecType());
        }
        return Optional.of(commTypeBiMap);
    }
}

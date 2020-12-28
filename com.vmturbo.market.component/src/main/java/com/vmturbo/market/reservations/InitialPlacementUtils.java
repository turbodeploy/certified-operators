package com.vmturbo.market.reservations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer.InitialPlacementCommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.market.reservations.InitialPlacementFinderResult.FailureInfo;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO.SumOfCommodity;
import com.vmturbo.platform.analysis.topology.Topology;
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
    private static final Set<Integer> PROVIDER_ENTITY_TYPES = ImmutableSet.of(EntityType.PHYSICAL_MACHINE_VALUE,
            EntityType.STORAGE_VALUE);

    /**
     * Constructor.
     */
    private InitialPlacementUtils() {}

    /**
     * Clones the economy which contains the latest broadcast entities. Only physical machine,
     * storage traders and existing reservation entities are kept in the cloned economy.
     *
     * @param originalEconomy the economy to be cloned
     * @return a copy of original economy which contains only PM and DS.
     */
    public static Economy cloneEconomy(@Nonnull final UnmodifiableEconomy originalEconomy) {
        Topology t = new Topology();
        Economy cloneEconomy = t.getEconomyForTesting();
        cloneEconomy.setTopology(t);
        Map<Long, Trader> cloneTraderToOidMap = t.getModifiableTraderOids();
        originalEconomy.getTraders().stream()
                .filter(trader -> PROVIDER_ENTITY_TYPES.contains(trader.getType()))
                .forEach(trader -> {
                    Trader cloneTrader = cloneEconomy.addTrader(trader.getType(), trader.getState(),
                            new Basket(trader.getBasketSold()), trader.getCliques());
                    cloneTrader.setOid(trader.getOid());

                    // Copy traderOids in clone economy
                    cloneTraderToOidMap.put(trader.getOid(), cloneTrader);

                    // Copy bare minimum trader properties
                    cloneTrader.setDebugInfoNeverUseInCode(
                            trader.getDebugInfoNeverUseInCode() + PLACEMENT_CLONE_SUFFIX);
                    cloneTrader.getSettings().setQuoteFunction(
                            trader.getSettings().getQuoteFunction());
                    cloneTrader.getSettings().setCanAcceptNewCustomers(true);
                    cloneTrader.getSettings().setIsShopTogether(true);
                    cloneCommoditiesSold(trader, cloneTrader);
                });
        return cloneEconomy;
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
            cloneCommSold.setCapacity(commSold.getCapacity());
            cloneCommSold.setQuantity(commSold.getQuantity());
            cloneCommSold.setPeakQuantity(commSold.getPeakQuantity());
            CommoditySoldSettings commSoldSettings = commSold.getSettings();
            CommoditySoldSettings cloneCommSoldSettings = cloneCommSold.getSettings();
            cloneCommSoldSettings.setPriceFunction(commSoldSettings.getPriceFunction())
                    .setUpdatingFunction(commSoldSettings.getUpdatingFunction())
                    .setUtilizationUpperBound(commSoldSettings.getUtilizationUpperBound());
        }
    }

    /**
     * Create traderTOs based on the given InitialPlacementBuyer list.
     *
     * @param buyer the given reservation buyer information.
     * @param commTypeToSpecMap topology dto commodity type to trader commodity specification map.
     * @param clusterCommPerSl the shopping list oid to boundary commodity type mapping.
     * @return a list of traderTOs.
     */
    public static TraderTO constructTraderTO(@Nonnull final InitialPlacementBuyer buyer,
            @Nonnull final BiMap<CommodityType, Integer> commTypeToSpecMap,
            @Nonnull final Map<Long, CommodityType> clusterCommPerSl) {
        TraderTO.Builder traderTO = TraderTO.newBuilder();
        boolean validConstraint = true;
        for (InitialPlacementCommoditiesBoughtFromProvider sl
                : buyer.getInitialPlacementCommoditiesBoughtFromProviderList()) {
            long slOid = sl.getCommoditiesBoughtFromProviderId();
            CommodityType boundaryCommType = clusterCommPerSl.get(slOid);
            List<CommodityBoughtTO> commBoughtTOs = constructCommBoughtTO(
                        sl.getCommoditiesBoughtFromProvider().getCommodityBoughtList(),
                        commTypeToSpecMap, boundaryCommType);
            if (commBoughtTOs.isEmpty()) {
                logger.warn(logPrefix + "Empty commodity bought created in this trader {} sl {}, skipping"
                                + " reservation for it", buyer.getBuyerId(),
                        sl.getCommoditiesBoughtFromProviderId());
                validConstraint = false;
                break;
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
        if (validConstraint) {
            traderTO.setOid(buyer.getBuyerId()).setDebugInfoNeverUseInCode(
                    buyer.getBuyerId() + PLACEMENT_CLONE_SUFFIX)
                    .setState(TraderStateTO.ACTIVE)
                    .setSettings(TraderSettingsTO.newBuilder()
                            .setIsShopTogether(true)
                            .setQuoteFunction(QuoteFunctionDTO.newBuilder()
                                    .setSumOfCommodity(SumOfCommodity.newBuilder().build())));
        }
        return traderTO.build();
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
     * @return a list of {@link TraderTO} lists. The order of reservation buyers follows the order
     * they were added.
     */
    public static List<List<TraderTO>> constructTraderTOListWithBoundary(
            @Nonnull final Economy economy,
            @Nonnull final BiMap<CommodityType, Integer> commTypeToSpecMap,
            @Nonnull final Map<Long, List<InitialPlacementDecision>> buyerOidToPlacement,
            @Nonnull final Map<Long, List<InitialPlacementBuyer>> existingReservations) {
        List<List<TraderTO>> placedBuyersPerRes = new ArrayList();
        // Create the reservations one by one following the order they were added
        existingReservations.values().forEach( buyers -> {
            List<TraderTO> placedBuyers = new ArrayList();
            buyers.forEach(buyer -> {
                List<InitialPlacementDecision> placementPerBuyer =
                        buyerOidToPlacement.get(buyer.getBuyerId());
                if (placementPerBuyer != null && placementPerBuyer.stream()
                        .allMatch(r -> r.supplier.isPresent())) {
                    // When all shopping lists of a buyer have a supplier, figure out the cluster
                    // boundaries from the suppliers.
                    placedBuyers.add(constructTraderTOWithBoundary(economy, commTypeToSpecMap,
                            placementPerBuyer, buyer));
                }
            });
            placedBuyersPerRes.add(placedBuyers);
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
    public static TraderTO constructTraderTOWithBoundary(
            @Nonnull final Economy economy,
            @Nonnull final BiMap<CommodityType, Integer> commTypeToSpecMap,
            @Nonnull final List<InitialPlacementDecision> decisions,
            @Nonnull final InitialPlacementBuyer buyer) {
        Map<Long, CommodityType> clusterCommPerSl = new HashMap();
        for (InitialPlacementDecision placement : decisions) {
            if (placement.supplier.isPresent()) {
                Trader supplier = economy.getTopology().getTradersByOid().get(placement.supplier.get());
                Optional<CommodityType> commType = findBoundaryComm(Optional.of(buyer),
                        placement.slOid, supplier, commTypeToSpecMap);
                if (commType.isPresent()) {
                    clusterCommPerSl.put(placement.slOid, commType.get());
                }
            }
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
     * Populate {@link FailureInfo}s based on {@link InfiniteQuoteExplanation}s.
     *
     * @param exp the {@link InfiniteQuoteExplanation} for unplacement.
     * @param commTypeToSpecMap the commodity type to commodity specification mapping.
     * @return a list of {@link FailureInfo}s.
     */
    public static List<FailureInfo> populateFailureInfos(@Nonnull final InfiniteQuoteExplanation exp,
            @Nonnull final BiMap<CommodityType, Integer> commTypeToSpecMap) {
        List<FailureInfo> failureInfos = new ArrayList();
        for (CommodityBundle bundle : exp.commBundle) {
            failureInfos.add(new FailureInfo(commTypeToSpecMap.inverse().get(bundle.commSpec.getType()),
                    exp.seller.isPresent() ? exp.seller.get().getOid() : 0,
                    bundle.maxAvailable.get(), bundle.requestedAmount));
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
        Map<Long, CommodityType> clusterCommPerSl = new HashMap();
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
     * Calculate the total used and capacity of each commodity requested by {@link InitialPlacementBuyer}
     * within the chosen cluster. If the buyer's supplier is not bounded by a cluster or storage cluster
     * commodity, calculate the total statistics of all traders.
     *
     * @param economy the economy to calculate the cluster stats.
     * @param commTypeToSpecMap the commodity type to commodity specification mapping.
     * @param successfulSlToClusterMap a map of reservation buyer's shopping list oid to its cluster mapping.
     * @param successfulBuyers a set of {@link InitialPlacementBuyer}s that are successful.
     * @return a map of shopping list oid -> commodity type -> {total use, total capacity} of a cluster.
     */
    public static Map<Long, Map<CommodityType, Pair<Double, Double>>> calculateClusterStatistics(
            @Nonnull final Economy economy,
            @Nonnull final BiMap<CommodityType, Integer> commTypeToSpecMap,
            @Nonnull final Map<Long, CommodityType> successfulSlToClusterMap,
            @Nonnull final Set<InitialPlacementBuyer> successfulBuyers) {
        // A map keep track of cluster commodity to the trader set bounded by that cluster.
        Map<CommodityType, Set<Trader>> tradersBoundedByCluster = groupTradersByCluster(economy,
                commTypeToSpecMap, successfulSlToClusterMap);
        Set<InitialPlacementCommoditiesBoughtFromProvider> sls = successfulBuyers.stream()
                .map(b -> b.getInitialPlacementCommoditiesBoughtFromProviderList())
                .flatMap(List::stream).collect(Collectors.toSet());
        Map<Long, Map<CommodityType, Pair<Double, Double>>> statsBySl = new HashMap();
        for (InitialPlacementCommoditiesBoughtFromProvider commBoughtPrd : sls) {
            long slOid = commBoughtPrd.getCommoditiesBoughtFromProviderId();
            CommodityType clusterComm = successfulSlToClusterMap.get(slOid);
            Set<Trader> tradersOfTheCluster = getTradersOfSlCluster(economy, clusterComm,
                    slOid, tradersBoundedByCluster);
            Map<CommodityType, Pair<Double, Double>> commodityTypePairMap = new HashMap();
            statsBySl.put(slOid, commodityTypePairMap);
            // Iterate all comm bought of a reservation buyer, sum up all used and capacity of
            // that commodity in the traders of the chosen cluster.
            for (CommodityBoughtDTO commBought : commBoughtPrd.getCommoditiesBoughtFromProvider()
                    .getCommodityBoughtList()) {
                Map<CommodityType, Map<CommodityType, Pair<Double, Double>>>
                        usedAndCapacityByCluster = new HashMap();
                CommodityType type = commBought.getCommodityType();
                if (!usedAndCapacityByCluster.containsKey(clusterComm)
                        || !usedAndCapacityByCluster.get(clusterComm).containsKey(type)) {
                    double used = 0d;
                    double capacity = 0d;
                    for (Trader t : tradersOfTheCluster) {
                        int indexOfCommSold = t.getBasketSold().indexOf(commTypeToSpecMap.get(type));
                        if (indexOfCommSold != -1) {
                            used += t.getCommoditiesSold().get(indexOfCommSold).getQuantity();
                            capacity += t.getCommoditiesSold().get(indexOfCommSold).getCapacity();
                        }
                    }
                    if (usedAndCapacityByCluster.containsKey(clusterComm)) {
                        usedAndCapacityByCluster.get(clusterComm).put(type, Pair.of(used, capacity));
                    } else {
                        Map<CommodityType, Pair<Double, Double>> commBoughtUsedAndCapacity = new HashMap();
                        commBoughtUsedAndCapacity.put(type, Pair.of(used, capacity));
                        usedAndCapacityByCluster.put(clusterComm, commBoughtUsedAndCapacity);
                    }
                }
                // Fill in the map with commodity total used and capacity of this type
                commodityTypePairMap.put(type, usedAndCapacityByCluster.get(clusterComm).get(type));
            }
        }
        return statsBySl;
    }

    /**
     * Obtain the traders that sells a given cluster commodity type. If no cluster commodity passed
     * in is null, return all PMs or all DSs in economy.
     *
     * @param economy the economy.
     * @param clusterComm a cluster commodity type for a shopping list, can be null
     * @param slOid the oid of a shopping list.
     * @param tradersBoundedByCluster traders grouped by cluster commodity.
     * @return the traders that sells a given cluster commodity type.
     */
    private static Set<Trader> getTradersOfSlCluster(@Nonnull final Economy economy,
            @Nullable final CommodityType clusterComm, final long slOid,
            @Nonnull final Map<CommodityType, Set<Trader>> tradersBoundedByCluster) {
        Set<Trader> tradersOfTheCluster = new HashSet();
        if (clusterComm == null) {
            // No cluster boundary for the buyer's shopping list, get all PMs or DSs
            // to calculate cluster stats.
            ShoppingList shoppingList = economy.getTopology().getShoppingListOids().inverse()
                    .get(slOid);
            if (shoppingList != null) {
                // In case there is no cluster comm for a shopping list, calculate stats based on
                // all PMs or all DSs.
                if (shoppingList.getSupplier().getType() == EntityType.PHYSICAL_MACHINE_VALUE) {
                    tradersOfTheCluster.addAll(economy.getTraders().stream().filter(e -> e.getType()
                                    == EntityType.PHYSICAL_MACHINE_VALUE).collect(Collectors.toSet()));
                } else {
                    tradersOfTheCluster.addAll(economy.getTraders().stream().filter(e -> e.getType()
                            == EntityType.STORAGE_VALUE).collect(Collectors.toSet()));
                }
            }
        } else {
            tradersOfTheCluster.addAll(tradersBoundedByCluster.get(clusterComm));
        }
        return tradersOfTheCluster;
    }

    /**
     * Group traders in the economy by a given cluster commodity type.
     *
     * @param economy the economy.
     * @param commTypeToSpecMap the commodity type to commodity specification mapping.
     * @param slToClusterMap a map of reservation buyer's shopping list oid to its cluster mapping.
     * @return trader sets grouped by cluster commodity type.
     */
    private static Map<CommodityType, Set<Trader>> groupTradersByCluster(@Nonnull final Economy economy,
            @Nonnull final BiMap<CommodityType, Integer> commTypeToSpecMap,
            @Nonnull final Map<Long, CommodityType> slToClusterMap) {
        Map<CommodityType, Set<Trader>> tradersBoundedByCluster = new HashMap();
        for (Map.Entry<Long, CommodityType> slByCluster : slToClusterMap.entrySet()) {
            CommodityType clusterComm = slByCluster.getValue();
            int commSpecType = commTypeToSpecMap.get(clusterComm);
            Set<Trader> traders = new HashSet();
            for (Market m : economy.getMarkets()) {
                if (m.getBasket().indexOf(commSpecType) != -1) {
                    // Find all traders sell in a market whose basket contains the given cluster comm
                    traders.addAll(m.getActiveSellers());
                    // NOTE: inactive sellers are included in the cluster statistics
                    traders.addAll(m.getInactiveSellers());
                }
            }
            tradersBoundedByCluster.put(clusterComm, traders);
        }
        return tradersBoundedByCluster;
    }

    /**
     * Make sellers that sell the given cluster commodities not eligible for placement.
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
                .map(c -> commTypeMap.get(c)).collect(Collectors.toSet());
        Set<Long> ineligibleSellers = new HashSet();
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
        economy.clearSellersFromMarkets();
        economy.getTraders().stream().forEach(t -> {
            if (ineligibleSellers.contains(t.getOid())) {
                t.getSettings().setCanAcceptNewCustomers(true);
            }
        });
    }
}

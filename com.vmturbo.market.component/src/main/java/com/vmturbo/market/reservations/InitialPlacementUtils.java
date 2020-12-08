package com.vmturbo.market.reservations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer.InitialPlacementCommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.market.reservations.InitialPlacementFinderResult.FailureInfo;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
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
                    cloneTrader.getSettings().setQuoteFunction(trader.getSettings().getQuoteFunction());
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
                logger.warn("Empty commodity bought created in this trader {} sl {}, skipping"
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
                logger.warn("The reservation is given a commodity type {} key {} which may be just"
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
        // Replay the reservation one by one following the order they were added
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
                Optional<CommodityType> commType = InitialPlacementUtils.findBoundaryComm(
                        supplier, commTypeToSpecMap);
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
     * @param provider a given trader which is a supplier.
     * @param commTypeToSpecMap the commodity type to commodity specification mapping.
     * @return CommodityType corresponding to cluster boundary.
     */
    public static Optional<TopologyDTO.CommodityType> findBoundaryComm(@Nonnull final Trader provider,
            @Nonnull final BiMap<CommodityType, Integer> commTypeToSpecMap) {
        // Assuming storage has at most 1 storage cluster commodity and pm has at most 1 cluster commodity
        Optional<CommoditySpecification> commSpec = provider.getBasketSold().stream()
                .filter(c -> (provider.getType() == EntityType.PHYSICAL_MACHINE_VALUE
                        && c.getBaseType() == CommodityDTO.CommodityType.CLUSTER_VALUE)
                || (provider.getType() == EntityType.STORAGE_VALUE && c.getBaseType()
                        == CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE)).findFirst();
        if (commSpec.isPresent()) {
            TopologyDTO.CommodityType type = commTypeToSpecMap.inverse().get(commSpec.get().getType());
            return type == null ? Optional.empty() : Optional.of(type);
        } else {
            return Optional.empty();
        }
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
}

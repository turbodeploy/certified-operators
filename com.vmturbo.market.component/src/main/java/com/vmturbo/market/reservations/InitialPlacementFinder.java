package com.vmturbo.market.reservations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer.InitialPlacementCommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.market.reservations.InitialPlacementFinderResult.FailureInfo;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.ede.Placement;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO;
import com.vmturbo.platform.analysis.protobuf.QuoteFunctionDTOs.QuoteFunctionDTO.SumOfCommodity;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;
import com.vmturbo.platform.analysis.utilities.PlacementResults;
import com.vmturbo.platform.analysis.utilities.QuoteTracker;
import com.vmturbo.platform.analysis.utilities.QuoteTracker.IndividualCommodityQuote;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * The class to support fast reservation placement.
 */
public class InitialPlacementFinder {

    private PlacementFinderState state = PlacementFinderState.NOT_READY;

    /**
     * State of initial placement, can be ready or not ready.
     */
    public enum PlacementFinderState {
        READY, NOT_READY;
    }

    // the lock to synchronize the change of cachedEconomy
    private Object economyLock = new Object();

    // an minimal version of realtime economy which only contains reservation entities and PM, DS
    private Economy cachedEconomy;

    // a map that stores the TopologyDTO.CommodityType to traderTO's CommoditySpecification
    // type mapping
    private Map<CommodityType, Integer> cachedCommTypeMap = Maps.newHashMap();

    // TraderId -> <ShoppingList -> ProviderId>
    private Map<Long, Map<ShoppingList, Long>> reservationBuyerToShoppingLists = new HashMap<>();

    private static final Logger logger = LogManager.getLogger();

    private static final String PLACEMENT_CLONE_SUFFIX = "_PLACEMENT_CLONE";

    private static final Set<Integer> PROVIDER_ENTITY_TYPES = ImmutableSet.of(EntityType.PHYSICAL_MACHINE_VALUE,
            EntityType.STORAGE_VALUE);

    /**
     * Constructor.
     */
    public InitialPlacementFinder() { }

    /**
     * Update cached economy with given economy and a commodity type to specification map.
     *
     * @param originalEconomy the economy to be cloned
     * @param commTypeToSpecMap the commodity type to commoditySpecification's type mapping
     */
    public void updateCachedEconomy(@Nonnull final UnmodifiableEconomy originalEconomy,
                                    @Nonnull final Map<TopologyDTO.CommodityType, Integer> commTypeToSpecMap) {
        Economy newEconomy = cloneEconomy(originalEconomy);
        synchronized (economyLock) {
            // add reservation entities to newEconomy which currently only contains PM and DS
            addReservationEntities(newEconomy);
            // update cachedEconomy
            cachedEconomy = newEconomy;
            // update commodity type to specification map, it can be different very market cycle
            cachedCommTypeMap = commTypeToSpecMap;
            // clear any reservation entities that already added to cachedEconomy
            reservationBuyerToShoppingLists.clear();
            // set state to ready once the market cycle calls updateCachedEconomy
            setState(PlacementFinderState.READY);
            logger.info("InitialPlacementFinder is ready now.");
        }
    }

    /**
     * Clones the economy which contains the latest broadcast entities. Only physical machine
     * and storage traders are kept in the cloned economy.
     *
     * @param originalEconomy the economy to be cloned
     * @return a copy of original economy which contains only PM and DS.
     */
    private Economy cloneEconomy(@Nonnull final UnmodifiableEconomy originalEconomy) {
        Topology t = new Topology();
        Economy cloneEconomy = t.getEconomyForTesting();
        cloneEconomy.setTopology(t);
        BiMap<Trader, Long> originalTraderToOidMap = originalEconomy.getTopology().getModifiableTraderOids();
        BiMap<Trader, Long> cloneTraderToOidMap = t.getModifiableTraderOids();
        originalEconomy.getTraders().stream()
                .filter(trader -> PROVIDER_ENTITY_TYPES.contains(trader.getType()))
                .forEach(trader -> {
                    Trader cloneTrader = cloneEconomy.addTrader(trader.getType(), trader.getState(),
                            new Basket(trader.getBasketSold()), trader.getCliques());

                    // Copy traderOids in clone economy
                    cloneTraderToOidMap.put(cloneTrader, originalTraderToOidMap.get(trader));

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
    private void cloneCommoditiesSold(Trader trader, Trader cloneTrader) {
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
     * Add reserved traders to the economy and apply its impact on providers' utilization.
     *
     * @param economy the economy for reservation
     */
    private void addReservationEntities(@Nonnull final Economy economy) {
        BiMap<Long, Trader> traderOidToTrader = economy.getTopology().getModifiableTraderOids().inverse();
        reservationBuyerToShoppingLists.forEach((traderId, shoppingListsToProvider) -> {
            if (!traderOidToTrader.containsKey(traderId)) {
                Trader t = economy.addTrader(EntityType.VIRTUAL_MACHINE_VALUE, TraderState.ACTIVE, new Basket());
                shoppingListsToProvider.forEach((sl, providerId) -> {
                    economy.addBasketBought(t, sl.getBasket());
                    if (providerId != null && traderOidToTrader.containsKey(providerId)) {
                        // Create Move and take it to update quantities of provider.
                        new Move(economy, sl, traderOidToTrader.get(providerId)).take();
                    }
                    // make sure the previous reservation buyers not movable so the economy only
                    // place current reservation entities.
                    sl.setMovable(false);
                });
                // adds the reserved trader to the topology's map to keep track of it
                traderOidToTrader.put(traderId, t);
            }
        });
    }

    /**
     * Create traderTOs based on the given InitialPlacementBuyer list.
     *
     * @param buyers the given reservation buyer information
     * @param commTypeToSpecMap topology dto commodity type to trader commodity specification map
     * @return a list of traderTOs
     */
    @VisibleForTesting
    protected List<TraderTO> constructTraderTOs(@Nonnull final List<InitialPlacementBuyer> buyers,
                                                @Nonnull final Map<CommodityType, Integer> commTypeToSpecMap) {
        List<TraderTO> traderTOs = new ArrayList<>();
        for (InitialPlacementBuyer buyer : buyers) {
            TraderTO.Builder traderTO = TraderTO.newBuilder();
            for (InitialPlacementCommoditiesBoughtFromProvider sl : buyer.getInitialPlacementCommoditiesBoughtFromProviderList()) {
                List<CommodityBoughtTO> commBoughtTOs = constructCommBoughtTO(sl
                        .getCommoditiesBoughtFromProvider().getCommodityBoughtList(), commTypeToSpecMap);
                if (commBoughtTOs.isEmpty()) {
                    logger.warn("Empty commodity bought created in this trader {} sl {}, skipping"
                            + " reservation for it", buyer.getBuyerId(), sl.getCommoditiesBoughtFromProviderId());
                    continue;
                }
                // NOTE: InitialPlacementShoppingList has a provider type attribute, but sl doesnt
                // have a counterpart representing it.
                ShoppingListTO slTO = ShoppingListTO.newBuilder()
                        .setOid(sl.getCommoditiesBoughtFromProviderId())
                        .setMovable(true)
                        .addAllCommoditiesBought(commBoughtTOs).build();
                traderTO.addShoppingLists(slTO);
            }
            traderTO.setOid(buyer.getBuyerId())
                    .setDebugInfoNeverUseInCode(buyer.getBuyerId() + PLACEMENT_CLONE_SUFFIX)
                    .setSettings(TraderSettingsTO.newBuilder()
                    .setIsShopTogether(true)
                    .setQuoteFunction(QuoteFunctionDTO.newBuilder().setSumOfCommodity(SumOfCommodity
                            .newBuilder().build())));
            traderTOs.add(traderTO.build());
        }
        return traderTOs;
    }

    /**
     * Create commodityBoughtTOs based on the list of InitialPlacementCommodityBoughtDTO.
     *
     * @param commBoughtList the given reservation commodityBoughtDTO of each entity
     * @param commTypeToSpecMap topology dto commodity type to trader commodity specification map
     * @return a list of commodityBoughtTO in traderTO
     */
    private List<CommodityBoughtTO> constructCommBoughtTO(@Nonnull final List<TopologyDTO.CommodityBoughtDTO> commBoughtList,
                                                          @Nonnull final Map<CommodityType, Integer> commTypeToSpecMap) {
        List<CommodityBoughtTO> commBoughtTOs = new ArrayList<>();
        for (TopologyDTO.CommodityBoughtDTO commBought : commBoughtList) {
            Integer commSpecType = commTypeToSpecMap.get(commBought.getCommodityType());
            if (commSpecType == null) {
                logger.warn("The reservation is given a commodity type {} key {} which may be just"
                        + " created in system, please wait one round of analysis and try this reservation again",
                        commBought.getCommodityType().getType(), commBought.getCommodityType().getKey());
                return commBoughtTOs;
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
        return commBoughtTOs;
    }

    /**
     * Find initial placement for a given list of reservation entities.
     *
     * @param buyers a list of reservation entities
     * @return a table whose row is reservation entity oid, column is shopping list oid and value
     * is the {@link InitialPlacementFinderResult}
     */
    public Table<Long, Long, InitialPlacementFinderResult> findPlacement(@Nonnull final List<InitialPlacementBuyer> buyers) {
        synchronized (economyLock) {
            // <BuyerId - ShoppingListId - ProviderId> Table
            Table<Long, Long, InitialPlacementFinderResult> reservationResult = HashBasedTable.create();
            if (state == PlacementFinderState.NOT_READY) {
                // set result with global failure in response
                return reservationResult;
            }

            List<TraderTO> reservationTraders = new ArrayList<>();
            cachedEconomy.resetMarketsPopulatedFlag();
            // buyers constructed using the cachedCommTypeMap, which has to be the same map for
            // cachedEconomy commodity construction
            BiMap commTypeToSpecMap = HashBiMap.create();
            cachedCommTypeMap.entrySet().forEach(e -> commTypeToSpecMap.put(e.getKey(), e.getValue()));
            reservationTraders = constructTraderTOs(buyers, commTypeToSpecMap);
            // NOTE: reservation id is not passed into traderTO
            reservationTraders.stream().forEach(
                    trader -> ProtobufToAnalysis.addTrader(cachedEconomy.getTopology(), trader));

            cachedEconomy.composeMarketSubsetForPlacement();
            cachedEconomy.populateMarketsWithSellersAndMergeConsumerCoverage();

            PlacementResults placementResults = Placement.placementDecisions(cachedEconomy);


            final Map<Long, ShoppingList> slOidToSlMap =
                    cachedEconomy.getTopology().getShoppingListOids().inverse();
            final Map<Trader, Long> traderToTraderOid = cachedEconomy.getTopology().getTraderOids();
            final Map<Long, Map<ShoppingList, Long>> tIdToSL = new HashMap<>();

            for (TraderTO trader : reservationTraders) {
                for (ShoppingListTO slTO : trader.getShoppingListsList()) {
                    if (slOidToSlMap.containsKey(slTO.getOid())) {
                        ShoppingList sl = slOidToSlMap.get(slTO.getOid());
                        Long supplierId = traderToTraderOid.get(sl.getSupplier());
                        if (supplierId != null) {
                            // when we find provider for reservation, the only field needs to be populated
                            // in ReservationResult is provider oid.
                            reservationResult.put(trader.getOid(), slTO.getOid(),
                                    new InitialPlacementFinderResult(Optional.of(supplierId), new ArrayList<>()));
                            // make sure the reservation entity start to consume on the supplier
                            new Move(cachedEconomy, sl, sl.getSupplier()).take();
                            logger.debug("Reservation succeeded for entity {} sl oid {} to be placed"
                                    + " on supplier {}", trader.getOid(), slTO.getOid(), supplierId);
                        }
                        // stop both placed and unplaced reservation entity moving
                        sl.setMovable(false);
                        tIdToSL.computeIfAbsent(trader.getOid(), v -> new HashMap<>()).put(sl,
                                supplierId);
                    }
                }
            }
            reservationBuyerToShoppingLists.putAll(tIdToSL);
            // process failed placement and pass information to caller
            if (!placementResults.getUnplacedTraders().isEmpty()) {
                Table<Long, Long, InitialPlacementFinderResult> failedResult = buildReservationFailureInfo(
                        placementResults, cachedEconomy, traderToTraderOid, commTypeToSpecMap);
                failedResult.cellSet().forEach(
                        c -> reservationResult.put(c.getRowKey(), c.getColumnKey(), c.getValue()));
            }
            return reservationResult;
        }
    }

    /**
     * Build reservation failure information by extracting insufficient commodity and its closest seller from
     * QuoteTracker.
     *
     * @param result placementResult containing {@link QuoteTracker}
     * @param economy the reservation economy
     * @param traderToOid trader to its oid mapping
     * @param commTypeToSpecMap  a bidirectional map of TopologyDTO.CommodityType and CommoditySpecification's type
     * @return a table of buyer oid, shopping list oid and {@link InitialPlacementFinderResult}
     */
    public Table<Long, Long, InitialPlacementFinderResult> buildReservationFailureInfo(@Nonnull final PlacementResults result,
            @Nonnull final Economy economy,
            @Nonnull final Map<Trader, Long> traderToOid,
            @Nonnull final BiMap<CommodityType, Integer> commTypeToSpecMap) {
        Table<Long, Long, InitialPlacementFinderResult> failureInfo = HashBasedTable.create();
        // iterate unplaced trader and its quote tracker collection to figure out the commodity that exceeds
        // the availability as well as its closest seller that can provide max quantity for that commodity
        for (Map.Entry<Trader, Collection<QuoteTracker>> entry : result.getUnplacedTraders().entrySet()) {
            Long unplacedTraderOid = traderToOid.get(entry.getKey());
            for (QuoteTracker quoteTracker : entry.getValue()) {
                // infiniteQuotesInfo contains the commodity specification to IndividualCommodityQuote mapping
                // IndividualCommodityQuote is a wrapper for a shoppinglist's quote and max quantity
                Map<CommoditySpecification, IndividualCommodityQuote> infiniteQuotesInfo =
                        quoteTracker.getIndividualCommodityQuotes();
                List<FailureInfo> failureInfoList = new ArrayList<>();
                ShoppingList sl = quoteTracker.getShoppingList();
                // populate a table which stores the commodity type, its max quantity available and the seller
                // that can provide the max quantity.
                for (Map.Entry<CommoditySpecification, IndividualCommodityQuote> e : infiniteQuotesInfo
                        .entrySet()) {
                    CommoditySpecification commSpec = e.getKey();
                    int commIndex = sl.getBasket().indexOf(commSpec);
                    IndividualCommodityQuote commQuote = e.getValue();
                    CommodityType commType = commTypeToSpecMap.inverse().get(commSpec.getType());
                    failureInfoList.add(new FailureInfo(commType, traderToOid.get(commQuote.quote.getSeller()),
                            commQuote.availableQuantity, commIndex == -1 ? 0 : sl.getQuantity(commIndex)));
                }
                // the ReservationResult's providerOid is empty in failed cases
                InitialPlacementFinderResult failedResult = new InitialPlacementFinderResult(Optional
                        .empty(), failureInfoList);
                Long slOid = economy.getTopology().getShoppingListOids().get(quoteTracker
                        .getShoppingList());
                failureInfo.put(unplacedTraderOid, slOid, failedResult);
            }
        }
        if (!failureInfo.isEmpty()) {
            logger.debug("Reservation failure : ");
            for (Cell<Long, Long, InitialPlacementFinderResult> failure : failureInfo.cellSet()) {
                logger.debug("Unplaced reservation entity id {}, sl id {} has the following commodities",
                        failure.getRowKey(), failure.getColumnKey());
                for (FailureInfo failureData : failure.getValue().getFailureInfoList()) {
                    logger.debug("commodity type {}, requested amount {}, max quantity available {},"
                            + " closest seller oid {}", failureData.getCommodityType(),
                            failureData.getRequestedAmount(), failureData.getMaxQuantity(),
                            failureData.getClosestSellerOid());
                }
            }
        }
        return failureInfo;
    }

    /**
     * Returns the placement finder state.
     *
     * @return the placement finder state.
     */
    public PlacementFinderState getState() {
        return state;
    }

    /**
     * Set the placement finder state.co
     * @param state state of the placement finder object.
     */
    private  void setState(PlacementFinderState state) {
        this.state = state;
    }
}

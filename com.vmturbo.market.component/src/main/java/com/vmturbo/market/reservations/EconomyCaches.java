package com.vmturbo.market.reservations;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.ede.Placement;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;
import com.vmturbo.platform.analysis.updatingfunction.UpdatingFunctionFactory;
import com.vmturbo.platform.analysis.utilities.InfiniteQuoteExplanation;
import com.vmturbo.platform.analysis.utilities.PlacementResults;

/**
 * The real time and historical economy caches used for find initial placements.
 */
public class EconomyCaches {

    /**
     * Logger.
     */
    private static final Logger logger = LogManager.getLogger();

    /**
     * prefix for initial placement log messages.
     */
    private final String logPrefix = "FindInitialPlacement: ";

    /**
     * The clock to record the reservation update time.
     */
    private final Clock clock = Clock.systemUTC();

    /**
     * The time to record the start of real time economy cache update.
     */
    private Instant realtimeCacheStartUpdateTime = Instant.EPOCH;

    /**
     * The time to record the end of real time economy cache update.
     */
    private Instant realtimeCacheEndUpdateTime = Instant.EPOCH;

    /**
     * The time to record the start of historical economy cache update.
     */
    private Instant historicalCacheStartUpdateTime = Instant.EPOCH;

    /**
     * The time to record the end of historical economy cache update.
     */
    private Instant historicalCacheEndUpdateTime = Instant.EPOCH;

    /**
     *  The state of the two economy caches.
     */
    private EconomyCachesState state = EconomyCachesState.NOT_READY;

    /**
     * State of economy caches.
     */
    public enum EconomyCachesState {

        /**
         * The market has realtime cache available.
         */

        REALTIME_READY,

        /**
         * The list of existing reservations are received from plan orchestrator.
         */
        RESERVATION_RECEIVED,

        /**
         * The market is ready for placement.
         */
        READY,

        /**
         * The market has not yet got cached economy.
         */
        NOT_READY;
    }

    // A minimal version of realtime economy which only contains reservation entities and PM, DS
    @VisibleForTesting
    protected Economy realtimeCachedEconomy;

    // A minimal version of economy loaded with systemLoad statistics with only reservation entities and PM, DS.
    // The historical economy cache will not be created until first headroom plan runs.
    @VisibleForTesting
    protected Economy historicalCachedEconomy = null;

    // A map that stores the TopologyDTO.CommodityType to traderTO's CommoditySpecification
    // type mapping used for real time economy cache
    private BiMap<CommodityType, Integer> realtimeCachedCommTypeMap = HashBiMap.create();

    // A map that stores the TopologyDTO.CommodityType to traderTO's CommoditySpecification
    // type mapping used for historical economy cache
    private BiMap<CommodityType, Integer> historicalCachedCommTypeMap = HashBiMap.create();

    /**
     * Constructor.
     */
    public EconomyCaches() {}

    /**
     * Returns the economy caches state.
     *
     * @return the economy caches state.
     */
    public EconomyCachesState getState() {
        return state;
    }

    /**
     * Set the economy caches state.
     * @param state state of the economy caches.
     */
    public void setState(EconomyCachesState state) {
        this.state = state;
    }

    /**
     * Update real time cached economy.
     *
     * @param originalEconomy the economy to be cloned.
     * @param commTypeToSpecMap the commodity type to commoditySpecification's type mapping.
     * @param buyerOidToPlacement a map of buyer oid to its placement decisions.
     * @param existingReservations a map of existing reservations by oid.
     */
    public void updateRealtimeCachedEconomy(@Nonnull final UnmodifiableEconomy originalEconomy,
            @Nonnull final Map<CommodityType, Integer> commTypeToSpecMap,
            @Nonnull final Map<Long, List<InitialPlacementDecision>> buyerOidToPlacement,
            @Nonnull final Map<Long, List<InitialPlacementBuyer>> existingReservations) {
        Economy newEconomy;
        try {
            realtimeCacheStartUpdateTime = clock.instant();
            newEconomy = InitialPlacementUtils.cloneEconomy(originalEconomy);
            // Add reservation entities to newEconomy which only contains PM and DS
            logger.debug(logPrefix + "Adding reservation {} with buyers {} on real time economy cache",
                    existingReservations.keySet(), buyerOidToPlacement.keySet());
            addExistingReservationEntities(newEconomy, HashBiMap.create(commTypeToSpecMap),
                    buyerOidToPlacement, existingReservations);
        } catch (Exception exception) {
            realtimeCacheEndUpdateTime = clock.instant();
            logger.error(logPrefix + "Skip refresh real time economy cache because of exception {}", exception);
            logger.info(logPrefix + "Real time reservation cache update time : " + realtimeCacheStartUpdateTime
                    .until(realtimeCacheEndUpdateTime, ChronoUnit.SECONDS) + " seconds");
            return;
        }
        // Update commodity type to specification map, it can be different every market cycle
        realtimeCachedCommTypeMap = HashBiMap.create(commTypeToSpecMap);
        // Update cachedEconomy
        realtimeCachedEconomy = newEconomy;
        // Set state to ready once reservations are received from PO and real time economy is ready.
        if (state == EconomyCachesState.RESERVATION_RECEIVED) {
            setState(EconomyCachesState.READY);
        } else if (state != EconomyCachesState.READY) {
            setState(EconomyCachesState.REALTIME_READY);
        }
        realtimeCacheEndUpdateTime = clock.instant();
        logger.info(logPrefix + "Real time economy cache is ready now.");
        logger.info(logPrefix + "Real time reservation cache update time : " + realtimeCacheStartUpdateTime
                .until(realtimeCacheEndUpdateTime, ChronoUnit.SECONDS) + " seconds");
    }

    /**
     * Update historical cached economy.
     *
     * @param originalEconomy the economy to be cloned.
     * @param commTypeToSpecMap the commodity type to commoditySpecification's type mapping.
     * @param buyerOidToPlacement a map of buyer oid to its placement decisions.
     * @param existingReservations a map of existing reservations by oid.
     * @return a map of buyer oid to its placement decisions after update.
     */
    public Map<Long, List<InitialPlacementDecision>> updateHistoricalCachedEconomy(
            @Nonnull final UnmodifiableEconomy originalEconomy,
            @Nonnull final Map<TopologyDTO.CommodityType, Integer> commTypeToSpecMap,
            @Nonnull final Map<Long, List<InitialPlacementDecision>> buyerOidToPlacement,
            @Nonnull final Map<Long, List<InitialPlacementBuyer>> existingReservations) {
        Economy newEconomy;
        Map<Long, List<InitialPlacementDecision>> newResult;
        try {
            historicalCacheStartUpdateTime = clock.instant();
            // Clone a new economy from cluster headroom plan with no workloads.
            newEconomy = InitialPlacementUtils.cloneEconomy(originalEconomy);
            // Replay all existing reservation entities to newEconomy which currently only contains PM and DS
            logger.debug(logPrefix + "Replaying reservation {} with buyers {} on historical economy cache",
                    existingReservations.keySet(), buyerOidToPlacement.keySet());
            newResult = replayReservationBuyers(newEconomy, HashBiMap.create(commTypeToSpecMap),
                    buyerOidToPlacement, existingReservations);
        }  catch (Exception exception) { // Return old placement decisions if update has exceptions.
            historicalCacheEndUpdateTime = clock.instant();
            logger.error(logPrefix + "Skip refresh historical economy cache because of exception {}", exception);
            logger.info(logPrefix + "Historical reservation cache update time : " + historicalCacheStartUpdateTime
                    .until(historicalCacheEndUpdateTime, ChronoUnit.SECONDS) + " seconds");
            return buyerOidToPlacement;
        }
        // Update commodity type to comm specification mapping, it can be different every update cycle
        historicalCachedCommTypeMap = HashBiMap.create(commTypeToSpecMap);
        // Update cachedEconomy
        historicalCachedEconomy = newEconomy;
        historicalCacheEndUpdateTime = clock.instant();
        logger.info(logPrefix + "Historical economy cache is ready now.");
        logger.info(logPrefix + "Historical reservation cache update time : " + historicalCacheStartUpdateTime
                .until(historicalCacheEndUpdateTime, ChronoUnit.SECONDS) + " seconds");
        return newResult;
    }



    /**
     * Add existing reservation buyers to the economy and apply its impact on providers' utilization.
     * Note: currently there is no need to rerun the reservations in real time because short term
     * reservations are supposed to give result in the initial request. Yet long term reservations
     * are supposed to give result regarding cluster choice not entity choice.
     *
     * @param economy the economy for reservation.
     * @param commTypeToSpecMap the commodity type to commodity specification mapping.
     * @param buyerOidToPlacement a map of buyer oid to its placement decisions.
     * @param existingReservations a map of existing reservations by oid.
     */
    private void addExistingReservationEntities(@Nonnull final Economy economy,
            @Nonnull final BiMap<CommodityType, Integer> commTypeToSpecMap,
            @Nonnull final Map<Long, List<InitialPlacementDecision>> buyerOidToPlacement,
            @Nonnull final Map<Long, List<InitialPlacementBuyer>> existingReservations) {
        if (existingReservations.isEmpty() || buyerOidToPlacement.isEmpty()) {
            return;
        }
        // Figure out the cluster constraint based on supplier, construct traderTO with cluster
        // boundaries.
        List<TraderTO> currentlyPlacedBuyers = InitialPlacementUtils
                .constructTraderTOListWithBoundary(economy, commTypeToSpecMap, buyerOidToPlacement,
                        existingReservations).stream().flatMap(List::stream).collect(Collectors.toList());
        List<Trader> addedTraders = new ArrayList();
        // Add reservation traders into economy. The economy only contains host and storage at this point.
        currentlyPlacedBuyers.forEach(traderTO -> {
            Trader trader = ProtobufToAnalysis.addTrader(economy.getTopology(), traderTO);
            addedTraders.add(trader);
        });
        InitialPlacementUtils.getEconomyReady(economy);
        for (Trader trader : addedTraders) {
            List<InitialPlacementDecision> placements = buyerOidToPlacement.get(trader.getOid());
            // Filter shopping list of a given buyer that already have a supplier.
            Map<Long, List<InitialPlacementDecision>> placedSl = placements.stream()
                    .filter(p -> p.supplier.isPresent())
                    .collect(Collectors.groupingBy(r -> r.slOid));
            for (ShoppingList sl : economy.getMarketsAsBuyer(trader).keySet()) {
                long slOid = economy.getTopology().getShoppingListOids().get(sl);
                List<InitialPlacementDecision> initialPlacementDecisions = placedSl.get(slOid);
                // Each shopping list has only 1 supplier at most
                if (initialPlacementDecisions != null && initialPlacementDecisions.size() == 1) {
                    // Place the sl on the given supplier and apply utilization on the supplier.
                    Trader supplierInEconomy = economy.getTopology().getTradersByOid()
                            .get(initialPlacementDecisions.get(0).supplier.get());
                    if (supplierInEconomy != null) {
                        new Move(economy, sl, supplierInEconomy).take();
                    }
                    // Make sure the previous reservation buyers not movable in real time cache
                    sl.setMovable(false);
                }
            }
        }
    }

    /**
     * Replay reservation buyers on the given economy following the same order in which reservations were
     * created. For reservation buyers that were previously placed, finds the cluster boundaries based on
     * previous suppliers, constructs {@link TraderTO}s that includes the cluster constraints, then
     * triggers a new round of placement. If new placement still finds valid suppliers, returns the new
     * placement result, otherwise, returns the {@link InitialPlacementDecision} containing
     * {@link com.vmturbo.market.reservations.InitialPlacementFinderResult.FailureInfo}s.
     *
     * @param economy the economy
     * @param commTypeToSpecMap the commodity type to commodity specification mapping.
     * @param buyerOidToPlacement a map of buyer oid to its placement decisions.
     * @param existingReservations a map of existing reservations by oid.
     * @return a map of buyer oid to its placement decisions after replay.
     */
    private @Nonnull Map<Long, List<InitialPlacementDecision>> replayReservationBuyers(
            @Nonnull final Economy economy,
            @Nonnull final BiMap<CommodityType, Integer> commTypeToSpecMap,
            @Nonnull final Map<Long, List<InitialPlacementDecision>> buyerOidToPlacement,
            @Nonnull final Map<Long, List<InitialPlacementBuyer>> existingReservations) {
        if (existingReservations.isEmpty() || buyerOidToPlacement.isEmpty()) {
            return new HashMap();
        }
        // Replay the reservation one by one following the order they were added
        List<List<TraderTO>> placedBuyersPerRes = InitialPlacementUtils
                .constructTraderTOListWithBoundary(economy, commTypeToSpecMap, buyerOidToPlacement,
                        existingReservations);
        // Run placement in historical economy cache and override the old placement result with new ones
        // The replay order follows the order reservations were added.
        for (List<TraderTO> tradersPerRes : placedBuyersPerRes) {
            Map<Long, List<InitialPlacementDecision>> newPlacementResult =
                    placeBuyerInCachedEconomy(tradersPerRes, economy, commTypeToSpecMap);
            if (newPlacementResult.values().stream().flatMap(List::stream)
                    .allMatch(i -> i.supplier.isPresent())) {
                // All buyers in a given reservation can still find providers in the chosen cluster.
                // Make them movable false in the historical cache. No need to add newPlacementResult
                // to buyerOidToPlacement because the provider is decided by realtime not historical.
                newPlacementResult.keySet().forEach(oid -> {
                    Trader placedBuyer = economy.getTopology().getTradersByOid().get(oid);
                    economy.getMarketsAsBuyer(placedBuyer).keySet().forEach(sl -> sl.setMovable(false));
                });
            } else {
                // At least 1 buyer in a reservation can not be placed. All buyers in the reservation
                // should become unplaced and removed from economy.
                newPlacementResult.values().stream().flatMap(List::stream).forEach(i -> i.supplier = Optional.empty());
                removeDeletedTraders(economy, newPlacementResult.keySet());
                removeDeletedTraders(realtimeCachedEconomy, newPlacementResult.keySet());
                // Update new result in buyerOidToPlacement because some buyers failed in the chosen
                // cluster.
                newPlacementResult.entrySet().forEach(e -> buyerOidToPlacement.put(e.getKey(), e.getValue()));
            }
        }

        return buyerOidToPlacement;
    }

    /**
     * Remove reservation buyers from  both real time and historical economy caches. Update the provider quantities
     * for already placed reservation buyers.
     *
     * @param buyersToBeDeleted oids of reservation buyers to be removed
     */
    public void clearDeletedBuyersFromCache(@Nonnull final Set<Long> buyersToBeDeleted) {
        if (state != EconomyCachesState.READY) {
            logger.warn(logPrefix + "Economy caches are not ready to remove any buyers");
            return;
        }
        removeDeletedTraders(realtimeCachedEconomy, buyersToBeDeleted);
        removeDeletedTraders(historicalCachedEconomy, buyersToBeDeleted);

    }

    /**
     * Remove and roll back traders from an economy.
     *
     * @param economy the given economy.
     * @param buyersToBeDeleted the oid set of buyers to be deleted.
     */
    public void removeDeletedTraders(@Nullable final Economy economy, @Nonnull final Set<Long> buyersToBeDeleted) {
        if (economy == null) { // Historical economy may not be created yet.
            return;
        }
        Map<Long, Trader> traderByOid = economy.getTopology().getTradersByOid();
        Map<ShoppingList, Long> slOidMap = economy.getTopology().getModifiableShoppingListOids();
        Set<Long> removeBuyers = new HashSet<>();
        for (long oid : buyersToBeDeleted) {
            Trader removeBuyer = traderByOid.get(oid);
            if (removeBuyer != null) {
                removeBuyers.add(oid);
            }
        }
        rollbackPlacedTraders(economy, removeBuyers, traderByOid);
        for (long removeOid : removeBuyers) {
            Trader buyer = traderByOid.get(removeOid);
            // remove deleted reservation shopping list from  topology
            economy.getMarketsAsBuyer(buyer).keySet().forEach(sl -> {
                slOidMap.remove(sl);
            });
            // remove deleted reservation entities from economy and topology
            economy.removeTrader(buyer);
            economy.getTopology().getModifiableTraderOids().remove(removeOid);
        }

        if (!removeBuyers.isEmpty()) {
            logger.info(logPrefix + "Removed reservation entities oid {}", removeBuyers.stream()
                    .collect(Collectors.toList()));
        }
    }

    /**
     * Check if traders are already placed in economy. If so, roll back shopping list to get rid of
     * the utilization impact on providers.
     *
     * @param economy the economy
     * @param rollBackTraders a set of trader oids
     * @param traderByOid the trader by oid map in economy
     */
    private static void rollbackPlacedTraders(@Nonnull Economy economy,
            @Nonnull final Set<Long> rollBackTraders,
            @Nonnull final Map<Long, Trader> traderByOid) {
        for (long oid : rollBackTraders) {
            Trader trader = traderByOid.get(oid);
            if (trader != null) {
                for (ShoppingList sl : economy.getMarketsAsBuyer(trader).keySet()) {
                    // roll back the already placed shopping list
                    Trader supplier = sl.getSupplier();
                    if (supplier != null) {
                        Move.updateQuantities(economy, sl, supplier, UpdatingFunctionFactory.SUB_COMM, false);
                        sl.move(null);
                    }
                    sl.setMovable(false);
                }
            }
        }
        return;
    }

    /**
     * Find placement for a list of {@link InitialPlacementBuyer}s that come from one reservation.
     *
     * @param buyers a list of {@link InitialPlacementBuyer}s from the same reservation.
     * @param slToClusterMap A map to keep track of successfully placed  reservation buyer's shopping
     * list oid to its cluster commodity mapping.
     * @param maxRetry the max number of retry the find placement logic.
     * @return a map of {@link InitialPlacementBuyer} oid to its placement decisions.
     */
    public Map<Long, List<InitialPlacementDecision>> findInitialPlacement(
            @Nonnull final List<InitialPlacementBuyer> buyers,
            @Nonnull final Map<Long, CommodityType> slToClusterMap,
            final int maxRetry) {
        if (state != EconomyCachesState.READY) {
            logger.warn(logPrefix + "Market is not ready to run reservation yet, wait for another broadcast to retry");
            return new HashMap();
        }
        // Create buyers and add into historical cache
        List<TraderTO> traderTOs = new ArrayList();
        Map<Long, List<InitialPlacementDecision>> firstRoundPlacement = new HashMap();
        Set<Long> buyerFailedInHistoricalCache = new HashSet();
        // A map of shopping list oid to its supplier's cluster commodity.
        final Map<Long, CommodityType> clusterCommPerSl = new HashMap();
        if (historicalCachedEconomy != null) {
            for (InitialPlacementBuyer buyer : buyers) {
                Optional<TraderTO> traderTO = InitialPlacementUtils.constructTraderTO(buyer, historicalCachedCommTypeMap,
                        new HashMap());
                if (traderTO.isPresent()) {
                    traderTOs.add(traderTO.get());
                } else {
                    return new HashMap();
                }
            }
            firstRoundPlacement = placeBuyerInCachedEconomy(traderTOs, historicalCachedEconomy,
                    historicalCachedCommTypeMap);
            logger.info(logPrefix + "Placing reservation buyers on historical economy cache");
            InitialPlacementUtils.printPlacementDecisions(firstRoundPlacement);
            clusterCommPerSl.putAll(InitialPlacementUtils.extractClusterBoundary(historicalCachedEconomy,
                    historicalCachedCommTypeMap, firstRoundPlacement, buyers, buyerFailedInHistoricalCache));
        }
        if (!buyerFailedInHistoricalCache.isEmpty()) {
            // Not all buyers given to this method can find placement, we should fail the reservation
            // when it is successful only on partial buyers. Remove all successful ones from economy.
            removeDeletedTraders(historicalCachedEconomy, firstRoundPlacement.keySet());
            // Unplace all the buyers because at least one buyer failed.
            firstRoundPlacement.values().stream().flatMap(List::stream).forEach(pl ->
                    pl.supplier = Optional.empty());
            logger.info(logPrefix + "Not all buyers in reservation {} can be placed according to historical stats.",
                    buyers.get(0).getReservationId());
            return firstRoundPlacement;
        }
        // All the buyers succeeded in historical cache, now place them on real time.
        List<TraderTO> placedBuyerTOs = new ArrayList();
        //construct traderTO from InitialPlacementBuyer including cluster boundary
        for (InitialPlacementBuyer buyer : buyers) {
            // Construct traderTO with cluster boundaries provided in clusterCommPerSl
            Optional<TraderTO> traderTO = InitialPlacementUtils.constructTraderTO(buyer,
                    realtimeCachedCommTypeMap, clusterCommPerSl);
            if (traderTO.isPresent()) {
                placedBuyerTOs.add(traderTO.get());
            } else {
                return new HashMap();
            }
        }
        Map<Long, List<InitialPlacementDecision>> secondRoundPlacement =
                placeBuyerInCachedEconomy(placedBuyerTOs, realtimeCachedEconomy,
                        realtimeCachedCommTypeMap);
        logger.info(logPrefix + "Placing reservation buyers on realtime economy cache");
        InitialPlacementUtils.printPlacementDecisions(secondRoundPlacement);
        Set<Long> failedBuyerOids = new HashSet();
        for (Map.Entry<Long, List<InitialPlacementDecision>> entry : secondRoundPlacement.entrySet()) {
            if (entry.getValue().stream().allMatch(pl -> pl.supplier.isPresent())) {
                // Populate the cluster commodity type for successful each buyer's shopping list.
                entry.getValue().forEach( d -> slToClusterMap.put(d.slOid, clusterCommPerSl.get(d.slOid)));
            } else {
                failedBuyerOids.add(entry.getKey());
            }
        }
        if ((maxRetry == 0 || historicalCachedEconomy == null) && !failedBuyerOids.isEmpty()) {
            // No need to retry, unplace the entire reservation and clear all buyers
            logger.info(logPrefix + "Not all buyers in reservation {} can be placed according to real time stats.",
                    buyers.get(0).getReservationId());
            processRetryPlacementsDecisions(secondRoundPlacement, slToClusterMap);
        } else if (!failedBuyerOids.isEmpty()) {
            // Retry means the cluster chosen in historical economy may not be good in real time. When
            // historicalCachedEconomy is null, no need to retry at all.
            Map<Long, List<InitialPlacementDecision>> retryResult = retryPlacement(buyers.stream()
                    .filter(b -> failedBuyerOids.contains(b.getBuyerId())).collect(Collectors.toList()),
                    clusterCommPerSl, slToClusterMap, maxRetry - 1);
            if (retryResult.values().stream().flatMap(List::stream)
                    .anyMatch(pl -> !pl.supplier.isPresent())) {
                // Retry still contains some no placements, unplace and clear all the buyers
                processRetryPlacementsDecisions(secondRoundPlacement, slToClusterMap);
            } else {
                secondRoundPlacement.putAll(retryResult);
            }
        } else if (historicalCachedEconomy == null && failedBuyerOids.isEmpty()) {
            // Populate the cluster map which will be used for stats
            slToClusterMap.putAll(InitialPlacementUtils.extractClusterBoundary(realtimeCachedEconomy,
                    realtimeCachedCommTypeMap, secondRoundPlacement, buyers, new HashSet()));
        }
        return secondRoundPlacement;
    }

    /**
     * Check placement decisions, if any one failed, remove the buyer from both economy caches.
     *
     * @param result a map of buyer oid to its list of {@link InitialPlacementDecision}s.
     * @param slToClusterMap the map to keep track of placed shopping list and cluster commodity.
     * @return true if all InitialPlacementDecision finds a supplier.
     */
    private boolean processRetryPlacementsDecisions(
            @Nonnull final Map<Long, List<InitialPlacementDecision>> result,
            @Nonnull final Map<Long, CommodityType> slToClusterMap) {
        boolean isSuccessful = true;
        if (result.values().stream().flatMap(List::stream).anyMatch(pl -> !pl.supplier.isPresent())) {
            result.values().stream().flatMap(List::stream).forEach(pl -> {
                pl.supplier = Optional.empty();
                slToClusterMap.remove(pl.slOid);
            });
            clearDeletedBuyersFromCache(result.keySet());
            isSuccessful = false;
        }
        return isSuccessful;
    }

    /**
     * Retry placement for a list of buyers. The cluster in which the buyer failed is provided so
     * that retry will exclude the given cluster.
     *
     * @param buyersToRetry a list of buyers to run placement.
     * @param clusterCommPerSl buyer's shopping list oid to the cluster that it failed to be placed.
     * @param slToClusterMap the map to keep track of placed shopping list and cluster commodity.
     * @param maxRetry the max number of rerun the findInitialPlacement.
     * @return a map of {@link InitialPlacementBuyer} oid to its placement decisions.
     */
    protected Map<Long, List<InitialPlacementDecision>> retryPlacement(
            @Nonnull final List<InitialPlacementBuyer> buyersToRetry,
            @Nonnull final Map<Long, CommodityType> clusterCommPerSl,
            @Nonnull final Map<Long, CommodityType> slToClusterMap,
            final int maxRetry) {
        if (buyersToRetry.isEmpty()) {
            return new HashMap();
        }
        Set<Long> buyerOids = buyersToRetry.stream().map(b -> b.getBuyerId())
                .collect(Collectors.toSet());
        logger.info(logPrefix + "Retrying for buyers {} in reservation {}", buyerOids, buyersToRetry.get(0).getReservationId());
        // Remove buyersToRetry from both economy caches.
        clearDeletedBuyersFromCache(buyerOids);
        // We are going to make sellers that sell cluster commodity as CanAcceptNewCustomers
        // false. This can avoid choosing sellers from the previously failed cluster.
        Set<Long> failedClusterSellers = InitialPlacementUtils.setSellersNotAcceptCustomers(
                historicalCachedEconomy, historicalCachedCommTypeMap, clusterCommPerSl);
        Map<Long, List<InitialPlacementDecision>> result = findInitialPlacement(buyersToRetry,
                slToClusterMap, maxRetry);
        // Reset back sellers after rerun the placement.
        InitialPlacementUtils.restoreCanNotAcceptNewCustomerSellers(historicalCachedEconomy,
                failedClusterSellers);
        return result;
    }

    /**
     * Run placement for a list of {@link TraderTO}s in a given economy.
     *
     * @param traderTOs the list of traderTOs to be placed.
     * @param economy the economy.
     * @param commTypeToSpecMap the commodity type to commodity specification mapping.
     * @return a map of reservation buyer oid to its placement decisions.
     */
    private Map<Long, List<InitialPlacementDecision>> placeBuyerInCachedEconomy(
            @Nonnull final List<TraderTO> traderTOs, @Nonnull final Economy economy,
            @Nonnull final BiMap<CommodityType, Integer> commTypeToSpecMap) {

        // make all shopping list of all traders except the current traders movable false.
        for (Trader trader : economy.getTraders()) {
            for (ShoppingList sl : economy.getMarketsAsBuyer(trader).keySet()) {
                sl.setMovable(false);
            }
        }
        Map<Long, List<InitialPlacementDecision>> traderIdToPlacement = new HashMap();
        traderTOs.stream().forEach(
                trader -> ProtobufToAnalysis.addTrader(economy.getTopology(), trader));
        InitialPlacementUtils.getEconomyReady(economy);
        Set<Long> buyerIds = traderTOs.stream().map(TraderTO::getOid).collect(
                Collectors.toSet());
        PlacementResults placementResults = Placement.placementDecisions(economy);
        Set<Long> unplacedBuyer = new HashSet();
        for (long oid : buyerIds) {
            Trader reservationBuyer = economy.getTopology().getTradersByOid().get(oid);
            for (ShoppingList sl : economy.getMarketsAsBuyer(reservationBuyer).keySet()) {
                // Create reservation placement for each sl, including those do not have a supplier
                // so that each sl has a reservationPlacement initialized
                InitialPlacementDecision resPlacement = new InitialPlacementDecision(
                        economy.getTopology().getShoppingListOids().get(sl), sl.getSupplier() == null
                        ? Optional.empty() : Optional.of(sl.getSupplier().getOid()), new ArrayList());
                if (!traderIdToPlacement.containsKey(oid)) {
                    traderIdToPlacement.put(oid, new ArrayList(Arrays.asList(resPlacement)));
                } else {
                    traderIdToPlacement.get(oid).add(resPlacement);
                }
                if (sl.getSupplier() == null) {
                    unplacedBuyer.add(oid);
                }
            }
        }
        if (!unplacedBuyer.isEmpty()) {
            // Populate explanation only if there is any unplaced buyer.
            placementResults.populateExplanationForInfinityQuoteTraders();
        }
        // Populate failureInfos in ReservationPlacement for unplaced buyers.
        for (Map.Entry<Trader, List<InfiniteQuoteExplanation>> entry
                : placementResults.getExplanations().entrySet()) {
            Trader unplaced = entry.getKey();
            if (!unplacedBuyer.contains(unplaced.getOid())) {
                continue;
            }
            Map<Long, List<InitialPlacementDecision>> placementPerSl = traderIdToPlacement
                    .get(unplaced.getOid()).stream().collect(Collectors.groupingBy(r -> r.slOid));
            for (InfiniteQuoteExplanation exp : entry.getValue()) {
                long slOid = economy.getTopology().getShoppingListOids().get(exp.shoppingList);
                List<InitialPlacementDecision> pl = placementPerSl.get(slOid);
                pl.forEach(p -> {
                    p.failureInfos = InitialPlacementUtils.populateFailureInfos(exp, commTypeToSpecMap);
                });
            }
        }
        return traderIdToPlacement;
    }

    /**
     * Calculate the real time cluster statistics after {@link InitialPlacementBuyer}s are placed.
     *
     * @param initialPlacements a map of buyer oid to its placement decisions.
     * @param buyers a list od {@link InitialPlacementBuyer}s.
     * @param slToClusterMap a map of reservation buyer's shopping list oid to its cluster mapping.
     * @return a map of shopping list oid -> commodity type -> {total use, total capacity} of a cluster.
     */
    public Map<Long, Map<CommodityType, Pair<Double, Double>>> calculateClusterStats(
            @Nonnull final Map<Long, List<InitialPlacementDecision>> initialPlacements,
            @Nonnull final List<InitialPlacementBuyer> buyers,
            @Nonnull final Map<Long, CommodityType> slToClusterMap) {
        // Group buyers by reservation id.
        Map<Long, List<InitialPlacementBuyer>> buyersByReservationId = buyers.stream().collect(
                Collectors.groupingBy(InitialPlacementBuyer::getReservationId));
        // Find buyers that are in a reservation that all buyers within are placed on suppliers.
        // We assume partial successful reservations do not need to construct cluster stats.
        Set<Long> fullySuccessfulReservationIds = buyersByReservationId.keySet().stream().collect(
                Collectors.toSet());
        buyersByReservationId.entrySet().forEach(e -> {
            e.getValue().stream().forEach(i -> {
                if (initialPlacements.getOrDefault(i.getBuyerId(),
                        new ArrayList<InitialPlacementDecision>()).stream()
                        .anyMatch(d -> !d.supplier.isPresent())) {
                    fullySuccessfulReservationIds.remove(e.getKey());
                }
            });
        });
        Set<InitialPlacementBuyer> successfulBuyers = fullySuccessfulReservationIds.stream()
                .map(reservationId -> buyersByReservationId.get(reservationId))
                .flatMap(List::stream).collect(Collectors.toSet());
        Set<Long> successfulBuyerOids = successfulBuyers.stream().map(b -> b.getBuyerId()).collect(
                Collectors.toSet());
        Map<Long, List<InitialPlacementDecision>> successfulPlacements = new HashMap();
        initialPlacements.entrySet().forEach(e -> {
            if (successfulBuyerOids.contains(e.getKey())) {
                successfulPlacements.put(e.getKey(), e.getValue());
            }
        });
        Map<Long, CommodityType> successfulSlToClusterMap = new HashMap();
        successfulPlacements.values().stream().flatMap(List::stream).forEach(pl -> {
            CommodityType cluster = slToClusterMap.get(pl.slOid);
            if (cluster != null) {
                successfulSlToClusterMap.put(pl.slOid, cluster);
            }
        });
        // Populate the stats for the reservation buyers.
        return InitialPlacementUtils.calculateClusterStatistics(realtimeCachedEconomy,
                realtimeCachedCommTypeMap, successfulSlToClusterMap, successfulBuyers);
    }

    /**
     * Update the HistoricalCachedEconomy to null.
     */
    public void clearHistoricalCachedEconomy() {
        historicalCachedEconomy = null;
    }
}


package com.vmturbo.market.reservations;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.market.reservations.EconomyCaches.EconomyCachesState;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;

/**
 * The class to support fast reservation placement.
 */
public class InitialPlacementFinder {

    // Whether the reservation cache should be constructed or not.
    private boolean prepareReservationCache;
    // the object to hold economy caches
    private EconomyCaches economyCaches;
    // a map to keep track of existing reservation ids and the buyers within each reservation. The order
    // is preserved so that replay is following the order as reservations were added.
    @VisibleForTesting
    protected Map<Long, List<InitialPlacementBuyer>> existingReservations = new LinkedHashMap();
    // A map of reservation buyer oid to its placement result.
    @VisibleForTesting
    protected Map<Long, List<InitialPlacementDecision>> buyerPlacements = new HashMap<>();
    // Whether the historical economy cache is being updated with historical stats.
    private boolean isHistoricalCacheUpdated = false;
    // the lock to synchronize the change of reservation
    private Object reservationLock = new Object();

    // Logger
    private static final Logger logger = LogManager.getLogger();


    /**
     * Constructor.
     * @param prepareReservationCache whether economy caches should be built.
     */
    public InitialPlacementFinder(final boolean prepareReservationCache) {
        economyCaches = new EconomyCaches();
        this.prepareReservationCache = prepareReservationCache;
    }

    /**
     * Whether economy caches should be constructed or not.
     *
     * @return true if the reservation feature is in use.
     */
    public boolean shouldConstructEconomyCache() {
        return prepareReservationCache;
    }

    /**
     * Triggers the update of economy caches.
     *
     * @param originalEconomy the economy to be used as model for clone.
     * @param commTypeToSpecMap the commodity type to commodity specification mapping.
     * @param isRealtime true if to update the real time cached economy, false if to update the
     * historical cached economy.
     */
    public void updateCachedEconomy(@Nonnull final UnmodifiableEconomy originalEconomy,
            @Nonnull final Map<CommodityType, Integer> commTypeToSpecMap, final boolean isRealtime) {
        synchronized (reservationLock) {
            if (isRealtime) {
                // Update the providers with the latest broadcast in real time economy cache,
                // Apply all successfully placed reservations to the same providers that were
                // recorded in buyerPlacements.
                economyCaches.updateRealtimeCachedEconomy(originalEconomy, commTypeToSpecMap,
                        buyerPlacements, existingReservations);
                if (!isHistoricalCacheUpdated) {
                    // Using real time for historical cache update before the system load stats is ready.
                    buyerPlacements = economyCaches.updateHistoricalCachedEconomy(originalEconomy,
                            commTypeToSpecMap, buyerPlacements, existingReservations);
                }
            } else {
                // Update the providers with providers generated in headroom plan in historical economy
                // cache. Rerun all successfully placed reservations and update the providers in
                // buyerPlacements.
                buyerPlacements = economyCaches.updateHistoricalCachedEconomy(originalEconomy,
                        commTypeToSpecMap, buyerPlacements, existingReservations);
                isHistoricalCacheUpdated = true;
            }
        }
    }

    /**
     * Remove buyers from existingReservations and buyerPlacements. Remove corresponding traders
     * from both economy caches.
     *
     * @param deleteBuyerOids the list of reservation buyer oids.
     * @return true if removal from the cached economy completes.
     */
    public boolean buyersToBeDeleted(List<Long> deleteBuyerOids) {
        synchronized (reservationLock) {
            logger.info("Prepare to delete reservation entities {} from both cached economies",
                    deleteBuyerOids);
            Set<Long> reservationsToRemove = new HashSet();
            for (Map.Entry<Long, List<InitialPlacementBuyer>> entry : existingReservations.entrySet()) {
                Set<Long> existingBuyerOids = entry.getValue().stream()
                        .map(InitialPlacementBuyer::getBuyerId).collect(Collectors.toSet());
                existingBuyerOids.removeAll(deleteBuyerOids);
                if (existingBuyerOids.isEmpty()) {
                    reservationsToRemove.add(entry.getKey());
                }
            }
            // Remove reservation buyers grouped by reservation oid one by one.
            for (Long oid : reservationsToRemove) {
                List<InitialPlacementBuyer> buyersToRemove = existingReservations.get(oid);
                Set<Long> buyerOids = buyersToRemove.stream()
                        .map(InitialPlacementBuyer::getBuyerId)
                        .collect(Collectors.toSet());
                try {
                    economyCaches.clearDeletedBuyersFromCache(buyerOids);
                    buyerPlacements.keySet().removeAll(buyerOids);
                    existingReservations.remove(oid);
                    logger.info("Reservation {} is successfully remove with {} entities.", oid,
                            buyerOids.size());
                } catch (Exception exception) {
                    // In case any reservation trader failed to be cleared from economy, ask user wait for
                    // both historical cache and realtime cache updated.
                    economyCaches.setState(EconomyCachesState.NOT_READY);
                    logger.warn("Setting economy caches state to NOT READY. Wait for 24 hours to run"
                            + " other reservation requests.");
                    logger.error("Reservation {} can not be remove with {} entities due to {}.",
                            oid, buyerOids.size(), exception);
                }
            }
        }
        return true;
    }

    /**
     * Find initial placement for a given list of reservation entities.
     *
     * @param buyers a list of reservation entities
     * @return a table whose row is reservation entity oid, column is shopping list oid and value
     * is the {@link InitialPlacementFinderResult}
     */
    public Table<Long, Long, InitialPlacementFinderResult> findPlacement(
            @Nonnull final List<InitialPlacementBuyer> buyers) {
        //TODO: implement this method in a different rb review
        return HashBasedTable.create();
    }
}

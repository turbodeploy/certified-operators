package com.vmturbo.market.reservations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.market.InitialPlacement.GetProvidersOfExistingReservationsResponse;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyerPlacementInfo;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementFailure;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityStats;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.UnplacementReason;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.UnplacementReason.FailedResources;
import com.vmturbo.market.reservations.EconomyCaches.EconomyCachesState;
import com.vmturbo.market.reservations.InitialPlacementFinderResult.FailureInfo;
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
    // The lock to synchronize the change of reservation
    private Object reservationLock = new Object();
    // The max number of retry if findInitialPlacement failed
    private static int maxRetry;
    // Logger
    private static final Logger logger = LogManager.getLogger();

    /**
     * Find the InitialPlacementDecision corresponding to buyer.
     *
     * @param buyerID the buyerID of interest.
     * @return InitialPlacementDecision associated with the buyer.
     */
    public List<InitialPlacementDecision> findExistingInitialPlacementDecisions(Long buyerID) {
        if (buyerPlacements.containsKey(buyerID)) {
            return buyerPlacements.get(buyerID);
        } else {
            return new ArrayList<>();
        }
    }

    /**
     * Constructor.
     * @param prepareReservationCache whether economy caches should be built.
     * @param  maxRetry The max number of retry if findInitialPlacement failed.
     */
    public InitialPlacementFinder(final boolean prepareReservationCache, int maxRetry) {
        economyCaches = new EconomyCaches();
        this.prepareReservationCache = prepareReservationCache;
        this.maxRetry = maxRetry;
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
        if (buyers.isEmpty()) {
            return HashBasedTable.create();
        }
        // Group buyers by reservation id.
        Map<Long, List<InitialPlacementBuyer>> buyersByReservationId = buyers.stream().collect(
                Collectors.groupingBy(InitialPlacementBuyer::getReservationId));
        // A map to keep all the reservations placement result. Key is buyer oid, value is a list
        // of InitialPlacementDecision.
        Map<Long, List<InitialPlacementDecision>> initialPlacements = new HashMap();
        // A map to keep track of reservation buyer's shopping list oid to its cluster mapping.
        Map<Long, CommodityType> slToClusterMap = new HashMap();
        synchronized (reservationLock) {
            // Find providers for buyers via running placements in economy caches. Keep track of
            // placement results in buyerPlacements.
            for (Map.Entry<Long, List<InitialPlacementBuyer>> buyersPerReservation
                    : buyersByReservationId.entrySet()) {
                Set<Long> buyersInOneRes = buyersPerReservation.getValue().stream().map(i ->
                        i.getBuyerId()).collect(Collectors.toSet());
                try {
                    Map<Long, List<InitialPlacementDecision>> initialPlacementPerReservation =
                            economyCaches.findInitialPlacement(buyersPerReservation.getValue(),
                                    slToClusterMap, maxRetry);
                    buyerPlacements.putAll(initialPlacementPerReservation);
                    // Keep incoming reservation buyers in the existingReservations map
                    existingReservations.put(buyersPerReservation.getKey(), buyersPerReservation.getValue());
                    initialPlacements.putAll(initialPlacementPerReservation);
                } catch (Exception exception) {
                    // Any request that encounters an exception should fail all the buyers in the
                    // same reservation.
                    logger.error("Find placement failed for reservation {} containing buyers {} with"
                            + " exception {} ", buyersPerReservation.getKey(), buyersInOneRes, exception);
                    // Make sure no buyers in the failed reservation are added into existingReservations
                    // or buyerPlacements and no such buyers exist in both economy caches.
                    existingReservations.remove(buyersPerReservation.getKey());
                    buyersInOneRes.stream().forEach(buyerOid -> buyerPlacements.remove(buyerOid));
                    economyCaches.clearDeletedBuyersFromCache(buyersInOneRes);
                    buyersPerReservation.getValue().stream()
                            .map(buyer -> buyer.getInitialPlacementCommoditiesBoughtFromProviderList())
                            .flatMap(List::stream)
                            .forEach(sl -> slToClusterMap.remove(sl.getCommoditiesBoughtFromProviderId()));
                    // Create empty InitialPlacementDecision for each buyer in the reservation that
                    // encounter exception.
                    List<InitialPlacementDecision> emptyDecisions = new ArrayList();
                    buyersPerReservation.getValue().forEach(b -> {
                        b.getInitialPlacementCommoditiesBoughtFromProviderList().stream()
                                .map(sl -> sl.getCommoditiesBoughtFromProviderId())
                                .forEach(id -> emptyDecisions.add(new InitialPlacementDecision(
                                        b.getBuyerId(), Optional.of(id), new ArrayList())));
                        initialPlacements.put(b.getBuyerId(), emptyDecisions);
                    });
                }
            }
            // process reservation result from sl to provider mapping
            return buildReservationResponse(initialPlacements, slToClusterMap,
                    economyCaches.calculateClusterStats(initialPlacements, buyers, slToClusterMap));
        }
    }

    /**
     * Build reservation response for a set of reservation buyers.
     *
     * @param reservationPlacements a map of reservation buyer oid to its placement decisions.
     * @param slToClusterMap a map of shopping list oid to cluster commodity type.
     * @param clusterUsedAndCapacity a map of shopping list oid -> commodity type -> {total use,
     * total capacity} of a cluster.
     * @return a table whose row is reservation entity oid, column is shopping list oid and value
     * is the {@link InitialPlacementFinderResult}
     */
    private Table<Long, Long, InitialPlacementFinderResult> buildReservationResponse(
            @Nonnull final Map<Long, List<InitialPlacementDecision>> reservationPlacements,
            @Nonnull final Map<Long, CommodityType> slToClusterMap,
            @Nonnull final Map<Long, Map<CommodityType, Pair<Double, Double>>> clusterUsedAndCapacity) {
        Table<Long, Long, InitialPlacementFinderResult> placementResult = HashBasedTable.create();
        for (Map.Entry<Long, List<InitialPlacementDecision>> buyerPlacement
                : reservationPlacements.entrySet()) {
            long buyerOid = buyerPlacement.getKey();
            List<InitialPlacementDecision> placements = buyerPlacement.getValue();
            for (InitialPlacementDecision placement : placements) {
                if (placement.supplier.isPresent()) { // the sl is successfully placed
                    List<CommodityStats> clusterStats = new ArrayList();
                    clusterUsedAndCapacity.get(placement.slOid).entrySet().forEach(e -> {
                        clusterStats.add(CommodityStats.newBuilder().setCommodityType(e.getKey())
                                .setTotalUsed(e.getValue().getKey())
                                .setTotalCapacity(e.getValue().getValue()).build());
                    });
                    placementResult.put(buyerOid, placement.slOid, new InitialPlacementFinderResult(
                            Optional.of(placement.supplier.get()),
                            Optional.ofNullable(slToClusterMap.get(placement.slOid)),
                            clusterStats, new ArrayList()));
                } else if (!placement.failureInfos.isEmpty()) { // the sl is unplaced, populate reason
                    placementResult.put(buyerOid, placement.slOid,
                            new InitialPlacementFinderResult(Optional.empty(), Optional.empty(),
                                    new ArrayList<>(), placement.failureInfos));
                    if (!placement.failureInfos.isEmpty()) {
                        logger.debug("Unplaced reservation entity id {}, sl id {} has the following"
                                + " commodities", buyerPlacement.getKey(), placement.slOid);
                        for (FailureInfo failureData : placement.failureInfos) {
                            logger.debug("commodity type {}, requested amount {}, max quantity"
                                    + " available {}, closest seller oid {}", failureData.getCommodityType(),
                                    failureData.getRequestedAmount(), failureData.getMaxQuantity(),
                                    failureData.getClosestSellerOid());
                        }
                    }
                } else { // the sl could be placed, but it has to be rolled back due to a partial
                    // success reservation.
                    placementResult.put(buyerOid, placement.slOid,
                            new InitialPlacementFinderResult(Optional.empty(), Optional.empty(),
                                    new ArrayList<>(), new ArrayList()));
                }
            }
        }
        return placementResult;
    }

    /**
     * Build the GetProvidersOfExistingReservationsResponse data structure based on existingReservations.
     * @return the constructed GetProvidersOfExistingReservationsResponse.
     */
    public GetProvidersOfExistingReservationsResponse buildGetProvidersOfExistingReservationsResponse() {
        GetProvidersOfExistingReservationsResponse.Builder response = GetProvidersOfExistingReservationsResponse
                .newBuilder();
        try {
            for (Entry<Long, List<InitialPlacementBuyer>> entry : existingReservations.entrySet()) {
                for (InitialPlacementBuyer initialPlacementBuyer : entry.getValue()) {
                    List<InitialPlacementDecision> initialPlacementDecisionList =
                            findExistingInitialPlacementDecisions(
                                    initialPlacementBuyer.getBuyerId());
                    for (InitialPlacementDecision initialPlacementDecision : initialPlacementDecisionList) {
                        InitialPlacementBuyerPlacementInfo.Builder initialPlacementBuyerPlacementInfoBuilder
                                = InitialPlacementBuyerPlacementInfo.newBuilder();
                        initialPlacementBuyerPlacementInfoBuilder.setBuyerId(initialPlacementBuyer.getBuyerId());
                        initialPlacementBuyerPlacementInfoBuilder
                                .setCommoditiesBoughtFromProviderId(initialPlacementDecision.slOid);
                        // if failure info is present setInitialPlacementFailure. If no supplier  and no
                        // failure info it means some other buyer in reservation failed. Send without
                        // InitialPlacementFailure or InitialPlacementSuccess.  Dont sen back successful
                        // buyers.
                        if (!initialPlacementDecision.failureInfos.isEmpty()) {
                            InitialPlacementFailure.Builder failureBuilder = InitialPlacementFailure.newBuilder();
                            for (FailureInfo info : initialPlacementDecision.failureInfos) {
                                CommodityType commodityType = info.getCommodityType();
                                UnplacementReason reason = UnplacementReason.newBuilder()
                                        .addFailedResources(FailedResources.newBuilder().setCommType(commodityType)
                                                .setRequestedAmount(info.getRequestedAmount())
                                                .setMaxAvailable(info.getMaxQuantity()).build())
                                        .setClosestSeller(info.getClosestSellerOid())
                                        .build();
                                failureBuilder.addUnplacedReason(reason);
                            }
                            initialPlacementBuyerPlacementInfoBuilder.setInitialPlacementFailure(failureBuilder);
                            response.addInitialPlacementBuyerPlacementInfo(initialPlacementBuyerPlacementInfoBuilder);
                        } else if (!initialPlacementDecision.supplier.isPresent()) {
                            response.addInitialPlacementBuyerPlacementInfo(initialPlacementBuyerPlacementInfoBuilder);
                        }
                    }
                }

            }
        } catch (Exception e) {
            logger.error("Failed to build GetProvidersOfExistingReservationsResponse with"
                    + " exception {} ", e);
        }
        return response.build();
    }
}

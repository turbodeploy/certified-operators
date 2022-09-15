package com.vmturbo.market.reservations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.market.InitialPlacement.FindInitialPlacementRequest;
import com.vmturbo.common.protobuf.market.InitialPlacement.GetProvidersOfExistingReservationsResponse;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer.InitialPlacementCommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyerPlacementInfo;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementDTO;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementFailure;
import com.vmturbo.common.protobuf.market.InitialPlacement.InvalidInfo;
import com.vmturbo.common.protobuf.market.InitialPlacement.InvalidInfo.MarketNotReady;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetExistingReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetExistingReservationsResponse;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.UnplacementReason;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.UnplacementReason.FailedResources;
import com.vmturbo.components.api.RetriableOperation;
import com.vmturbo.components.api.RetriableOperation.RetriableOperationFailedException;
import com.vmturbo.market.diagnostics.AnalysisDiagnosticsCollector;
import com.vmturbo.market.diagnostics.AnalysisDiagnosticsCollector.AnalysisDiagnosticsCollectorFactory;
import com.vmturbo.market.diagnostics.AnalysisDiagnosticsCollector.AnalysisMode;
import com.vmturbo.market.reservations.InitialPlacementFinderResult.FailureInfo;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * The class to support fast reservation placement.
 */
public class InitialPlacementFinder {

    // Whether the reservation cache should be constructed or not.
    private boolean prepareReservationCache;
    // the object to hold economy caches
    @VisibleForTesting
    protected EconomyCaches economyCaches;
    // a map to keep track of existing reservation ids and the buyers within each reservation. The order
    // is preserved so that replay is following the order as reservations were added.
    @VisibleForTesting
    protected Map<Long, InitialPlacementDTO> existingReservations = new LinkedHashMap<>();
    // A map of reservation buyer oid to its placement result.
    @VisibleForTesting
    protected Map<Long, List<InitialPlacementDecision>> buyerPlacements = new HashMap<>();
    // The number diags to retain on placement generation errors
    private int numPlacementDiagsToRetain;
    // The max number of retry if findInitialPlacement failed
    private static int maxRetry;
    // The max number of attempts to fit all buyers of a reservation within a certain grouping.
    private int maxGroupingRetry;
    // A grpc blocking service stub
    private ReservationServiceBlockingStub blockingStub;
    // Logger
    private static final Logger logger = LogManager.getLogger();
    /**
     * prefix for initial placement log messages.
     */
    protected final String logPrefix = "FindInitialPlacement: ";

    private final boolean enableOP;

    private final AnalysisDiagnosticsCollectorFactory analysisDiagnosticsCollectorFactory;

    public EconomyCaches getEconomyCaches() {
        return economyCaches;
    }

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
     *
     * @param dsl the data base context.
     * @param stub reservation rpc service blocking stub.
     * @param prepareReservationCache whether economy caches should be built.
     * @param maxRetry The max number of retry if findInitialPlacement failed.
     * @param maxGroupingRetry The max number of attempts to fit all buyers of a reservation
     *          within a certain grouping.
     * @param analysisDiagnosticsCollectorFactory is the factory used for saving diags.
     */
    public InitialPlacementFinder(@Nonnull DSLContext dsl,
            @Nonnull final ReservationServiceBlockingStub stub,
            final boolean prepareReservationCache, int maxRetry, final int maxGroupingRetry,
            AnalysisDiagnosticsCollectorFactory analysisDiagnosticsCollectorFactory,
            int numPlacementDiagsToRetain, boolean enableOP) {
        economyCaches = new EconomyCaches(dsl);
        this.blockingStub = stub;
        this.prepareReservationCache = prepareReservationCache;
        this.maxRetry = maxRetry;
        this.maxGroupingRetry = maxGroupingRetry;
        this.analysisDiagnosticsCollectorFactory = analysisDiagnosticsCollectorFactory;
        this.numPlacementDiagsToRetain = numPlacementDiagsToRetain;
        this.enableOP = enableOP;
    }

    /**
     * Loads the historical economy cache from database. Fetch the latest reservation decisions
     * from plan orchestrator. Reconstruct the economy cache with latest reservations.
     *
     * @param maxRequestTimeout the max retrying time that is allowed to query plan orchestrator.
     */
    public void restoreEconomyCaches(final long maxRequestTimeout) {
        // If the reservation feature is enabled, then load data from table.
        if (shouldConstructEconomyCache()) {
            // Load the BLOB persisted in table and build an economy with reservation buyers,
            // hosts and storages.
            economyCaches.loadHistoricalEconomyCache();
            // Get the list reservation from plan orchestrator.
            queryExistingReservations(maxRequestTimeout);
            if (economyCaches.getState().isReservationReceived()
                    && economyCaches.getState().isHistoricalCacheReceived()) {
                // Clear all previous reservations and apply the latest reservation buyers
                // coming from plan orchestrator.
                economyCaches.restoreHistoricalEconomyCache(buyerPlacements, existingReservations);
            }
        }
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
     * For all the exisiting reservation update the id of cluster provider.
     * @param buyerOidToPlacement the existing reservations
     * @param hostIdToClusterId the map frm host oid to cluster oid.
     */
    private void updateClusterIdOfExistingReservation(@Nonnull final Map<Long, List<InitialPlacementDecision>> buyerOidToPlacement,
            Map<Long, Long> hostIdToClusterId) {
        // update the  id of the clustersl supplier
        for (List<InitialPlacementDecision> placementDecisions : buyerOidToPlacement.values()) {
            Optional<InitialPlacementDecision> clusterPlacementDecision = placementDecisions.stream()
                    .filter(p -> p.supplier.isPresent() && p.providerType.isPresent()
                            && p.providerType.get() == EntityType.CLUSTER_VALUE).findFirst();
            Optional<InitialPlacementDecision> hostPlacementDecision = placementDecisions.stream()
                    .filter(p -> p.supplier.isPresent() && p.providerType.isPresent()
                            && p.providerType.get() == CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE_VALUE).findFirst();
            if (hostPlacementDecision.isPresent() && clusterPlacementDecision.isPresent()) {
                Long newClusterId = hostIdToClusterId.get(hostPlacementDecision.get().supplier.get());
                if (newClusterId != null) {
                    placementDecisions.remove(clusterPlacementDecision.get());
                    placementDecisions.add(new InitialPlacementDecision(
                            clusterPlacementDecision.get().slOid,
                            Optional.of(newClusterId),
                            clusterPlacementDecision.get().failureInfos,
                            clusterPlacementDecision.get().invalidConstraints,
                            clusterPlacementDecision.get().isFailedInRealtimeCache,
                            clusterPlacementDecision.get().providerType
                    ));
                }
            }
        }

    }

    /**
     * Triggers the update of economy caches.
     *
     * @param clonedEconomy the economy to be used as model updation.
     * @param commTypeToSpecMap the commodity type to commodity specification mapping.
     * @param isRealtime true if to update the real time cached economy, false if to update the
     * historical cached economy.
     */
    public void updateCachedEconomy(@Nonnull final Economy clonedEconomy,
            @Nonnull final Map<CommodityType, Integer> commTypeToSpecMap, final boolean isRealtime,
            Map<Long, Long> hostIdToClusterId) {
        if (enableOP) {
            updateClusterIdOfExistingReservation(buyerPlacements, hostIdToClusterId);
        } else {
            InitialPlacementUtils.deleteClusterEntitiesFromEconomy(clonedEconomy);
        }
        if (isRealtime) {
            // Update the providers with the latest broadcast in real time economy cache,
            // Apply all successfully placed reservations to the same providers that were
            // recorded in buyerPlacements.
            economyCaches.updateRealtimeCachedEconomy(clonedEconomy, commTypeToSpecMap,
                        buyerPlacements, existingReservations);
        } else {
            // Update the providers with providers generated in headroom plan in historical economy
            // cache. Rerun all successfully placed reservations and update the providers in
            // buyerPlacements.
            buyerPlacements = economyCaches.updateHistoricalCachedEconomy(clonedEconomy, commTypeToSpecMap,
                    buyerPlacements, existingReservations);
        }
    }

    /**
     * Remove buyers from existingReservations and buyerPlacements. Remove corresponding traders
     * from both economy caches.
     *
     * @param deleteBuyerOids the list of reservation buyer oids.
     * @param deployed if true the vm is just deployed. don't delete from historical.
     * @return true if removal from the cached economy completes.
     */
    public boolean buyersToBeDeleted(@Nonnull List<Long> deleteBuyerOids, boolean deployed) {
        if (deployed) {
            updateDeployedReservations(deleteBuyerOids);
        }
        logger.info(logPrefix + "Prepare to delete reservation entities {} from {} cached economies",
                deleteBuyerOids, deployed ? "realtime" : "both");
        Set<Long> reservationsToRemove = new HashSet<>();
        for (Map.Entry<Long, InitialPlacementDTO> entry : existingReservations.entrySet()) {
            Set<Long> existingBuyerOids = entry.getValue().getInitialPlacementBuyerList().stream()
                    .map(InitialPlacementBuyer::getBuyerId).collect(Collectors.toSet());
            existingBuyerOids.removeAll(deleteBuyerOids);
            if (existingBuyerOids.isEmpty()) {
                reservationsToRemove.add(entry.getKey());
            }
        }
        // Remove reservation buyers grouped by reservation oid one by one.
        for (Long oid : reservationsToRemove) {
            List<InitialPlacementBuyer> buyersToRemove = existingReservations.get(oid).getInitialPlacementBuyerList();
            Set<Long> buyerOids = buyersToRemove.stream()
                    .map(InitialPlacementBuyer::getBuyerId)
                    .collect(Collectors.toSet());
            try {
                economyCaches.clearDeletedBuyersFromCache(buyerOids, deployed);
                if (!deployed) {
                    buyerPlacements.keySet().removeAll(buyerOids);
                    existingReservations.remove(oid);
                }
                logger.info(logPrefix + "Reservation {} is successfully remove with {} entities.", oid,
                        buyerOids.size());
            } catch (Exception exception) {
                // In case any reservation trader failed to be cleared from economy, ask user wait for
                // both historical cache and realtime cache updated.
                economyCaches.getState().setHistoricalCacheReceived(false);
                economyCaches.getState().setRealtimeCacheReceived(false);
                logger.warn(logPrefix + "Setting economy caches state to NOT READY. Wait for 24 hours to run"
                        + " other reservation requests.");
                logger.error(logPrefix + "Reservation {} can not be remove with {} entities due to {}.",
                        oid, buyerOids.size(), exception);
            }
        }
        return true;
    }

    /**
     * Update Reservations that are deployed.
     *
     * @param deleteBuyerOids buyers that need to be modified.
     */
    private void updateDeployedReservations(@Nonnull List<Long> deleteBuyerOids) {
        for (Map.Entry<Long, InitialPlacementDTO> entry : existingReservations.entrySet()) {
            InitialPlacementDTO initialPlacement = entry.getValue();
            List<InitialPlacementBuyer> modifiedBuyers = new ArrayList<>();
            for (InitialPlacementBuyer buyer : entry.getValue().getInitialPlacementBuyerList()) {
                InitialPlacementBuyer.Builder modifiedBuyer = buyer.toBuilder();
                if (deleteBuyerOids.contains(buyer.getBuyerId())) {
                    modifiedBuyer.setDeployed(true);
                }
                modifiedBuyers.add(modifiedBuyer.build());
            }
            InitialPlacementDTO findInitialPlacementNew = InitialPlacementDTO.newBuilder()
                .addAllInitialPlacementBuyer(modifiedBuyers)
                .setReservationMode(initialPlacement.getReservationMode())
                .setReservationGrouping(initialPlacement.getReservationGrouping())
                .setId(initialPlacement.getId()).build();
            existingReservations.put(entry.getKey(), findInitialPlacementNew);
        }
    }

    /**
     * Find initial placement for a given list of reservation entities.
     *
     * @param request the request for initial placement.
     * @return a table whose row is reservation entity oid, column is shopping list oid and value
     * is the {@link InitialPlacementFinderResult}
     */
    @Nonnull
    public Table<Long, Long, InitialPlacementFinderResult> findPlacement(
            @Nonnull final FindInitialPlacementRequest request) {
        List<InitialPlacementBuyer> allBuyers = request.getInitialPlacementList().stream()
            .map(InitialPlacementDTO::getInitialPlacementBuyerList)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
        if (allBuyers.isEmpty()) {
            return HashBasedTable.create();
        }
        Map<Long, List<InitialPlacementDecision>> initialPlacements = new HashMap<>();
        // A map to keep track of reservation buyer's shopping list oid to its cluster mapping.
        Map<Long, CommodityType> slToClusterMap = new HashMap<>();
        if (!economyCaches.getState().isEconomyReady()) {
            logger.warn(logPrefix + "Market is not ready to run reservation yet, wait for another broadcast to retry");
            return buildMarketNotReadyResults(allBuyers);
        }
        // Find providers for buyers via running placements in economy caches. Keep track of
        // placement results in buyerPlacements.
        try {
            // Save the diagnostics if debug is turned on, before placements are created so the
            // economy isn't corrupted
            saveInitialPlacementDiags(request.getInitialPlacementList(), false);
        } catch (Exception e) {
            logger.error("Error when attempting to save InitialPlacement diags", e);
        }
        for (InitialPlacementDTO buyersPerReservation: request.getInitialPlacementList()) {
            findPlacement(buyersPerReservation, initialPlacements, slToClusterMap);
        }
        // process reservation result from sl to provider mapping
        return buildReservationResponse(initialPlacements, slToClusterMap, request.getInitialPlacementList());
    }

    /**
     * Find placement for reservations.
     *
     * @param initialPlacementDTO the InitialPlacement objects per reservation.
     * @param initialPlacements the initialPlacement decisions.
     * @param slToClusterMap the mapping of sl to cluster.
     */
    private void findPlacement(@Nonnull InitialPlacementDTO initialPlacementDTO,
        @Nonnull Map<Long, List<InitialPlacementDecision>> initialPlacements,
        @Nonnull Map<Long, CommodityType> slToClusterMap) {
        Set<Long> buyersInOneRes = initialPlacementDTO
                .getInitialPlacementBuyerList().stream().map(i -> i.getBuyerId())
                    .collect(Collectors.toSet());
        boolean hasErrorOccured = false;
        try {
            Map<Long, List<InitialPlacementDecision>> initialPlacementPerReservation
                = economyCaches.findInitialPlacement(initialPlacementDTO
                    .getInitialPlacementBuyerList(), slToClusterMap, maxRetry,
                    initialPlacementDTO.getReservationMode(),
                    initialPlacementDTO.getReservationGrouping(),
                    maxGroupingRetry, initialPlacementDTO.getProvidersList());
            buyerPlacements.putAll(initialPlacementPerReservation);
            // Keep incoming reservation buyers in the existingReservations map
            existingReservations.put(initialPlacementDTO.getId(),
                    initialPlacementDTO);
            initialPlacements.putAll(initialPlacementPerReservation);

            //save diags on if returned initialPlacementPerReservation has any placement errors
            for (Map.Entry<Long, List<InitialPlacementDecision>> buyerPlacement : initialPlacementPerReservation.entrySet()) {
                List<InitialPlacementDecision> placements = buyerPlacement.getValue();
                for (InitialPlacementDecision placement : placements) {
                    if (!placement.failureInfos.isEmpty()) {
                        hasErrorOccured = true;
                    }
                }
            }
            try {
                // If the AnalysisDiagnosticsCollector we would have saved the diags in the
                // beginning of market run itself. no need to save it again. Also if the
                // AnalysisDiagnosticsCollector is disabled we have to save the diags only when an
                // error has occured.
                if (hasErrorOccured && !AnalysisDiagnosticsCollector.isEnabled()) {
                    saveInitialPlacementDiags(Collections.singletonList(initialPlacementDTO),
                            hasErrorOccured);
                }
            } catch (Exception e) {
                logger.error("Error when attempting to save InitialPlacement diags", e);
            }
        } catch (Exception exception) {
            // Any request that encounters an exception should fail all the buyers in the
            // same reservation.
            logger.error(logPrefix + "Find placement failed for reservation {} containing buyers {} with"
                    + " exception {} ", initialPlacementDTO.getId(), buyersInOneRes, exception);
            // Make sure no buyers in the failed reservation are added into existingReservations
            // or buyerPlacements and no such buyers exist in both economy caches.
            existingReservations.remove(initialPlacementDTO.getId());
            buyersInOneRes.stream().forEach(buyerOid -> buyerPlacements.remove(buyerOid));
            economyCaches.clearDeletedBuyersFromCache(buyersInOneRes, false);
            initialPlacementDTO.getInitialPlacementBuyerList().stream()
                    .map(buyer -> buyer.getInitialPlacementCommoditiesBoughtFromProviderList())
                    .flatMap(List::stream)
                    .forEach(sl -> slToClusterMap.remove(sl.getCommoditiesBoughtFromProviderId()));
            // Create empty InitialPlacementDecision for each buyer in the reservation that
            // encounter exception.
            List<InitialPlacementDecision> emptyDecisions = new ArrayList<>();
            initialPlacementDTO.getInitialPlacementBuyerList().forEach(b -> {
                b.getInitialPlacementCommoditiesBoughtFromProviderList().stream()
                        .map(sl -> sl.getCommoditiesBoughtFromProviderId())
                        .forEach(id -> emptyDecisions.add(new InitialPlacementDecision(
                                b.getBuyerId(), Optional.empty(), new ArrayList<>(), Optional.empty(), false, Optional.empty())));
                initialPlacements.put(b.getBuyerId(), emptyDecisions);
            });
        }
    }

    private Table<Long, Long, InitialPlacementFinderResult> buildMarketNotReadyResults(List<InitialPlacementBuyer> allBuyers) {
        Table<Long, Long, InitialPlacementFinderResult> invalidResult = HashBasedTable.create();
        InvalidInfo invalidInfo = InvalidInfo.newBuilder().setMarketNotReady(MarketNotReady.getDefaultInstance()).build();
        for (InitialPlacementBuyer buyer : allBuyers) {
            for (InitialPlacementCommoditiesBoughtFromProvider commBoughtGrouping
                    : buyer.getInitialPlacementCommoditiesBoughtFromProviderList()) {
                invalidResult.put(buyer.getBuyerId(), commBoughtGrouping.getCommoditiesBoughtFromProviderId(),
                        new InitialPlacementFinderResult(Optional.empty(), Optional.empty(), new ArrayList<>(), Optional.of(invalidInfo)));
            }
        }
        return invalidResult;
    }

    /**
     * Build reservation response for a set of reservation buyers.
     *
     * @param reservationPlacements a map of reservation buyer oid to its placement decisions.
     * @param slToClusterMap a map of shopping list oid to cluster commodity type.
     * @return a table whose row is reservation entity oid, column is shopping list oid and value
     * is the {@link InitialPlacementFinderResult}
     */
    private Table<Long, Long, InitialPlacementFinderResult> buildReservationResponse(
            @Nonnull final Map<Long, List<InitialPlacementDecision>> reservationPlacements,
            @Nonnull final Map<Long, CommodityType> slToClusterMap,
            @Nonnull final List<InitialPlacementDTO> initialPlacementDTOs) {
        Table<Long, Long, InitialPlacementFinderResult> placementResult = HashBasedTable.create();
        for (Map.Entry<Long, List<InitialPlacementDecision>> buyerPlacement
                : reservationPlacements.entrySet()) {
            long buyerOid = buyerPlacement.getKey();
            List<InitialPlacementDecision> placements = buyerPlacement.getValue();
            for (InitialPlacementDecision placement : placements) {
                if (placement.supplier.isPresent()) { // the sl is successfully placed
                    placementResult.put(buyerOid, placement.slOid, new InitialPlacementFinderResult(
                            Optional.of(placement.supplier.get()),
                            Optional.ofNullable(slToClusterMap.get(placement.slOid)),
                            new ArrayList<>(), Optional.empty()));
                } else if (!placement.failureInfos.isEmpty()) { // the sl is unplaced, populate reason
                    placementResult.put(buyerOid, placement.slOid,
                            new InitialPlacementFinderResult(Optional.empty(), Optional.empty(),
                                    placement.failureInfos, Optional.empty()));
                    if (!placement.failureInfos.isEmpty()) {
                        logger.debug(logPrefix + "Unplaced reservation entity id {}, sl id {} has the following"
                                + " commodities", buyerPlacement.getKey(), placement.slOid);
                        for (FailureInfo failureData : placement.failureInfos) {
                            logger.debug(logPrefix + "commodity type {}, requested amount {}, max quantity"
                                    + " available {}, closest seller oid {}, closest seller cluster oid list {}", failureData.getCommodityType(),
                                    failureData.getRequestedAmount(), failureData.getMaxQuantity(),
                                    failureData.getClosestSellerOid(), failureData.getClosestSellerCluster().toString());
                        }
                    }
                } else { // the sl could be placed, but it has to be rolled back due to a partial
                    // success reservation.
                    placementResult.put(buyerOid, placement.slOid,
                            new InitialPlacementFinderResult(Optional.empty(), Optional.empty(),
                                    new ArrayList<>(),
                                    placement.invalidConstraints.isPresent()
                                            ? Optional.of(InvalidInfo.newBuilder().setInvalidConstraints(placement.invalidConstraints.get()).build())
                                            : Optional.empty()));
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
            for (Entry<Long, InitialPlacementDTO> entry : existingReservations.entrySet()) {
                for (InitialPlacementBuyer initialPlacementBuyer : entry.getValue().getInitialPlacementBuyerList()) {
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
            logger.error(logPrefix + "Failed to build GetProvidersOfExistingReservationsResponse with"
                    + " exception {} ", e);
        }
        return response.build();
    }

    /**
     * Fetch the existing reservations from plan orchestrator.
     *
     * @param timeOut the max timeout allowed to retry the query.
     */
    public void queryExistingReservations(final long timeOut) {
        if (economyCaches.getState().isReservationReceived()) {
            return;
        }
        GetExistingReservationsRequest request =
                GetExistingReservationsRequest.newBuilder().build();
        try {
            logger.info(logPrefix + "Trying to get a list of existing reservation buyers from"
                    + " plan orchestrator.");
            GetExistingReservationsResponse response = RetriableOperation
                    .newOperation(() -> blockingStub.getExistingReservations(request))
                    .retryOnException(e -> e instanceof StatusRuntimeException)
                    .backoffStrategy(curTry -> 120000) // wait 2 min between retries
                    .run(timeOut, TimeUnit.SECONDS);
            populateExistingReservations(response.getInitialPlacementList());
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                // Reset interrupt status.
                Thread.currentThread().interrupt();
                logger.error(logPrefix + "Trying to fetch the reservations from plan orchestrator but"
                                + " thread is interrupted", e);
            } else if (e instanceof RetriableOperationFailedException | e instanceof TimeoutException) {
                logger.error(logPrefix + "Trying to fetch the reservations from plan orchestrator but"
                                + " grpc call failed  with multiple retries", e);
            } else if (e instanceof StatusRuntimeException) {
                logger.error(logPrefix + "Trying to fetch the reservations from plan orchestrator"
                        + " but grpc call failed  with status error {}", ((StatusRuntimeException)e).getStatus());
            } else {
                logger.error(logPrefix + "Trying to fetch the reservations from plan orchestrator for {}"
                                + " minutes but still failed. Please make sure the plan orchestrator is up and"
                                + " running.", timeOut, e);
            }
        }
    }

    /**
     * Populate the reservation buyers and keep them in existingReservations and buyerPlacements
     * maps.
     *
     * @param initialPlacements a list of {@link InitialPlacementDTO}s.
     */
    private void populateExistingReservations(List<InitialPlacementDTO> initialPlacements) {
        // Populate existing reservation buyers received from PO into in memory data structures
        initialPlacements.forEach(initialPlacement -> {
            initialPlacement.getInitialPlacementBuyerList().forEach(buyer -> {
                List<InitialPlacementDecision> decisions = new ArrayList<>();
                buyer.getInitialPlacementCommoditiesBoughtFromProviderList().forEach(sl -> {
                    long slOid = sl.getCommoditiesBoughtFromProviderId();
                    Optional<Long> supplier = sl.getCommoditiesBoughtFromProvider().hasProviderId()
                            ? Optional.of(sl.getCommoditiesBoughtFromProvider().getProviderId())
                            : Optional.empty();
                    Optional<Integer> providerType = sl.getCommoditiesBoughtFromProvider().hasProviderId()
                            ? Optional.of(sl.getCommoditiesBoughtFromProvider().getProviderEntityType())
                            : Optional.empty();
                    decisions.add(new InitialPlacementDecision(slOid, supplier, new ArrayList<>(), Optional.empty(), false,
                            providerType));
                });
                buyerPlacements.put(buyer.getBuyerId(), decisions);
                if (!existingReservations.containsKey(initialPlacement.getId())) {
                    existingReservations.put(buyer.getReservationId(), initialPlacement);
                }
            });
        });
        logger.info(logPrefix + "Existing reservations are: reservation ids {}", existingReservations.keySet());
        // Set state to ready once reservations are received from PO and real time economy is ready.
        economyCaches.getState().setReservationReceived(true);
        logger.info(logPrefix + "Economy caches state is set to RESERVATION_RECEIVED");
    }

    /**
     * Update the historicalCacheReceived flag to false.
     */
    public void resetHistoricalCacheReceived() {
        economyCaches.getState().setHistoricalCacheReceived(false);
    }

    /**
     * Save the economy stats if the AnalysisDiagnosticsCollector is set to DEBUG mode.
     * @param initialPlacements the current mapping of initialPlacements.
     */
    private void saveInitialPlacementDiags(List<InitialPlacementDTO> initialPlacements, boolean hasErrorOccured) {
        String now = String.valueOf((new Date()).getTime());
        analysisDiagnosticsCollectorFactory.newDiagsCollector(now, AnalysisMode.INITIAL_PLACEMENT)
                .ifPresent(diagsCollector -> {
                    diagsCollector.saveInitialPlacementDiagsIfEnabled(now,
                            economyCaches.getHistoricalCachedCommTypeMap(),
                            economyCaches.getRealtimeCachedCommTypeMap(),
                            initialPlacements,
                            economyCaches.getHistoricalCachedEconomy(),
                            economyCaches.getRealtimeCachedEconomy(), hasErrorOccured,
                            numPlacementDiagsToRetain);
                });
    }
}

package com.vmturbo.market.reservations;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.market.InitialPlacement.FindInitialPlacementRequest;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.market.diagnostics.AnalysisDiagnosticsCollector.AnalysisDiagnosticsCollectorFactory;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;

/**
 * The class to support fast reservation placement.
 */
public class InitialPlacementHandler {

    /*
     * Executor service containing a single thread that handles access to economy cache.
     */
    final ExecutorService cacheAccessService;

    // Logger
    private static final Logger logger = LogManager.getLogger();

    @VisibleForTesting
    protected InitialPlacementFinder placementFinder;

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
    public InitialPlacementHandler(@Nonnull DSLContext dsl,
            @Nonnull final ReservationServiceBlockingStub stub,
            final boolean prepareReservationCache, int maxRetry, final int maxGroupingRetry,
            AnalysisDiagnosticsCollectorFactory analysisDiagnosticsCollectorFactory) {
        this.placementFinder = new InitialPlacementFinder(dsl, stub,
            prepareReservationCache, maxRetry, maxGroupingRetry, analysisDiagnosticsCollectorFactory);
        this.cacheAccessService = Executors.newFixedThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("Economy-cache-accessor").build());
    }

    /**
     * Loads the historical economy cache from database. Fetch the latest reservation decisions
     * from plan orchestrator. Reconstruct the economy cache with latest reservations.
     *
     * @param maxRequestTimeout the max retrying time that is allowed to query plan orchestrator.
     * @return future indicating completion of cache restoration.
     */
    public Future<?> restoreEconomyCaches(final long maxRequestTimeout) {
        return cacheAccessService.submit(() -> placementFinder.restoreEconomyCaches(maxRequestTimeout));
    }

    /**
     * Triggers the update of economy caches.
     *
     * @param originalEconomy the economy to be used as model for clone.
     * @param commTypeToSpecMap the commodity type to commodity specification mapping.
     * @param isRealtime true if to update the real time cached economy, false if to update the
     * historical cached economy.
     * @return future indicating completion of cache updation.
     */
    public Future<?> updateCachedEconomy(@Nonnull final UnmodifiableEconomy originalEconomy,
            @Nonnull final Map<CommodityType, Integer> commTypeToSpecMap, final boolean isRealtime) {
        return cacheAccessService.submit(() ->
            placementFinder.updateCachedEconomy(originalEconomy, commTypeToSpecMap, isRealtime));
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
        Future buyerDeletionFuture = cacheAccessService.submit(() ->
                placementFinder.buyersToBeDeleted(deleteBuyerOids, deployed));
        try {
            return (boolean)buyerDeletionFuture.get();
        } catch (ExecutionException e) {
            logger.error(placementFinder.logPrefix
                    + "Unable to delete buyers in cache due to ExecutionException.", e);
            return false;
        } catch (InterruptedException e) {
            logger.error(placementFinder.logPrefix
                    + "Unable to delete buyers in cache due to InterruptedException.", e);
            return false;
        }
    }

    /**
     * Find initial placement for a given list of reservation entities.
     *
     * @param request the request for initial placement.
     * @return a table whose row is reservation entity oid, column is shopping list oid and value
     * is the {@link InitialPlacementFinderResult}
     * @throws InterruptedException        if we're interrupted
     * @throws ExecutionException          if failure in asynchronous processing
     */
    @Nonnull
    public Table<Long, Long, InitialPlacementFinderResult> findPlacement(
            @Nonnull final FindInitialPlacementRequest request) throws ExecutionException, InterruptedException {
        return cacheAccessService.submit(() ->
                placementFinder.findPlacement(request)).get();
    }

    /**
     * Update the historicalCacheReceived flag to false.
     */
    public void resetHistoricalCacheReceived() {
        cacheAccessService.submit(() ->
                placementFinder.resetHistoricalCacheReceived());
    }

    public InitialPlacementFinder getPlacementFinder() {
        return placementFinder;
    }

}

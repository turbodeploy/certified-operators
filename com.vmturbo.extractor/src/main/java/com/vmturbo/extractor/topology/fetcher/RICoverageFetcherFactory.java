package com.vmturbo.extractor.topology.fetcher;

import java.util.function.Consumer;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.longs.Long2FloatOpenHashMap;

import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub;
import com.vmturbo.components.common.utils.MultiStageTimer;

/**
 * Factory class for the {@link RICoverageFetcher}.
 */
public class RICoverageFetcherFactory {

    private final ReservedInstanceUtilizationCoverageServiceBlockingStub riCoverageService;

    /**
     * Constructor.
     *
     * @param riCoverageService To access the RI Coverage RPC.
     */
    public RICoverageFetcherFactory(@Nonnull final ReservedInstanceUtilizationCoverageServiceBlockingStub riCoverageService) {
        this.riCoverageService = riCoverageService;
    }

    /**
     * Create a new fetcher for fetching current RI coverage for all entities in real time topology.
     *
     * @param timer timer
     * @param consumer consumer for the resulting {@link RICoverageData}.
     * @return The {@link RICoverageFetcher}.
     */
    @Nonnull
    public RICoverageFetcher newCurrentRiCoverageFetcher(MultiStageTimer timer, Consumer<RICoverageData> consumer) {
        return new RICoverageFetcher(timer, consumer, riCoverageService, false, null);
    }

    /**
     * Create a new fetcher for fetching projected RI coverage for all entities in the given
     * topology.
     *
     * @param timer timer
     * @param consumer consumer for the resulting {@link RICoverageData}.
     * @param topologyContextId id of the topology context, like real time or plan topology
     * @return The {@link RICoverageFetcher}.
     */
    public RICoverageFetcher newProjectedRiCoverageFetcher(MultiStageTimer timer, Consumer<RICoverageData> consumer,
            long topologyContextId) {
        return new RICoverageFetcher(timer, consumer, riCoverageService, true, topologyContextId);
    }

    /**
     * Utility object to make caching and using the RI Coverage data easier.
     */
    public static class RICoverageData {
        private final Long2FloatOpenHashMap riCoveragePercentageByEntity = new Long2FloatOpenHashMap();

        /**
         * Add RI Coverage percentage for given entity.
         *
         * @param entityId id of the entity
         * @param riCoveragePercentage ri coverage percentage
         */
        void addRICoveragePercentage(final long entityId, final float riCoveragePercentage) {
            riCoveragePercentageByEntity.put(entityId, riCoveragePercentage);
        }

        /**
         * Get the RI coverage percentage for given entity.
         *
         * @param entityId id of the entity
         * @return RI coverage percentage, or -1 if not available
         */
        public float getRiCoveragePercentage(long entityId) {
            return riCoveragePercentageByEntity.getOrDefault(entityId, -1);
        }

        /**
         * Number of entities with RI Coverage data.
         *
         * @return number of entities
         */
        public int size() {
            return riCoveragePercentageByEntity.size();
        }
    }
}

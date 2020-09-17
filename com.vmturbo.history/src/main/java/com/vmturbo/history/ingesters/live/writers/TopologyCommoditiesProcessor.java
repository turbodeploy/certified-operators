package com.vmturbo.history.ingesters.live.writers;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.IChunkProcessor;
import com.vmturbo.history.ingesters.common.writers.ProjectedTopologyWriterBase;
import com.vmturbo.history.ingesters.live.ProjectedRealtimeTopologyIngester;
import com.vmturbo.history.stats.projected.ProjectedStatsStore;
import com.vmturbo.history.stats.projected.TopologyCommoditiesSnapshot;
import com.vmturbo.history.stats.projected.TopologyCommoditiesSnapshot.Builder;

/**
 * {@link TopologyCommoditiesProcessor} updates the in-memory {@link ProjectedStatsStore}.
 *
 * <p>It is not a "writer" in the sense of writing anything to a database, but it's one of the
 * "writers" configured for {@link ProjectedRealtimeTopologyIngester}.
 * </p>
 */
public class TopologyCommoditiesProcessor extends ProjectedTopologyWriterBase {

    private final ProjectedStatsStore projectedStatsStore;

    private final Map<Long, Double> projectedPriceIndexByEntity = new HashMap<>();
    private final Builder topologyCommoditiesSnapshotBuilder;

    /**
     * Create a new instance.
     *
     * @param projectedStatsStore projected stats store to update
     */
    private TopologyCommoditiesProcessor(@Nonnull ProjectedStatsStore projectedStatsStore) {
        this.projectedStatsStore = projectedStatsStore;
        this.topologyCommoditiesSnapshotBuilder = new TopologyCommoditiesSnapshot.Builder(projectedStatsStore.getExcludedCommodities());
    }

    @Override
    public ChunkDisposition processChunk(@Nonnull final Collection<ProjectedTopologyEntity> chunk,
                                         @Nonnull final String infoSummary) {
        chunk.forEach(topologyCommoditiesSnapshotBuilder::addProjectedEntity);
        return ChunkDisposition.SUCCESS;
    }

    @Override
    public void finish(int entityCount, final boolean expedite, final String infoSummary) {
        projectedStatsStore.updateProjectedTopology(topologyCommoditiesSnapshotBuilder);
    }

    /**
     * Factory to create new writer instances.
     */
    public static class Factory extends ProjectedTopologyWriterBase.Factory {

        private final ProjectedStatsStore projectedStatsStore;

        /**
         * Create a new factory instance.
         *
         * @param projectedStatsStore projected stats store
         */
        public Factory(@Nonnull final ProjectedStatsStore projectedStatsStore) {
            this.projectedStatsStore = projectedStatsStore;
        }

        @Override
        public Optional<IChunkProcessor<ProjectedTopologyEntity>>
        getChunkProcessor(@Nonnull final TopologyInfo topologyInfo,
                          @Nonnull final SimpleBulkLoaderFactory loaders) {
            return Optional.of(new TopologyCommoditiesProcessor(projectedStatsStore));
        }
    }
}

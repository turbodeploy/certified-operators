package com.vmturbo.history.ingesters.live.writers;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.DataPacks.DataPack;
import com.vmturbo.components.common.utils.DataPacks.IDataPack;
import com.vmturbo.components.common.utils.MemReporter;
import com.vmturbo.history.ingesters.common.IChunkProcessor;
import com.vmturbo.history.ingesters.common.TopologyIngesterBase.IngesterState;
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
public class TopologyCommoditiesProcessor extends ProjectedTopologyWriterBase implements MemReporter {

    private final ProjectedStatsStore projectedStatsStore;

    private final Map<Long, Double> projectedPriceIndexByEntity = new HashMap<>();
    private final Builder topologyCommoditiesSnapshotBuilder;
    private final IngesterState state;

    /**
     * Create a new instance.
     *
     * @param projectedStatsStore projected stats store to update
     * @param state               shared ingester state
     */
    private TopologyCommoditiesProcessor(@Nonnull ProjectedStatsStore projectedStatsStore, IngesterState state) {
        super();
        this.projectedStatsStore = projectedStatsStore;
        IDataPack<String> commodityNamePack = new DataPack<>();
        this.state = state;
        this.topologyCommoditiesSnapshotBuilder = new TopologyCommoditiesSnapshot.Builder(
                projectedStatsStore.getExcludedCommodities(), commodityNamePack,
                state.getOidPack(), state.getKeyPack());
    }

    @Override
    public ChunkDisposition processEntities(@Nonnull final Collection<ProjectedTopologyEntity> chunk,
                                         @Nonnull final String infoSummary) {
        chunk.forEach(topologyCommoditiesSnapshotBuilder::addProjectedEntity);
        return ChunkDisposition.SUCCESS;
    }

    @Override
    public void finish(int entityCount, final boolean expedite, final String infoSummary) {
        state.getOidPack().freeze(false); // we still need to look up long oids supplied b API
        state.getKeyPack().freeze(true);
        projectedStatsStore.updateProjectedTopology(topologyCommoditiesSnapshotBuilder,
                state.getOidPack(), state.getKeyPack());
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
                          @Nonnull final IngesterState state) {
            return Optional.of(new TopologyCommoditiesProcessor(projectedStatsStore, state));
        }
    }

    @Override
    public Long getMemSize() {
        return null;
    }

    @Override
    public List<MemReporter> getNestedMemReporters() {
        return Arrays.asList(
                new SimpleMemReporter("projectedPriceIndexByEntity", projectedPriceIndexByEntity),
                topologyCommoditiesSnapshotBuilder,
                projectedStatsStore
        );
    }
}

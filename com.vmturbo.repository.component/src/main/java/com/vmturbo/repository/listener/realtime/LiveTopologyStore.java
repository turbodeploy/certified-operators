package com.vmturbo.repository.listener.realtime;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.repository.listener.realtime.ProjectedRealtimeTopology.ProjectedTopologyBuilder;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology.SourceRealtimeTopologyBuilder;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.graph.supplychain.GlobalSupplyChainCalculator;
import com.vmturbo.topology.graph.util.BaseTopologyStore;

/**
 * Stores the current {@link SourceRealtimeTopology} and {@link ProjectedRealtimeTopology}.
 */
public class LiveTopologyStore extends BaseTopologyStore<SourceRealtimeTopology, SourceRealtimeTopologyBuilder, RepoGraphEntity, RepoGraphEntity.Builder> {

    private final GlobalSupplyChainCalculator globalSupplyChainCalculator;

    private final SearchResolver<RepoGraphEntity> searchResolver;

    /**
     * Create a {@link LiveTopologyStore}.
     *
     * @param globalSupplyChainCalculator a global supply chain calculator
     * @param searchResolver Search resolver to use to resolve queries against the source topology.
     */
    public LiveTopologyStore(@Nonnull GlobalSupplyChainCalculator globalSupplyChainCalculator,
                             @Nonnull SearchResolver<RepoGraphEntity> searchResolver) {
        this.globalSupplyChainCalculator = globalSupplyChainCalculator;
        this.searchResolver = searchResolver;
    }

    @GuardedBy("topologyLock")
    private Optional<ProjectedRealtimeTopology> projectedTopology = Optional.empty();

    @Override
    protected SourceRealtimeTopologyBuilder newBuilder(TopologyInfo topologyInfo,
            Consumer<SourceRealtimeTopology> consumer) {
        return new SourceRealtimeTopologyBuilder(topologyInfo, consumer, globalSupplyChainCalculator);
    }

    protected void updateRealtimeProjectedTopology(@Nonnull final ProjectedRealtimeTopology newProjectedRealtimeTopology) {
        synchronized (topologyLock) {
            this.projectedTopology = Optional.of(newProjectedRealtimeTopology);
        }
        logger.info("Updated realtime PROJECTED topology. New topology has {} entities.",
            newProjectedRealtimeTopology.size());
    }

    @Nonnull
    public Optional<ProjectedRealtimeTopology> getProjectedTopology() {
        return projectedTopology;
    }

    /**
     * Create a builder for a new projected topology, which, when finished, will replace the current
     * projected topology in the store.
     *
     * @param topologyId The ID of the new topology.
     * @param originalTopologyInfo Original topology info.
     * @return The {@link ProjectedTopologyBuilder}.
     */
    @Nonnull
    public ProjectedTopologyBuilder newProjectedTopology(final long topologyId,
                                                         @Nonnull final TopologyInfo originalTopologyInfo) {
        return new ProjectedTopologyBuilder(this::updateRealtimeProjectedTopology, topologyId, originalTopologyInfo);
    }

    /**
     * Search the realtime topology and return results.
     *
     * @param query The query.
     * @return Stream of {@link RepoGraphEntity} that match the query. Empty stream if there is no
     *         realtime topology available.
     */
    @Nonnull
    public Stream<RepoGraphEntity> queryRealtimeTopology(@Nonnull final SearchQuery query) {
        return getSourceTopology()
            .map(realtimeTopology -> {
                if (query.getSearchParametersList().isEmpty()) {
                    return realtimeTopology.entityGraph().entities();
                } else {
                    return searchResolver.search(query, realtimeTopology.entityGraph());
                }
            })
            .orElse(Stream.empty());
    }
}

package com.vmturbo.repository.listener.realtime;

import java.util.Optional;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.repository.listener.realtime.ProjectedRealtimeTopology.ProjectedTopologyBuilder;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology.SourceRealtimeTopologyBuilder;
import com.vmturbo.topology.graph.supplychain.GlobalSupplyChainCalculator;
import com.vmturbo.topology.graph.util.BaseTopologyStore;

/**
 * Stores the current {@link SourceRealtimeTopology} and {@link ProjectedRealtimeTopology}.
 */
public class LiveTopologyStore extends BaseTopologyStore<SourceRealtimeTopology, SourceRealtimeTopologyBuilder, RepoGraphEntity, RepoGraphEntity.Builder> {

    private final GlobalSupplyChainCalculator globalSupplyChainCalculator;

    /**
     * Create a {@link LiveTopologyStore}.
     *
     * @param globalSupplyChainCalculator a global supply chain calculator
     */
    public LiveTopologyStore(@Nonnull GlobalSupplyChainCalculator globalSupplyChainCalculator) {
        this.globalSupplyChainCalculator = globalSupplyChainCalculator;
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
}

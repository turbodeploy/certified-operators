package com.vmturbo.repository.listener.realtime;

import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.gson.Gson;

import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.StreamingDiagnosable;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity.Builder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphCreator;

/**
 * Represents an in-memory "source" topology for the realtime (live) context.
 * Each topology broadcast will result in a different {@link SourceRealtimeTopology}.
 * The {@link LiveTopologyStore} is responsible for keeping a reference to the most up-do-date
 * topology.
 */
@Immutable
@ThreadSafe
public class SourceRealtimeTopology implements StreamingDiagnosable {
    private final TopologyInfo topologyInfo;

    /**
     * The graph for the entities in the topology.
     */
    private final TopologyGraph<RepoGraphEntity> entityGraph;

    /**
     * Lazily-computed global supply chain.
     * The overhead on a topology of 200k is about 4MB.
     * It's lazily computed because we only need it if a user logs in and wants to see
     * the global supply chain (which doesn't happen for most topologies with a 10-min update
     * interval).
     */
    private final SetOnce<Map<UIEntityType, SupplyChainNode>> globalSupplyChain = new SetOnce<>();

    private final GlobalSupplyChainCalculator globalSupplyChainCalculator;

    private SourceRealtimeTopology(@Nonnull final TopologyInfo topologyInfo,
                                   @Nonnull final TopologyGraph<RepoGraphEntity> entityGraph,
                                   @Nonnull final GlobalSupplyChainCalculator globalSupplyChainCalculator) {
        this.topologyInfo = topologyInfo;
        this.entityGraph = entityGraph;
        this.globalSupplyChainCalculator = globalSupplyChainCalculator;
    }

    /**
     * @return The {@link TopologyInfo} for the topology this object represents.
     */
    @Nonnull
    public TopologyInfo topologyInfo() {
        return topologyInfo;
    }

    /**
     * @return The {@link TopologyGraph} for the entities in the topology.
     */
    @Nonnull
    public TopologyGraph<RepoGraphEntity> entityGraph() {
        return entityGraph;
    }

    /**
     * @return the size of the topology.
     */
    public int size() {
        return entityGraph.size();
    }

    /**
     * Get the global supply chain. This is lazily cached, so the first call will take longer
     * than the subsequent ones.
     *
     * @return (entity type) -> ({@link SupplyChainNode} for the entity type)
     */
    @Nonnull
    public synchronized Map<UIEntityType, SupplyChainNode> globalSupplyChainNodes() {
        if (!globalSupplyChain.getValue().isPresent()) {
            globalSupplyChain.trySetValue(
                globalSupplyChainCalculator.computeGlobalSupplyChain(entityGraph));
        }
        return globalSupplyChain.getValue().get();
    }

    @Nonnull
    @Override
    public Stream<String> collectDiags() throws DiagnosticsException {
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        return entityGraph.entities()
            .map(RepoGraphEntity::getFullTopologyEntity)
            .map(gson::toJson);
    }

    @Override
    public void restoreDiags(@Nonnull final Stream<String> collectedDiags) throws DiagnosticsException {
        // Restoring diags not supported for now.
        //
        // It's not an important use case - we typically restore diags to Topology Processor and
        // broadcast.
    }

    /**
     * Builder class for the {@link SourceRealtimeTopology}.
     * <p/>
     * Use: {@link SourceRealtimeTopologyBuilder#addEntities(Collection)} to add entities,
     * and {@link SourceRealtimeTopologyBuilder#finish()} when done.
     */
    public static class SourceRealtimeTopologyBuilder {
        private final TopologyInfo topologyInfo;
        private final Consumer<SourceRealtimeTopology> onFinish;

        private final GlobalSupplyChainCalculator globalSupplyChainCalculator;

        private final TopologyGraphCreator<Builder, RepoGraphEntity> graphCreator =
            new TopologyGraphCreator<>();

        SourceRealtimeTopologyBuilder(
                @Nonnull final TopologyInfo topologyInfo,
                @Nonnull final GlobalSupplyChainCalculator globalSupplyChainCalculator,
                @Nonnull final Consumer<SourceRealtimeTopology> onFinish) {
            this.topologyInfo = topologyInfo;
            this.onFinish = onFinish;
            this.globalSupplyChainCalculator = globalSupplyChainCalculator;
        }

        public void addEntities(@Nonnull final Collection<TopologyEntityDTO> entities) {
            for (TopologyEntityDTO entity : entities) {
                graphCreator.addEntity(RepoGraphEntity.newBuilder(entity));
            }
        }

        @Nonnull
        public SourceRealtimeTopology finish() {
            final TopologyGraph<RepoGraphEntity> graph = graphCreator.build();

            final SourceRealtimeTopology sourceRealtimeTopology =
                new SourceRealtimeTopology(topologyInfo, graph, globalSupplyChainCalculator);
            onFinish.accept(sourceRealtimeTopology);
            return sourceRealtimeTopology;
        }

    }
}

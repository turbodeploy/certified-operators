package com.vmturbo.repository.listener.realtime;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Stopwatch;
import com.google.gson.Gson;

import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.common.protobuf.topology.UIEnvironmentType;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.StreamingDiagnosable;
import com.vmturbo.proactivesupport.DataMetricSummary;
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
    private final Map<UIEnvironmentType, SetOnce<Map<UIEntityType, SupplyChainNode>>> globalSupplyChain = new HashMap<>();

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
    public synchronized Map<UIEntityType, SupplyChainNode> globalSupplyChainNodes(
            @Nonnull final Optional<UIEnvironmentType> envType) {
        UIEnvironmentType environmentType = envType.orElse(UIEnvironmentType.HYBRID);
        if (environmentType == UIEnvironmentType.UNKNOWN) {
            return Collections.emptyMap();
        }

        final SetOnce<Map<UIEntityType, SupplyChainNode>> envTypeNodes =
            globalSupplyChain.computeIfAbsent(environmentType, k -> new SetOnce<>());

        return envTypeNodes.ensureSet(() -> globalSupplyChainCalculator.computeGlobalSupplyChain(entityGraph, environmentType));
    }

    @Nonnull
    @Override
    public Stream<String> collectDiags() throws DiagnosticsException {
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        return entityGraph.entities()
            .map(RepoGraphEntity::getTopologyEntity)
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
        private static final Logger logger = LogManager.getLogger();

        private final TopologyInfo topologyInfo;
        private final Consumer<SourceRealtimeTopology> onFinish;

        private final GlobalSupplyChainCalculator globalSupplyChainCalculator;

        private final TopologyGraphCreator<Builder, RepoGraphEntity> graphCreator =
            new TopologyGraphCreator<>();

        /**
         * The size of the compression buffer in the most recently completed projected topology
         * builder. We assume that (in general) topologies stay roughly the same size once targets
         * are added, so we can use the last topology's buffer size to avoid unnecessary allocations
         * on the next one.
         *
         * Note - the buffer will be relavitely small - it is bounded by the largest
         * entity in the topology.
         */
        private static volatile int sharedBufferSize = 0;

        /**
         * Reuse the same intermediate compression buffer for the full topology.
         */
        private final SharedByteBuffer compressionBuffer;

        private final Stopwatch builderStopwatch = Stopwatch.createUnstarted();

        SourceRealtimeTopologyBuilder(
                @Nonnull final TopologyInfo topologyInfo,
                @Nonnull final GlobalSupplyChainCalculator globalSupplyChainCalculator,
                @Nonnull final Consumer<SourceRealtimeTopology> onFinish) {
            this.topologyInfo = topologyInfo;
            this.onFinish = onFinish;
            this.globalSupplyChainCalculator = globalSupplyChainCalculator;
            this.compressionBuffer = new SharedByteBuffer(sharedBufferSize);
        }

        public void addEntities(@Nonnull final Collection<TopologyEntityDTO> entities) {
            builderStopwatch.start();
            for (TopologyEntityDTO entity : entities) {
                graphCreator.addEntity(RepoGraphEntity.newBuilder(entity, compressionBuffer));
            }
            builderStopwatch.stop();
        }

        @Nonnull
        public SourceRealtimeTopology finish() {
            builderStopwatch.start();
            final TopologyGraph<RepoGraphEntity> graph = graphCreator.build();

            final SourceRealtimeTopology sourceRealtimeTopology =
                new SourceRealtimeTopology(topologyInfo, graph, globalSupplyChainCalculator);
            onFinish.accept(sourceRealtimeTopology);
            builderStopwatch.stop();

            final long elapsedSec = builderStopwatch.elapsed(TimeUnit.SECONDS);
            Metrics.CONSTRUCTION_TIME_SUMMARY.observe((double)elapsedSec);
            logger.info("Spent total of {}s to construct source realtime topology.", elapsedSec);

            // Update the shared buffer size, so the next projected topology can use it
            // as a starting point.
            sharedBufferSize = compressionBuffer.getSize();

            return sourceRealtimeTopology;
        }

    }

    private static class Metrics {
        private static final DataMetricSummary CONSTRUCTION_TIME_SUMMARY = DataMetricSummary.builder()
            .withName("repo_source_realtime_construction_seconds")
            .withHelp("Total time taken to build the source realtime topology.")
            .build()
            .register();
    }
}

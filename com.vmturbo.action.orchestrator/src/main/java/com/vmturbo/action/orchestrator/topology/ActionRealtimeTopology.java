package com.vmturbo.action.orchestrator.topology;

import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.util.BaseTopology;

/**
 * Represents a topology of {@link ActionGraphEntity}s in the action orchestrator, used for fast
 * access to topology-related data.
 */
public class ActionRealtimeTopology extends BaseTopology<ActionGraphEntity> {

    private ActionRealtimeTopology(TopologyInfo topologyInfo,
            TopologyGraph<ActionGraphEntity> entityGraph) {
        super(topologyInfo, entityGraph);
    }

    /**
     * Builder for the {@link ActionRealtimeTopology}.
     */
    public static class ActionRealtimeTopologyBuilder extends BaseTopologyBuilder<ActionRealtimeTopology, ActionGraphEntity, ActionGraphEntity.Builder> {

        ActionRealtimeTopologyBuilder(
                @Nonnull final TopologyInfo topologyInfo,
                @Nonnull final Consumer<ActionRealtimeTopology> onFinish) {
            super(Metrics.CONSTRUCTION_TIME_SUMMARY, topologyInfo, onFinish);
        }

        @Override
        protected ActionGraphEntity.Builder newBuilder(TopologyEntityDTO entity) {
            return new ActionGraphEntity.Builder(entity);
        }

        @Override
        protected ActionRealtimeTopology newTopology(TopologyInfo topologyInfo,
                TopologyGraph<ActionGraphEntity> graph) {
            return new ActionRealtimeTopology(topologyInfo, graph);
        }
    }

    /**
     * Container for relevant metrics.
     */
    private static class Metrics {
        private Metrics() {}

        private static final DataMetricSummary CONSTRUCTION_TIME_SUMMARY = DataMetricSummary.builder()
                .withName("ao_source_realtime_construction_seconds")
                .withHelp("Total time taken to build the action realtime topology.")
                .build()
                .register();
    }
}

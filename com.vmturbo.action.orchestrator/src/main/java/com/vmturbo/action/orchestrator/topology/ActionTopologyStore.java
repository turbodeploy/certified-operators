package com.vmturbo.action.orchestrator.topology;

import java.util.function.Consumer;

import com.vmturbo.action.orchestrator.topology.ActionRealtimeTopology.ActionRealtimeTopologyBuilder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.topology.graph.util.BaseTopologyStore;

/**
 * Stores the {@link ActionTopologyStore} and updates it as new topologies come in over Kafka.
 */
public class ActionTopologyStore extends BaseTopologyStore<ActionRealtimeTopology, ActionRealtimeTopologyBuilder, ActionGraphEntity, ActionGraphEntity.Builder> {

    @Override
    protected ActionRealtimeTopologyBuilder newBuilder(TopologyInfo topologyInfo,
            Consumer<ActionRealtimeTopology> consumer) {
        return new ActionRealtimeTopologyBuilder(topologyInfo, consumer);
    }
}

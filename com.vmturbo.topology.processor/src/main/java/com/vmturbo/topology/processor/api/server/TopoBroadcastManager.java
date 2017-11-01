package com.vmturbo.topology.processor.api.server;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;

/**
 * Represents entry-point of Topology processor notifications.
 */
public interface TopoBroadcastManager {
    /**
     * Start a topology broadcast to all the recipients. This topology is denoted by a
     * topologyContextId, which may associated related topologies, and a topologyId which
     * is always unique. The topologyContextId may be retrieved by
     * {@link TopologyBroadcast#getTopologyContextId()} ()}, and the topologyId
     * may be retrieved by {@link TopologyBroadcast#getTopologyId()}.
     *
     * @param topologyInfo The {@link TopologyInfo} object containing information about the topology.
     * @return {@link TopologyBroadcast} object to put all the entities to broadcast into
     */
    @Nonnull
    TopologyBroadcast broadcastTopology(@Nonnull final TopologyInfo topologyInfo);
}

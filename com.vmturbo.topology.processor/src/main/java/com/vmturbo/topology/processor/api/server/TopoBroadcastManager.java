package com.vmturbo.topology.processor.api.server;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;

/**
 * Represents entry-point of Topology processor notifications.
 */
public interface TopoBroadcastManager {
    /**
     * Start a live topology broadcast to all the recipients. This topology is denoted by a
     * topologyContextId, which may associated related topologies, and a topologyId which
     * is always unique. The topologyContextId may be retrieved by
     * {@link TopologyBroadcast#getTopologyContextId()} ()}, and the topologyId
     * may be retrieved by {@link TopologyBroadcast#getTopologyId()}.
     *
     * @param topologyInfo The {@link TopologyInfo} object containing information about the topology.
     * @return {@link TopologyBroadcast} object to put all the entities to broadcast into
     */
    @Nonnull
    TopologyBroadcast broadcastLiveTopology(@Nonnull final TopologyInfo topologyInfo);

    /**
     * Start a user plan topology broadcast to all the recipients. This topology is denoted by a
     * topologyContextId, which may associated related topologies, and a topologyId which
     * is always unique. The topologyContextId may be retrieved by
     * {@link TopologyBroadcast#getTopologyContextId()} ()}, and the topologyId
     * may be retrieved by {@link TopologyBroadcast#getTopologyId()}.
     *
     * @param topologyInfo The {@link TopologyInfo} object containing information about the topology.
     * @return {@link TopologyBroadcast} object to put all the entities to broadcast into
     */
    @Nonnull
    TopologyBroadcast broadcastUserPlanTopology(@Nonnull final TopologyInfo topologyInfo);

    /**
     * Start a scheduled plan topology broadcast to all the recipients. This topology is denoted
     * by a topologyContextId, which may associated related topologies, and a topologyId which
     * is always unique. The topologyContextId may be retrieved by
     * {@link TopologyBroadcast#getTopologyContextId()} ()}, and the topologyId
     * may be retrieved by {@link TopologyBroadcast#getTopologyId()}.
     *
     * @param topologyInfo The {@link TopologyInfo} object containing information about the topology.
     * @return {@link TopologyBroadcast} object to put all the entities to broadcast into
     */
    @Nonnull
    TopologyBroadcast broadcastScheduledPlanTopology(@Nonnull final TopologyInfo topologyInfo);
}
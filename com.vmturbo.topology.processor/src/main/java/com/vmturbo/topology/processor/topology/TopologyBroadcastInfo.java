package com.vmturbo.topology.processor.topology;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.topology.processor.api.server.TopologyBroadcast;

/**
 * A container for information about topology broadcasts.
 */
@Immutable
public class TopologyBroadcastInfo {
    /**
     * The number of entities in the broadcast.
     */
    private final long entityCount;

    /**
     * The ID of the topology that was broadcast.
     */
    private final long topologyId;

    /**
     * The ID of the topology context for the topology that was broadcast.
     */
    private final long topologyContextId;

    public TopologyBroadcastInfo(@Nonnull final TopologyBroadcast broadcast,
                                 final long entityCount) {
        this.entityCount = entityCount;
        this.topologyId = broadcast.getTopologyId();
        this.topologyContextId = broadcast.getTopologyContextId();
    }

    public long getEntityCount() {
        return entityCount;
    }

    public long getTopologyId() {
        return topologyId;
    }

    public long getTopologyContextId() {
        return topologyContextId;
    }
}

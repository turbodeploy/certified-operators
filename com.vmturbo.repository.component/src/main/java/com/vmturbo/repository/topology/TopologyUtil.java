package com.vmturbo.repository.topology;

import java.util.function.Predicate;

import com.vmturbo.repository.topology.TopologyID.TopologyType;

public class TopologyUtil {

    /**
     * Checks if a {@link TopologyID} is a plan projected topology.
     *
     * @param topologyID to check
     * @param realtimeTopologyContextId
     * @return true if the {@link TopologyID} is a plan projected topology
     */
    public static boolean isPlanProjectedTopology(TopologyID topologyID, long realtimeTopologyContextId) {
        return (TopologyType.PROJECTED == topologyID.getType()) &&
                (realtimeTopologyContextId != topologyID.getContextId());
    }

}

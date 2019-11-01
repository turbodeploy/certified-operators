package com.vmturbo.repository.topology;

import com.vmturbo.repository.topology.TopologyID.TopologyType;

public class TopologyUtil {

    /**
     * Checks if a {@link TopologyID} is a plan source topology.
     *
     * @param topologyID to check
     * @param realtimeTopologyContextId the context ID of the realtime market
     * @return true if the {@link TopologyID} is a plan source topology
     */
    public static boolean isPlanSourceTopology(TopologyID topologyID, long realtimeTopologyContextId) {
        return (TopologyType.SOURCE == topologyID.getType()) &&
            (realtimeTopologyContextId != topologyID.getContextId());
    }

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

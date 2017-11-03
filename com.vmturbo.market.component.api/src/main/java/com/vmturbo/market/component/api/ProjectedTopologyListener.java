package com.vmturbo.market.component.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;

/**
 * An object that listens to the market's notifications about
 * traders after the optimal state has been attained
 */
public interface ProjectedTopologyListener {

    /**
     * Callback receiving the trader after the market has completed running.
     *
     * @param projectedTopologyId id of the projected topology
     * @param sourceTopologyInfo contains basic information of source topology.
     * @param topology contains the traders after the plan has completed.
     */
    void onProjectedTopologyReceived(long projectedTopologyId, TopologyInfo sourceTopologyInfo,
            @Nonnull RemoteIterator<TopologyEntityDTO> topology);
}

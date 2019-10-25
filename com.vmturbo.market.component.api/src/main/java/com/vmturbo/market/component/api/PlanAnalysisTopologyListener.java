package com.vmturbo.market.component.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;

/**
 * Object that receives a plan analysis topology -- this is a topology that may have been edited
 * in accordance with the plan scenario and/or scoped according to the plan scope configuration.
 */
public interface PlanAnalysisTopologyListener {

    /**
     * A plan analysis topology has been broadcasted -- onPlanAnalysisTopology is your chance to
     * process it.
     *
     * @param topologyInfo TopologyInfo describing the topology
     * @param topologyDTOs A remote iterator for receiving the plan analysis topology entities.
     */
    void onPlanAnalysisTopology(TopologyInfo topologyInfo,
                                @Nonnull RemoteIterator<TopologyDTO.Topology.DataSegment> topologyDTOs);

}

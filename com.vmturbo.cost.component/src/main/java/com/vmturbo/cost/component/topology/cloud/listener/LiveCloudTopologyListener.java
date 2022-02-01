package com.vmturbo.cost.component.topology.cloud.listener;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.component.topology.LiveTopologyEntitiesListener;

/**
 * An interface to representing an individual task to be accomplished by the cost component in a separate thread.
 * The {@link LiveTopologyEntitiesListener} consists of a list of these Listeners and invokes them in a
 * thread pool.
 */
public interface LiveCloudTopologyListener {
    /**
     * Takes in a cloud topology and topology info and processes it accordingly to the class implementing
     * it.
     *
     * @param cloudTopology The cloud topology to process.
     * @param topologyInfo Info about the topology
     */
    void process(CloudTopology cloudTopology, TopologyInfo topologyInfo);
}

package com.vmturbo.cloud.commitment.analysis.writer;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;

/**
 * An interface used for defining the cloud commitment demand writer.
 */
public interface CloudCommitmentDemandWriter {

    /**
     * A method for constructing the allocation demand to be written to the db.
     *
     * @param cloudTopology The cloud topology.
     * @param topologyInfo The topology information.
     */
    void writeAllocationDemand(CloudTopology cloudTopology, TopologyInfo topologyInfo);
}

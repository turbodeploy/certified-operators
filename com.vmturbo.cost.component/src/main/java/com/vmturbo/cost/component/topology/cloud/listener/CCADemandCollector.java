package com.vmturbo.cost.component.topology.cloud.listener;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.persistence.CloudCommitmentDemandWriter;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;

/**
 * The CCA Demand collector records demand in the cca related db tables.
 */
public class CCADemandCollector implements LiveCloudTopologyListener {

    private final CloudCommitmentDemandWriter cloudCommitmentDemandWriter;

    /**
     * The constructor for the CCA Demand collector.
     *
     * @param cloudCommitmentDemandWriter The instance of the cloud commitment demand writer which
     * records demand to the db table.
     */
    public CCADemandCollector(@Nonnull final CloudCommitmentDemandWriter cloudCommitmentDemandWriter) {
        this.cloudCommitmentDemandWriter = cloudCommitmentDemandWriter;
    }

    @Override
    public void process(CloudTopology cloudTopology, TopologyInfo topologyInfo) {
        // Store allocation demand in db RI Buy 2.0
        cloudCommitmentDemandWriter.writeAllocationDemand(cloudTopology, topologyInfo);
    }
}


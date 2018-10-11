package com.vmturbo.market.component.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;

/**
 * A listener for projected per-entity costs broadcast by the market as part of its results.
 */
public interface ProjectedEntityCostsListener {

    /**
     * This method will be called when new information about projected entity costs is broadcast
     * by the market.
     *
     * @param projectedTopologyId The ID of the projected topology. This is the topology the costs
     *                            are calculated from.
     * @param originalTopologyInfo Information about the original topology the analysis was ran on.
     * @param entityCosts A {@link RemoteIterator} over the projected entity costs for entities
     *                    in the projected topology.
     */
    void onProjectedEntityCostsReceived(final long projectedTopologyId,
                                        @Nonnull final TopologyInfo originalTopologyInfo,
                                        @Nonnull final RemoteIterator<EntityCost> entityCosts);
}

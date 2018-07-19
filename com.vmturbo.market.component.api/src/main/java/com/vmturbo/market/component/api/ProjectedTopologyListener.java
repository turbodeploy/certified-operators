package com.vmturbo.market.component.api;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
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
     * @param skippedEntities The OIDs of entities in the original topology that were skipped,
     *                        and therefore not considered for market analysis.
     * @param topology contains the traders after the plan has completed.
     */
    void onProjectedTopologyReceived(final long projectedTopologyId,
                                 @Nonnull final TopologyInfo sourceTopologyInfo,
                                 @Nonnull final Set<Long> skippedEntities,
                                 @Nonnull final RemoteIterator<ProjectedTopologyEntity> topology);
}

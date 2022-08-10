package com.vmturbo.cost.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.TopologyOnDemandCostChunk;

/**
 * Listener to handle TopologyOnDemandCostChunks.
 */
public interface TopologyCostListener {

    /**
     * Method to process received TopologyOnDemandCostChunks.
     *
     * @param costChunk chunk that was received
     */
    void onTopologyCostReceived(@Nonnull TopologyOnDemandCostChunk costChunk);

}

package com.vmturbo.topology.processor.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;

/**
 * Listener for {@link TopologySummary} events.
 */
public interface TopologySummaryListener {

    /**
     * Called when a new {@link TopologySummary} message is received.
     * Note that a {@link TopologySummary} may represent either a topology that is sent out from
     * the topology processor, or a topology that failed to broadcast.
     *
     * @param topologySummary The summary of the topology.
     */
    void onTopologySummary(@Nonnull TopologySummary topologySummary);

}

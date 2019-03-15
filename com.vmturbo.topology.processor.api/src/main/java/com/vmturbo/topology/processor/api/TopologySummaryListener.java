package com.vmturbo.topology.processor.api;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;

/**
 * Listener for {@link TopologySummary} events.
 */
public interface TopologySummaryListener {
    /**
     * Called when a new {@link TopologySummary} message is received.
     *
     * @param topologySummary
     */
    void onTopologySummary(TopologySummary topologySummary);
}

package com.vmturbo.market.component.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisSummary;

/**
 * Listener for {@link AnalysisSummary} events.
 */
public interface AnalysisSummaryListener {
    /**
     * Called when a new {@link AnalysisSummary} message is received.
     *
     * @param analysisSummary
     */
    void onAnalysisSummary(@Nonnull AnalysisSummary analysisSummary);
}

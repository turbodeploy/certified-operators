package com.vmturbo.market.runner;

import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;

/**
 * This class tracks the health of analysis.
 * For the time being - for Analysis to be considered unhealthy, a timeout needs to have occurred in the
 * previous cycle. Any exceptions which occurred and caused analysis to go to FAILED state are not being
 * considered unhealthy.
 */
public class AnalysisHealthTracker {

    private boolean isInitialized;
    private boolean didPreviousCycleTimeout;

    /**
     * Update the health tracker with analysis from current cycle.
     * @param analysis the analysis from current cycle
     */
    public synchronized void update(Analysis analysis) {
        // We only want to update real time status
        if (TopologyDTOUtil.isPlan(analysis.getTopologyInfo())) {
            return;
        }
        didPreviousCycleTimeout = analysis.isStopAnalysis();
        isInitialized = true;
    }

    /**
     * For the time being - for Analysis to be considered unhealthy, a timeout needs to have occurred in the
     * previous cycle. Any exceptions which occurred and caused analysis to go to FAILED state are not being
     * considered unhealthy.
     *
     * @return true if analysis is healthy, false otherwise.
     */
    public synchronized boolean isAnalysisHealthy() {
        if (!isInitialized) {
            return true;
        }
        return !didPreviousCycleTimeout;
    }
}
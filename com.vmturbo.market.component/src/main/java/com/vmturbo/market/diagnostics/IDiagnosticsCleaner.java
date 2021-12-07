package com.vmturbo.market.diagnostics;

import java.util.concurrent.Future;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.analysis.economy.Economy;

/**
 * Interface for diagnostics cleaner.
 */
public interface IDiagnosticsCleaner {

    /**
     * Perform any necessary cleanup any diagnostics written.
     * @param economy the economy
     * @param topologyInfo topology info for current cycle
     */
    void cleanup(Economy economy, TopologyInfo topologyInfo);

    /**
     * Store the future for checking if the save of analysis diags completed.
     * @param saveAnalysisDiagsFuture analysis diags future
     */
    void setSaveAnalysisDiagsFuture(Future<?> saveAnalysisDiagsFuture);

    /**
     * Gets the number of real time anlaysis diags to retain on disk.
     * @return the number of real time anlaysis diags to retain on disk
     */
    int getNumRealTimeAnalysisDiagsToRetain();
}
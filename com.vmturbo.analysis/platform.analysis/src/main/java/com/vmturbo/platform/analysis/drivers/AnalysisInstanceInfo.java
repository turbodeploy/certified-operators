package com.vmturbo.platform.analysis.drivers;

import com.vmturbo.platform.analysis.topology.Topology;

/**
 * Wrapper class for topology sent from opsmanager to market2 to be analyzed.
 * @author weiduan
 *
 */
public class AnalysisInstanceInfo {
    // It is possible that some exceptional event like a connection drop will result in an
    // incomplete topology. e.g. if we receive a START_DISCOVERED_TOPOLOGY, then some
    // DISCOVERED_TRADER messages and then the connection resets and we receive a
    // START_DISCOVERED_TOPOLOGY again. In that context, lastComplete_ is the last complete topology
    // we've received and currentPartial_ is the topology we are currently populating.
    private Topology lastComplete_ = new Topology();
    private Topology currentPartial_ = new Topology();
    // a flag to denote if we should classify Actions
    private boolean classifyActions = false;
    // a flag to denote if we should replay Actions
    private boolean replayActions = false;
    // a flag to decide if provision algorithm should run or not
    private boolean isProvisionEnabled = true;
    // a flag to decide if suspension algorithm should run or not
    private boolean isSuspensionEnabled = true;
    // a flag to decide if resize algorithm should run or not
    private boolean isResizeEnabled = true;
    // market name
    private String marketName_;
    // market data
    private String marketData_;
    // true if this run of analysis is for RT
    private boolean isRealTime;

    /**
     * Sets the boolean which indicates if action classification is needed.
     * @param classifyActions
     */
    public void setClassifyActions(boolean classifyActions) {
        this.classifyActions = classifyActions;
    }

    /**
     * True if action classification is needed.
     * @return
     */
    public boolean isClassifyActions() {
        return classifyActions;
    }

    /**
     * Sets the boolean which indicates if action replay is needed.
     * @param replayActions
     */
    public void setReplayActions(boolean replayActions) {
        this.replayActions = replayActions;
    }

    /**
     * True if action replay is needed.
     * @return
     */
    public boolean isReplayActions() {
        return replayActions;
    }

    /**
     * True if provision algorithm is enabled.
     * @return
     */
    public boolean isProvisionEnabled() {
        return isProvisionEnabled;
    }

    /**
     * Sets the global flag which enables the provision algorithm.
     * @param isProvisionEnabled
     */
    public void setProvisionEnabled(boolean isProvisionEnabled) {
        this.isProvisionEnabled = isProvisionEnabled;
    }

    /**
     * True if suspension algorithm is enabled.
     * @return
     */
    public boolean isSuspensionEnabled() {
        return isSuspensionEnabled;
    }

    /**
     * Returns the market name of the topology.
     * @return
     */
    public String getMarketName() {
        return marketName_;
    }

    /**
     * Returns the market data which is used by the stats file.
     * @return
     */
    public String getMarketData() {
        return marketData_;
    }

    /**
     * Sets the global flag which enables the suspension algorithm.
     * @param isSuspensionEnabled
     */
    public void setSuspensionEnabled(boolean isSuspensionEnabled) {
        this.isSuspensionEnabled = isSuspensionEnabled;
    }

    /**
     * True if the resize algorithm is enabled.
     * @return
     */
    public boolean isResizeEnabled() {
        return isResizeEnabled;
    }

    /**
     * Sets the global flag which enables the resize algorithm.
     * @param isResizeEnabled
     */
    public void setResizeEnabled(boolean isResizeEnabled) {
        this.isResizeEnabled = isResizeEnabled;
    }

    /**
     * Returns the topology which is the most recent and complete.
     * @return
     */
    public Topology getLastComplete() {
        return lastComplete_;
    }

    /**
     * Sets the topology which is the most recent and complete.
     * @param lastComplete
     */
    public void setLastComplete(Topology lastComplete) {
        this.lastComplete_ = lastComplete;
    }

    /**
     * Returns the topology currently being populated.
     * @return
     */
    public Topology getCurrentPartial() {
        return currentPartial_;
    }

    /**
     * Sets the topology currently being populated.
     * @param currentPartial
     */
    public void setCurrentPartial(Topology currentPartial) {
        this.currentPartial_ = currentPartial;
    }

    /**
     * Sets the market name of the topology.
     * @param marketName
     */
    public void setMarketName(String marketName) {
        marketName_ = marketName;
    }

    /**
     * Sets the market data which is used by the stats file.
     * @param marketData
     */
    public void setMarketData(String marketData) {
        marketData_ = marketData;
    }

    /**
     * True if the topology is from real time market.
     * @return
     */
    public boolean isRealTime() {
        return isRealTime;
    }

    /**
     * Sets {@link isRealTime} true if the topology is from real time market.
     * @param isRealTime
     */
    public void setRealTime(boolean isRealTime) {
        this.isRealTime = isRealTime;
    }
}

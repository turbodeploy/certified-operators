package com.vmturbo.platform.analysis.drivers;

import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
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
    // true if this run of analysis is for balanced deploy market.
    private boolean isBalanceDeploy;
    // true if moves throttling is enabled
    private boolean isMovesThrottling;
    // suspension throttling config
    private SuspensionsThrottlingConfig suspensionsThrottlingConfig;

    /**
     * Sets the boolean which indicates if action classification is needed.
     *
     * @param classifyActions The new value for the flag.
     */
    public void setClassifyActions(boolean classifyActions) {
        this.classifyActions = classifyActions;
    }

    /**
     * True if action classification is needed.
     */
    public boolean isClassifyActions() {
        return classifyActions;
    }

    /**
     * Sets the boolean which indicates if action replay is needed.
     *
     * @param replayActions The new value for the flag.
     */
    public void setReplayActions(boolean replayActions) {
        this.replayActions = replayActions;
    }

    /**
     * True if action replay is needed.
     */
    public boolean isReplayActions() {
        return replayActions;
    }

    /**
     * True if provision algorithm is enabled.
     */
    public boolean isProvisionEnabled() {
        return isProvisionEnabled;
    }

    /**
     * Sets the global flag which enables the provision algorithm.
     *
     * @param isProvisionEnabled The new value for the flag.
     */
    public void setProvisionEnabled(boolean isProvisionEnabled) {
        this.isProvisionEnabled = isProvisionEnabled;
    }

    /**
     * True if suspension algorithm is enabled.
     */
    public boolean isSuspensionEnabled() {
        return isSuspensionEnabled;
    }

    /**
     * Returns the market name of the topology.
     */
    public String getMarketName() {
        return marketName_;
    }

    /**
     * Returns the market data which is used by the stats file.
     */
    public String getMarketData() {
        return marketData_;
    }

    /**
     * Sets the global flag which enables the suspension algorithm.
     *
     * @param isSuspensionEnabled The new value for the flag.
     */
    public void setSuspensionEnabled(boolean isSuspensionEnabled) {
        this.isSuspensionEnabled = isSuspensionEnabled;
    }

    /**
     * True if the resize algorithm is enabled.
     */
    public boolean isResizeEnabled() {
        return isResizeEnabled;
    }

    /**
     * Sets the global flag which enables the resize algorithm.
     *
     * @param isResizeEnabled The new value for the flag.
     */
    public void setResizeEnabled(boolean isResizeEnabled) {
        this.isResizeEnabled = isResizeEnabled;
    }

    /**
     * Returns the topology which is the most recent and complete.
     */
    public Topology getLastComplete() {
        return lastComplete_;
    }

    /**
     * Sets the topology which is the most recent and complete.
     */
    public void setLastComplete(Topology lastComplete) {
        this.lastComplete_ = lastComplete;
    }

    /**
     * Returns the topology currently being populated.
     */
    public Topology getCurrentPartial() {
        return currentPartial_;
    }

    /**
     * Sets the topology currently being populated.
     */
    public void setCurrentPartial(Topology currentPartial) {
        this.currentPartial_ = currentPartial;
    }

    /**
     * Sets the market name of the topology.
     *
     * @param marketName The new name.
     */
    public void setMarketName(String marketName) {
        marketName_ = marketName;
    }

    /**
     * Sets the market data which is used by the stats file.
     */
    public void setMarketData(String marketData) {
        marketData_ = marketData;
    }

    /**
     * True if the topology is from real time market.
     */
    public boolean isRealTime() {
        return isRealTime;
    }

    /**
     * Sets {@link #isRealTime()} to true if the topology is from a real time market.
     */
    public void setRealTime(boolean isRealTime) {
        this.isRealTime = isRealTime;
    }

    public boolean isMovesThrottling() {
        return isMovesThrottling;
    }

    public void setMovesThrottling(boolean isMovesThrottling) {
        this.isMovesThrottling = isMovesThrottling;
    }

    public SuspensionsThrottlingConfig getSuspensionsThrottlingConfig() {
        return suspensionsThrottlingConfig;
    }

    public void setSuspensionsThrottlingConfig(SuspensionsThrottlingConfig suspensionsThrottligConfig) {
        this.suspensionsThrottlingConfig = suspensionsThrottligConfig;
    }

    /**
     * @return the isBalanceDeploy.
     */
    public boolean isBalanceDeploy() {
        return isBalanceDeploy;
    }

    /**
     * @param isBalanceDeploy the isBalanceDeploy to set.
     */
    public void setBalanceDeploy(boolean isBalanceDeploy) {
        this.isBalanceDeploy = isBalanceDeploy;
    }
}

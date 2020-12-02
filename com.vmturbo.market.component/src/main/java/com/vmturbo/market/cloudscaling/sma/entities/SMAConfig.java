package com.vmturbo.market.cloudscaling.sma.entities;

/**
 * The Stable Marriage algorithm base config.
 */
public class SMAConfig {
    /*
     * what mode the SMA is running.
     */
    private final boolean reduceDependency;

    /**
     * SMAConfig constructor.
     * @param reduceDependency if true will reduce relinquishing
     */
    public SMAConfig(boolean reduceDependency) {
        this.reduceDependency = reduceDependency;
    }

    /**
     * SMAConfig constructor.
     */
    public SMAConfig() {
        this.reduceDependency = false;
    }

    public boolean isReduceDependency() {
        return reduceDependency;
    }
}

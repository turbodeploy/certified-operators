package com.vmturbo.components.common.health;

import java.time.Instant;

/**
 * A simple health status interface
 */
public interface HealthStatus {

    /**
     * Is the implementor healthy?
     * @return true if it's health, false otherwise
     */
    public boolean isHealthy();

    /**
     * Retrieves a string that provides additional detail about the health status
     * @return additional detail associated with the health status
     */
    public String getDetails();

    /**
     * The time this check was performed
     * @return the time this check was performed
     */
    public Instant getCheckTime();

    /**
     * The time the current health state was first observed
     * @return the time the current health state was first observed
     */
    public Instant getSince();
}

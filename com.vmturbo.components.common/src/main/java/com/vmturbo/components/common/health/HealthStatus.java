package com.vmturbo.components.common.health;

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
}

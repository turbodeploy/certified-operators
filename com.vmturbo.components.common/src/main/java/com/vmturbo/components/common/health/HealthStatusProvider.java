package com.vmturbo.components.common.health;

/**
 * Objects implementing HealthStatusProvider are expected to be able to report a health status
 */
public interface HealthStatusProvider {

    /**
     * Get the name of the provider
     * @return the name of the health status provider
     */
    public String getName();

    /**
     * Get the current health state of the provider
     * @return the current HealthStatus of the provider
     */
    public HealthStatus getHealthStatus();
}

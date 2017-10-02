package com.vmturbo.components.common.health;

/**
 * Objects implementing HealthStatusProvider are expected to be able to report a health status
 */
public interface HealthStatusProvider {

    /**
     * Get the current health state of the provider
     * @return the current HealthStatus of the provider
     */
    public HealthStatus getHealthStatus();
}

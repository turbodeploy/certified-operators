package com.vmturbo.components.common.health;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A simple health status provider that implements {@link HealthStatusProvider}.
 *
 * In addition to tracking health status, it will record the time the health was last set.
 */
@ThreadSafe
public class SimpleHealthStatusProvider implements HealthStatusProvider {

    private SimpleHealthStatus lastHealthStatus;

    public SimpleHealthStatusProvider() {
        // the first health status is going to be "initializing."
        reportUnhealthy("Initializing.");
    }

    /**
     * Convenience function to report the status as healthy. The synchronization is just to ensure
     * that the properties are updated together.
     */
    public synchronized SimpleHealthStatus reportHealthy() {
        SimpleHealthStatus newHealthStatus = new SimpleHealthStatus(true,"");
        setHealthStatus(newHealthStatus);
        return newHealthStatus;
    }

    /**
     * Convenience function to report an unhealthy status. The synchronization is just to ensure
     * that the properties are updated together.
     *
     * @param message associated with the unhealthy status
     */
    public synchronized SimpleHealthStatus reportUnhealthy(String message) {
        SimpleHealthStatus newStatus = new SimpleHealthStatus(false,message);
        setHealthStatus(newStatus);
        return newStatus;
    }

    /**
     * Get the HealthStatus from the provider.
     *
     * @return the current HealthStatus of the provider
     */
    @Override
    public synchronized SimpleHealthStatus getHealthStatus() {
        return lastHealthStatus;
    }

    /**
     * Sets the health status of this provider
     * @param newStatus the new status to provide.
     */
    protected synchronized void setHealthStatus(SimpleHealthStatus newStatus) {
        lastHealthStatus = newStatus;
    }
}

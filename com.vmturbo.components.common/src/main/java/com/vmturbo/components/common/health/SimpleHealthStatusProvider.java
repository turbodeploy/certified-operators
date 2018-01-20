package com.vmturbo.components.common.health;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * A simple health status provider that implements {@link HealthStatusProvider}.
 *
 * In addition to tracking health status, it will record the time the health was last set.
 */
@ThreadSafe
public class SimpleHealthStatusProvider implements HealthStatusProvider {
    private Logger logger = LogManager.getLogger();

    private String name;

    private SimpleHealthStatus lastHealthStatus;

    /**
     * We will, on request, support a live stream of status updates. The statusFlux can be subscribed
     * to for recieving these updates.
     */
    private Flux<SimpleHealthStatus> statusFlux;

    /**
     * The statusEmitter is used to push updates to the statusFlux subscribers.
     */
    private FluxSink<SimpleHealthStatus> statusEmitter;

    public SimpleHealthStatusProvider(String name) {
        this.name = name;
        // the first health status is going to be "initializing."
        reportUnhealthy("Initializing.");
    }

    /**
     * Get the name of this health status provider
     *
     * @return the name of this health status provider
     */
    public String getName() {
        return name;
    }

    /**
     * Convenience function to report the status as healthy without recording a message. The
     * synchronization is just to ensure that the properties are updated together.
     */
    public synchronized SimpleHealthStatus reportHealthy() {
        return reportHealthy("");
    }

    /**
     * Convenience function to report a healthy status with an associated message.
     *
     * @param message the message that will be recorded with the health status
     * @return the new health status instance
     */
    public synchronized SimpleHealthStatus reportHealthy(@Nonnull String message) {
        SimpleHealthStatus newHealthStatus = new SimpleHealthStatus(true, message, lastHealthStatus);
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
        SimpleHealthStatus newStatus = new SimpleHealthStatus(false,message, lastHealthStatus);
        setHealthStatus(newStatus);
        return newStatus;
    }

    /**
     * Provides access to an asynchronous "hot" stream of status results.
     * @return a flux that one could subscribe to.
     */
    public synchronized Flux<SimpleHealthStatus> getStatusStream() {
        if (statusFlux == null) {
            // create a new status flux
            statusFlux = Flux.create(emitter -> statusEmitter = emitter);
            // start publishing immediately w/o waiting for a consumer to signal demand.
            // Future subscribers will pick up on future statuses
            statusFlux.publish().connect();
        }
        return statusFlux;
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
        // no-op if the same
        if (newStatus.equals(lastHealthStatus)) {
            logger.warn("Status for {} is the same, won't update", this.getName());
            return;
        }

        if ((lastHealthStatus != null) && (newStatus.getSince() != lastHealthStatus.getSince())) {
            // log about the health state change
            if (newStatus.isHealthy()) {
                logger.info("Health Check {} is now healthy.", this::getName);
            } else {
                logger.warn("Health Check {} is now unhealthy({})", this::getName, newStatus::getDetails);
            }
        }
        lastHealthStatus = newStatus;

        // publish to the status emitter, if it exists.
        if (statusEmitter != null) {
            // TODO: if no active consumer demand, shut down the emitter and fluxes.
            statusEmitter.next(newStatus);
        }
    }
}

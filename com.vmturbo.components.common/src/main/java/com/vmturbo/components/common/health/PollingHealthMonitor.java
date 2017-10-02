package com.vmturbo.components.common.health;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * Polling Health Monitor implements a scheduled health check.
 */
public abstract class PollingHealthMonitor extends SimpleHealthStatusProvider {
    private Logger log = LogManager.getLogger();

    private final ScheduledExecutorService healthCheckScheduler = Executors.newScheduledThreadPool(1);

    /**
     * How often to execute the health check, in seconds. Defaults to a minute.
     */
    private double pollingIntervalSecs = 60;

    /**
     * We will, on request, support a live stream of status updates. The statusFlux can be subscribed
     * to for recieving these updates.
     */
    private Flux<SimpleHealthStatus> statusFlux;

    /**
     * The statusEmitter is used to push updates to the statusFlux subscribers.
     */
    private FluxSink<SimpleHealthStatus> statusEmitter;

    /**
     * Construct the instance with the selected polling interval
     * @param intervalSecs the polling interval to use, in seconds
     */
    public PollingHealthMonitor(double intervalSecs)  {
        if (intervalSecs <= 0) {
            throw new IllegalArgumentException("Polling interval "+ pollingIntervalSecs +" requested, but must be greater than zero.");
        }

        pollingIntervalSecs = intervalSecs;
        scheduleHealthChecks();
    }

    /**
     * Get the polling interval that the PollingHealthMonitor is currently using.
     *
     * @return the current polling interval setting (in seconds)
     */
    public double getPollingInterval() { return pollingIntervalSecs; }

    /**
     * Use changes to health status to feed into an optional stream
     * @param newStatus the new status to provide.
     */
    @Override
    protected synchronized void setHealthStatus(final SimpleHealthStatus newStatus) {
        // no-op if the same
        if (newStatus.equals(getHealthStatus())) {
            return;
        }

        super.setHealthStatus(newStatus);
        // publish to the status emitter, if it exists.
        if (statusEmitter != null) {
            // TODO: if no active consumer demand, shut down the emitter and fluxes.
            statusEmitter.next(newStatus);
        }
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

    private void scheduleHealthChecks() {
        // the first check will be immediate
        long pollingIntervalMillis = Math.round(pollingIntervalSecs * 1000); // convert secs to millis
        ScheduledFuture schedule = healthCheckScheduler.scheduleWithFixedDelay(
                new HealthCheckRunner(), 0, pollingIntervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * updateHealthStatus is expected to result in a report of a healthy/unhealthy status on the monitor.
     */
    abstract public void updateHealthStatus();

    /**
     * Implements the scheduled health check
     */
    private class HealthCheckRunner implements Runnable {

        /**
         * call the health check function every time the run() method is called.
         */
        @Override
        public void run() {
            // remember our last health status so we can log changed results
            boolean wasHealthyLastTime = getHealthStatus().isHealthy();

            try {
                updateHealthStatus();
            } catch(Throwable t) {
                // Treat all errors on this check as a sign of unhealthiness -- we'll be checking
                // again later.
                reportUnhealthy("Error:"+ t.toString());
            } finally {
                // check if the health status has changed -- if so, log a message about it.
                boolean isHealthyNow = getHealthStatus().isHealthy();
                if (wasHealthyLastTime != isHealthyNow) {
                    // log the change in status.
                    if (isHealthyNow) {
                        log.info(this.getClass().getSimpleName() +" is now healthy.");
                    } else {
                        log.warn(this.getClass().getSimpleName() +" is now unhealthy: "+ getHealthStatus().getDetails());
                    }
                }
            }
        }
    }

}


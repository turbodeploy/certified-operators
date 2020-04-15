package com.vmturbo.components.common.health;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Polling Health Monitor implements a scheduled health check.
 *
 * Important: Make sure to call stop() if you plan to dispose of the health check.
 */
public abstract class PollingHealthMonitor extends SimpleHealthStatusProvider {
    private Logger log = LogManager.getLogger();

    private static final long INITIAL_CHECK_DELAY_MS = 10;

    protected static final ScheduledExecutorService healthCheckScheduler = Executors.newScheduledThreadPool(1);

    /**
     * How often to execute the health check, in seconds. Defaults to a minute.
     */
    private double pollingIntervalSecs = 60;

    private ScheduledFuture scheduledFuture;

    /**
     * Construct the instance with the selected polling interval
     * @param name the name of the health monitor
     * @param intervalSecs the polling interval to use, in seconds
     */
    public PollingHealthMonitor(String name, double intervalSecs)  {
        super(name);
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
     * Stop this polling health monitor, if it is running. It's important to call this method when
     * cleaning up, as failure to do so will leave a running reference to this check in the scheduler.
     */
    public synchronized void stop() {
        if (scheduledFuture != null) {
            log.info("Cancelling polling health monitor {}", getName());
            scheduledFuture.cancel(true);
            scheduledFuture = null;
        }
    }

    /**
     * Use changes to health status to feed into an optional stream
     * @param newStatus the new status to provide.
     */
    @Override
    protected synchronized void setHealthStatus(final SimpleHealthStatus newStatus) {
        super.setHealthStatus(newStatus);
    }

    private synchronized void scheduleHealthChecks() {
        // if this is already scheduled, do nothing
        if (scheduledFuture != null) {
            log.warn("Polling health monitor {} already scheduled -- will not reschedule.", getName());
            return;
        }
        // the first check will be almost immediate
        long pollingIntervalMillis = Math.round(pollingIntervalSecs * 1000); // convert secs to millis
        scheduledFuture = healthCheckScheduler.scheduleWithFixedDelay(
                new HealthCheckRunner(), INITIAL_CHECK_DELAY_MS, pollingIntervalMillis, TimeUnit.MILLISECONDS);
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
            try {
                updateHealthStatus();
            } catch(Throwable t) {
                // Treat all errors on this check as a sign of unhealthiness -- we'll be checking
                // again later.
                log.warn("Exception while checking health", t);
                reportUnhealthy("Error:"+ t.toString());
            }
        }
    }

}


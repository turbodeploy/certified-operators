package com.vmturbo.components.common.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Create gatherers for "scheduled" component metrics. Since these are typically observations about
 * the environment (e.g. system uptime), rather than about component activity, these operate on a
 * fixed interval of observation, rather than being updated as part of specific activities being
 * measured.
 *
 * This is a singleton class.
 */
public class ScheduledMetrics {
    private Logger logger = LogManager.getLogger();

    // this is a singleton class
    private static ScheduledMetrics instance = null;

    // the list of metric observers we want to trigger on the schedule
    private List<ScheduledMetricsObserver> observers = new ArrayList() {
        @Override
        public boolean add(Object o) {
            logger.debug("Adding observer " + o);
            return super.add(o);
        }
    };

    // the scheduler -- we aren't necessarily looking for blazing performance here, so let's start
    // with a single threaded pool.
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    /**
     * Create a scheduled metrics collector with the specified parameters.
     * @param periodMs the amount of time (in milliseconds) to wait between observations
     * @param initialDelayMs the amount of time to wait before making the first observation
     */
    private ScheduledMetrics(long periodMs, long initialDelayMs) {
        instance = this;
        // add the default observers
        observers.add(new JVMRuntimeMetrics());
        observers.add(ComponentLifespanMetrics.getInstance());
        if (NativeMemoryTrackingMetrics.isSupported()) {
            logger.info("Adding Native Memory tracking metrics.");
            observers.add(new NativeMemoryTrackingMetrics());
        }

        scheduler.scheduleWithFixedDelay(this::collectMetrics, initialDelayMs, periodMs, TimeUnit.MILLISECONDS);
        logger.info("ScheduledMetrics scheduled to collect every {} ms.", periodMs);
    }

    /**
     * Configure the scheduled metrics instance, if it wasn't yet created.
     * @param periodInMs
     * @param initialDelayMs
     * @return
     */
    public static synchronized ScheduledMetrics initializeScheduledMetrics(long periodInMs, long initialDelayMs) {
        if (instance == null) {
            // initialize the scheduled metrics
            instance = new ScheduledMetrics(periodInMs, initialDelayMs);
        }
        return instance;
    }

    private void collectMetrics() {
        logger.debug("Observing scheduled metrics");
        for (ScheduledMetricsObserver observer : observers) {
            observer.observe();
        }
    }

    public interface ScheduledMetricsObserver {
        void observe();
    }
}

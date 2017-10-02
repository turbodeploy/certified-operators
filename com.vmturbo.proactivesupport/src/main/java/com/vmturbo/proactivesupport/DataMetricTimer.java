package com.vmturbo.proactivesupport;

import java.util.function.Consumer;

/**
 * Timer utility class for capturing time measurements (in seconds!)
 */
public class DataMetricTimer implements AutoCloseable {
    private static final double NANOS_PER_SECOND = 1E9;
    /*
     * NO_OBSERVATION is a reserved value for timeElapsedSecs that represents the state when an
     * observation has not been made on the timer yet.
     */
    private static final double NO_OBSERVATION = -1;

    private Consumer<Double> updater;
    private long startTime;
    private double timeElapsedSecs = NO_OBSERVATION;

    /**
     * Instantiate a timer that isn't associated with a metric updater function. You can use this
     * for generic time measurement if you really want a timer that tracks things in seconds.
     */
    public DataMetricTimer() {
        start();
    }

    /**
     * Instantiate a DataMetricTimer that will call the updater function when it's completed.
     *
     * @param updater the update interface to call
     */
    public DataMetricTimer(Consumer<Double> updater) {
        this.updater = updater;
        start(); // start instantly by default. a subsequent start() call will just move the start
                 // time up.
    }

    /**
     * Mark the start of a timing window.
     *
     * @return a reference to itself, to facilitate try-with-resources
     */
    public DataMetricTimer start() {
        startTime = System.nanoTime();
        return this;
    }

    /**
     * Check the time elapsed (in seconds) since the timer was started. If there is an associated
     * data metric, it will also record the observation to the associated metric.
     *
     * @return the time elapsed since the timer was started, in seconds.
     */
    public double observe() {
        timeElapsedSecs = calculateTimeElapsedSecs();

        // record the timer event with the associated metric
        if (updater != null) {
            updater.accept(timeElapsedSecs);
        }
        return timeElapsedSecs;
    }

    /**
     * As an alternative to start/observe, you can use the time(...) utility function to time a
     * Runnable instance. This timer will be restarted, then call the run() method on the runnable
     * instance, then observe the timer and record the duration when the run() completes.
     *
     * @param runnable The runnable thread to time the run() execution for
     * @return the length of time (in seconds) it took for run() to complete
     */
    public double time(Runnable runnable) {
        start();

        double durationSecs;
        try {
            runnable.run();
        }
        finally {
            durationSecs = observe();
        }
        return durationSecs;
    }

    private double calculateTimeElapsedSecs() {
        long durationNanos = System.nanoTime() - startTime;
        // convert duration to seconds.
        return (durationNanos / NANOS_PER_SECOND);
    }

    /**
     * How long did this timer run? This method will return current time elapsed, if the timer has
     * not been observed yet, otherwise the last observation will be returned.
     *
     * @return the time elapsed, in seconds.
     */
    public double getTimeElapsedSecs() {
        if (timeElapsedSecs == NO_OBSERVATION) {
            // no observation yet -- return current time elapsed
            return calculateTimeElapsedSecs();
        }
        // we have an observation to report -- use that instead
        return timeElapsedSecs;
    }

    @Override
    public void close() {
        observe();
    }
}

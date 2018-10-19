package com.vmturbo.cost.component.reserved.instance;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsStore;

/**
 * The class defined the scheduler for buy reserved instance analysis. And for the first time
 * to run buy reserved instance analysis, it use a initialIntervalHours parameter to defined
 * the scheduler interval, after first run succeed, it use a normalIntervalHours to defined
 * the scheduler interval. The reason is that, if Cost component is down, we want to trigger
 * buy reserved instance analysis immediately and its frequency should be less than normal
 * frequency, after first run succeed, we should change its frequency to normal interval.
 */
public class BuyRIAnalysisScheduler {

    private final Logger logger = LogManager.getLogger();

    private final ScheduledExecutorService schedulerExecutor;

    private ScheduledFuture<?> scheduledTask;

    private final ComputeTierDemandStatsStore computeTierDemandStatsStore;

    // The delay milliseconds for the initial buy reserved instance analysis.
    private static long INITIAL_DELAY_MILLIS = 300000;

    public BuyRIAnalysisScheduler(@Nonnull final ScheduledExecutorService schedulerExecutor,
                                  @Nonnull final ComputeTierDemandStatsStore computeTierDemandStatsStore,
                                  final long initialIntervalHours,
                                  final long normalIntervalHours) {
        this.schedulerExecutor = Objects.requireNonNull(schedulerExecutor);
        this.computeTierDemandStatsStore = Objects.requireNonNull(computeTierDemandStatsStore);
        this.scheduledTask = createInitialScheduledTask(initialIntervalHours,
                normalIntervalHours);
    }

    /**
     * Set a new interval to current scheduler. Note that after call this function, it will trigger
     * buy RI analysis immediately.
     *
     * @param newInterval a new interval value.
     * @param unit {@link TimeUnit}.
     * @return the new interval value has been set.
     */
    public long setBuyRIAnalysisSchedule(final long newInterval, @Nonnull final TimeUnit unit) {
        if (newInterval < 0) {
            throw new IllegalArgumentException("Illegal new interval value: " + newInterval);
        }
        final long newIntervalMillis = TimeUnit.MILLISECONDS.convert(newInterval, unit);
        logger.info("Setting buy reserved instance analysis interval to: " + newIntervalMillis + "" +
                " milliseconds");
        // cancel the ongoing scheduled task.
        scheduledTask.cancel(false);
        final ScheduledFuture<?> newScheduledTask = schedulerExecutor.scheduleAtFixedRate(
                this::normalTriggerBuyRIAnalysis,
                0,
                newIntervalMillis,
                TimeUnit.MILLISECONDS
        );
        scheduledTask = newScheduledTask;
        logger.info("Successfully setting buy reserved instance analysis interval to: "
                + newIntervalMillis + " milliseconds");
        return newIntervalMillis;
    }

    /**
     * Create a {@link ScheduledFuture} for the first time to trigger buy reserved instance analysis.
     *
     * @param initialIntervalHours the interval value of initial run.
     * @param normalIntervalHours the interval value of normal run.
     * @return a {@link ScheduledFuture}.
     */
    private ScheduledFuture<?> createInitialScheduledTask(final long initialIntervalHours,
                                                          final long normalIntervalHours) {
        final long intervalMillis = TimeUnit.MILLISECONDS.convert(initialIntervalHours, TimeUnit.HOURS);
        final ScheduledFuture<?> initialScheduledTask = schedulerExecutor.scheduleAtFixedRate(
                () -> initialTriggerBuyRIAnalysis(normalIntervalHours),
                INITIAL_DELAY_MILLIS,
                intervalMillis,
                TimeUnit.MILLISECONDS
        );
        return initialScheduledTask;
    }

    /**
     * Create a {@link ScheduledFuture} for the normally trigger buy reserved instance analysis.
     *
     * @param normalIntervalHours the interval value of normal run.
     * @return a {@link ScheduledFuture}.
     */
    private ScheduledFuture<?> createNormalScheduledTask(final long normalIntervalHours) {
        final long intervalMillis = TimeUnit.MILLISECONDS.convert(normalIntervalHours, TimeUnit.HOURS);

        final ScheduledFuture<?> normallyScheduledTask = schedulerExecutor.scheduleAtFixedRate(
                this::normalTriggerBuyRIAnalysis,
                intervalMillis,
                intervalMillis,
                TimeUnit.MILLISECONDS
        );
        return normallyScheduledTask;
    }

    /**
     * Trigger buy reserved instance analysis for the first time. If first time run succeed,
     * it will change scheduler interval to normal frequency.
     *
     * @param normalIntervalHours the interval value of normal run.
     */
    @VisibleForTesting
    public void initialTriggerBuyRIAnalysis(final long normalIntervalHours) {
        try {
            logger.info("Start running initially buy reserved instance analysis...");
            if (computeTierDemandStatsStore.containsDataOverWeek()) {
                //TODO: call buy reserved instance analysis algorithm.

                // if initially buy reserved instance analysis succeed, need to cancel the initial scheduled
                // task, and also create a new scheduled task with different interval time.
                scheduledTask.cancel(false);
                scheduledTask = createNormalScheduledTask(normalIntervalHours);
                logger.info("Finished initially buy reserved instance analysis...");
            } else {
                logger.info("There is no over one week data available, waiting for more data to" +
                        " trigger buy RI analysis.");
            }

        } catch (RuntimeException e) {
            logger.error("Unable to run initially buy reserved instance analysis: {}", e);
        }
    }

    /**
     * Trigger buy reserved instance analysis in normal frequency.
     */
    private void normalTriggerBuyRIAnalysis() {
        try {
            logger.info("Start running normally buy reserved instance analysis...");
            //TODO: call buy reserved instance analysis algorithm.
            logger.info("Finished normally buy reserved instance analysis...");
        } catch (RuntimeException e) {
            logger.error("Unable to run normally buy reserved instance analysis: {}", e);
        }
    }
}

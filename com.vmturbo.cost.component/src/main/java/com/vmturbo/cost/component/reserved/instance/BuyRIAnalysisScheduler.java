package com.vmturbo.cost.component.reserved.instance;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisInvoker;

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

    private final ReservedInstanceAnalysisInvoker invoker;

    public BuyRIAnalysisScheduler(@Nonnull final ScheduledExecutorService schedulerExecutor,
                                  @Nonnull ReservedInstanceAnalysisInvoker invoker,
                                  final long normalIntervalHours) {
        this.schedulerExecutor = Objects.requireNonNull(schedulerExecutor);
        this.invoker = invoker;
        this.scheduledTask = createNormalScheduledTask(normalIntervalHours);
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
     * Trigger buy reserved instance analysis in normal frequency.
     */
    protected void normalTriggerBuyRIAnalysis() {
        try {
            logger.info("Triggering RI Buy Analysis.");
            final StartBuyRIAnalysisRequest startBuyRIAnalysisRequest = invoker.getStartBuyRIAnalysisRequest();
            invoker.invokeBuyRIAnalysis(startBuyRIAnalysisRequest);
            logger.info("Finished RI Buy Analysis.");
        } catch (RuntimeException e) {
            logger.error("Unable to run RI Buy Analysis.", e);
        }
    }
}

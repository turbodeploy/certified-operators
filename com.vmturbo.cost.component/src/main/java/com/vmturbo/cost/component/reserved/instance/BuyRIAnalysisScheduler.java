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
 * This class defines the scheduler for Buy RI Analysis. First run is scheduled one hour after Cost starts.
 * Every subsequent run is scheduled as directed by normalBuyRIAnalysisIntervalHours configuration parameter.
 */
public class BuyRIAnalysisScheduler {
    private final Logger logger = LogManager.getLogger();

    private final ScheduledExecutorService schedulerExecutor;

    private ScheduledFuture<?> scheduledTask;

    private final ReservedInstanceAnalysisInvoker invoker;

    long riBuyIntervalHr = 1;

    public BuyRIAnalysisScheduler(@Nonnull final ScheduledExecutorService schedulerExecutor,
                                  @Nonnull ReservedInstanceAnalysisInvoker invoker,
                                  final long riBuyIntervalHr) {
        this.schedulerExecutor = Objects.requireNonNull(schedulerExecutor);
        this.invoker = invoker;
        this.riBuyIntervalHr = riBuyIntervalHr;
        this.scheduledTask = createNormalScheduledTask(riBuyIntervalHr);
    }

    /**
     * Set a new interval to current scheduler. Note that after this function's called, it triggers
     * the Buy RI Analysis immediately.
     *
     * @param newInterval a new interval value.
     * @param unit {@link TimeUnit}.
     * @return the new interval for RI Buy Analysis.
     */
    public long setBuyRIAnalysisSchedule(final long newInterval, @Nonnull final TimeUnit unit) {
        if (newInterval < 0) {
            throw new IllegalArgumentException("Illegal new interval value: " + newInterval);
        }
        final long newIntervalMillis = TimeUnit.MILLISECONDS.convert(newInterval, unit);
        logger.info("Setting Buy RI Analysis interval to {} ms", newIntervalMillis);
        // cancel the ongoing scheduled task.
        scheduledTask.cancel(false);
        final ScheduledFuture<?> newScheduledTask = schedulerExecutor.scheduleAtFixedRate(
                this::normalTriggerBuyRIAnalysis,
                0,  // Trigger first run without delay.
                newIntervalMillis,
                TimeUnit.MILLISECONDS);
        scheduledTask = newScheduledTask;
        return newIntervalMillis;
    }

    /**
     * Create a {@link ScheduledFuture} for the normally trigger buy reserved instance analysis.
     *
     * @param riBuyIntervalHr the interval value of normal run.
     * @return a {@link ScheduledFuture}.
     */
    private ScheduledFuture<?> createNormalScheduledTask(final long riBuyIntervalHr) {
        final long intervalMillis = TimeUnit.MILLISECONDS.convert(riBuyIntervalHr, TimeUnit.HOURS);
        final ScheduledFuture<?> normallyScheduledTask = schedulerExecutor.scheduleAtFixedRate(
                this::normalTriggerBuyRIAnalysis,
                // First time - 1 hour after Cost starts. This is a heartbeat and telltale that the
                // system is healthy and working well. Every subsequent run is to start as directed
                // by "normalBuyRIAnalysisIntervalHours" config parameter.
                TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS),
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
            logger.info("Finished RI Buy Analysis. Next run is to start in {} hour(s)", riBuyIntervalHr);
        } catch (RuntimeException e) {
            logger.error("Unable to run RI Buy Analysis.", e);
        }
    }
}

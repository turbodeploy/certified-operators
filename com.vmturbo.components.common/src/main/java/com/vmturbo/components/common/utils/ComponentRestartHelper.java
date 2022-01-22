package com.vmturbo.components.common.utils;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Helper class to restart a component by killing the JVM if the component pipeline hasn't
 * succeeded for the given period of time.
 */
public class ComponentRestartHelper {

    private static final Logger logger = LogManager.getLogger();

    private final int failureHoursBeforeRestart;
    private final long failureMillisBeforeRestart;
    private final long startupTime = System.currentTimeMillis();
    private volatile long lastSuccessTime = -1;
    private volatile long lastFailureTime = -1;

    /**
     * Constructor.
     *
     * @param failureHoursBeforeRestart failure hours to wait before restart.
     */
    public ComponentRestartHelper(int failureHoursBeforeRestart) {
        this.failureHoursBeforeRestart = failureHoursBeforeRestart;
        this.failureMillisBeforeRestart = TimeUnit.MILLISECONDS.convert(failureHoursBeforeRestart,
                TimeUnit.HOURS);
        logger.info("Component pipelineFailureHours configured to {}", failureHoursBeforeRestart);
        // check every minute, with initial delay of 10 minutes
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::checkRestart, 10, 1,
                TimeUnit.MINUTES);
    }

    /**
     * Update the time when the pipeline last succeeded or failed.
     *
     * @param success whether it succeeded or not this cycle
     */
    public void updateResult(boolean success) {
        final long now = System.currentTimeMillis();
        if (success) {
            this.lastSuccessTime = now;
        } else {
            this.lastFailureTime = now;
        }
    }

    /**
     * Check if we need to restart component, if the failure time exceeds the threshold, it will
     * terminate the JVM to force a restart.
     */
    private void checkRestart() {
        try {
            if (lastSuccessTime == -1 && lastFailureTime == -1) {
                // the pipeline hasn't been invoked yet, no need to restart
                return;
            }

            final long delta;
            if (lastSuccessTime == -1) {
                // pipeline invoked, but never succeeded
                delta = System.currentTimeMillis() - startupTime;
            } else {
                delta = System.currentTimeMillis() - lastSuccessTime;
            }

            if (delta >= failureMillisBeforeRestart) {
                logger.info("Restarting component since pipeline keeps failing for {} hours"
                                + ", last success time: {}", failureHoursBeforeRestart,
                        lastSuccessTime != -1 ? new Date(lastSuccessTime) : "None");
                System.exit(1);
            }
        } catch (Exception e) {
            logger.error("Error when checking restarting the component", e);
        }
    }
}

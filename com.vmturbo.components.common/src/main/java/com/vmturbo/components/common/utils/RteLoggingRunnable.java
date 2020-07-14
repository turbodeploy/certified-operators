package com.vmturbo.components.common.utils;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A wrapper for a {@link Runnable} which guards from a {@link RuntimeException} thrown from
 * the underlying runnable. This class is useful when you want to add a scheduled task and do not
 * want it to be terminated after {@link RuntimeException} occurred
 */
public class RteLoggingRunnable implements Runnable {
    private final Logger logger = LogManager.getLogger(getClass());
    private final Runnable internalRunnable;
    private final String taskName;

    /**
     * Constructs {@link RuntimeException} catching and logging runnable.
     *
     * @param internalRunnable runnable to wrap
     * @param taskName task name (for out put in the logs, if {@link RuntimeException}
     *         occurred
     */
    public RteLoggingRunnable(@Nonnull Runnable internalRunnable, @Nonnull String taskName) {
        this.internalRunnable = Objects.requireNonNull(internalRunnable);
        this.taskName = Objects.requireNonNull(taskName);
    }

    @Override
    public void run() {
        try {
            internalRunnable.run();
        } catch (RuntimeException e) {
            logger.error("Failed executing \"" + taskName + '\"', e);
        }
    }
}

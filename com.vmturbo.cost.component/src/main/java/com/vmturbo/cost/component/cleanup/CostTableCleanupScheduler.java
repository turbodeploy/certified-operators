package com.vmturbo.cost.component.cleanup;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.vmturbo.components.api.SetOnce;
import com.vmturbo.cost.component.cleanup.CostTableCleanup.Trimmer;

/**
 * Class for trimming and cleaning up the Cost Stats tables (RI utilization and coverage, entity costs etc)
 * based on retention periods. Runs on a scheduler.
 */
public class CostTableCleanupScheduler {

    private static final Logger logger = LogManager.getLogger();

    private final Map<ScheduledCleanupInformation, CostTableCleanupTask> scheduledCleanups = new HashMap<>();

    private final Object scheduledCleanupsLock = new Object();

    private final ExecutorService executorService;

    private final CleanupTaskFactory cleanupFactory;

    private final ThreadPoolTaskScheduler schedulerExecutor;

    private final Duration cleanupInterval;

    /**
     * The tables to clean up.
     */
    private final List<CostTableCleanup> tables;


    /**
     * Constructor for the Cost stats cleanup scheduler.
     *
     * @param tables the tables to be cleaned up.
     * @param executorService The executor service to cleanup the tables.
     * @param taskScheduler The task scheduler which invokes the cleanups.
     * @param cleanupInterval The period after which the cleanup scheduler runs.
     */
    public CostTableCleanupScheduler(@Nonnull final List<CostTableCleanup> tables,
                                     @Nonnull final ExecutorService executorService,
                                     @Nonnull final ThreadPoolTaskScheduler taskScheduler,
                                     @Nonnull final Duration cleanupInterval) {
        this (tables, executorService, CostTableCleanupTask::new, taskScheduler, cleanupInterval);
    }

    /**
     * Constructor for the Cost stats cleanup scheduler.
     *
     * @param tables the tables to be cleaned up.
     * @param executorService The executor service to cleanup the tables.
     * @param cleanupFactory The cleanup task factory
     * @param taskScheduler The task scheduler which invokes the cleanups.
     * @param cleanupInterval period after which the cleanup scheduler runs.
     */
    public CostTableCleanupScheduler(@Nonnull final List<CostTableCleanup> tables,
                                     @Nonnull final ExecutorService executorService,
                                     @Nonnull CleanupTaskFactory cleanupFactory,
                                     @Nonnull ThreadPoolTaskScheduler taskScheduler,
                                     @Nonnull final Duration cleanupInterval) {
        this.tables = tables;
        this.cleanupFactory = cleanupFactory;
        this.executorService = executorService;
        this.schedulerExecutor = taskScheduler;
        this.cleanupInterval = cleanupInterval;

        initializeCleanupSchedule();
    }

    /**
     * Method to schedule the cleanups.
     */
   public void scheduleCleanups() {
        synchronized (scheduledCleanupsLock) {

            List<Callable<Object>> cleanups = new ArrayList<>();
            for (final CostTableCleanup table : tables) {
                final LocalDateTime trimToTime = table.getTrimTime();
                final ScheduledCleanupInformation cleanupInfo = ImmutableScheduledCleanupInformation.builder()
                        .trimToTime(trimToTime)
                        .tableWriter(table.writer())
                        .build();
                final CostTableCleanupTask existingCleanup = scheduledCleanups.get(cleanupInfo);
                final boolean existingStillRunning = existingCleanup != null
                        && !existingCleanup.completionStatus().isPresent();
                // Although we removed completed cleanups earlier in the function, some of them
                // may have completed since then.
                final boolean existingSucceeded = existingCleanup != null
                        && existingCleanup.completionStatus().orElse(false);
                if (!existingStillRunning && !existingSucceeded) {
                    logger.info("Scheduling cleanup [Table Name={}, Trim Time={}]",
                            table.tableInfo().shortTableName(), trimToTime);
                    final CostTableCleanupTask newCleanup = cleanupFactory.newCleanup(cleanupInfo);
                    scheduledCleanups.put(cleanupInfo, newCleanup);
                    cleanups.add(Executors.callable(newCleanup));
                } else {
                    logger.info("Skipping scheduling of cleanup {} because {}",
                            cleanupInfo, existingStillRunning
                                    ? "it's still running."
                                    : "it already succeeded.");
                }
            }

            final int beforeSize = scheduledCleanups.size();
            List<Future<Object>> futures = new ArrayList<>();
            try {
                futures = executorService.invokeAll(cleanups);
            } catch (InterruptedException e) {
                logger.error("Interrupted Exception encountered while cleaning the Cost stats tables.", e);
            }
            for (Future cleanupFuture : futures) {
                try {
                    cleanupFuture.get();
                } catch (InterruptedException | ExecutionException e) {
                    logger.error("Execution Exception encountered while cleaning the Cost stats tables.", e);
                }
            }
            if (scheduledCleanups.values().removeIf(s -> s.completionStatus().isPresent() && s.completionStatus().get())) {
                logger.info("Trimmed {} completed cleanups.", beforeSize - scheduledCleanups.size());
            }
        }
    }

    /**
     * Information about a scheduled cleanup.
     */
    @Value.Immutable
    public interface ScheduledCleanupInformation {

        /**
         * The time to trim against.
         * @return The time to trim against.
         */
        @Nonnull
        LocalDateTime trimToTime();

        /**
         * The table trimmer.
         * @return The table trimmer.
         */
        @Nonnull
        Trimmer tableWriter();
    }

    /**
     * Factory class for {@link CostTableCleanupTask}s, for Dependency Injection
     * (mainly for unit tests).
     */
    @FunctionalInterface
    interface CleanupTaskFactory {

        /**
         * Creates a new cleanup task.
         * @param scheduledCleanupInfo The cleanup info.
         * @return The newly created task.
         */
        @Nonnull
        CostTableCleanupTask newCleanup(@Nonnull ScheduledCleanupInformation scheduledCleanupInfo);
    }


    private void initializeCleanupSchedule() {
        logger.info("Running the Cost Stat Tables cleanup scheduler.");
        schedulerExecutor.scheduleWithFixedDelay(this::scheduleCleanups, cleanupInterval);
    }

    @VisibleForTesting
    Map<ScheduledCleanupInformation, CostTableCleanupTask> getScheduledCleanups() {
        synchronized (scheduledCleanupsLock) {
            return ImmutableMap.copyOf(scheduledCleanups);
        }
    }

    /**
     * A cleanup task.
     */
    @VisibleForTesting
    static class CostTableCleanupTask implements Runnable {

        private final ScheduledCleanupInformation cleanupInfo;

        private final SetOnce<Boolean> completed = new SetOnce<>();

        CostTableCleanupTask(@Nonnull final ScheduledCleanupInformation cleanupInfo) {
            this.cleanupInfo = cleanupInfo;
        }

        @Override
        public void run() {
            try {
                logger.debug("Starting cleanup of Cost stat table:{}", cleanupInfo);
                cleanupInfo.tableWriter().trim(cleanupInfo.trimToTime());
                completed.trySetValue(true);
                logger.debug("Completed cleanup of Cost stat table: {}", cleanupInfo);
            } catch (RuntimeException e) {
                logger.error("Failed to roll up {}. Error: {}",
                        cleanupInfo, e.getLocalizedMessage());
                completed.trySetValue(false);
            }
        }

        public Optional<Boolean> completionStatus() {
            return completed.getValue();
        }
    }
}

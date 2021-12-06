package com.vmturbo.cost.component.cleanup;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cost.component.cleanup.CostTableCleanup.TableCleanupInfo;
import com.vmturbo.cost.component.cleanup.CostTableCleanup.TrimTimeResolver;
import com.vmturbo.cost.component.cleanup.CostTableCleanup.Trimmer;

/**
 * A cleanup worker, responsible for a single {@link CostTableCleanup} configuration. The worker
 * is responsible for creating and observing discrete cleanup tasks for the target table.
 */
public class TableCleanupWorker {

    private final Logger logger = LogManager.getLogger();

    private final AtomicBoolean isLongRunning = new AtomicBoolean(false);

    private final CostTableCleanup tableCleanup;

    private final TableCleanupInfo cleanupInfo;

    private final ExecutorService taskExecutorService;

    private final Semaphore cleanupLock = new Semaphore(1);

    private TableCleanupWorker(@Nonnull CostTableCleanup tableCleanup,
                               @Nonnull ExecutorService taskExecutorService) {

        this.tableCleanup = Objects.requireNonNull(tableCleanup);
        this.cleanupInfo = tableCleanup.tableInfo();
        this.taskExecutorService = Objects.requireNonNull(taskExecutorService);
    }

    /**
     * The {@link TableCleanupInfo} for this worker.
     * @return The {@link TableCleanupInfo} for this worker.
     */
    @Nonnull
    public TableCleanupInfo cleanupInfo() {
        return cleanupInfo;
    }

    /**
     * Determines whether a current cleanup task is long running, based on {@link TableCleanupInfo#longRunningDuration()}.
     * @return Determines whether a current cleanup task is long running, based
     * on {@link TableCleanupInfo#longRunningDuration()}.
     */
    public boolean isLongRunning() {
        return isLongRunning.get();
    }

    /**
     * Creates and invokes a cleanup task, if one is not already running.
     */
    public void cleanupTable() {

        if (cleanupLock.tryAcquire()) {
            try {

                logger.info("Creating cleanup task for '{}'", cleanupInfo.shortTableName());
                isLongRunning.set(false);

                final int retryLimit = cleanupInfo.retryLimit();
                int failureCount = 0;

                boolean cleanupSuccess;
                while (!(cleanupSuccess = invokeCleanupTask()) && ++failureCount < retryLimit) {
                    logger.warn("Cleanup of '{}' failed. Rescheduling in '{}' (Failure Count={}, Retry Limit={})",
                            cleanupInfo.retryDelay(), cleanupInfo.shortTableName(), failureCount, retryLimit);
                    Uninterruptibles.sleepUninterruptibly(cleanupInfo.retryDelay());
                }

                if (cleanupSuccess) {
                    logger.info("Cleanup of '{}' succeeded", cleanupInfo.shortTableName());
                } else {
                    logger.error("Cleanup of '{}' failed up to the retry limit", cleanupInfo.shortTableName());
                }
            } finally {

                isLongRunning.set(false);
                cleanupLock.release();
            }
        }
    }

    private boolean invokeCleanupTask() {

        final TableTrimTask trimTask = TableTrimTask.create(tableCleanup);
        final Future<Boolean> trimTaskFuture = taskExecutorService.submit(trimTask);
        final Stopwatch taskTimer = Stopwatch.createStarted();

        while (true) {
            try {
                return trimTaskFuture.get(cleanupInfo.longRunningDuration().toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {

                isLongRunning.set(true);
                logger.warn("Cleanup task for '{}' is long running (Duration={})",
                        cleanupInfo.shortTableName(), taskTimer);

            } catch (Exception e) {
                logger.warn("Clean task for '{}' threw an exception", cleanupInfo.shortTableName(), e);
                return false;
            }
        }
    }

    /**
     * A factory class for producing {@link TableCleanupWorker} instances.
     */
    public static class TableCleanupWorkerFactory {

        private final ExecutorService taskExecutorService;

        /**
         * Constructs a new {@link TableCleanupWorkerFactory} instance.
         * @param executorService The executor service, used to run cleanup tasks.
         */
        public TableCleanupWorkerFactory(@Nonnull ExecutorService executorService) {
            this.taskExecutorService = Objects.requireNonNull(executorService);
        }

        /**
         * Creates a new {@link TableCleanupWorker} instance.
         * @param tableCleanup The table cleanup to manage.
         * @return Creates a new {@link TableCleanupWorker} instance.
         */
        @Nonnull
        public TableCleanupWorker createWorker(@Nonnull CostTableCleanup tableCleanup) {
            return new TableCleanupWorker(tableCleanup, taskExecutorService);
        }

    }

    /**
     * A discrete cleanup task for a given {@link CostTableCleanup}.
     */
    private static class TableTrimTask implements Callable<Boolean> {

        private final Logger logger = LogManager.getLogger();

        private final CostTableCleanup tableCleanup;

        private TableTrimTask(@Nonnull CostTableCleanup tableCleanup) {
            this.tableCleanup = Objects.requireNonNull(tableCleanup);
        }

        @Override
        public Boolean call() throws Exception {
            try {

                final Trimmer tableTrimmer = tableCleanup.writer();
                final TrimTimeResolver trimTimeResolver = tableCleanup.trimTimeResolver();
                tableTrimmer.trim(trimTimeResolver);
                return true;

            } catch (Exception e) {
                logger.warn("Exception in clean up for '{}'",
                        tableCleanup.tableInfo().shortTableName(), e);
                return false;
            }
        }

        public static TableTrimTask create(@Nonnull CostTableCleanup tableCleanup) {
            return new TableTrimTask(tableCleanup);
        }
    }
}

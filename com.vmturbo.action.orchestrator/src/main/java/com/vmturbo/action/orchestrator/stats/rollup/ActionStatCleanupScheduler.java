package com.vmturbo.action.orchestrator.stats.rollup;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;
import com.vmturbo.proactivesupport.DataMetricGauge;

/**
 * Drives the cleanup of stats - removing action stats that are outside of the
 * configured retention periods.
 * <p>
 * The logic is similar to {@link ActionStatRollupScheduler}, but since we only need
 * one cleanup operation per table we just schedule all cleanups pretty much unconditionally.
 * <p>
 * The scheduled cleanups are not persisted anywhere, but each cleanups should be transactional. If
 * the action orchestrator restarts we can just re-schedule all possible cleanups by checking
 * the relevant tables.
 */
public class ActionStatCleanupScheduler {

    private static final Logger logger = LogManager.getLogger();

    private final Clock clock;

    /**
     * The tables to clean up.
     */
    private final List<ActionStatTable> tables;

    /**
     * The executor handling the asynchronous cleanups.
     */
    private final ExecutorService executorService;

    private final ActionStatCleanupFactory cleanupFactory;

    /**
     * The source of truth for retention periods.
     */
    private final RetentionPeriodFetcher retentionPeriodFetcher;

    /**
     * Currently scheduled {@link ActionStatCleanup}s. May contain some {@link ActionStatCleanup}s
     * that have already completed.
     */
    @GuardedBy("scheduledCleanupsLock")
    private final Map<ScheduledCleanupInfo, ActionStatCleanup> scheduledCleanups = new HashMap<>();

    private final Object scheduledCleanupsLock = new Object();

    private Instant lastCleanupTime = Instant.ofEpochMilli(0);

    private final long minMillisBetweenCleanups;

    public ActionStatCleanupScheduler(@Nonnull final Clock clock,
                                      @Nonnull final List<ActionStatTable> tables,
                                      @Nonnull final RetentionPeriodFetcher retentionPeriodFetcher,
                                      @Nonnull final ExecutorService executorService,
                                      final long minTimeBetweenCleanups,
                                      @Nonnull final TimeUnit timeBetweenCleanupsUnit) {
        this(clock, tables, retentionPeriodFetcher, executorService,
            minTimeBetweenCleanups, timeBetweenCleanupsUnit, ActionStatCleanup::new);
    }

    @VisibleForTesting
    ActionStatCleanupScheduler(@Nonnull final Clock clock,
                               @Nonnull final List<ActionStatTable> tables,
                              @Nonnull final RetentionPeriodFetcher retentionPeriodFetcher,
                              @Nonnull final ExecutorService executorService,
                              final long minTimeBetweenCleanup,
                              @Nonnull final TimeUnit timeBetweenCleanupsUnit,
                              @Nonnull final ActionStatCleanupFactory cleanupFactory) {
        this.clock = Objects.requireNonNull(clock);
        this.tables = Objects.requireNonNull(tables);
        this.executorService = Objects.requireNonNull(executorService);
        this.cleanupFactory = Objects.requireNonNull(cleanupFactory);
        this.retentionPeriodFetcher = Objects.requireNonNull(retentionPeriodFetcher);
        this.minMillisBetweenCleanups = timeBetweenCleanupsUnit.toMillis(minTimeBetweenCleanup);
    }


    /**
     * Schedule any possible cleanups.
     *
     * @return The number of cleanups scheduled by this call.
     */
    public int scheduleCleanups() {
        synchronized (scheduledCleanupsLock) {
            // Remove completed cleanups before we schedule new ones.
            // This means we will retry any failed cleanups.
            final int beforeSize = scheduledCleanups.size();
            if (scheduledCleanups.values().removeIf(cleanup -> cleanup.completionStatus().isPresent())) {
                logger.info("Trimmed {} completed cleanups.", beforeSize - scheduledCleanups.size());
            }

            final long millisSinceLastScheduling =
                Duration.between(lastCleanupTime, clock.instant()).toMillis();
            if (millisSinceLastScheduling < minMillisBetweenCleanups) {
                logger.info("Not scheduling cleanups for at least another {} millis.",
                    minMillisBetweenCleanups - millisSinceLastScheduling);
                return 0;
            }

            final RetentionPeriods retentionPeriods = retentionPeriodFetcher.getRetentionPeriods();

            int numQueued = 0;
            for (final ActionStatTable table : tables) {
                // We don't actually run a query to see if there are any rows to delete.
                // The number of tables is ~5, so we can just schedule a task for every table,
                // and just try the delete.
                final LocalDateTime trimToTime = table.getTrimTime(retentionPeriods);
                final ScheduledCleanupInfo cleanupInfo = ImmutableScheduledCleanupInfo.builder()
                    .trimToTime(trimToTime)
                    .tableWriter(table.writer())
                    .build();
                final ActionStatCleanup existingCleanup = scheduledCleanups.get(cleanupInfo);
                final boolean existingStillRunning = existingCleanup != null &&
                    !existingCleanup.completionStatus().isPresent();
                // Although we removed completed cleanups earlier in the function, some of them
                // may have completed since then.
                final boolean existingSucceeded = existingCleanup != null &&
                    existingCleanup.completionStatus().orElse(false);
                if (!existingStillRunning && !existingSucceeded) {
                    logger.info("Scheduling cleanup: {}", cleanupInfo);
                    final ActionStatCleanup newCleanup = cleanupFactory.newCleanup(cleanupInfo);
                    scheduledCleanups.put(cleanupInfo, newCleanup);
                    // We track the number queued separately from the metric, because
                    // the metric may be decremented at any time by one of the scheduled cleanups
                    // on a different thread.
                    numQueued++;
                    Metrics.NUM_CLEANUPS_QUEUED.increment();
                    executorService.submit(newCleanup);
                } else {
                    logger.info("Skipping scheduling of cleanup {} because {}",
                        cleanupInfo, existingStillRunning ?
                            "it's still running." : "it already succeeded.");
                }
            }

            // Set the last cleanup after we scheduled everything, so that if there is any
            // error during scheduling we would re-try sooner.
            lastCleanupTime = clock.instant();
            return numQueued;
        }
    }

    /**
     * Get the currently scheduled rollups. Used for testing and for debugging purposes.
     *
     * @return The {@link ActionStatCleanup}s queued by this {@link ActionStatCleanupScheduler}.
     *         All {@link ActionStatCleanup}s that have not yet completed are guaranteed to be in
     *         the returned map. Completed (succeeded or failed) cleanups may not be present -
     *         they are cleared as part of {@link ActionStatCleanupScheduler#scheduledCleanups}.
     */
    @Nonnull
    public Map<ScheduledCleanupInfo, ActionStatCleanup> getScheduledCleanups() {
        synchronized (scheduledCleanupsLock) {
            return ImmutableMap.copyOf(scheduledCleanups);
        }
    }

    /**
     * Information about a scheduled cleanup.
     */
    @Value.Immutable
    public interface ScheduledCleanupInfo {
        LocalDateTime trimToTime();

        ActionStatTable.Writer tableWriter();
    }

    /**
     * Factory class for {@link ActionStatCleanup}s, for Dependency Injection
     * (mainly for unit tests).
     */
    @FunctionalInterface
    interface ActionStatCleanupFactory {
        @Nonnull
        ActionStatCleanup newCleanup(@Nonnull final ScheduledCleanupInfo scheduledRollupInfo);
    }

    static class Metrics {

        static final DataMetricGauge NUM_CLEANUPS_QUEUED = DataMetricGauge.builder()
            .withName("ao_num_stat_cleanups_queued")
            .withHelp("Number of action stat cleanup operations currently queued.")
            .build()
            .register();

        static final DataMetricGauge NUM_CLEANUPS_RUNNING = DataMetricGauge.builder()
            .withName("ao_num_stat_cleanups_running")
            .withHelp("Number of action stat cleanups currently running.")
            .build()
            .register();
    }

    /**
     * The actual cleanup operation.
     */
    @VisibleForTesting
    static class ActionStatCleanup implements Runnable {

        private final ScheduledCleanupInfo cleanupInfo;

        private final SetOnce<Boolean> completed = new SetOnce<>();

        public ActionStatCleanup(@Nonnull final ScheduledCleanupInfo cleanupInfo) {
            this.cleanupInfo = cleanupInfo;
        }

        @Override
        public void run() {
            // No longer queued.
            Metrics.NUM_CLEANUPS_QUEUED.decrement();
            // Running now.
            Metrics.NUM_CLEANUPS_RUNNING.increment();
            try {
                logger.debug("Starting cleanup:{}", cleanupInfo);
                cleanupInfo.tableWriter().trim(cleanupInfo.trimToTime());
                completed.trySetValue(true);
                logger.debug("Completed cleanup: {}", cleanupInfo);
            } catch (RuntimeException e) {
                logger.error("Failed to roll up {}. Error: {}",
                    cleanupInfo, e.getLocalizedMessage());
                completed.trySetValue(false);
            } finally {
                // No longer running.
                Metrics.NUM_CLEANUPS_RUNNING.decrement();
            }

        }

        /**
         * Returns the completion status of the rollup.
         *
         * @return An empty {@link Optional} if the rollup has not finished. If finished, an
         *         {@link Optional} containing whether the rollup succeeded or failed otherwise.
         */
        public Optional<Boolean> completionStatus() {
            return completed.getValue();
        }
    }
}

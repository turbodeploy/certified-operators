package com.vmturbo.cost.component.stats;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;
import com.vmturbo.cost.component.stats.CostStatTable.Trimmer;

/**
 * Class for trimming and cleaning up the Cost Stats tables (RI utilization and coverage, entity costs etc)
 * based on retention periods. Runs on a scheduler.
 */
public class CostStatCleanupScheduler {

    private static final Logger logger = LogManager.getLogger();

    private final Map<ScheduledCleanupInformation, CostsStatCleanup> scheduledCleanups = new HashMap<>();

    private final Clock clock;

    private final Object scheduledCleanupsLock = new Object();

    private Instant lastCleanupTime = Instant.ofEpochMilli(0);

    private final RetentionPeriodFetcher retentionPeriodFetcher;

    private final long minMillisBetweenCleanups;

    private final ExecutorService executorService;

    private final CostStatCleanupFactory cleanupFactory;

    private final ThreadPoolTaskScheduler schedulerExecutor;

    private final long cleanupSchedulerPeriod;

    /**
     * The tables to clean up.
     */
    private final List<CostStatTable> tables;


    /**
     * Constructor for the Cost stats cleanup scheduler.
     *
     * @param clock the clock.
     * @param tables the tables to be cleaned up.
     * @param retentionPeriodFetcher the retention period fetcher containing info about retention
     *                               periods of tables.
     * @param executorService The executor service to cleanup the tables.
     * @param minTimeBetweenCleanup The minimum time between cleanups.
     * @param timeBetweenCleanupsUnit The unit of time of minimum time between cleanups.
     * @param taskScheduler The task scheduler which invokes the cleanups.
     * @param cleanupSchedulerPeriod The period after which the cleanup scheduler runs.
     */
    public CostStatCleanupScheduler(@Nonnull final Clock clock,
                                    @Nonnull final List<CostStatTable> tables,
                                    @Nonnull final RetentionPeriodFetcher retentionPeriodFetcher,
                                    @Nonnull final ExecutorService executorService,
                                    @Nonnull final long minTimeBetweenCleanup,
                                    @Nonnull final TimeUnit timeBetweenCleanupsUnit,
                                    @Nonnull final ThreadPoolTaskScheduler taskScheduler,
                                    @Nonnull final long cleanupSchedulerPeriod) {
        this (clock, tables, retentionPeriodFetcher, executorService, minTimeBetweenCleanup, timeBetweenCleanupsUnit,
                CostsStatCleanup::new, taskScheduler, cleanupSchedulerPeriod);
    }

    /**
     * Constructor for the Cost stats cleanup scheduler.
     *
     * @param clock the clock.
     * @param tables the tables to be cleaned up.
     * @param retentionPeriodFetcher the retention period fetcher containing info about retention
     *                                     periods of tables.
     * @param executorService The executor service to cleanup the tables.
     * @param minTimeBetweenCleanup The minimum time between cleanups.
     * @param timeBetweenCleanupsUnit The unit of time of minimum time between cleanups.
     * @param costStatCleanupFactory The reserved Instance Stat cleanup factory.
     * @param taskScheduler The task scheduler which invokes the cleanups.
     * @param cleanupSchedulerPeriod The period after which the cleanup scheduler runs.
     */
    public CostStatCleanupScheduler(@Nonnull final Clock clock,
                                    @Nonnull final List<CostStatTable> tables,
                                    @Nonnull final RetentionPeriodFetcher retentionPeriodFetcher,
                                    @Nonnull final ExecutorService executorService,
                                    @Nonnull final long minTimeBetweenCleanup,
                                    @Nonnull final TimeUnit timeBetweenCleanupsUnit,
                                    @Nonnull CostStatCleanupFactory costStatCleanupFactory,
                                    @Nonnull ThreadPoolTaskScheduler taskScheduler,
                                    @Nonnull final long cleanupSchedulerPeriod) {
        this.clock = clock;
        this.tables = tables;
        this.retentionPeriodFetcher = Objects.requireNonNull(retentionPeriodFetcher);
        this.minMillisBetweenCleanups = timeBetweenCleanupsUnit.toMillis(minTimeBetweenCleanup);
        this.cleanupFactory = costStatCleanupFactory;
        this.executorService = executorService;
        this.schedulerExecutor = taskScheduler;
        this.cleanupSchedulerPeriod = cleanupSchedulerPeriod;

        initializeCleanupSchedule();
    }

    /**
     * Method to schedule the cleanups.
     */
   public void scheduleCleanups() {
        synchronized (scheduledCleanupsLock) {
            final long millisSinceLastScheduling =
                    Duration.between(lastCleanupTime, clock.instant()).toMillis();
            if (millisSinceLastScheduling < minMillisBetweenCleanups) {
                logger.info("Not scheduling cleanups for at least another {} millis.",
                        minMillisBetweenCleanups - millisSinceLastScheduling);
                return;
            }
            List<Callable<Object>> cleanups = new ArrayList<>();
            final RetentionPeriods retentionPeriods = retentionPeriodFetcher.getRetentionPeriods();
            for (final CostStatTable table : tables) {
                final LocalDateTime trimToTime = table.getTrimTime(retentionPeriods);
                final ScheduledCleanupInformation cleanupInfo = ImmutableScheduledCleanupInformation.builder()
                        .trimToTime(trimToTime)
                        .tableWriter(table.writer())
                        .build();
                final CostsStatCleanup existingCleanup = scheduledCleanups.get(cleanupInfo);
                final boolean existingStillRunning = existingCleanup != null &&
                        !existingCleanup.completionStatus().isPresent();
                // Although we removed completed cleanups earlier in the function, some of them
                // may have completed since then.
                final boolean existingSucceeded = existingCleanup != null &&
                        existingCleanup.completionStatus().orElse(false);
                if (!existingStillRunning && !existingSucceeded) {
                    logger.info("Scheduling cleanup: {}", cleanupInfo);
                    final CostsStatCleanup newCleanup = cleanupFactory.newCleanup(cleanupInfo);
                    scheduledCleanups.put(cleanupInfo, newCleanup);
                    cleanups.add(Executors.callable(newCleanup));
                } else {
                logger.info("Skipping scheduling of cleanup {} because {}",
                        cleanupInfo, existingStillRunning ?
                                "it's still running." : "it already succeeded.");
            }
        }
            final int beforeSize = scheduledCleanups.size();
            List<Future<Object>> futures = new ArrayList<>();
            try {
                futures = executorService.invokeAll(cleanups);
            } catch (InterruptedException e) {
                logger.error("Interrupted Exception encountered while cleaning the Cost stats tables.", e);
            }
            for (Future cleanupFuture: futures) {
                try {
                    cleanupFuture.get();
                }catch (InterruptedException | ExecutionException e) {
                    logger.error("Execution Exception encountered while cleaning the Cost stats tables.", e);
                }
            }
            if (scheduledCleanups.values().removeIf(s -> s.completionStatus().isPresent() && s.completionStatus().get())) {
                logger.info("Trimmed {} completed cleanups.", beforeSize - scheduledCleanups.size());
            }

       // Set the last cleanup after we scheduled everything, so that if there is any
       // error during scheduling we would re-try sooner.
       lastCleanupTime = clock.instant();
        }
    }

    /**
     * Information about a scheduled cleanup.
     */
    @Value.Immutable
    public interface ScheduledCleanupInformation {
        LocalDateTime trimToTime();

        Trimmer tableWriter();
    }

    /**
     * Factory class for {@link CostsStatCleanup}s, for Dependency Injection
     * (mainly for unit tests).
     */
    @FunctionalInterface
    interface CostStatCleanupFactory {
        @Nonnull
        CostsStatCleanup newCleanup(@Nonnull ScheduledCleanupInformation scheduledRollupInfo);
    }

    public void initializeCleanupSchedule() {
        logger.info("Running the Cost Stat Tables cleanup scheduler.");
        schedulerExecutor.scheduleWithFixedDelay(this::scheduleCleanups, new Date(System.currentTimeMillis()), cleanupSchedulerPeriod );
    }

    @VisibleForTesting
    public Map<ScheduledCleanupInformation, CostsStatCleanup> getScheduledCleanups() {
        synchronized (scheduledCleanupsLock) {
            return ImmutableMap.copyOf(scheduledCleanups);
        }
    }


    static class CostsStatCleanup implements Runnable {

        private final ScheduledCleanupInformation cleanupInfo;

        private final SetOnce<Boolean> completed = new SetOnce<>();

        CostsStatCleanup(@Nonnull final ScheduledCleanupInformation cleanupInfo) {
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

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

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
import com.vmturbo.cost.component.stats.ReservedInstanceStatTable.Trimmer;

/**
 * Class for trimming and cleaning up the Reserved Instance Stats tables (utilization and coverage)
 * based on retention periods. Runs on a scheduler.
 */
public class ReservedInstanceStatCleanupScheduler {

    private static final Logger logger = LogManager.getLogger();

    private final Map<ScheduledCleanupInformation, ReservedInstanceStatCleanup> scheduledCleanups = new HashMap<>();

    private final Clock clock;

    private final Object scheduledCleanupsLock = new Object();

    private Instant lastCleanupTime = Instant.ofEpochMilli(0);

    private final RetentionPeriodFetcher retentionPeriodFetcher;

    private final long minMillisBetweenCleanups;

    private final ExecutorService executorService;

    private final ReservedInstanceStatCleanupFactory cleanupFactory;

    private final ThreadPoolTaskScheduler schedulerExecutor;

    private final long cleanupSchedulerPeriod;

    /**
     * The tables to clean up.
     */
    private final List<ReservedInstanceStatTable> tables;


    /**
     * Constructor for the Reserved Instance Stat tables.
     *
     * @param clock the clock.
     * @param tables the tables to be cleaned up.
     * @param retentionPeriodFetcher the retention period fetcher containing info about retentin
     *                               periods of tables.
     * @param executorService The executor service to cleanup the tables.
     * @param minTimeBetweenCleanup The minimum time between cleanups.
     * @param timeBetweenCleanupsUnit The unit of time of minimum time between cleanups.
     * @param taskScheduler The task scheduler which invokes the cleanups.
     * @param cleanupSchedulerPeriod The period after which the cleanup scheduler runs.
     */
    public ReservedInstanceStatCleanupScheduler(@Nonnull final Clock clock,
                                       @Nonnull final List<ReservedInstanceStatTable> tables,
                                       @Nonnull final RetentionPeriodFetcher retentionPeriodFetcher,
                                       @Nonnull final ExecutorService executorService,
                                       @Nonnull final long minTimeBetweenCleanup,
                                       @Nonnull final TimeUnit timeBetweenCleanupsUnit,
                                       @Nonnull final ThreadPoolTaskScheduler taskScheduler,
                                                @Nonnull final long cleanupSchedulerPeriod) {
        this (clock, tables, retentionPeriodFetcher, executorService, minTimeBetweenCleanup, timeBetweenCleanupsUnit,
                ReservedInstanceStatCleanup::new, taskScheduler, cleanupSchedulerPeriod);
    }

    /**
     * Constructor for the Reserved Instance Stat tables.
     *
     * @param clock the clock.
     * @param tables the tables to be cleaned up.
     * @param retentionPeriodFetcher the retention period fetcher containing info about retentin
     *                                     periods of tables.
     * @param executorService The executor service to cleanup the tables.
     * @param minTimeBetweenCleanup The minimum time between cleanups.
     * @param timeBetweenCleanupsUnit The unit of time of minimum time between cleanups.
     * @param reservedInstanceStatCleanupFactory The reserved Instance Stat cleanup factory.
     * @param taskScheduler The task scheduler which invokes the cleanups.
     * @param cleanupSchedulerPeriod The period after which the cleanup scheduler runs.
     */
    public ReservedInstanceStatCleanupScheduler(@Nonnull final Clock clock,
                                                @Nonnull final List<ReservedInstanceStatTable> tables,
                                                @Nonnull final RetentionPeriodFetcher retentionPeriodFetcher,
                                                @Nonnull final ExecutorService executorService,
                                                @Nonnull final long minTimeBetweenCleanup,
                                                @Nonnull final TimeUnit timeBetweenCleanupsUnit,
                                                @Nonnull ReservedInstanceStatCleanupFactory reservedInstanceStatCleanupFactory,
                                                @Nonnull ThreadPoolTaskScheduler taskScheduler,
                                                @Nonnull final long cleanupSchedulerPeriod) {
        this.clock = clock;
        this.tables = tables;
        this.retentionPeriodFetcher = Objects.requireNonNull(retentionPeriodFetcher);
        this.minMillisBetweenCleanups = timeBetweenCleanupsUnit.toMillis(minTimeBetweenCleanup);
        this.cleanupFactory = reservedInstanceStatCleanupFactory;
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
            for (final ReservedInstanceStatTable table : tables) {
                final LocalDateTime trimToTime = table.getTrimTime(retentionPeriods);
                final ScheduledCleanupInformation cleanupInfo = ImmutableScheduledCleanupInformation.builder()
                        .trimToTime(trimToTime)
                        .tableWriter(table.writer())
                        .build();
                final ReservedInstanceStatCleanup existingCleanup = scheduledCleanups.get(cleanupInfo);
                final boolean existingStillRunning = existingCleanup != null &&
                        !existingCleanup.completionStatus().isPresent();
                // Although we removed completed cleanups earlier in the function, some of them
                // may have completed since then.
                final boolean existingSucceeded = existingCleanup != null &&
                        existingCleanup.completionStatus().orElse(false);
                if (!existingStillRunning && !existingSucceeded) {
                    logger.info("Scheduling cleanup: {}", cleanupInfo);
                    final ReservedInstanceStatCleanup newCleanup = cleanupFactory.newCleanup(cleanupInfo);
                    scheduledCleanups.put(cleanupInfo, newCleanup);
                    cleanups.add(Executors.callable(newCleanup));
                } else {
                logger.info("Skipping scheduling of cleanup {} because {}",
                        cleanupInfo, existingStillRunning ?
                                "it's still running." : "it already succeeded.");
            }
        }
            final int beforeSize = scheduledCleanups.size();
            try {
                List<Future<Object>> futures = executorService.invokeAll(cleanups);
                for (Future cleanupFuture: futures) {
                    cleanupFuture.get();
                }
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Exception encountered while cleaning the Reserved Instance stats tables.", e);
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
     * Factory class for {@link ReservedInstanceStatCleanup}s, for Dependency Injection
     * (mainly for unit tests).
     */
    @FunctionalInterface
    interface ReservedInstanceStatCleanupFactory {
        @Nonnull
        ReservedInstanceStatCleanup newCleanup(@Nonnull ScheduledCleanupInformation scheduledRollupInfo);
    }

    public void initializeCleanupSchedule() {
        logger.info("Running the Reserved Instance Stat Tables cleanup scheduler.");
        schedulerExecutor.scheduleWithFixedDelay(this::scheduleCleanups, new Date(System.currentTimeMillis()), cleanupSchedulerPeriod );
    }

    @VisibleForTesting
    public Map<ScheduledCleanupInformation, ReservedInstanceStatCleanup> getScheduledCleanups() {
        synchronized (scheduledCleanupsLock) {
            return ImmutableMap.copyOf(scheduledCleanups);
        }
    }


    static class ReservedInstanceStatCleanup implements Runnable {

        private final ScheduledCleanupInformation cleanupInfo;

        private final SetOnce<Boolean> completed = new SetOnce<>();

        ReservedInstanceStatCleanup(@Nonnull final ScheduledCleanupInformation cleanupInfo) {
            this.cleanupInfo = cleanupInfo;
        }

        @Override
        public void run() {
            try {
                logger.debug("Starting cleanup of Reserved Instance table:{}", cleanupInfo);
                cleanupInfo.tableWriter().trim(cleanupInfo.trimToTime());
                completed.trySetValue(true);
                logger.debug("Completed cleanup of Reserved Instance: {}", cleanupInfo);
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

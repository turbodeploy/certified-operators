package com.vmturbo.action.orchestrator.stats.rollup;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionStats;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RollupReadyInfo;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.TableInfo;
import com.vmturbo.action.orchestrator.stats.rollup.export.RollupExporter;
import com.vmturbo.action.orchestrator.stats.rollup.v2.ActionRollupAlgorithmMigrator;
import com.vmturbo.action.orchestrator.stats.rollup.v2.RollupAlgorithmVersion;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.proactivesupport.DataMetricGauge;
import com.vmturbo.proactivesupport.DataMetricSummary;

/**
 * Drives the roll-ups of action stats.
 * <p>
 * Schedules a separate {@link ActionStatRollup} for every
 * (table pair, start time, {@link MgmtUnitSubgroup}). This helps keep each database operation
 * relatively small.
 * <p>
 * We should trigger all possible rollups after every action plan is processed by the action stats
 * framework.
 * <p>
 * The scheduled rollups are not persisted anywhere, but each rollup should be transactional. If
 * the action orchestrator restarts we can just re-schedule all possible rollups by checking
 * the relevant tables.
 */
public class ActionStatRollupScheduler implements IActionStatRollupScheduler {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The directions of roll-ups - i.e. which tables to roll up "FROM" and "TO."
     */
    private final List<RollupDirection> rollupDirections;

    /**
     * The executor handling the asynchronous rollups.
     */
    private final ExecutorService executorService;

    private final ActionStatRollupFactory rollupFactory;

    /**
     * Currently scheduled {@link ActionStatRollup}s. May contain some {@link ActionStatRollup}s
     * that have already completed.
     */
    @GuardedBy("scheduledRollupsLock")
    private final Map<ScheduledRollupInfo, ActionStatRollup> scheduledRollups = new HashMap<>();

    private final Object scheduledRollupsLock = new Object();

    public ActionStatRollupScheduler(@Nonnull final List<RollupDirection> rollupDirections,
                                     @Nonnull final DSLContext dsl,
                                     @Nonnull final Clock clock,
                                     @Nonnull final ExecutorService executorService) {
        this(rollupDirections, executorService,
            new ActionRollupAlgorithmMigrator(dsl, clock, RollupAlgorithmVersion.V1),
            ActionStatRollup::new);
    }

    @VisibleForTesting
    ActionStatRollupScheduler(@Nonnull final List<RollupDirection> rollupDirections,
                              @Nonnull final ExecutorService executorService,
                              @Nonnull final ActionRollupAlgorithmMigrator algorithmMigrator,
                              @Nonnull final ActionStatRollupFactory rollupFactory) {
        this.rollupDirections = Objects.requireNonNull(rollupDirections);
        this.executorService = Objects.requireNonNull(executorService);
        this.rollupFactory = Objects.requireNonNull(rollupFactory);
        // The migration will delete some rows if we were using the V2 rollup algorithm and are
        // switching back to V1.
        new Thread(algorithmMigrator::doMigration).start();
    }


    /**
     * Schedule any possible roll-ups.
     *
     * @return The number of rollups scheduled by this call.
     */
    @Override
    public void scheduleRollups() {
        synchronized (scheduledRollupsLock) {
            // Remove completed rollups before we schedule new ones.
            // This means we will retry any failed rollups.
            final int beforeSize = scheduledRollups.size();
            if (scheduledRollups.values().removeIf(rollup -> rollup.completionStatus().isPresent())) {
                logger.info("Trimmed {} completed rollups.", beforeSize - scheduledRollups.size());
            }

            int numQueued = 0;
            for (final RollupDirection rollupDirection : rollupDirections) {
                final List<RollupReadyInfo> rollupReadyTimes;
                try {
                    rollupReadyTimes = rollupDirection.fromTableReader().rollupReadyTimes();
                } catch (DataAccessException e) {
                    logger.error("Failed to get rollup-ready times for rollup {}. " +
                        "Skipping this rollup. Error: {}", rollupDirection, e.getLocalizedMessage());
                    continue;
                }

                logger.debug("{} time periods ready for rollup {}",
                    rollupReadyTimes.size(), rollupDirection);


                for (final RollupReadyInfo time : rollupReadyTimes) {
                    for (final Integer mgmtUnitSubgroupId : time.managementUnits()) {
                        final ScheduledRollupInfo rollupInfo = ImmutableScheduledRollupInfo.builder()
                            .rollupDirection(rollupDirection)
                            .startTime(time.startTime())
                            .mgmtUnitSubgroupId(mgmtUnitSubgroupId)
                            .build();
                        final ActionStatRollup existingRollup = scheduledRollups.get(rollupInfo);
                        final boolean existingStillRunning = existingRollup != null &&
                            !existingRollup.completionStatus().isPresent();
                        final boolean existingSucceeded =
                            existingRollup != null && existingRollup.completionStatus().orElse(false);
                        if (!existingStillRunning && !existingSucceeded) {
                            logger.trace("Scheduling rollup: {}", rollupInfo);
                            final ActionStatRollup newRollup = rollupFactory.newRollup(rollupInfo);
                            scheduledRollups.put(rollupInfo, newRollup);
                            // Increment the number of queued rollups.
                            //
                            // We track the number queued separately from the metric, because
                            // the metric may be decremented at any time by one of the scheduled rollups
                            // on a different thread.
                            numQueued++;
                            Metrics.NUM_ROLLUPS_QUEUED.increment();
                            executorService.submit(newRollup);
                        } else {
                            logger.trace("Skipping scheduling of rollup {} because {}",
                                rollupInfo, existingStillRunning ?
                                    "it's still running." : "it already succeeded.");
                        }
                    };
                }
            }

            if (numQueued > 0) {
                logger.info("Scheduled {} rollups.", numQueued);
            }
        }
    }

    /**
     * Get the currently scheduled rollups. Used for testing and for debugging purposes.
     *
     * @return The {@link ActionStatRollup}s queued by this {@link ActionStatRollupScheduler}.
     *         All {@link ActionStatRollup}s that have not yet completed are guaranteed to be in
     *         the returned map. Completed (succeeded or failed) rollups may not be present -
     *         they are cleared as part of {@link ActionStatRollupScheduler#scheduledRollups}.
     */
    @Nonnull
    public Map<ScheduledRollupInfo, ActionStatRollup> getScheduledRollups() {
        synchronized (scheduledRollupsLock) {
            return ImmutableMap.copyOf(scheduledRollups);
        }
    }

    /**
     * Information required by a scheduled {@link ActionStatRollup}.
     * Each rollup handles stats for a single {@link MgmtUnitSubgroup} and a single temporal unit.
     * For example, when rolling up from latest to hourly, each scheduled rollup
     * covers one hour of records (e.g. 17:00 - 17:59:59).
     */
    @Value.Immutable
    public interface ScheduledRollupInfo {
        /**
         * The direction of the rollup.
         */
        RollupDirection rollupDirection();

        /**
         * The start time of the rollup. This should be truncated to the "time unit" of the "TO"
         * table (see: {@link TableInfo#temporalUnit()}. We don't need an explicit end time because
         * each rollup spans one "time unit" of the "TO" table.
         */
        LocalDateTime startTime();

        /**
         * The id of the {@link MgmtUnitSubgroup} the rollup is for.
         */
        int mgmtUnitSubgroupId();

    }

    /**
     * Describes the direction of an action stat rollup, and contains references to the classes
     * required to do the rollup.
     */
    @Value.Immutable
    public abstract static class RollupDirection {
        /**
         * A {@link ActionStatTable.Reader} to use to roll up records from the "FROM" table.
         */
        abstract ActionStatTable.Reader fromTableReader();

        /**
         * A {@link ActionStatTable.Writer} to write rolled-up records into the "TO" table.
         */
        abstract ActionStatTable.Writer toTableWriter();

        /**
         * The description of the direction, for debugging.
         */
        abstract String description();

        /**
         * Exporter to use to export rollups going to the "toTable".
         *
         * @return The {@link RollupExporter}.
         */
        abstract Optional<RollupExporter> exporter();

        @Override
        public String toString() {
            return description();
        }
    }

    /**
     * Factory class for {@link ActionStatRollup}s, for Dependency Injection
     * (mainly for unit tests).
     */
    @FunctionalInterface
    public interface ActionStatRollupFactory {
        @Nonnull
        ActionStatRollup newRollup(@Nonnull final ScheduledRollupInfo scheduledRollupInfo);
    }

    static class Metrics {
        static final DataMetricGauge NUM_ROLLUPS_QUEUED = DataMetricGauge.builder()
            .withName("ao_num_stat_rollups_queued")
            .withHelp("Number of action stat rollups currently queued.")
            .build()
            .register();

        static final DataMetricGauge NUM_ROLLUPS_RUNNING = DataMetricGauge.builder()
            .withName("ao_num_stat_rollups_running")
            .withHelp("Number of action stat rollups currently running.")
            .build()
            .register();
    }

    /**
     * Rolls records from one table to the other.
     */
    public static class ActionStatRollup implements Runnable {

        static final String STAT_ROLLUP_LABEL = "step";

        static final String TABLE_NAME_LABEL = "table";

        static final String ROLLUP_STEP = "rollup";

        static final String INSERT_STEP = "insert";

        static final DataMetricSummary STAT_ROLLUP_SUMMARY = DataMetricSummary.builder()
            .withName("ao_stat_rollup_duration_seconds")
            .withHelp("Duration of an action stat rollup.")
            .withLabelNames(STAT_ROLLUP_LABEL, TABLE_NAME_LABEL)
            .build()
            .register();

        private static final Logger logger = LogManager.getLogger();

        private final ScheduledRollupInfo scheduledRollupInfo;

        private final SetOnce<Boolean> completed = new SetOnce<>();

        ActionStatRollup(@Nonnull final ScheduledRollupInfo scheduledRollupInfo) {
            this.scheduledRollupInfo = scheduledRollupInfo;
        }

        @Override
        public void run() {
            // No longer queued.
            Metrics.NUM_ROLLUPS_QUEUED.decrement();
            // Running now.
            Metrics.NUM_ROLLUPS_RUNNING.increment();
            try {
                logger.debug("Starting rollup:{}", scheduledRollupInfo);
                final RollupDirection rollupDirection = scheduledRollupInfo.rollupDirection();
                final int mgmtUnitSubgroupId = scheduledRollupInfo.mgmtUnitSubgroupId();
                final Optional<RolledUpActionStats> rolledUpStatsOpt = rollupDirection.fromTableReader()
                    .rollup(mgmtUnitSubgroupId, scheduledRollupInfo.startTime());
                rolledUpStatsOpt.ifPresent(rolledUpStats -> {
                    rollupDirection.toTableWriter().insert(mgmtUnitSubgroupId, rolledUpStats);
                    scheduledRollupInfo.rollupDirection().exporter().ifPresent(rollupExporter -> {
                        rollupExporter.exportRollup(mgmtUnitSubgroupId, rolledUpStats);
                    });
                });

                completed.trySetValue(true);
                logger.debug("Completed rollup: {}", scheduledRollupInfo);
            } catch (RuntimeException e) {
                logger.error("Failed to roll up {}. Error: {}",
                    scheduledRollupInfo, e.getLocalizedMessage());
                completed.trySetValue(false);
            } finally {
                // No longer running.
                Metrics.NUM_ROLLUPS_RUNNING.decrement();
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

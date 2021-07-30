package com.vmturbo.action.orchestrator.stats.rollup.v2;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.stats.rollup.IActionStatRollupScheduler;
import com.vmturbo.action.orchestrator.stats.rollup.RolledUpStatCalculator;
import com.vmturbo.action.orchestrator.stats.rollup.export.RollupExporter;
import com.vmturbo.proactivesupport.DataMetricGauge;
import com.vmturbo.proactivesupport.DataMetricHistogram;
import com.vmturbo.proactivesupport.DataMetricSummary;

/**
 * Second iteration of the action stat rollup logic.
 *
 * <p/>Instead of doing the rollups at the end of every time period (hourly every hour, daily every day,
 * monthly every month), this approach uses upserts to roll up every hours worth of data into hour,
 * day, and month.
 */
public class ActionStatRollupSchedulerV2 implements IActionStatRollupScheduler {
    private static final Logger logger = LogManager.getLogger();

    private final HourActionRollupFactory rollupFactory;
    private final ExecutorService executorService;
    private final DSLContext dsl;
    private final Clock clock;

    /**
     * Create a new scheduler.
     *
     * @param dsl To access the database.
     * @param clock To access (and allow mocking of) the time.
     * @param rollupExporter Used to export hourly rollups to Kafka.
     * @param rolledUpStatCalculator Used to aggregate an hours worth of stats for a single management
     * unit subgroup and action group into a single {@link com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat}.
     */
    public ActionStatRollupSchedulerV2(final DSLContext dsl,
            final Clock clock,
            final RollupExporter rollupExporter,
            final RolledUpStatCalculator rolledUpStatCalculator) {
        this(new HourActionRollupFactory(rollupExporter, rolledUpStatCalculator, clock),
            new ActionRollupAlgorithmMigrator(dsl, clock, RollupAlgorithmVersion.V2),
            // We use a single-threaded executor to enforce ordering of rollups, so we do
            // instantiate it as part of the class' implementation.
            Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                .setNameFormat("action-stat-rollup-$d")
                .build()),
            dsl, clock);
    }

    @VisibleForTesting
    ActionStatRollupSchedulerV2(final HourActionRollupFactory rollupFactory,
            final ActionRollupAlgorithmMigrator algorithmMigrator,
            final ExecutorService executorService,
            final DSLContext dsl,
            final Clock clock) {
        this.rollupFactory = rollupFactory;
        this.executorService = executorService;
        this.dsl = dsl;
        this.clock = clock;
        // In the single-executor case this will prevent any rollups from happening until
        // the migration is finished.
        executorService.submit(algorithmMigrator::doMigration);
    }

    @Override
    public void scheduleRollups() {
        // Do the actual rollup on a different thread.
        // It's important that this be a single threaded executor, because we need to
        // do the rollups in order.
        Metrics.NUM_ROLLUPS_QUEUED.increment();
        executorService.submit(() -> {
            Metrics.NUM_ROLLUPS_QUEUED.decrement();
            dsl.transaction(context -> {
                final DSLContext transactionDsl = DSL.using(context);
                // Sorted map to ensure that earlier rollups happen before later rollups
                // This will typically have 0 or 1 entries, but could have 2 entries in corner cases.
                final SortedMap<LocalDateTime, List<LocalDateTime>> rollupReadyHours;
                try {
                    rollupReadyHours = findReadyRollupTimes(transactionDsl);
                } catch (DataAccessException e) {
                    logger.error("Failed to get rollup-ready times for latest tablerollup. "
                            + "Skipping this rollup. Error: {}", e.getLocalizedMessage());
                    return;
                }

                logger.debug("{} time periods ready", rollupReadyHours.size());

                rollupReadyHours.forEach((hourTime, snapshotTimes) -> {
                    final HourActionStatRollup rollup = rollupFactory.newRollup(transactionDsl,
                            hourTime, snapshotTimes.size());
                    rollup.run();
                });
            });
        });
    }

    /**
     * Return the hours that are ready for rollup in the action_stats_latest table, sorted in
     * ascending order.
     *
     * <p/>An hour is considered "ready for rollup" if there will be no more stats recorded
     * for that hour. This happens once time has moved on to the next hour.
     *
     * @param transaction The transaction to use for the query.
     * @return Map of (hour time) -> (list of individual snapshot times in that hour).
     */
    private SortedMap<LocalDateTime, List<LocalDateTime>> findReadyRollupTimes(DSLContext transaction) {
        final List<LocalDateTime> allSnapshotTimes =
                transaction.select(Tables.ACTION_SNAPSHOT_LATEST.ACTION_SNAPSHOT_TIME)
                        .from(Tables.ACTION_SNAPSHOT_LATEST)
                        .fetch(Tables.ACTION_SNAPSHOT_LATEST.ACTION_SNAPSHOT_TIME);
        if (allSnapshotTimes.isEmpty()) {
            return Collections.emptySortedMap();
        }

        final SortedMap<LocalDateTime, List<LocalDateTime>> snapshotsByTruncatedTime = new TreeMap<>();
        for (LocalDateTime snapshotTime : allSnapshotTimes) {
            final LocalDateTime truncatedTime = snapshotTime.truncatedTo(ChronoUnit.HOURS);
            snapshotsByTruncatedTime.computeIfAbsent(truncatedTime, k -> new ArrayList<>()).add(snapshotTime);
        }

        // Remove the most recent time unit - we don't know if we'll get more snapshots before
        // this time unit is over. e.g. if rolling up the hour, and it's currently 16:45,
        // 16:00 is not ready for roll-up yet because we may get another snapshot.
        final LocalDateTime curTruncatedTime = LocalDateTime.now(clock).truncatedTo(ChronoUnit.HOURS);
        snapshotsByTruncatedTime.remove(curTruncatedTime);

        // Remove hours that have already been rolled up.
        final Set<LocalDateTime> alreadyRolledUp =
                transaction.select(Tables.ACTION_SNAPSHOT_HOUR.HOUR_TIME)
                        .from(Tables.ACTION_SNAPSHOT_HOUR)
                        .where(Tables.ACTION_SNAPSHOT_HOUR.HOUR_TIME.in(snapshotsByTruncatedTime.keySet()))
                        .fetchSet(Tables.ACTION_SNAPSHOT_HOUR.HOUR_TIME);
        alreadyRolledUp.forEach(snapshotsByTruncatedTime::remove);

        return snapshotsByTruncatedTime;
    }

    /**
     * Rollup-related metrics.
     */
    static class Metrics {

        static final DataMetricGauge NUM_ROLLUPS_QUEUED = DataMetricGauge.builder()
                .withName("ao_num_stat_rollups_queued")
                .withHelp("Number of action stat rollups currently queued.")
                .build()
                .register();

        static final DataMetricHistogram NUM_ROWS_ROLLED_UP = DataMetricHistogram.builder()
                .withName("ao_stat_table_num_rows_rolled_up")
                .withHelp("The amount of rows in a particular rollup (for a specific time + mgmt subunit)")
                // We expect most management units to only have actions for a small subset of action
                // groups.
                .withBuckets(50, 100, 200, 500, 1000)
                .build()
                .register();

        static final DataMetricSummary STAT_ROLLUP_SUMMARY = DataMetricSummary.builder()
                .withName("ao_stat_rollup_duration_seconds")
                .withHelp("Duration of an action stat rollup.")
                .withLabelNames("step")
                .build()
                .register();
    }
}

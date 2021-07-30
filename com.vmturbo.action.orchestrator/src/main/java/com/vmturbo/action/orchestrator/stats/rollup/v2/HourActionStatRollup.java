package com.vmturbo.action.orchestrator.stats.rollup.v2;

import static com.vmturbo.action.orchestrator.db.Tables.ACTION_STATS_BY_DAY;
import static com.vmturbo.action.orchestrator.db.Tables.ACTION_STATS_BY_HOUR;
import static com.vmturbo.action.orchestrator.db.Tables.ACTION_STATS_BY_MONTH;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Insert;
import org.jooq.InsertOnDuplicateSetMoreStep;
import org.jooq.InsertOnDuplicateSetStep;
import org.jooq.Record;
import org.jooq.TableField;
import org.jooq.impl.DSL;

import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByDayRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByHourRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByMonthRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionStats;
import com.vmturbo.action.orchestrator.stats.rollup.ImmutableRolledUpActionStats;
import com.vmturbo.action.orchestrator.stats.rollup.RolledUpStatCalculator;
import com.vmturbo.action.orchestrator.stats.rollup.export.RollupExporter;
import com.vmturbo.action.orchestrator.stats.rollup.v2.ActionStatRollupSchedulerV2.Metrics;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.Detail;

/**
 * Responsible for rolling up one hour's worth of data from the action_stats_latest table
 * to the hourly, daily, and monthly tables.
 */
public class HourActionStatRollup implements Runnable {

    private static final Logger logger = LogManager.getLogger();

    private final LocalDateTime hourTime;
    private final LocalDateTime dayTime;
    private final LocalDateTime monthTime;
    private final int numSnapshots;

    private final RollupExporter rollupExporter;

    private final RolledUpStatCalculator rollupCalculator;

    private final DSLContext transactionContext;
    private final Clock clock;

    @VisibleForTesting
    HourActionStatRollup(final LocalDateTime hourTime, final int numSnapshots,
            final RollupExporter rollupExporter, final RolledUpStatCalculator rollupCalculator,
            final DSLContext transactionContext, final Clock clock) {
        this.hourTime = hourTime;
        this.dayTime = hourTime.truncatedTo(ChronoUnit.DAYS);
        this.monthTime = LocalDateTime.of(hourTime.getYear(), hourTime.getMonth(), 1, 0, 0);
        this.numSnapshots = numSnapshots;
        this.rollupExporter = rollupExporter;
        this.rollupCalculator = rollupCalculator;
        this.transactionContext = transactionContext;
        this.clock = clock;
    }

    @Override
    public void run() {
        logger.info("Starting rollup for hour: {} with {} snapshots", hourTime, numSnapshots);
        final MultiStageTimer timer = new MultiStageTimer(logger);

        timer.start("rollup");
        final Int2ObjectMap<Int2ObjectMap<RolledUpActionGroupStat>> rolledUpStatsByAgAndMsu =
                rollupLatestStats(transactionContext);

        insertRolledUpStats(timer, rolledUpStatsByAgAndMsu);

        timer.start("export");
        rolledUpStatsByAgAndMsu.forEach((mgmtUnitSubgroupId, rolledUpStatsByAg) -> {
            final RolledUpActionStats statsForExport =
                    ImmutableRolledUpActionStats.builder()
                            .startTime(hourTime)
                            .numActionSnapshots(numSnapshots)
                            .putAllStatsByActionGroupId(rolledUpStatsByAg)
                            .build();
            rollupExporter.exportRollup(mgmtUnitSubgroupId, statsForExport);
        });

        timer.log(Level.INFO,
                FormattedString.format("Completed rollup for time {} with {} snapshots", hourTime,
                        numSnapshots), Detail.STAGE_SUMMARY);
        timer.visit((stageName, stopped, totalDurationMs) -> {
            if (stopped) {
                Metrics.STAT_ROLLUP_SUMMARY.labels(stageName).observe(
                        (double)TimeUnit.MILLISECONDS.toSeconds(totalDurationMs));
            }
        });
    }

    private <T extends Record, V> InsertOnDuplicateSetMoreStep<T> updateMin(
            InsertOnDuplicateSetStep<T> step, TableField<T, V> field, V value) {
        return step.set(field, DSL.least(field, DSL.val(value)));
    }

    private <T extends Record, V> InsertOnDuplicateSetMoreStep<T> updateMax(
            InsertOnDuplicateSetStep<T> step, TableField<T, V> field, V value) {
        return step.set(field, DSL.greatest(field, DSL.val(value)));
    }

    private <T extends Record, V extends BigDecimal> InsertOnDuplicateSetMoreStep<T> updateAvg(
            InsertOnDuplicateSetStep<T> step, TableField<T, V> field, V value,
            int curNumSnapshots) {
        int newDivisor = curNumSnapshots + numSnapshots;
        BigDecimal totalToAdd = value.multiply(BigDecimal.valueOf(numSnapshots));

        // (cur_avg * cur_num_samples + total_to_add) / (cur_num_samples + samples_to_add)
        return step.set(field, field.times(curNumSnapshots).plus(totalToAdd).divide(newDivisor));
    }

    private <R extends Record> int getCurDaySnapshots(TableField<R, Integer> tableField,
            Condition c) {
        Integer snapshotCount = transactionContext.select(tableField)
                .from(tableField.getTable())
                .where(c)
                .fetchOne(tableField);
        return snapshotCount == null ? 0 : snapshotCount;
    }

    private void rollupToMonth(
            Int2ObjectMap<Int2ObjectMap<RolledUpActionGroupStat>> actionStatsByMsuAndAg) {
        // Update the monthly stat tables, then update the snapshot table.
        // Get the snapshots snapshots via query to avoid joins in the inserts.
        final int curMonthSnapshots = getCurDaySnapshots(
                Tables.ACTION_SNAPSHOT_MONTH.NUM_ACTION_SNAPSHOTS,
                Tables.ACTION_SNAPSHOT_MONTH.MONTH_TIME.eq(monthTime));
        final List<Insert<?>> monthInserts = new ArrayList<>(
                actionStatsByMsuAndAg.values().stream().mapToInt(Int2ObjectMap::size).sum());
        actionStatsByMsuAndAg.forEach((mgmtUnitSubgroupId, actionStatsByAg) -> {
            actionStatsByAg.forEach((agId, stats) -> {
                ActionStatsByMonthRecord record = monthRecord(mgmtUnitSubgroupId, agId, stats);
                InsertOnDuplicateSetMoreStep<ActionStatsByMonthRecord> step =
                        transactionContext.insertInto(ACTION_STATS_BY_MONTH)
                                .set(record)
                                .onDuplicateKeyUpdate()
                                // Don't update prior action count - that's only relevant the first time.
                                // Increment the new action count by the number of snapshots in this hour.
                                .set(ACTION_STATS_BY_MONTH.NEW_ACTION_COUNT,
                                        ACTION_STATS_BY_MONTH.NEW_ACTION_COUNT.add(
                                                record.getNewActionCount()));
                updateAvg(step, ACTION_STATS_BY_MONTH.AVG_ACTION_COUNT, record.getAvgActionCount(),
                        curMonthSnapshots);
                updateMax(step, ACTION_STATS_BY_MONTH.MAX_ACTION_COUNT, record.getMaxActionCount());
                updateMin(step, ACTION_STATS_BY_MONTH.MIN_ACTION_COUNT, record.getMinActionCount());

                updateAvg(step, ACTION_STATS_BY_MONTH.AVG_ENTITY_COUNT, record.getAvgEntityCount(),
                        curMonthSnapshots);
                updateMax(step, ACTION_STATS_BY_MONTH.MAX_ENTITY_COUNT, record.getMaxEntityCount());
                updateMin(step, ACTION_STATS_BY_MONTH.MIN_ENTITY_COUNT, record.getMinEntityCount());

                updateAvg(step, ACTION_STATS_BY_MONTH.AVG_SAVINGS, record.getAvgSavings(),
                        curMonthSnapshots);
                updateMax(step, ACTION_STATS_BY_MONTH.MAX_SAVINGS, record.getMaxSavings());
                updateMin(step, ACTION_STATS_BY_MONTH.MIN_SAVINGS, record.getMinSavings());
                updateAvg(step, ACTION_STATS_BY_MONTH.AVG_INVESTMENT, record.getAvgInvestment(),
                        curMonthSnapshots);
                updateMax(step, ACTION_STATS_BY_MONTH.MAX_INVESTMENT, record.getMaxInvestment());
                updateMin(step, ACTION_STATS_BY_MONTH.MIN_INVESTMENT, record.getMinInvestment());
                monthInserts.add(step);
            });
        });

        transactionContext.batch(monthInserts).execute();

        // Update the daily snapshot table.
        transactionContext.insertInto(Tables.ACTION_SNAPSHOT_MONTH)
                .set(Tables.ACTION_SNAPSHOT_MONTH.MONTH_TIME, monthTime)
                .set(Tables.ACTION_SNAPSHOT_MONTH.NUM_ACTION_SNAPSHOTS, numSnapshots)
                .set(Tables.ACTION_SNAPSHOT_MONTH.MONTH_ROLLUP_TIME, LocalDateTime.now(clock))
                .onDuplicateKeyUpdate()
                // Increment the number of snapshots.
                .set(Tables.ACTION_SNAPSHOT_MONTH.NUM_ACTION_SNAPSHOTS,
                        Tables.ACTION_SNAPSHOT_MONTH.NUM_ACTION_SNAPSHOTS.add(numSnapshots))
                .set(Tables.ACTION_SNAPSHOT_MONTH.MONTH_ROLLUP_TIME, LocalDateTime.now(clock))
                .execute();
    }

    private void rollupToDay(
            Int2ObjectMap<Int2ObjectMap<RolledUpActionGroupStat>> actionStatsByMsuAndAg) {
        // Update the hour -> day tables, then update the snapshot table.
        // We update the table before the snapshot table so that we have access to the old
        // number of samples.
        final int curDaySnapshots = getCurDaySnapshots(
                Tables.ACTION_SNAPSHOT_DAY.NUM_ACTION_SNAPSHOTS,
                Tables.ACTION_SNAPSHOT_DAY.DAY_TIME.eq(dayTime));
        final List<Insert<?>> dayInserts = new ArrayList<>(
                actionStatsByMsuAndAg.values().stream().mapToInt(Int2ObjectMap::size).sum());
        actionStatsByMsuAndAg.forEach((mgmtUnitSubgroupId, actionStatsByAg) -> {
            actionStatsByAg.forEach((agId, stats) -> {
                final ActionStatsByDayRecord record = dayRecord(mgmtUnitSubgroupId, agId, stats);
                final InsertOnDuplicateSetMoreStep<ActionStatsByDayRecord> step = transactionContext
                        .insertInto(ACTION_STATS_BY_DAY)
                        .set(record)
                        .onDuplicateKeyUpdate()
                        // Don't update prior action count - that's only relevant the first time.
                        // Increment the new action count by the number of snapshots in this hour.
                        .set(ACTION_STATS_BY_DAY.NEW_ACTION_COUNT,
                                ACTION_STATS_BY_DAY.NEW_ACTION_COUNT.add(
                                        record.getNewActionCount()));
                updateAvg(step, ACTION_STATS_BY_DAY.AVG_ACTION_COUNT, record.getAvgActionCount(),
                        curDaySnapshots);
                updateMax(step, ACTION_STATS_BY_DAY.MAX_ACTION_COUNT, record.getMaxActionCount());
                updateMin(step, ACTION_STATS_BY_DAY.MIN_ACTION_COUNT, record.getMinActionCount());

                updateAvg(step, ACTION_STATS_BY_DAY.AVG_ENTITY_COUNT, record.getAvgEntityCount(),
                        curDaySnapshots);
                updateMax(step, ACTION_STATS_BY_DAY.MAX_ENTITY_COUNT, record.getMaxEntityCount());
                updateMin(step, ACTION_STATS_BY_DAY.MIN_ENTITY_COUNT, record.getMinEntityCount());

                updateAvg(step, ACTION_STATS_BY_DAY.AVG_SAVINGS, record.getAvgSavings(),
                        curDaySnapshots);
                updateMax(step, ACTION_STATS_BY_DAY.MAX_SAVINGS, record.getMaxSavings());
                updateMin(step, ACTION_STATS_BY_DAY.MIN_SAVINGS, record.getMinSavings());
                updateAvg(step, ACTION_STATS_BY_DAY.AVG_INVESTMENT, record.getAvgInvestment(),
                        curDaySnapshots);
                updateMax(step, ACTION_STATS_BY_DAY.MAX_INVESTMENT, record.getMaxInvestment());
                updateMin(step, ACTION_STATS_BY_DAY.MIN_INVESTMENT, record.getMinInvestment());
                dayInserts.add(step);
            });
        });

        transactionContext.batch(dayInserts).execute();

        // Update the daily snapshot table.
        transactionContext.insertInto(Tables.ACTION_SNAPSHOT_DAY)
                .set(Tables.ACTION_SNAPSHOT_DAY.DAY_TIME, dayTime)
                .set(Tables.ACTION_SNAPSHOT_DAY.NUM_ACTION_SNAPSHOTS, numSnapshots)
                .set(Tables.ACTION_SNAPSHOT_DAY.DAY_ROLLUP_TIME, LocalDateTime.now(clock))
                .onDuplicateKeyUpdate()
                // Increment the number of snapshots.
                .set(Tables.ACTION_SNAPSHOT_DAY.NUM_ACTION_SNAPSHOTS,
                        Tables.ACTION_SNAPSHOT_DAY.NUM_ACTION_SNAPSHOTS.add(numSnapshots))
                .set(Tables.ACTION_SNAPSHOT_DAY.DAY_ROLLUP_TIME, LocalDateTime.now(clock))
                .execute();
    }

    private void rollupToHour(
            Int2ObjectMap<Int2ObjectMap<RolledUpActionGroupStat>> actionStatsByMsuAndAg) {
        // Update the hourly stats tables in a single batch insert.
        final List<ActionStatsByHourRecord> dataRecords = new ArrayList<>();
        actionStatsByMsuAndAg.forEach((mgmtUnitSubgroupId, actionStatsByAg) -> {
            actionStatsByAg.forEach((agId, stats) -> {
                dataRecords.add(hourRecord(mgmtUnitSubgroupId, agId, stats));
            });
        });
        // Do the insert.
        transactionContext.batchInsert(dataRecords).execute();

        // Update the hour snapshot table.
        transactionContext.insertInto(Tables.ACTION_SNAPSHOT_HOUR).set(
                Tables.ACTION_SNAPSHOT_HOUR.HOUR_TIME, hourTime).set(
                Tables.ACTION_SNAPSHOT_HOUR.NUM_ACTION_SNAPSHOTS, numSnapshots).set(
                Tables.ACTION_SNAPSHOT_HOUR.HOUR_ROLLUP_TIME, LocalDateTime.now(clock)).execute();
    }

    private void insertRolledUpStats(final MultiStageTimer timer,
            Int2ObjectMap<Int2ObjectMap<RolledUpActionGroupStat>> actionStatsByMsuAndAg) {
        timer.start("hour");
        rollupToHour(actionStatsByMsuAndAg);
        timer.start("day");
        rollupToDay(actionStatsByMsuAndAg);
        timer.start("month");
        rollupToMonth(actionStatsByMsuAndAg);
    }

    @Nonnull
    private Int2ObjectMap<Int2ObjectMap<RolledUpActionGroupStat>> rollupLatestStats(
            @Nonnull final DSLContext transaction) {
        final LocalDateTime endTime = hourTime.plus(1, ChronoUnit.HOURS)
                // Subtract a microsecond to get to the latest possible time before the start of
                // the next rollup period. We use a microsecond because that's MariaDB's precision:
                // https://mariadb.com/kb/en/library/timestamp/
                .minus(1, ChronoUnit.MICROS);

        final AtomicInteger rowCount = new AtomicInteger(0);
        final Int2ObjectMap<Int2ObjectMap<List<ActionStatsLatestRecord>>> byMSUandAG =
                new Int2ObjectOpenHashMap<>();
        transaction.selectFrom(Tables.ACTION_STATS_LATEST)
                .where(Tables.ACTION_STATS_LATEST.ACTION_SNAPSHOT_TIME.between(hourTime, endTime))
                .fetch()
                .forEach(record -> {
                    rowCount.incrementAndGet();
                    byMSUandAG.computeIfAbsent(record.getMgmtUnitSubgroupId(),
                            k -> new Int2ObjectOpenHashMap<>()).computeIfAbsent(
                            record.getActionGroupId(), k -> new ArrayList<>()).add(record);
                });

        // Record the number of rows we're going to roll up.
        Metrics.NUM_ROWS_ROLLED_UP.observe(rowCount.doubleValue());

        logger.debug("{} relevant records across {} action groups for hour starting from {}",
                rowCount.intValue(), byMSUandAG.size(), hourTime);

        final Int2ObjectMap<Int2ObjectMap<RolledUpActionGroupStat>> rolledUpStats =
                new Int2ObjectOpenHashMap<>();
        byMSUandAG.forEach((mgmtUnitSubgroupId, byAG) -> {
            final Int2ObjectMap<RolledUpActionGroupStat> rolledupByAg = new Int2ObjectOpenHashMap<>(
                    byAG.size());
            byAG.forEach((actionGroupId, recordsForGroup) -> {
                rollupCalculator.rollupLatestRecords(numSnapshots, recordsForGroup).ifPresent(
                        rolledUpGroupStat -> {
                            rolledupByAg.put(actionGroupId, rolledUpGroupStat);
                        });
            });
            rolledUpStats.put(mgmtUnitSubgroupId, rolledupByAg);
        });
        return rolledUpStats;
    }

    private ActionStatsByHourRecord hourRecord(final int mgmtUnitSubgroupId, final int actionGroupId,
            @Nonnull final RolledUpActionGroupStat rolledUpActionGroupStat) {
        ActionStatsByHourRecord hourRecord = new ActionStatsByHourRecord();
        hourRecord.setHourTime(hourTime);
        return populateStatRecord(mgmtUnitSubgroupId, actionGroupId, rolledUpActionGroupStat,
                hourRecord);
    }

    private ActionStatsByDayRecord dayRecord(final int mgmtUnitSubgroupId, final int actionGroupId,
            @Nonnull final RolledUpActionGroupStat rolledUpActionGroupStat) {
        ActionStatsByDayRecord record = new ActionStatsByDayRecord();
        record.setDayTime(dayTime);
        return populateStatRecord(mgmtUnitSubgroupId, actionGroupId, rolledUpActionGroupStat,
                record);
    }

    private ActionStatsByMonthRecord monthRecord(final int mgmtUnitSubgroupId,
            final int actionGroupId,
            @Nonnull final RolledUpActionGroupStat rolledUpActionGroupStat) {
        ActionStatsByMonthRecord record = new ActionStatsByMonthRecord();
        record.setMonthTime(monthTime);
        return populateStatRecord(mgmtUnitSubgroupId, actionGroupId, rolledUpActionGroupStat,
                record);
    }

    private <T extends Record> T populateStatRecord(final int mgmtUnitSubgroupId,
            final int actionGroupId,
            @Nonnull final RolledUpActionGroupStat rolledUpActionGroupStats, T record) {
        // The names for all these are consistent across tables.
        record.set(ACTION_STATS_BY_HOUR.ACTION_GROUP_ID, actionGroupId);
        record.set(ACTION_STATS_BY_HOUR.MGMT_UNIT_SUBGROUP_ID, mgmtUnitSubgroupId);

        record.set(ACTION_STATS_BY_HOUR.PRIOR_ACTION_COUNT,
                rolledUpActionGroupStats.priorActionCount());
        record.set(ACTION_STATS_BY_HOUR.NEW_ACTION_COUNT,
                rolledUpActionGroupStats.newActionCount());
        record.set(ACTION_STATS_BY_HOUR.AVG_ACTION_COUNT,
                BigDecimal.valueOf(rolledUpActionGroupStats.avgActionCount()));
        record.set(ACTION_STATS_BY_HOUR.MAX_ACTION_COUNT,
                rolledUpActionGroupStats.maxActionCount());
        record.set(ACTION_STATS_BY_HOUR.MIN_ACTION_COUNT,
                rolledUpActionGroupStats.minActionCount());

        record.set(ACTION_STATS_BY_HOUR.AVG_ENTITY_COUNT,
                BigDecimal.valueOf(rolledUpActionGroupStats.avgEntityCount()));
        record.set(ACTION_STATS_BY_HOUR.MAX_ENTITY_COUNT,
                rolledUpActionGroupStats.maxEntityCount());
        record.set(ACTION_STATS_BY_HOUR.MIN_ENTITY_COUNT,
                rolledUpActionGroupStats.minEntityCount());

        record.set(ACTION_STATS_BY_HOUR.AVG_SAVINGS,
                BigDecimal.valueOf(rolledUpActionGroupStats.avgSavings()));
        record.set(ACTION_STATS_BY_HOUR.MAX_SAVINGS,
                BigDecimal.valueOf(rolledUpActionGroupStats.maxSavings()));
        record.set(ACTION_STATS_BY_HOUR.MIN_SAVINGS,
                BigDecimal.valueOf(rolledUpActionGroupStats.minSavings()));

        record.set(ACTION_STATS_BY_HOUR.AVG_INVESTMENT,
                BigDecimal.valueOf(rolledUpActionGroupStats.avgInvestment()));
        record.set(ACTION_STATS_BY_HOUR.MAX_INVESTMENT,
                BigDecimal.valueOf(rolledUpActionGroupStats.maxInvestment()));
        record.set(ACTION_STATS_BY_HOUR.MIN_INVESTMENT,
                BigDecimal.valueOf(rolledUpActionGroupStats.minInvestment()));
        return record;
    }
}

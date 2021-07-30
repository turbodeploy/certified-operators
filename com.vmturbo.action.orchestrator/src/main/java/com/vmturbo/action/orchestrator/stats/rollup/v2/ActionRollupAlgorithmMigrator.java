package com.vmturbo.action.orchestrator.stats.rollup.v2;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotDayRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotHourRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotMonthRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByDayRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByHourRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByMonthRecord;
import com.vmturbo.action.orchestrator.stats.rollup.BaseActionStatTableReader.StatWithSnapshotCnt;
import com.vmturbo.action.orchestrator.stats.rollup.ImmutableStatWithSnapshotCnt;
import com.vmturbo.action.orchestrator.stats.rollup.RolledUpStatCalculator;

/**
 * Responsible for ensuring that the records for the current day and month are in line with the target
 * {@link RollupAlgorithmVersion}.
 *
 * <p/>If the version is {@link RollupAlgorithmVersion#V1} and we are migrating from V2, the migration is simple - we drop the
 * data for the current day and month. The V1 algorithm will roll them up again at the end of the
 * day and month respectively.
 *
 * <p/>If the version if {@link RollupAlgorithmVersion#V2} and we are migrating from V1, we need to
 * populate the current day with the existing data from the current day's hours and the current month
 * with the existing data from the current month's days.
 *
 * <p/>If we the desired version is the same as the current version we don't need to do anything.
 */
public class ActionRollupAlgorithmMigrator {

    private static final Logger logger = LogManager.getLogger();

    private final RolledUpStatCalculator statCalculator = new RolledUpStatCalculator();

    private final DSLContext dsl;

    private final Clock clock;

    private final RollupAlgorithmVersion targetVersion;

    /**
     * Constructor.
     *
     * @param dsl To access the database.
     * @param clock To tell the time.
     * @param version The desired version of the algorithm. This will affect the migration strategy.
     */
    public ActionRollupAlgorithmMigrator(@Nonnull final DSLContext dsl,
            @Nonnull final Clock clock,
            @Nonnull final RollupAlgorithmVersion version) {
        this.dsl = dsl;
        this.clock = clock;
        this.targetVersion = version;
    }

    @Nonnull
    private RollupAlgorithmVersion inferPreviousRollupVersion(@Nonnull final DSLContext transaction) {
        final LocalDateTime now = LocalDateTime.now(clock);
        Integer daySnapshots = transaction.selectCount()
            .from(Tables.ACTION_SNAPSHOT_DAY)
                .where(Tables.ACTION_SNAPSHOT_DAY.DAY_TIME.eq(now.truncatedTo(ChronoUnit.DAYS)))
            .fetchOne().component1();
        Integer monthSnapshots = transaction.selectCount()
                .from(Tables.ACTION_SNAPSHOT_MONTH)
                .where(Tables.ACTION_SNAPSHOT_MONTH.MONTH_TIME.eq(truncateMonth(now)))
                .fetchOne().component1();
        // If there is a snapshot entry for this day or month then we have been using V2 of the
        // rollup algorithm. Note: the other explanation is that there is no data.
        // In that case it's safe and quick to run the V1 -> V2 migration again.
        return (daySnapshots + monthSnapshots) > 0 ? RollupAlgorithmVersion.V2 : RollupAlgorithmVersion.V1;
    }

    private LocalDateTime truncateMonth(LocalDateTime time) {
        return LocalDateTime.of(time.getYear(), time.getMonth(), 1, 0, 0);
    }

    /**
     * Do the migration, if necessary.
     */
    public void doMigration() {
        try {
            logger.info("Beginning rollup algorithm migration to version {} ...", targetVersion);
            dsl.transaction(transactionContext -> {
                final DSLContext transactionDsl = DSL.using(transactionContext);
                RollupAlgorithmVersion previousVersion = inferPreviousRollupVersion(transactionDsl);
                if (previousVersion != targetVersion) {
                    logger.info("Detected change from {} to {}.", previousVersion, targetVersion);
                    if (targetVersion == RollupAlgorithmVersion.V1) {
                        onV1Enable(transactionDsl);
                    } else {
                        onV2Enable(transactionDsl);
                    }
                    logger.info("Finished rollup algorithm migration to version {} ...", targetVersion);
                } else {
                    logger.info("No migration necessary - inferred previous version matches desired version {}",
                            targetVersion);
                }
            });
        } catch (DataAccessException e) {
            logger.error("Failed to migrate database.", e);
        } catch (Exception e) {
            logger.error("Encountered unexpected exception", e);
        }
    }

    private void onV1Enable(@Nonnull final DSLContext transactionDsl) {
        final LocalDateTime now = LocalDateTime.now(clock);
        final LocalDateTime thisDay = now.truncatedTo(ChronoUnit.DAYS);
        final LocalDateTime thisMonth = LocalDateTime.of(now.getYear(), now.getMonth(), 1, 0, 0);
        int numDeletedRows = 0;
        numDeletedRows += transactionDsl.deleteFrom(Tables.ACTION_STATS_BY_DAY)
                .where(Tables.ACTION_STATS_BY_DAY.DAY_TIME.eq(thisDay))
                .execute();
        numDeletedRows += transactionDsl.deleteFrom(Tables.ACTION_SNAPSHOT_DAY)
                .where(Tables.ACTION_SNAPSHOT_DAY.DAY_TIME.eq(thisDay))
                .execute();
        numDeletedRows += transactionDsl.deleteFrom(Tables.ACTION_STATS_BY_MONTH)
                .where(Tables.ACTION_STATS_BY_MONTH.MONTH_TIME.eq(thisMonth))
                .execute();
        numDeletedRows += transactionDsl.deleteFrom(Tables.ACTION_SNAPSHOT_MONTH)
                .where(Tables.ACTION_SNAPSHOT_MONTH.MONTH_TIME.eq(thisMonth))
                .execute();
        logger.info("Finished migrating to {}. Deleted {} rows.", targetVersion, numDeletedRows);
    }

    private void onV2Enable(@Nonnull final DSLContext transactionDsl) {
        final LocalDateTime now = LocalDateTime.now(clock);
        logger.info("Migrating todays hours to daily table.");
        todaysHoursToDaily(transactionDsl, now);
        logger.info("Migrating this months days to monthly table.");
        thisMonthsDaysToMonthly(transactionDsl, now);
    }

    private void thisMonthsDaysToMonthly(@Nonnull final DSLContext transactionContext,
            @Nonnull final LocalDateTime now) {
        // The start of this month.
        final LocalDateTime startOfMonth = LocalDateTime.of(now.getYear(), now.getMonth(), 1, 0, 0);
        // Get all daily snapshots for this month.
        // Previous months will have been rolled up using the old algorithm.
        final Map<LocalDateTime, ActionSnapshotDayRecord> daySnapshots = transactionContext.selectFrom(Tables.ACTION_SNAPSHOT_DAY)
                .where(Tables.ACTION_SNAPSHOT_DAY.DAY_TIME.greaterOrEqual(startOfMonth))
                .fetch().stream()
                .collect(Collectors.toMap(ActionSnapshotDayRecord::getDayTime, Function.identity()));

        final ActionSnapshotMonthRecord monthRecord = new ActionSnapshotMonthRecord();
        monthRecord.setNumActionSnapshots(daySnapshots.values().stream()
                .mapToInt(ActionSnapshotDayRecord::getNumActionSnapshots)
                .sum());
        monthRecord.setMonthTime(startOfMonth);
        monthRecord.setMonthRollupTime(now);
        int modifiedRows = transactionContext.insertInto(Tables.ACTION_SNAPSHOT_MONTH)
                .set(monthRecord)
                .onDuplicateKeyIgnore()
                .execute();
        if (modifiedRows == 0) {
            logger.error("Failed to insert into action_snapshot_month for time {}."
                    + " Not rolling up this months days to monthly stats table.", startOfMonth);
            return;
        }

        // At most 24 of these.
        final Map<Integer, Map<Integer, List<StatWithSnapshotCnt<ActionStatsByDayRecord>>>> recordsByMsuAndAg = new HashMap<>();
        transactionContext.selectFrom(Tables.ACTION_STATS_BY_DAY)
                .where(Tables.ACTION_STATS_BY_DAY.DAY_TIME.greaterOrEqual(startOfMonth))
                // Return in ascending order so that the resulting lists of records are also sorted.
                .orderBy(Tables.ACTION_STATS_BY_DAY.DAY_TIME.asc())
                .fetch().forEach(record -> {
            ActionSnapshotDayRecord thisDayRecord = daySnapshots.get(record.getDayTime());
            recordsByMsuAndAg.computeIfAbsent(record.getMgmtUnitSubgroupId(), k -> new HashMap<>())
                    .computeIfAbsent(record.getActionGroupId(), k -> new ArrayList<>())
                    .add(ImmutableStatWithSnapshotCnt.<ActionStatsByDayRecord>builder()
                            .record(record)
                            .numActionSnapshots(thisDayRecord.getNumActionSnapshots())
                            .build());
        });

        final List<ActionStatsByMonthRecord> monthRecords = new ArrayList<>();
        recordsByMsuAndAg.forEach((msuId, recordsByAg) -> {
            recordsByAg.forEach((agId, dayRecords) -> {
                statCalculator.rollupDayRecords(monthRecord.getNumActionSnapshots(), dayRecords)
                        .ifPresent(rolledUpStats -> {
                            final ActionStatsByMonthRecord record = new ActionStatsByMonthRecord();
                            record.setMonthTime(monthRecord.getMonthTime());
                            record.setActionGroupId(agId);
                            record.setMgmtUnitSubgroupId(msuId);

                            record.setPriorActionCount(rolledUpStats.priorActionCount());
                            record.setNewActionCount(rolledUpStats.newActionCount());
                            record.setAvgActionCount(BigDecimal.valueOf(rolledUpStats.avgActionCount()));
                            record.setMaxActionCount(rolledUpStats.maxActionCount());
                            record.setMinActionCount(rolledUpStats.minActionCount());

                            record.setAvgEntityCount(BigDecimal.valueOf(rolledUpStats.avgEntityCount()));
                            record.setMaxEntityCount(rolledUpStats.maxEntityCount());
                            record.setMinEntityCount(rolledUpStats.minEntityCount());

                            record.setAvgSavings(BigDecimal.valueOf(rolledUpStats.avgSavings()));
                            record.setMaxSavings(BigDecimal.valueOf(rolledUpStats.maxSavings()));
                            record.setMinSavings(BigDecimal.valueOf(rolledUpStats.minSavings()));

                            record.setAvgInvestment(BigDecimal.valueOf(rolledUpStats.avgInvestment()));
                            record.setMaxInvestment(BigDecimal.valueOf(rolledUpStats.maxInvestment()));
                            record.setMinInvestment(BigDecimal.valueOf(rolledUpStats.minInvestment()));

                            monthRecords.add(record);
                        });
            });
        });

        transactionContext.batchInsert(monthRecords).execute();
    }

    private void todaysHoursToDaily(DSLContext transactionContext, final LocalDateTime now) {
        // The start of today.
        final LocalDateTime startOfDay = now.truncatedTo(ChronoUnit.DAYS);
        // Get all hourly snapshots for the day.
        // Previous days will have been rolled up using the old algorithm.
        final Map<LocalDateTime, ActionSnapshotHourRecord> hourSnapshots = transactionContext.selectFrom(Tables.ACTION_SNAPSHOT_HOUR)
            .where(Tables.ACTION_SNAPSHOT_HOUR.HOUR_TIME.greaterOrEqual(startOfDay))
            .fetch().stream()
            .collect(Collectors.toMap(ActionSnapshotHourRecord::getHourTime, Function.identity()));

        final ActionSnapshotDayRecord dayRecord = new ActionSnapshotDayRecord();
        dayRecord.setNumActionSnapshots(hourSnapshots.values().stream()
                .mapToInt(ActionSnapshotHourRecord::getNumActionSnapshots)
                .sum());
        dayRecord.setDayTime(startOfDay);
        dayRecord.setDayRollupTime(now);
        int modifiedRows = transactionContext.insertInto(Tables.ACTION_SNAPSHOT_DAY)
            .set(dayRecord)
            .onDuplicateKeyIgnore()
            .execute();
        if (modifiedRows == 0) {
            logger.error("Failed to insert into action_snapshot_day for time {}."
                + " Not rolling up this days hours to daily stats table.", startOfDay);
            return;
        }

        // At most 24 of these.
        final Map<Integer, Map<Integer, List<StatWithSnapshotCnt<ActionStatsByHourRecord>>>> recordsByMsuAndAg = new HashMap<>();
        transactionContext.selectFrom(Tables.ACTION_STATS_BY_HOUR)
                .where(Tables.ACTION_STATS_BY_HOUR.HOUR_TIME.greaterOrEqual(startOfDay))
                // Return in ascending order so that the resulting lists of records are also sorted.
                .orderBy(Tables.ACTION_STATS_BY_HOUR.HOUR_TIME.asc())
                .fetch().forEach(record -> {
            ActionSnapshotHourRecord thisHourRecord = hourSnapshots.get(record.getHourTime());
            recordsByMsuAndAg.computeIfAbsent(record.getMgmtUnitSubgroupId(), k -> new HashMap<>())
                    .computeIfAbsent(record.getActionGroupId(), k -> new ArrayList<>())
                    .add(ImmutableStatWithSnapshotCnt.<ActionStatsByHourRecord>builder()
                            .record(record)
                            .numActionSnapshots(thisHourRecord.getNumActionSnapshots())
                            .build());
        });

        final List<ActionStatsByDayRecord> dayRecords = new ArrayList<>();
        recordsByMsuAndAg.forEach((msuId, recordsByAg) -> {
            recordsByAg.forEach((agId, hourRecords) -> {
                statCalculator.rollupHourRecords(dayRecord.getNumActionSnapshots(), hourRecords)
                    .ifPresent(rolledUpStats -> {
                        final ActionStatsByDayRecord record = new ActionStatsByDayRecord();
                        record.setDayTime(startOfDay);
                        record.setActionGroupId(agId);
                        record.setMgmtUnitSubgroupId(msuId);

                        record.setPriorActionCount(rolledUpStats.priorActionCount());
                        record.setNewActionCount(rolledUpStats.newActionCount());
                        record.setAvgActionCount(BigDecimal.valueOf(rolledUpStats.avgActionCount()));
                        record.setMaxActionCount(rolledUpStats.maxActionCount());
                        record.setMinActionCount(rolledUpStats.minActionCount());

                        record.setAvgEntityCount(BigDecimal.valueOf(rolledUpStats.avgEntityCount()));
                        record.setMaxEntityCount(rolledUpStats.maxEntityCount());
                        record.setMinEntityCount(rolledUpStats.minEntityCount());

                        record.setAvgSavings(BigDecimal.valueOf(rolledUpStats.avgSavings()));
                        record.setMaxSavings(BigDecimal.valueOf(rolledUpStats.maxSavings()));
                        record.setMinSavings(BigDecimal.valueOf(rolledUpStats.minSavings()));

                        record.setAvgInvestment(BigDecimal.valueOf(rolledUpStats.avgInvestment()));
                        record.setMaxInvestment(BigDecimal.valueOf(rolledUpStats.maxInvestment()));
                        record.setMinInvestment(BigDecimal.valueOf(rolledUpStats.minInvestment()));

                        dayRecords.add(record);
                    });
            });
        });

        transactionContext.batchInsert(dayRecords).execute();
    }

}

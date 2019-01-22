package com.vmturbo.action.orchestrator.stats.rollup;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;

import com.google.common.base.Preconditions;

import com.vmturbo.action.orchestrator.stats.groups.ActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroupStore.MatchedActionGroups;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatRollupScheduler.ActionStatRollup;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionStats;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RollupReadyInfo;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.TableInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionCountsQuery.TimeRange;
import com.vmturbo.proactivesupport.DataMetricHistogram;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Base implementation of {@link ActionStatTable.Reader}. Since all the action stat tables have
 * similar structures, pretty much all the code that interacts with the database can be shared.
 *
 * @param <STAT_RECORD> The type of record in the specific {@link ActionStatTable} implementation
 *                      whose {@link ActionStatTable.Reader} extends this class.
 */
public abstract class BaseActionStatTableReader<STAT_RECORD extends Record,
                                                SNAPSHOT_RECORD extends Record>
            implements ActionStatTable.Reader {

    private final Logger logger = LogManager.getLogger(getClass());

    private final DSLContext dslContext;

    private final Clock clock;

    /**
     * The {@link TableInfo} for the table the reader is for.
     */
    private final TableInfo<STAT_RECORD, SNAPSHOT_RECORD> tableInfo;

    /**
     * The {@link TableInfo} for the destination table - the table we'll roll up this table's
     * action stats into.
     */
    private final Optional<TableInfo<? extends Record, ? extends Record>> toTableOpt;


    protected BaseActionStatTableReader(@Nonnull final DSLContext dslContext,
                @Nonnull final Clock clock,
                @Nonnull final TableInfo<STAT_RECORD, SNAPSHOT_RECORD> tableInfo,
                @Nonnull final Optional<TableInfo<? extends Record, ? extends Record>> toTable) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.clock = Objects.requireNonNull(clock);
        this.tableInfo = Objects.requireNonNull(tableInfo);
        this.toTableOpt = Objects.requireNonNull(toTable);
    }

    /**
     * Roll up a set of database records. Each table's {@link ActionStatTable.Reader} must implement
     * this method.
     *
     * @param numStatSnapshotsInRange The number of snapshots in the time range being rolled up.
     * @param recordsByActionGroupId The database records to roll up, arranged by action group ID.
     * @return The {@link RolledUpActionGroupStat} for each action group ID.
     */
    protected abstract Map<Integer, RolledUpActionGroupStat> rollupRecords(
            final int numStatSnapshotsInRange,
            @Nonnull final Map<Integer, List<StatWithSnapshotCnt<STAT_RECORD>>> recordsByActionGroupId);

    protected abstract int numSnapshotsInSnapshotRecord(@Nonnull final SNAPSHOT_RECORD snapshotRecord);

    protected abstract RolledUpActionGroupStat recordToGroupStat(STAT_RECORD record);

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Map<LocalDateTime, Map<ActionGroup, RolledUpActionGroupStat>> query(
            @Nonnull final TimeRange timeRange,
            @Nonnull final Set<Integer> mgmtUnitSubgroups,
            @Nonnull final MatchedActionGroups matchedActionGroups) {

        Preconditions.checkArgument(!mgmtUnitSubgroups.isEmpty());

        final LocalDateTime startTime = LocalDateTime.ofInstant(
            Instant.ofEpochMilli(timeRange.getStartTime()),
            clock.getZone());
        final LocalDateTime endTime = LocalDateTime.ofInstant(
            Instant.ofEpochMilli(timeRange.getEndTime()),
            clock.getZone());

        final Map<LocalDateTime, Map<ActionGroup, RolledUpActionGroupStat>> results =
            // Use linked hash map to preserve order of inserts.
            new LinkedHashMap<>();
        try (DataMetricTimer timer = Metrics.QUERY_TIME.labels(tableInfo.shortTableName()).startTimer()) {
            dslContext.select(tableInfo.snapshotTableSnapshotTime())
                .from(tableInfo.snapshotTable())
                .where(tableInfo.snapshotTableSnapshotTime().between(startTime, endTime))
                // Important - this will make sure the timestamps are returned in the right order.
                //
                // TODO (roman, Jan 17 2019): We may want to provide the sort order externally.
                // For now doesn't seem like there's a need.
                .orderBy(tableInfo.snapshotTableSnapshotTime().asc())
                .fetch(tableInfo.snapshotTableSnapshotTime())
                .forEach(snapshotTime -> results.put(snapshotTime, new HashMap<>()));

            final List<Condition> conditions = new ArrayList<>(3);
            conditions.add(tableInfo.statTableSnapshotTime().in(results.keySet()));
            conditions.add(tableInfo.mgmtUnitSubgroupIdField().in(mgmtUnitSubgroups));

            // If all action groups are acceptable, don't add the condition.
            if (!matchedActionGroups.allActionGroups()) {
                conditions.add(tableInfo.actionGroupIdField().in(
                    matchedActionGroups.specificActionGroupsById().keySet()));
            }


            dslContext.selectFrom(tableInfo.statTable())
                .where(conditions)
                .fetch()
                .forEach(record -> {
                    final LocalDateTime time = record.get(tableInfo.statTableSnapshotTime());
                    final Integer actionGroupId = record.get(tableInfo.actionGroupIdField());
                    final ActionGroup actionGroup =
                        matchedActionGroups.specificActionGroupsById().get(actionGroupId);
                    final RolledUpActionGroupStat stat = recordToGroupStat(record);
                    results.get(time).put(actionGroup, stat);
                });
        }

        return results;
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<RolledUpActionStats> rollup(final int mgmtUnitSubgroupId,
                                                @Nonnull final LocalDateTime startTime) {
        if (!toTableOpt.isPresent()) {
            logger.error("Attempting to rollup table that shouldn't get rolled up: {}",
                tableInfo);
            return Optional.empty();
        } else {
            final TableInfo<? extends Record, ? extends Record> toTable = toTableOpt.get();
            // Check that the start time is an "even" amount of the time unit to roll up to.
            // For example, if rolling up from "latest" to "hour" the start time should be the top of
            // the hour - i.e 17:00, not 17:01
            logger.debug("Rollup mgmt unit subgroup {} with temporal unit {} for time range " +
                "starting at {}", mgmtUnitSubgroupId, toTable.temporalUnit(), startTime);
            Preconditions.checkArgument(toTable.timeTruncateFn().apply(startTime).equals(startTime));

            try (DataMetricTimer timer = ActionStatRollup.STAT_ROLLUP_SUMMARY
                .labels(ActionStatRollup.ROLLUP_STEP, tableInfo.shortTableName())
                .startTimer()) {
                return dslContext.transactionResult(transactionContext -> {
                    final DSLContext transaction = DSL.using(transactionContext);
                    return doRollup(transaction, toTable, mgmtUnitSubgroupId, startTime);
                });
            }
        }
    }

    @Nonnull
    private Optional<RolledUpActionStats> doRollup(
                @Nonnull final DSLContext transaction,
                @Nonnull final TableInfo<? extends Record, ? extends Record> toTable,
                final int mgmtUnitSubgroupId,
                @Nonnull final LocalDateTime startTime) {
        final LocalDateTime endTime = startTime.plus(1, toTable.temporalUnit())
                // Subtract a microsecond to get to the latest possible time before the start of
                // the next rollup period. We use a microsecond because that's MariaDB's precision:
                // https://mariadb.com/kb/en/library/timestamp/
                .minus(1, ChronoUnit.MICROS);
        final Map<LocalDateTime, SNAPSHOT_RECORD> snapshotRecordsInRangeByTime =
            transaction.selectFrom(tableInfo.snapshotTable())
                .where(tableInfo.snapshotTableSnapshotTime().between(startTime, endTime))
                .fetch()
                .stream()
                .collect(Collectors.toMap(
                    snapshotRecord -> snapshotRecord.get(tableInfo.snapshotTableSnapshotTime()),
                    Function.identity()));
        final int actionPlanSnapshotsInRange = snapshotRecordsInRangeByTime.values().stream()
            .mapToInt(this::numSnapshotsInSnapshotRecord)
            .sum();

        logger.debug("Mgmt unit subgroup {} has {} snapshot records " +
                "(representing {} action plan snapshots) in range",
            mgmtUnitSubgroupId, snapshotRecordsInRangeByTime.size(), actionPlanSnapshotsInRange);

        if (actionPlanSnapshotsInRange == 0) {
            return Optional.empty();
        }

        final AtomicInteger rowCount = new AtomicInteger(0);
        final Map<Integer, List<StatWithSnapshotCnt<STAT_RECORD>>> relevantRecords =
            transaction.selectFrom(tableInfo.statTable())
                .where(tableInfo.mgmtUnitSubgroupIdField().eq(mgmtUnitSubgroupId))
                .and(tableInfo.statTableSnapshotTime().between(startTime, endTime))
                .fetch()
                .stream()
                .map(statRecord -> {
                    rowCount.incrementAndGet();
                    final SNAPSHOT_RECORD snapshotRecord =
                        snapshotRecordsInRangeByTime.get(statRecord.get(tableInfo.statTableSnapshotTime()));
                    return ImmutableStatWithSnapshotCnt.<STAT_RECORD>builder()
                        .record(statRecord)
                        .numActionSnapshots(numSnapshotsInSnapshotRecord(snapshotRecord))
                        .build();
                })
                .collect(Collectors.groupingBy(record -> tableInfo.actionGroupIdExtractor().apply(record.record())));

        // Record the number of rows we're going to roll up.
        Metrics.NUM_ROWS_ROLLED_UP.labels(tableInfo.shortTableName()).observe(rowCount.doubleValue());

        logger.debug("Mgmt unit subgroup {} has snapshot records for {} action groups " +
            "in time range. Total number of rows: {}",
            mgmtUnitSubgroupId, relevantRecords.keySet().size(), rowCount.intValue());

        final Map<Integer, RolledUpActionGroupStat> statsByActionGroup =
            rollupRecords(actionPlanSnapshotsInRange, relevantRecords);
        return Optional.of(ImmutableRolledUpActionStats.builder()
            .startTime(startTime)
            .numActionSnapshots(actionPlanSnapshotsInRange)
            .putAllStatsByActionGroupId(statsByActionGroup)
            .build());
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<RollupReadyInfo> rollupReadyTimes() {
        if (toTableOpt.isPresent()) {
            final TableInfo<? extends Record, ? extends Record> toTable = toTableOpt.get();
            try (final DataMetricTimer timer = Metrics.ROLLUP_READY_TIME
                    .labels(tableInfo.shortTableName())
                    .startTimer()) {
                return dslContext.transactionResult(transactionContext -> {
                    final DSLContext transaction = DSL.using(transactionContext);
                    return findRollupReadyTimes(transaction, toTable);
                });
            }
        } else {
            logger.error("Asking for rollup ready times in table that doesn't get rolled up: {}",
                tableInfo);
            return Collections.emptyList();
        }
    }

    @Nonnull
    private List<RollupReadyInfo> findRollupReadyTimes(
            @Nonnull final DSLContext transaction,
            @Nonnull final TableInfo<? extends Record, ? extends Record> toTable) {
        final List<LocalDateTime> allSnapshotTimes =
            transaction.select(tableInfo.snapshotTableSnapshotTime())
                .from(tableInfo.snapshotTable())
                .fetch(tableInfo.snapshotTableSnapshotTime());
        if (allSnapshotTimes.isEmpty()) {
            return Collections.emptyList();
        }

        LocalDateTime latestTruncatedTime = LocalDateTime.MIN;
        final Map<LocalDateTime, List<LocalDateTime>> snapshotsByTruncatedTime = new HashMap<>();
        for (LocalDateTime snapshotTime : allSnapshotTimes) {
            final LocalDateTime truncatedTime = toTable.timeTruncateFn().apply(snapshotTime);
            final List<LocalDateTime> daySnapshots =
                snapshotsByTruncatedTime.computeIfAbsent(truncatedTime, k -> new ArrayList<>());
            daySnapshots.add(snapshotTime);
            if (truncatedTime.isAfter(latestTruncatedTime)) {
                latestTruncatedTime = truncatedTime;
            }
        }

        // Remove the most recent time unit - we don't know if we'll get more snapshots before
        // this time unit is over. e.g. if rolling up the hour, and it's currently 16:45,
        // 16:00 is not ready for roll-up yet because we may get another snapshot.
        snapshotsByTruncatedTime.remove(latestTruncatedTime);

        // Remove hours that have already been rolled up.
        final Set<LocalDateTime> alreadyRolledUp =
            transaction.select(toTable.snapshotTableSnapshotTime())
                .from(toTable.snapshotTable())
                .where(toTable.snapshotTableSnapshotTime().in(snapshotsByTruncatedTime.keySet()))
                .fetchSet(toTable.snapshotTableSnapshotTime());
        alreadyRolledUp.forEach(snapshotsByTruncatedTime::remove);

        final List<RollupReadyInfo> retList = new ArrayList<>();
        snapshotsByTruncatedTime.forEach((truncatedTime, snapshotTimes) -> {
            final Set<Integer> mgmtUnitSubgroups =
                transaction.selectDistinct(tableInfo.mgmtUnitSubgroupIdField())
                    .from(tableInfo.statTable())
                    // This is probably faster than doing a range search, and the number
                    // of snapshots in each time unit should be small (<=31) so it won't be an
                    // overly long query.
                    .where(tableInfo.statTableSnapshotTime().in(snapshotTimes))
                    .fetchSet(tableInfo.mgmtUnitSubgroupIdField());

            retList.add(ImmutableRollupReadyInfo.builder()
                .addAllManagementUnits(mgmtUnitSubgroups)
                .startTime(truncatedTime)
                .build());
        });
        return retList;
    }

    /**
     * A stat record, and the number of action snapshots that are rolled up into the record.
     * See: {@link RolledUpActionStats#numActionSnapshots()}.
     *
     * @param <STAT_RECORD_> The type of record.
     */
    @Value.Immutable
    public interface StatWithSnapshotCnt<STAT_RECORD_ extends Record> {
        int numActionSnapshots();
        STAT_RECORD_ record();
    }

    static class Metrics {
        static final String TABLE_NAME_LABEL = "table";

        static final DataMetricHistogram ROLLUP_READY_TIME = DataMetricHistogram.builder()
            .withName("ao_stat_table_rollup_ready_time_seconds")
            .withHelp("The amount of time it took to calculate the times ready for rollup in a table.")
            .withLabelNames(TABLE_NAME_LABEL)
            // We expect most rollup time calculations to be quick - it's a relatively simple
            // query plus some client-side calculation.
            .withBuckets(0.5, 1.0, 5.0, 10.0, 30.0)
            .build()
            .register();

        static final DataMetricHistogram QUERY_TIME = DataMetricHistogram.builder()
            .withName("ao_stat_table_query_time_seconds")
            .withHelp("The amount of time it took to execute a stat query on a stat table.")
            .withLabelNames(TABLE_NAME_LABEL)
            // Generally stats queries should be fairly quick - targetting an individual
            // table and hitting indexes.
            .withBuckets(0.1, 0.5, 1.0, 4.0, 10.0)
            .build()
            .register();

        static final DataMetricHistogram NUM_ROWS_ROLLED_UP = DataMetricHistogram.builder()
            .withName("ao_stat_table_num_rows_rolled_up")
            .withHelp("The amount of rows in a particular rollup (for a specific time + mgmt subunit)")
            // We expect most management units to only have actions for a small subset of action
            // groups.
            .withBuckets(50, 100, 200, 500, 1000)
            .withLabelNames(TABLE_NAME_LABEL)
            .build()
            .register();
    }
}

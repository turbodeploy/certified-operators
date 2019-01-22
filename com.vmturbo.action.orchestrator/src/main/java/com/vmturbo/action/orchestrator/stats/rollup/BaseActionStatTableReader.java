package com.vmturbo.action.orchestrator.stats.rollup;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;

import com.google.common.base.Preconditions;

import com.vmturbo.action.orchestrator.stats.rollup.ActionStatRollupScheduler.ActionStatRollup;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionStats;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RollupReadyInfo;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.TableInfo;
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

    /**
     * The {@link TableInfo} for the table the reader is for.
     */
    private final TableInfo<STAT_RECORD, SNAPSHOT_RECORD> tableInfo;

    /**
     * The {@link TableInfo} for the destination table - the table we'll roll up this table's
     * action stats into.
     */
    private final TableInfo<? extends Record, ? extends Record> toTable;

    protected final RolledUpStatCalculator statCalculator;

    protected BaseActionStatTableReader(@Nonnull final DSLContext dslContext,
                                        @Nonnull final RolledUpStatCalculator statCalculator,
                                        @Nonnull final TableInfo<STAT_RECORD, SNAPSHOT_RECORD> tableInfo,
                                        @Nonnull final TableInfo<? extends Record, ? extends Record> toTable) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.statCalculator = Objects.requireNonNull(statCalculator);
        this.tableInfo = Objects.requireNonNull(tableInfo);
        this.toTable = Objects.requireNonNull(toTable);
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

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<RolledUpActionStats> rollup(final int mgmtUnitSubgroupId,
                                                @Nonnull final LocalDateTime startTime) {
        // Check that the start time is an "even" amount of the time unit to roll up to.
        // For example, if rolling up from "latest" to "hour" the start time should be the top of
        // the hour - i.e 17:00, not 17:01
        Preconditions.checkArgument(toTable.timeTruncateFn().apply(startTime).equals(startTime));
        logger.debug("Rollup mgmt unit subgroup {} with temporal unit {} for time range " +
                "starting at {}", mgmtUnitSubgroupId, toTable.temporalUnit(), startTime);

        try (DataMetricTimer timer = ActionStatRollup.STAT_ROLLUP_SUMMARY
                .labels(ActionStatRollup.ROLLUP_STEP)
                .startTimer()) {
            return dslContext.transactionResult(transactionContext -> {
                final DSLContext transaction = DSL.using(transactionContext);
                return doRollup(transaction, mgmtUnitSubgroupId, startTime);
            });
        }
    }

    @Nonnull
    private Optional<RolledUpActionStats> doRollup(@Nonnull final DSLContext transaction,
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

        final Map<Integer, List<StatWithSnapshotCnt<STAT_RECORD>>> relevantRecords =
            transaction.selectFrom(tableInfo.statTable())
                .where(tableInfo.mgmtUnitSubgroupIdField().eq(mgmtUnitSubgroupId))
                .and(tableInfo.statTableSnapshotTime().between(startTime, endTime))
                .fetch()
                .stream()
                .map(statRecord -> {
                    final SNAPSHOT_RECORD snapshotRecord =
                        snapshotRecordsInRangeByTime.get(statRecord.get(tableInfo.statTableSnapshotTime()));
                    return ImmutableStatWithSnapshotCnt.<STAT_RECORD>builder()
                        .record(statRecord)
                        .numActionSnapshots(numSnapshotsInSnapshotRecord(snapshotRecord))
                        .build();
                })
                .collect(Collectors.groupingBy(record -> tableInfo.actionGroupId().apply(record.record())));

        logger.debug("Mgmt unit subgroup {} has snapshot records for {} action groups " +
            "in time range.", mgmtUnitSubgroupId, relevantRecords.keySet().size());

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
        return dslContext.transactionResult(transactionContext -> {
            final DSLContext transaction = DSL.using(transactionContext);
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
        });
    }

    /**
     * A stat record, and the number of action snapshots that are rolled up into the record.
     * See: {@link RolledUpActionStats#numActionSnapshots()}.
     *
     * @param <STAT_RECORD> The type of record.
     */
    @Value.Immutable
    public interface StatWithSnapshotCnt<STAT_RECORD extends Record> {
        int numActionSnapshots();
        STAT_RECORD record();
    }
}

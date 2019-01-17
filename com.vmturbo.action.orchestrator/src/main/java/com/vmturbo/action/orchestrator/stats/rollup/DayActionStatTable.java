package com.vmturbo.action.orchestrator.stats.rollup;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.Record;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotDayRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByDayRecord;

/**
 * An {@link ActionStatTable} for action stats by day.
 */
public class DayActionStatTable implements ActionStatTable {

    public static final TableInfo<ActionStatsByDayRecord, ActionSnapshotDayRecord> DAY_TABLE_INFO =
        ImmutableTableInfo.<ActionStatsByDayRecord, ActionSnapshotDayRecord>builder()
            .statTableSnapshotTime(Tables.ACTION_STATS_BY_DAY.DAY_TIME)
            .snapshotTableSnapshotTime(Tables.ACTION_SNAPSHOT_DAY.DAY_TIME)
            .mgmtUnitSubgroupIdField(Tables.ACTION_STATS_BY_DAY.MGMT_UNIT_SUBGROUP_ID)
            .statTable(Tables.ACTION_STATS_BY_DAY)
            .snapshotTable(Tables.ACTION_SNAPSHOT_DAY)
            .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.DAYS))
            .temporalUnit(ChronoUnit.DAYS)
            .actionGroupId(ActionStatsByDayRecord::getActionGroupId)
            .build();

    private final DSLContext dslContext;

    private final Clock clock;

    private final RolledUpStatCalculator statCalculator;

    private final TableInfo<? extends Record, ? extends Record> toTableInfo;

    public DayActionStatTable(@Nonnull final DSLContext dslContext,
                              @Nonnull final Clock clock,
                              @Nonnull final RolledUpStatCalculator statCalculator,
                              @Nonnull final TableInfo<? extends Record, ? extends Record> toTableInfo) {
        this.dslContext = dslContext;
        this.clock = clock;
        this.statCalculator = statCalculator;
        this.toTableInfo = toTableInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Reader> reader() {
        return Optional.of(new DailyReader(dslContext, statCalculator, toTableInfo));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Writer> writer() {
        return Optional.of(new DailyWriter(dslContext, clock));
    }

    /**
     * The {@link ActionStatTable.Reader} for the daily stats table.
     */
    @VisibleForTesting
    static class DailyReader extends BaseActionStatTableReader<ActionStatsByDayRecord, ActionSnapshotDayRecord> {

        private DailyReader(final DSLContext dslContext,
                             final RolledUpStatCalculator statCalculator,
                             final TableInfo<? extends Record, ? extends Record> toTableInfo) {
            super(dslContext, statCalculator, DAY_TABLE_INFO, toTableInfo);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected Map<Integer, RolledUpActionGroupStat> rollupRecords(
            final int numSnapshotsInRange,
            @Nonnull final Map<Integer, List<StatWithSnapshotCnt<ActionStatsByDayRecord>>> recordsByActionGroupId) {
            final Map<Integer, RolledUpActionGroupStat> rolledUpStats = new HashMap<>();
            recordsByActionGroupId.forEach((actionGroupId, recordsForGroup) -> {
                statCalculator.rollupDayRecords(numSnapshotsInRange, recordsForGroup)
                    .ifPresent(rolledUpGroupStat -> {
                        rolledUpStats.put(actionGroupId, rolledUpGroupStat);
                    });
            });
            return rolledUpStats;
        }

        @Override
        protected int numSnapshotsInSnapshotRecord(@Nonnull final ActionSnapshotDayRecord actionSnapshotDayRecord) {
            return actionSnapshotDayRecord.getNumActionSnapshots();
        }
    }

    /**
     * The {@link ActionStatTable.Writer} for the daily stats table.
     */
    @VisibleForTesting
    static class DailyWriter extends BaseActionStatTableWriter<ActionStatsByDayRecord, ActionSnapshotDayRecord> {

        private DailyWriter(final DSLContext dslContext, final Clock clock) {
            super(dslContext, clock, DAY_TABLE_INFO);
        }

        /**
         * {@inheritDoc}
         */
        @Nonnull
        @Override
        protected ActionStatsByDayRecord statRecord(final int mgmtUnitSubgroupId,
                                                    final int actionGroupId,
                                                    @Nonnull final LocalDateTime startTime,
                                                    @Nonnull final RolledUpActionGroupStat rolledUpActionGroupStats) {
            Preconditions.checkArgument(DAY_TABLE_INFO.timeTruncateFn().apply(startTime).equals(startTime));
            final ActionStatsByDayRecord record = new ActionStatsByDayRecord();
            record.setDayTime(startTime);
            record.setActionGroupId(actionGroupId);
            record.setMgmtUnitSubgroupId(mgmtUnitSubgroupId);

            record.setAvgActionCount(BigDecimal.valueOf(rolledUpActionGroupStats.avgActionCount()));
            record.setMaxActionCount(rolledUpActionGroupStats.maxActionCount());
            record.setMinActionCount(rolledUpActionGroupStats.minActionCount());

            record.setAvgEntityCount(BigDecimal.valueOf(rolledUpActionGroupStats.avgEntityCount()));
            record.setMaxEntityCount(rolledUpActionGroupStats.maxEntityCount());
            record.setMinEntityCount(rolledUpActionGroupStats.minEntityCount());

            record.setAvgSavings(BigDecimal.valueOf(rolledUpActionGroupStats.avgSavings()));
            record.setMaxSavings(BigDecimal.valueOf(rolledUpActionGroupStats.maxSavings()));
            record.setMinSavings(BigDecimal.valueOf(rolledUpActionGroupStats.minSavings()));

            record.setAvgInvestment(BigDecimal.valueOf(rolledUpActionGroupStats.avgInvestment()));
            record.setMaxInvestment(BigDecimal.valueOf(rolledUpActionGroupStats.maxInvestment()));
            record.setMinInvestment(BigDecimal.valueOf(rolledUpActionGroupStats.minInvestment()));
            return record;
        }

        /**
         * {@inheritDoc}
         */
        @Nonnull
        @Override
        protected ActionSnapshotDayRecord snapshotRecord(@Nonnull final LocalDateTime snapshotTime,
                                                         final int numActionSnaphots) {
            final ActionSnapshotDayRecord dayRecord = new ActionSnapshotDayRecord();
            dayRecord.setDayTime(snapshotTime);
            dayRecord.setDayRollupTime(LocalDateTime.now(clock));
            dayRecord.setNumActionSnapshots(numActionSnaphots);
            return dayRecord;
        }
    }
}

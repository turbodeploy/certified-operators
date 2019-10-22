package com.vmturbo.action.orchestrator.stats.rollup;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotMonthRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByMonthRecord;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;

/**
 * An {@link ActionStatTable} for action stats by month.
 */
public class MonthActionStatTable implements ActionStatTable {

    public static final TableInfo<ActionStatsByMonthRecord, ActionSnapshotMonthRecord> MONTH_TABLE_INFO =
        ImmutableTableInfo.<ActionStatsByMonthRecord, ActionSnapshotMonthRecord>builder()
            .statTableSnapshotTime(Tables.ACTION_STATS_BY_MONTH.MONTH_TIME)
            .snapshotTableSnapshotTime(Tables.ACTION_SNAPSHOT_MONTH.MONTH_TIME)
            .mgmtUnitSubgroupIdField(Tables.ACTION_STATS_BY_MONTH.MGMT_UNIT_SUBGROUP_ID)
            .statTable(Tables.ACTION_STATS_BY_MONTH)
            .snapshotTable(Tables.ACTION_SNAPSHOT_MONTH)
            .timeTruncateFn(time -> LocalDateTime.of(time.getYear(), time.getMonth(), 1, 0, 0))
            .temporalUnit(ChronoUnit.MONTHS)
            .actionGroupIdField(Tables.ACTION_STATS_BY_MONTH.ACTION_GROUP_ID)
            .actionGroupIdExtractor(ActionStatsByMonthRecord::getActionGroupId)
            .shortTableName("month")
            .build();

    private final DSLContext dslContext;
    private final Clock clock;

    public MonthActionStatTable(@Nonnull final DSLContext dslContext,
                                @Nonnull final Clock clock) {
        this.dslContext = dslContext;
        this.clock = clock;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Reader reader() {
        // No reader for the monthly table because it doesn't roll over into any other table.
        return new MonthlyReader(dslContext, clock);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Writer writer() {
        return new MonthlyWriter(dslContext, clock);
    }

    @Nonnull
    @Override
    public LocalDateTime getTrimTime(@Nonnull final RetentionPeriods retentionPeriods) {
        return MONTH_TABLE_INFO.timeTruncateFn().apply(
            LocalDateTime.now(clock).minusMonths(retentionPeriods.monthlyRetentionMonths()));
    }

    /**
     * The {@link ActionStatTable.Reader} for the monthly stats table.
     */
    @VisibleForTesting
    static class MonthlyReader extends BaseActionStatTableReader<ActionStatsByMonthRecord, ActionSnapshotMonthRecord> {

        private MonthlyReader(@Nonnull final DSLContext dslContext,
                             @Nonnull final Clock clock) {
            super(dslContext, clock, MONTH_TABLE_INFO, Optional.empty());
        }

        @Override
        protected Map<Integer, RolledUpActionGroupStat> rollupRecords(final int numStatSnapshotsInRange,
                @Nonnull final Map<Integer, List<StatWithSnapshotCnt<ActionStatsByMonthRecord>>> recordsByActionGroupId) {
            throw new UnsupportedOperationException("Unexpected rollup on monthly record.");
        }

        @Override
        protected int numSnapshotsInSnapshotRecord(@Nonnull final ActionSnapshotMonthRecord record) {
            return record.getNumActionSnapshots();
        }

        @Override
        protected RolledUpActionGroupStat recordToGroupStat(final ActionStatsByMonthRecord record) {
            // note: priorActionCount and newActionCount are not recorded for action_stats_by_month
            return ImmutableRolledUpActionGroupStat.builder()
                .priorActionCount(0)
                .newActionCount(0)
                .avgActionCount(record.getAvgActionCount().doubleValue())
                .minActionCount(record.getMinActionCount())
                .maxActionCount(record.getMaxActionCount())
                .avgEntityCount(record.getAvgEntityCount().doubleValue())
                .minEntityCount(record.getMinEntityCount())
                .maxEntityCount(record.getMaxEntityCount())
                .avgSavings(record.getAvgSavings().doubleValue())
                .minSavings(record.getMinSavings().doubleValue())
                .maxSavings(record.getMaxSavings().doubleValue())
                .avgInvestment(record.getAvgInvestment().doubleValue())
                .minInvestment(record.getMinInvestment().doubleValue())
                .maxInvestment(record.getMaxInvestment().doubleValue())
                .build();
        }
    }

    @VisibleForTesting
    static class MonthlyWriter extends BaseActionStatTableWriter<ActionStatsByMonthRecord, ActionSnapshotMonthRecord> {

        private MonthlyWriter(final DSLContext dslContext, final Clock clock) {
            super(dslContext, clock, MONTH_TABLE_INFO);
        }

        /**
         * {@inheritDoc}
         */
        @Nonnull
        @Override
        protected ActionStatsByMonthRecord statRecord(final int mgmtUnitSubgroupId,
                                                      final int actionGroupId,
                                                      @Nonnull final LocalDateTime startTime,
                                                      @Nonnull final RolledUpActionGroupStat rolledUpActionGroupStats) {
            Preconditions.checkArgument(MONTH_TABLE_INFO.timeTruncateFn().apply(startTime).equals(startTime));
            final ActionStatsByMonthRecord record = new ActionStatsByMonthRecord();
            record.setMonthTime(startTime);
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
        protected ActionSnapshotMonthRecord snapshotRecord(@Nonnull final LocalDateTime snapshotTime,
                                                           final int numActionSnapshots) {
            final ActionSnapshotMonthRecord dayRecord = new ActionSnapshotMonthRecord();
            dayRecord.setMonthTime(snapshotTime);
            dayRecord.setMonthRollupTime(LocalDateTime.now(clock));
            dayRecord.setNumActionSnapshots(numActionSnapshots);
            return dayRecord;
        }
    }
}

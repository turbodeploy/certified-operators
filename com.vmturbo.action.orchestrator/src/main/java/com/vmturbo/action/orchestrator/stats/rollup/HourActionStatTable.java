package com.vmturbo.action.orchestrator.stats.rollup;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.Record;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotHourRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByHourRecord;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;

/**
 * An {@link ActionStatTable} for action stats by hour.
 */
public class HourActionStatTable implements ActionStatTable {

    public static final TableInfo<ActionStatsByHourRecord, ActionSnapshotHourRecord> HOUR_TABLE_INFO =
        ImmutableTableInfo.<ActionStatsByHourRecord, ActionSnapshotHourRecord>builder()
            .statTableSnapshotTime(Tables.ACTION_STATS_BY_HOUR.HOUR_TIME)
            .snapshotTableSnapshotTime(Tables.ACTION_SNAPSHOT_HOUR.HOUR_TIME)
            .mgmtUnitSubgroupIdField(Tables.ACTION_STATS_BY_HOUR.MGMT_UNIT_SUBGROUP_ID)
            .statTable(Tables.ACTION_STATS_BY_HOUR)
            .snapshotTable(Tables.ACTION_SNAPSHOT_HOUR)
            .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.HOURS))
            .temporalUnit(ChronoUnit.HOURS)
            .actionGroupIdField(Tables.ACTION_STATS_BY_HOUR.ACTION_GROUP_ID)
            .actionGroupIdExtractor(ActionStatsByHourRecord::getActionGroupId)
            .shortTableName("hour")
            .build();

    private final DSLContext dslContext;
    private final Clock clock;
    private final RolledUpStatCalculator statCalculator;
    private final TableInfo<? extends Record, ? extends Record> toTableInfo;

    public HourActionStatTable(final DSLContext dslContext,
                               final Clock clock,
                               final RolledUpStatCalculator statCalculator,
                               final TableInfo<? extends Record, ? extends Record> toTableInfo) {
        this.dslContext = dslContext;
        this.clock = clock;
        this.statCalculator = statCalculator;
        this.toTableInfo = toTableInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Reader reader() {
        return new HourlyReader(dslContext, clock, statCalculator, toTableInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Writer writer() {
        return new HourlyWriter(dslContext, clock);
    }

    @Nonnull
    @Override
    public LocalDateTime getTrimTime(@Nonnull final RetentionPeriods retentionPeriods) {
        return HOUR_TABLE_INFO.timeTruncateFn().apply(
            LocalDateTime.now(clock).minusHours(retentionPeriods.hourlyRetentionHours()));
    }

    /**
     * The {@link ActionStatTable.Reader} for the hourly stats table.
     */
    @VisibleForTesting
    static class HourlyReader extends BaseActionStatTableReader<ActionStatsByHourRecord, ActionSnapshotHourRecord> {

        private final RolledUpStatCalculator statCalculator;

        private HourlyReader(@Nonnull final DSLContext dslContext,
                             @Nonnull final Clock clock,
                             @Nonnull final RolledUpStatCalculator statCalculator,
                             @Nonnull final TableInfo<? extends Record, ? extends Record> toTableInfo) {
            super(dslContext, clock, HOUR_TABLE_INFO, Optional.of(toTableInfo));
            this.statCalculator = Objects.requireNonNull(statCalculator);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @Nonnull
        protected Map<Integer, RolledUpActionGroupStat> rollupRecords(
                    final int numSnapshotsInRange,
                    @Nonnull final Map<Integer, List<StatWithSnapshotCnt<ActionStatsByHourRecord>>>
                        recordsByActionGroupId) {
            final Map<Integer, RolledUpActionGroupStat> rolledUpStats = new HashMap<>();
            recordsByActionGroupId.forEach((actionGroupId, recordsForGroup) ->
                statCalculator.rollupHourRecords(numSnapshotsInRange, recordsForGroup)
                    .ifPresent(rolledUpGroupStat ->
                        rolledUpStats.put(actionGroupId, rolledUpGroupStat)));
            return rolledUpStats;
        }

        @Override
        protected int numSnapshotsInSnapshotRecord(@Nonnull final ActionSnapshotHourRecord
                                                               actionSnapshotHourRecord) {
            return actionSnapshotHourRecord.getNumActionSnapshots();
        }

        @Override
        protected RolledUpActionGroupStat recordToGroupStat(final ActionStatsByHourRecord record) {
            return ImmutableRolledUpActionGroupStat.builder()
                .priorActionCount(record.getPriorActionCount())
                .newActionCount(record.getNewActionCount())
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

    /**
     * The {@link ActionStatTable.Writer} for the hourly stats table.
     */
    @VisibleForTesting
    static class HourlyWriter extends BaseActionStatTableWriter<ActionStatsByHourRecord, ActionSnapshotHourRecord> {

        private HourlyWriter(final DSLContext dslContext, final Clock clock) {
            super(dslContext, clock, HOUR_TABLE_INFO);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @Nonnull
        protected ActionStatsByHourRecord statRecord(final int mgmtUnitSubgroupId,
                                                     final int actionGroupId,
                                                     @Nonnull final LocalDateTime startTime,
                                                     @Nonnull final RolledUpActionGroupStat rolledUpActionGroupStats) {
            Preconditions.checkArgument(HOUR_TABLE_INFO.timeTruncateFn().apply(startTime).equals(startTime));
            final ActionStatsByHourRecord record = new ActionStatsByHourRecord();
            record.setHourTime(startTime);
            record.setActionGroupId(actionGroupId);
            record.setMgmtUnitSubgroupId(mgmtUnitSubgroupId);

            record.setPriorActionCount(rolledUpActionGroupStats.priorActionCount());
            record.setNewActionCount(rolledUpActionGroupStats.newActionCount());
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
        @Override
        @Nonnull
        protected ActionSnapshotHourRecord snapshotRecord(@Nonnull final LocalDateTime snapshotTime,
                                                          final int totalNumActionSnapshots) {
            final ActionSnapshotHourRecord hourRecord = new ActionSnapshotHourRecord();
            hourRecord.setHourTime(snapshotTime);
            hourRecord.setHourRollupTime(LocalDateTime.now(clock));
            hourRecord.setNumActionSnapshots(totalNumActionSnapshots);
            return hourRecord;
        }
    }
}

package com.vmturbo.action.orchestrator.stats.rollup;

import java.time.Clock;
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

import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotLatestRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;

/**
 * An {@link ActionStatTable} for latest action stats. This is the table all stats initially go to.
 */
public class LatestActionStatTable implements ActionStatTable {

    public static final TableInfo<ActionStatsLatestRecord, ActionSnapshotLatestRecord> LATEST_TABLE_INFO =
        ImmutableTableInfo.<ActionStatsLatestRecord, ActionSnapshotLatestRecord>builder()
            .statTableSnapshotTime(Tables.ACTION_STATS_LATEST.ACTION_SNAPSHOT_TIME)
            .snapshotTableSnapshotTime(Tables.ACTION_SNAPSHOT_LATEST.ACTION_SNAPSHOT_TIME)
            .mgmtUnitSubgroupIdField(Tables.ACTION_STATS_LATEST.MGMT_UNIT_SUBGROUP_ID)
            .statTable(Tables.ACTION_STATS_LATEST)
            .snapshotTable(Tables.ACTION_SNAPSHOT_LATEST)
            // Not really necessary/used for "LATEST" table.
            .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.MINUTES))
            .temporalUnit(ChronoUnit.MINUTES)
            .actionGroupIdField(Tables.ACTION_STATS_LATEST.ACTION_GROUP_ID)
            .actionGroupIdExtractor(ActionStatsLatestRecord::getActionGroupId)
            .shortTableName("latest")
            .build();

    private final DSLContext dslContext;
    private final Clock clock;
    private final RolledUpStatCalculator statCalculator;
    private final TableInfo<? extends Record, ? extends Record> toTableInfo;

    public LatestActionStatTable(@Nonnull final DSLContext dslContext,
                                 @Nonnull final Clock clock,
                                 @Nonnull final RolledUpStatCalculator statCalculator,
                                 @Nonnull final TableInfo<? extends Record, ? extends Record> toTableInfo) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.clock = Objects.requireNonNull(clock);
        this.statCalculator = Objects.requireNonNull(statCalculator);
        this.toTableInfo = Objects.requireNonNull(toTableInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Reader reader() {
        return new LatestReader(dslContext, clock, statCalculator, toTableInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Writer> writer() {
        // No writer for the latest table, because no tables roll over into it.
        return Optional.empty();
    }

    /**
     * The {@link ActionStatTable.Reader} for the daily stats table.
     */
    @VisibleForTesting
    static class LatestReader extends BaseActionStatTableReader<ActionStatsLatestRecord, ActionSnapshotLatestRecord> {

        private final RolledUpStatCalculator statCalculator;

        private LatestReader(@Nonnull final DSLContext dsl,
                             @Nonnull final Clock clock,
                             @Nonnull final RolledUpStatCalculator statCalculator,
                             @Nonnull final TableInfo<? extends Record, ? extends Record> toTableInfo) {
            super(dsl, clock, LATEST_TABLE_INFO, Optional.of(toTableInfo));
            this.statCalculator = Objects.requireNonNull(statCalculator);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected Map<Integer, RolledUpActionGroupStat> rollupRecords(
                final int numSnapshotsInRange,
                @Nonnull final Map<Integer, List<StatWithSnapshotCnt<ActionStatsLatestRecord>>> recordsByActionGroupId) {
            final Map<Integer, RolledUpActionGroupStat> rolledUpStats = new HashMap<>();
            recordsByActionGroupId.forEach((actionGroupId, recordsForGroup) -> {
                statCalculator.rollupLatestRecords(numSnapshotsInRange, recordsForGroup)
                    .ifPresent(rolledUpGroupStat -> {
                        rolledUpStats.put(actionGroupId, rolledUpGroupStat);
                    });
            });
            return rolledUpStats;
        }

        @Override
        protected int numSnapshotsInSnapshotRecord(@Nonnull final ActionSnapshotLatestRecord actionSnapshotLatestRecord) {
            // Each latest snapshot record represents exactly one action plan snapshot.
            return 1;
        }

        @Override
        protected RolledUpActionGroupStat recordToGroupStat(final ActionStatsLatestRecord record) {
            return ImmutableRolledUpActionGroupStat.builder()
                .minActionCount(record.getTotalActionCount())
                .avgActionCount(record.getTotalActionCount())
                .maxActionCount(record.getTotalActionCount())
                .minEntityCount(record.getTotalEntityCount())
                .avgEntityCount(record.getTotalEntityCount())
                .maxEntityCount(record.getTotalEntityCount())
                .avgSavings(record.getTotalSavings().doubleValue())
                .minSavings(record.getTotalSavings().doubleValue())
                .maxSavings(record.getTotalSavings().doubleValue())
                .avgInvestment(record.getTotalInvestment().doubleValue())
                .minInvestment(record.getTotalInvestment().doubleValue())
                .maxInvestment(record.getTotalInvestment().doubleValue())
                .build();
        }
    }
}

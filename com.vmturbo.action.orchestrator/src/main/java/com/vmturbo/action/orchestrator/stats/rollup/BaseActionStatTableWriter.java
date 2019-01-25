package com.vmturbo.action.orchestrator.stats.rollup;

import java.time.Clock;
import java.time.LocalDateTime;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;

import com.vmturbo.action.orchestrator.stats.groups.ActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatRollupScheduler.ActionStatRollup;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionStats;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.TableInfo;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.Writer;
import com.vmturbo.proactivesupport.DataMetricHistogram;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Base implementation of {@link ActionStatTable.Writer}. Since all the action stat tables have
 * similar structures, pretty much all the code that interacts with the database can be shared.
 *
 * @param <SNAPSHOT_RECORD> The type of record in the specific {@link ActionStatTable} implementation
 *                      whose {@link ActionStatTable.Writer} extends this class.
 *
 */
public abstract class BaseActionStatTableWriter<STAT_RECORD extends Record, SNAPSHOT_RECORD extends Record> implements Writer {

    private final Logger logger = LogManager.getLogger(getClass());

    private final DSLContext dslContext;

    private final TableInfo<STAT_RECORD, SNAPSHOT_RECORD> tableInfo;

    protected final Clock clock;

    protected BaseActionStatTableWriter(final DSLContext dslContext,
                                        final Clock clock,
                                        final TableInfo<STAT_RECORD, SNAPSHOT_RECORD> tableInfo) {
        this.dslContext = dslContext;
        this.clock = clock;
        this.tableInfo = tableInfo;
    }

    /**
     * Create a stat record that can be inserted into the stat database table this writer is for.
     *
     * @param mgmtUnitSubgroupId The ID of the {@link MgmtUnitSubgroup} for the record.
     * @param actionGroupId The ID of the {@link ActionGroup} for the record.
     * @param rolledUpStat The {@link RolledUpActionGroupStat} containing the statistical information.
     * @return A {@link STAT_RECORD} that can be inserted into the database.
     */
    @Nonnull
    protected abstract STAT_RECORD statRecord(final int mgmtUnitSubgroupId,
                                              final int actionGroupId,
                                              @Nonnull final LocalDateTime startTime,
                                              @Nonnull final RolledUpActionGroupStat rolledUpStat);

    /**
     * Create a snapshot record that can be inserted into the snapshot database table this writer
     * is for.
     *
     * @param snapshotTime The time for the snapshot.
     * @param totalNumActionSnapshots The total number of action snapshots.
     *                               See: {@link RolledUpActionStats#numActionSnapshots()}.
     * @return A {@link SNAPSHOT_RECORD} that can be inserted into the database.
     */
    @Nonnull
    protected abstract SNAPSHOT_RECORD snapshotRecord(@Nonnull final LocalDateTime snapshotTime,
                                                      final int totalNumActionSnapshots);

    /**
     * {@inheritDoc}
     */
    @Override
    public void insert(final int mgmtUnitSubgroupId,
                       @Nonnull final RolledUpActionStats rolledUpStats) {
        try (DataMetricTimer timer = ActionStatRollup.STAT_ROLLUP_SUMMARY
            .labels(ActionStatRollup.INSERT_STEP, tableInfo.shortTableName())
            .startTimer()) {
            dslContext.transaction(transactionContext -> {
                final DSLContext transactionDsl = DSL.using(transactionContext);
                logger.debug("Inserting rolled up stats for {} action groups at time {} " +
                    "for mgmt unit subgroup {}", rolledUpStats.statsByActionGroupId().size(),
                    rolledUpStats.startTime(), mgmtUnitSubgroupId);
                doInsert(transactionDsl, mgmtUnitSubgroupId, rolledUpStats);
            });
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void trim(@Nonnull final LocalDateTime trimToTime) {
        try (DataMetricTimer timer = Metrics.CLEANUP_DURATION_HIST
                .labels(tableInfo.shortTableName())
                .startTimer()) {
            final int numRowsDeleted = dslContext.deleteFrom(tableInfo.statTable())
                .where(tableInfo.statTableSnapshotTime().lessThan(trimToTime))
                .execute();

            Metrics.NUM_ROWS_CLEANED_UP
                .labels(tableInfo.shortTableName())
                .observe((double)numRowsDeleted);

            final int numSnapshotsDeleted = dslContext.deleteFrom(tableInfo.snapshotTable())
                .where(tableInfo.snapshotTableSnapshotTime().lessThan(trimToTime))
                .execute();

            Metrics.NUM_SNAPSHOTS_CLEANED_UP
                .labels(tableInfo.shortTableName())
                .observe((double)numSnapshotsDeleted);

            logger.info("Trimed {} stat rows and {} snapshot rows.", numRowsDeleted, numSnapshotsDeleted);
        }
    }

    private void doInsert(@Nonnull final DSLContext transaction,
                          final int mgmtUnitSubgroupId,
                          @Nonnull final RolledUpActionStats rolledUpStats) {
        rolledUpStats.statsByActionGroupId().forEach((actionGroupId, rolledUpGroupStat) -> {
            final STAT_RECORD statRecord =
                statRecord(mgmtUnitSubgroupId, actionGroupId,
                    rolledUpStats.startTime(), rolledUpGroupStat);
            final int insertedRecords = transaction.insertInto(tableInfo.statTable())
                .set(statRecord)
                .execute();
            if (insertedRecords != 1) {
                logger.error("Got {} inserted records (instead of 1) for action group {}," +
                    " mgmt unit subgroup {}, and stat {}",
                    actionGroupId, mgmtUnitSubgroupId, rolledUpGroupStat);
            }
        });

        // This shouldn't really happen, but there's no harm inserting the snapshot record,
        // because the only reasonable way we get here is because there is a time unit
        // that needs to be rolled up.
        if (rolledUpStats.statsByActionGroupId().isEmpty()) {
            logger.warn("No stats for mgmt unit subgroup {} at time {}. Inserting " +
                "snapshot record anyway.", mgmtUnitSubgroupId, rolledUpStats.startTime());
        }

        // Make sure the snapshot table contains a snapshot with the start time of the
        // rollup we're inserting.
        final SNAPSHOT_RECORD snapshotRecord = snapshotRecord(rolledUpStats.startTime(),
            rolledUpStats.numActionSnapshots());
        transaction.insertInto(tableInfo.snapshotTable())
            .set(snapshotRecord)
            .onDuplicateKeyIgnore()
            .execute();
    }

    static class Metrics {

        static final String TABLE_NAME_LABEL = "table";

        static final DataMetricHistogram NUM_ROWS_CLEANED_UP = DataMetricHistogram.builder()
            .withName("ao_stat_table_num_rows_cleaned_up")
            .withHelp("The amount of rows in a particular cleanup operation (for a specific time range)")
            // Since we schedule cleanups soon after a time falls out of range, we don't expect
            // to be removing a lot of rows - roughly as much as can be accumulated in a single
            // time unit. However, we keep some large buckets to be able to see if our assumptions
            // hold in an actual customer's environment.
            .withBuckets(500, 1000, 5000, 10_000, 50_000, 100_000, 1_000_000)
            .withLabelNames(TABLE_NAME_LABEL)
            .build()
            .register();

        static final DataMetricHistogram NUM_SNAPSHOTS_CLEANED_UP = DataMetricHistogram.builder()
            .withName("ao_stat_table_num_snapshots_cleaned_up")
            .withHelp("The amount of snapshots in a particular cleanup operation (for a specific time range)")
            // Generally we should be doing the cleanups eagerly, so if the number of snapshots
            // cleaned up is high it means we're falling behind!
            .withBuckets(1.1, 2, 5, 10, 20)
            .withLabelNames(TABLE_NAME_LABEL)
            .build()
            .register();

        static final DataMetricHistogram CLEANUP_DURATION_HIST = DataMetricHistogram.builder()
            .withName("ao_stat_table_cleanup_duration_seconds")
            .withHelp("The duration of a particular cleanup operation, in seconds (for a specific time range).")
            // Cleanup operations are not very time critical, so we don't care about the
            // very low time ranges, but we expect them to finish relatively quickly.
            // The high buckets are just to track how "bad" long-running cleanups are.
            .withBuckets(5, 10, 30, 60, 300, 1800, 7200)
            .withLabelNames(TABLE_NAME_LABEL)
            .build()
            .register();
    }
}

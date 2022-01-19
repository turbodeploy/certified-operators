package com.vmturbo.action.orchestrator.stats.rollup;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.action.orchestrator.TestActionOrchestratorDbEndpointConfig;
import com.vmturbo.action.orchestrator.db.Action;
import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotLatestRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Tests for {@link BaseActionStatTableWriter}.
 */
@RunWith(Parameterized.class)
public class BaseActionStatTableWriterTest extends MultiDbTestBase {

    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public BaseActionStatTableWriterTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Action.ACTION, configurableDbDialect, dialect, "action-orchestrator",
                TestActionOrchestratorDbEndpointConfig::actionOrchestratorEndpoint);
        this.dsl = super.getDslContext();
    }

    /** Rule chain to manage db provisioning and lifecycle. */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    private BaseActionStatTableWriter<ActionStatsLatestRecord, ActionSnapshotLatestRecord> writer;

    private RollupTestUtils rollupTestUtils;

    /**
     * Set up for tests.
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if thread has been interrupted
     */
    @Before
    public void setup() throws SQLException, UnsupportedDialectException, InterruptedException {
        writer = spy(
            new BaseActionStatTableWriter<ActionStatsLatestRecord, ActionSnapshotLatestRecord>(
                dsl, Clock.systemUTC(), LatestActionStatTable.LATEST_TABLE_INFO) {

            @Nonnull
            @Override
            protected ActionStatsLatestRecord statRecord(final int mgmtUnitSubgroupId,
                                                         final int actionGroupId,
                                                         @Nonnull final LocalDateTime startTime,
                                                         @Nonnull final RolledUpActionGroupStat rolledUpActionGroupStat) {
                return null;
            }

            @Nonnull
            @Override
            protected ActionSnapshotLatestRecord snapshotRecord(@Nonnull final LocalDateTime snapshotTime,
                                                                final int numActionSnapshots) {
                return null;
            }
        });

        rollupTestUtils = new RollupTestUtils(dsl);
    }

    private static final int MGMT_UNIT_SUBGROUP = 1;

    private static final int ACTION_GROUP = 7;

    private static final int NUM_ACTION_PLAN_SNAPSHOTS = 3;

    @Test
    public void testInsert() {
        final LocalDateTime time = RollupTestUtils.time(12, 0);
        rollupTestUtils.insertMgmtUnit(MGMT_UNIT_SUBGROUP);
        rollupTestUtils.insertActionGroup(ACTION_GROUP);

        final RolledUpActionGroupStat groupStat = ImmutableRolledUpActionGroupStat.builder()
            .priorActionCount(10)
            .newActionCount(5)
            .avgEntityCount(1)
            .avgActionCount(1)
            .avgInvestment(1.0)
            .avgSavings(1.0)
            .minActionCount(1)
            .minEntityCount(1)
            .minInvestment(1.0)
            .minSavings(1.0)
            .maxActionCount(1)
            .maxEntityCount(1)
            .maxInvestment(1.0)
            .maxSavings(1.0)
            .build();


        final ActionStatsLatestRecord insertedRecord =
            rollupTestUtils.dummyRecord(MGMT_UNIT_SUBGROUP, ACTION_GROUP, time);
        when(writer.statRecord(eq(MGMT_UNIT_SUBGROUP), eq(ACTION_GROUP), eq(time), eq(groupStat)))
            .thenReturn(insertedRecord);

        final ActionSnapshotLatestRecord insertedSnapshotRecord = new ActionSnapshotLatestRecord();
        insertedSnapshotRecord.setActionSnapshotTime(time);
        insertedSnapshotRecord.setSnapshotRecordingTime(time.plusMinutes(1));
        insertedSnapshotRecord.setTopologyId(1L);
        insertedSnapshotRecord.setActionsCount(1);
        when(writer.snapshotRecord(time, NUM_ACTION_PLAN_SNAPSHOTS)).thenReturn(insertedSnapshotRecord);

        writer.insert(MGMT_UNIT_SUBGROUP, ImmutableRolledUpActionStats.builder()
            .startTime(time)
            .numActionSnapshots(NUM_ACTION_PLAN_SNAPSHOTS)
            .putStatsByActionGroupId(ACTION_GROUP, groupStat)
            .build());

        verify(writer).statRecord(eq(MGMT_UNIT_SUBGROUP), eq(ACTION_GROUP), eq(time), eq(groupStat));
        verify(writer).snapshotRecord(time, NUM_ACTION_PLAN_SNAPSHOTS);

        final List<ActionStatsLatestRecord> records =
            dsl.selectFrom(Tables.ACTION_STATS_LATEST).fetch();
        assertThat(records.size(), is(1));
        rollupTestUtils.compareRecords(records.get(0), insertedRecord);

        final List<ActionSnapshotLatestRecord> snapshotRecords =
            dsl.selectFrom(Tables.ACTION_SNAPSHOT_LATEST).fetch();
        assertThat(snapshotRecords.size(), is(1));
        rollupTestUtils.compareSnapshotRecords(snapshotRecords.get(0), insertedSnapshotRecord);
    }

    @Test
    public void testTrim() {
        final LocalDateTime trimTime = RollupTestUtils.time(12, 0);
        rollupTestUtils.insertMgmtUnit(MGMT_UNIT_SUBGROUP);
        rollupTestUtils.insertActionGroup(ACTION_GROUP);

        final ActionStatsLatestRecord recordToTrim =
            rollupTestUtils.dummyRecord(MGMT_UNIT_SUBGROUP, ACTION_GROUP, trimTime.minusMinutes(10));
        final ActionStatsLatestRecord borderlineRecord =
            rollupTestUtils.dummyRecord(MGMT_UNIT_SUBGROUP, ACTION_GROUP, trimTime);
        final ActionStatsLatestRecord recordToKeep =
            rollupTestUtils.dummyRecord(MGMT_UNIT_SUBGROUP, ACTION_GROUP, trimTime.plusMinutes(10));

        final ActionSnapshotLatestRecord trimSnapshotRecord = new ActionSnapshotLatestRecord();
        trimSnapshotRecord.setActionSnapshotTime(trimTime.minusMinutes(10));
        trimSnapshotRecord.setSnapshotRecordingTime(trimTime.plusMinutes(1));
        trimSnapshotRecord.setTopologyId(1L);
        trimSnapshotRecord.setActionsCount(1);

        final ActionSnapshotLatestRecord borderlineSnapshot = trimSnapshotRecord.copy();
        borderlineSnapshot.setActionSnapshotTime(trimTime);

        final ActionSnapshotLatestRecord snapshotToKeep = trimSnapshotRecord.copy();
        snapshotToKeep.setActionSnapshotTime(trimTime.plusMinutes(10));

        dsl.insertInto(Tables.ACTION_STATS_LATEST).set(recordToTrim).execute();
        dsl.insertInto(Tables.ACTION_STATS_LATEST).set(borderlineRecord).execute();
        dsl.insertInto(Tables.ACTION_STATS_LATEST).set(recordToKeep).execute();

        dsl.insertInto(Tables.ACTION_SNAPSHOT_LATEST).set(trimSnapshotRecord).execute();
        dsl.insertInto(Tables.ACTION_SNAPSHOT_LATEST).set(borderlineSnapshot).execute();
        dsl.insertInto(Tables.ACTION_SNAPSHOT_LATEST).set(snapshotToKeep).execute();

        writer.trim(trimTime);

        final List<ActionStatsLatestRecord> records = dsl.selectFrom(Tables.ACTION_STATS_LATEST)
            .orderBy(Tables.ACTION_STATS_LATEST.ACTION_SNAPSHOT_TIME.asc())
            .fetch();
        assertThat(records.size(), is(2));
        rollupTestUtils.compareRecords(records.get(0), borderlineRecord);
        rollupTestUtils.compareRecords(records.get(1), recordToKeep);

        final List<ActionSnapshotLatestRecord> snapshotRecords =
            dsl.selectFrom(Tables.ACTION_SNAPSHOT_LATEST)
                .orderBy(Tables.ACTION_SNAPSHOT_LATEST.ACTION_SNAPSHOT_TIME.asc())
                .fetch();
        assertThat(snapshotRecords.size(), is(2));
        rollupTestUtils.compareSnapshotRecords(snapshotRecords.get(0), borderlineSnapshot);
        rollupTestUtils.compareSnapshotRecords(snapshotRecords.get(1), snapshotToKeep);

    }

    /**
     * Test that if there are no stats for the management unit we still insert a stat record.
     */
    @Test
    public void testInsertNoStatsSnapshotRecord() {
        final LocalDateTime time = RollupTestUtils.time(12, 0);
        final ActionSnapshotLatestRecord insertedSnapshotRecord = new ActionSnapshotLatestRecord();
        insertedSnapshotRecord.setActionSnapshotTime(time);
        insertedSnapshotRecord.setSnapshotRecordingTime(time.plusMinutes(1));
        insertedSnapshotRecord.setTopologyId(1L);
        insertedSnapshotRecord.setActionsCount(1);
        when(writer.snapshotRecord(time, NUM_ACTION_PLAN_SNAPSHOTS)).thenReturn(insertedSnapshotRecord);

        writer.insert(MGMT_UNIT_SUBGROUP, ImmutableRolledUpActionStats.builder()
            .startTime(time)
            .numActionSnapshots(NUM_ACTION_PLAN_SNAPSHOTS)
            .build());

        verify(writer).snapshotRecord(time, NUM_ACTION_PLAN_SNAPSHOTS);

        final List<ActionSnapshotLatestRecord> snapshotRecords =
            dsl.selectFrom(Tables.ACTION_SNAPSHOT_LATEST).fetch();
        assertThat(snapshotRecords.size(), is(1));
        rollupTestUtils.compareSnapshotRecords(snapshotRecords.get(0), insertedSnapshotRecord);
    }

    /**
     * Test that recording stats for more than one mgmt unit within the same snapshot time makes
     * just one (proper) snapshot table entry.
     */
    @Test
    public void testDuplicateSnapshotRecordIgnored() {
        final LocalDateTime time = RollupTestUtils.time(12, 0);
        final ActionSnapshotLatestRecord insertedSnapshotRecord = new ActionSnapshotLatestRecord();
        insertedSnapshotRecord.setActionSnapshotTime(time);
        insertedSnapshotRecord.setSnapshotRecordingTime(time.plusMinutes(1));
        insertedSnapshotRecord.setTopologyId(1L);
        insertedSnapshotRecord.setActionsCount(1);

        when(writer.snapshotRecord(time, NUM_ACTION_PLAN_SNAPSHOTS)).thenReturn(insertedSnapshotRecord);

        writer.insert(MGMT_UNIT_SUBGROUP, ImmutableRolledUpActionStats.builder()
            .startTime(time)
            .numActionSnapshots(NUM_ACTION_PLAN_SNAPSHOTS)
            .build());
        // The same snapshot, a different mgmt unit subgroup.
        writer.insert(MGMT_UNIT_SUBGROUP + 1, ImmutableRolledUpActionStats.builder()
            .startTime(time)
            .numActionSnapshots(NUM_ACTION_PLAN_SNAPSHOTS)
            .build());

        verify(writer, times(2)).snapshotRecord(time, NUM_ACTION_PLAN_SNAPSHOTS);

        final List<ActionSnapshotLatestRecord> snapshotRecords =
            dsl.selectFrom(Tables.ACTION_SNAPSHOT_LATEST).fetch();
        assertThat(snapshotRecords.size(), is(1));
        rollupTestUtils.compareSnapshotRecords(snapshotRecords.get(0), insertedSnapshotRecord);
    }
}

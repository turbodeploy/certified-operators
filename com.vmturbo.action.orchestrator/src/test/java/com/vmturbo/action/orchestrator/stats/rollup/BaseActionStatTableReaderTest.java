package com.vmturbo.action.orchestrator.stats.rollup;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotLatestRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionStats;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RollupReadyInfo;
import com.vmturbo.action.orchestrator.stats.rollup.BaseActionStatTableReader.StatWithSnapshotCnt;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        loader = AnnotationConfigContextLoader.class,
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=action"})
public class BaseActionStatTableReaderTest {
    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private DSLContext dsl;

    private static final int ACTION_GROUP_ID = 1123;

    private RolledUpStatCalculator calculator =
        mock(RolledUpStatCalculator.class);

    private BaseActionStatTableReader<ActionStatsLatestRecord, ActionSnapshotLatestRecord> baseReader;

    private RollupTestUtils rollupTestUtils;

    @Captor
    public ArgumentCaptor<Map<Integer, List<StatWithSnapshotCnt<ActionStatsLatestRecord>>>> recordsMapCaptor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        // Clean the database and bring it up to the production configuration before running test
        flyway = dbConfig.flyway();
        flyway.clean();
        flyway.migrate();

        // Grab a handle for JOOQ DB operations
        dsl = dbConfig.dsl();

        baseReader = spy(new BaseActionStatTableReader<ActionStatsLatestRecord, ActionSnapshotLatestRecord>(dsl,
                calculator, LatestActionStatTable.LATEST_TABLE_INFO,
                HourActionStatTable.HOUR_TABLE_INFO) {
            @Override
            protected Map<Integer, RolledUpActionGroupStat> rollupRecords(
                    final int numStatSnapshotsInRange,
                    @Nonnull final Map<Integer, List<StatWithSnapshotCnt<ActionStatsLatestRecord>>> recordsByActionGroupId) {
                return null;
            }

            @Override
            protected int numSnapshotsInSnapshotRecord(@Nonnull final ActionSnapshotLatestRecord record) {
                return 1;
            }
        });

        rollupTestUtils = new RollupTestUtils(dsl);
    }

    @Test
    public void testReaderRollupReadyTimes() {

        final LocalDateTime time = RollupTestUtils.time(12, 30);
        final LocalDateTime time1 = RollupTestUtils.time(12, 45);
        final int mgmtSubgroup1 = 1;
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroup1, ACTION_GROUP_ID, time);
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroup1, ACTION_GROUP_ID, time1);

        final LocalDateTime time2 = RollupTestUtils.time(13, 0);
        final int mgmtSubgroup2 = 2;
        final int mgmtSubgroup3 = 3;
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroup2, ACTION_GROUP_ID, time2);
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroup3, ACTION_GROUP_ID, time2);

        final List<RollupReadyInfo> rollupReadyInfo = baseReader.rollupReadyTimes();
        // The latest hour does not get included, because it's still not ready for rollup
        // until there is a snapshot AFTER it.
        assertThat(rollupReadyInfo.size(), is(1));
        assertThat(rollupReadyInfo.get(0), is(ImmutableRollupReadyInfo.builder()
                .addManagementUnits(mgmtSubgroup1)
                .startTime(RollupTestUtils.time(12, 0))
                .build()));
    }

    @Test
    public void testReaderRollupReadyAlreadyRolledUp() {
        final LocalDateTime time = RollupTestUtils.time(12, 30);
        final LocalDateTime time1 = RollupTestUtils.time(12, 45);
        final int mgmtSubgroup1 = 1;
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroup1, ACTION_GROUP_ID, time);
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroup1, ACTION_GROUP_ID, time1);

        final LocalDateTime time2 = RollupTestUtils.time(13, 0);
        final int mgmtSubgroup2 = 2;
        final int mgmtSubgroup3 = 3;
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroup2, ACTION_GROUP_ID, time2);
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroup3, ACTION_GROUP_ID, time2);

        // Insert hourly snapshot for the time
        rollupTestUtils.insertHourlySnapshotOnly(time);

        assertTrue(baseReader.rollupReadyTimes().isEmpty());
    }

    @Test
    public void testReaderRollupReadyNoData() {
        assertTrue(baseReader.rollupReadyTimes().isEmpty());
    }

    @Test
    public void testReaderRollup() {
        final int mgmtUnitSubgroupId = 7;
        final LocalDateTime startTime = RollupTestUtils.time(12, 0);
        Map<Integer, RolledUpActionGroupStat> rolledUpRecords = Collections.emptyMap();
        when(baseReader.rollupRecords(anyInt(), any())).thenReturn(rolledUpRecords);

        // Insert the snapshot records.
        ActionSnapshotLatestRecord snapshotRecord1 = new ActionSnapshotLatestRecord();
        snapshotRecord1.setActionSnapshotTime(startTime);
        snapshotRecord1.setSnapshotRecordingTime(startTime.plusMinutes(5));
        snapshotRecord1.setActionsCount(1);
        snapshotRecord1.setTopologyId(1L);

        ActionSnapshotLatestRecord snapshotRecord2 = snapshotRecord1.copy();
        snapshotRecord2.setActionSnapshotTime(startTime.plusMinutes(59));

        dsl.insertInto(Tables.ACTION_SNAPSHOT_LATEST)
            .set(snapshotRecord1)
            .execute();
        dsl.insertInto(Tables.ACTION_SNAPSHOT_LATEST)
            .set(snapshotRecord2)
            .execute();

        // Insert the stats records. Two action groups.
        final int actionGroup1 = 1;
        final int actionGroup2 = 2;
        rollupTestUtils.insertActionGroup(actionGroup1);
        rollupTestUtils.insertActionGroup(actionGroup2);
        rollupTestUtils.insertMgmtUnit(mgmtUnitSubgroupId);

        final ActionStatsLatestRecord statRecord1 = dsl.newRecord(Tables.ACTION_STATS_LATEST);
        statRecord1.setActionSnapshotTime(snapshotRecord1.getActionSnapshotTime());
        statRecord1.setMgmtUnitSubgroupId(mgmtUnitSubgroupId);
        statRecord1.setActionGroupId(actionGroup1);
        statRecord1.setTotalActionCount(5);
        statRecord1.setTotalEntityCount(7);
        statRecord1.setTotalInvestment(BigDecimal.ZERO);
        statRecord1.setTotalSavings(BigDecimal.ZERO);

        final ActionStatsLatestRecord statRecord2 = dsl.newRecord(Tables.ACTION_STATS_LATEST);
        statRecord2.setActionSnapshotTime(snapshotRecord2.getActionSnapshotTime());
        statRecord2.setMgmtUnitSubgroupId(mgmtUnitSubgroupId);
        statRecord2.setActionGroupId(actionGroup2);
        statRecord2.setTotalActionCount(6);
        statRecord2.setTotalEntityCount(8);
        statRecord2.setTotalInvestment(BigDecimal.ZERO);
        statRecord2.setTotalSavings(BigDecimal.ZERO);

        statRecord1.store();
        statRecord2.store();

        Optional<RolledUpActionStats> retSummary = baseReader.rollup(mgmtUnitSubgroupId, startTime);
        assertThat(retSummary.get(), is(ImmutableRolledUpActionStats.builder()
            .putAllStatsByActionGroupId(rolledUpRecords)
            .startTime(startTime)
            .numActionSnapshots(2)
            .build()));

        verify(baseReader).rollupRecords(eq(2), recordsMapCaptor.capture());

        final Map<Integer, List<StatWithSnapshotCnt<ActionStatsLatestRecord>>> records = recordsMapCaptor.getValue();
        assertThat(records.keySet(), containsInAnyOrder(actionGroup1, actionGroup2));
        assertThat(records.get(actionGroup1).size(), is(1));
        assertThat(records.get(actionGroup2).size(), is(1));
        assertThat(records.get(actionGroup1).get(0).numActionSnapshots(), is(1));
        assertThat(records.get(actionGroup2).get(0).numActionSnapshots(), is(1));
        rollupTestUtils.compareRecords(records.get(actionGroup1).get(0).record(), statRecord1);
        rollupTestUtils.compareRecords(records.get(actionGroup2).get(0).record(), statRecord2);
    }

    @Test
    public void testReaderRollupNoSnapshotsInRange() {
        final int mgmtUnitSubgroupId = 7;
        final LocalDateTime startTime = RollupTestUtils.time(12, 0);

        // Add records just before and just after the time range.
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtUnitSubgroupId, 1, startTime.minusNanos(1));
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtUnitSubgroupId, 1, startTime.plusHours(1));

        Optional<RolledUpActionStats> retSummary = baseReader.rollup(mgmtUnitSubgroupId, startTime);
        assertFalse(retSummary.isPresent());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReaderRollupInvalidStartTime() {
        final int mgmtUnitSubgroupId = 7;
        final LocalDateTime startTime = RollupTestUtils.time(12, 1);
        baseReader.rollup(mgmtUnitSubgroupId, startTime);
    }


    @After
    public void teardown() {
        flyway.clean();
    }
}

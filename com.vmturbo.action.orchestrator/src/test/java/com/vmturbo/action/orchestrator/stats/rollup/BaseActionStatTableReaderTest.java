package com.vmturbo.action.orchestrator.stats.rollup;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.contains;
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
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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

import com.google.common.collect.ImmutableMap;

import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotLatestRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMatchedActionGroups;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionStats;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RollupReadyInfo;
import com.vmturbo.action.orchestrator.stats.rollup.BaseActionStatTableReader.StatWithSnapshotCnt;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionCountsQuery.TimeRange;
import com.vmturbo.components.api.test.MutableFixedClock;
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

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

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
                clock, LatestActionStatTable.LATEST_TABLE_INFO,
                Optional.of(HourActionStatTable.HOUR_TABLE_INFO)) {
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

            protected RolledUpActionGroupStat recordToGroupStat(final ActionStatsLatestRecord record) {
                return null;
            }
        });

        rollupTestUtils = new RollupTestUtils(dsl);
    }

    @Test
    public void testReaderRollupReadyTimes() {
        final LocalDateTime curTime = RollupTestUtils.time(13, 00);
        clock.changeInstant(curTime.toInstant(ZoneOffset.UTC));

        final LocalDateTime time = RollupTestUtils.time(12, 30);
        final LocalDateTime time1 = RollupTestUtils.time(12, 45);
        final int mgmtSubgroup1 = 1;
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroup1, ACTION_GROUP_ID, time);
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroup1, ACTION_GROUP_ID, time1);

        // The 12:00 time should be ready for rollup, because it's now past 12:59.
        final List<RollupReadyInfo> rollupReadyInfo = baseReader.rollupReadyTimes();
        assertThat(rollupReadyInfo.size(), is(1));
        assertThat(rollupReadyInfo.get(0), is(ImmutableRollupReadyInfo.builder()
                .addManagementUnits(mgmtSubgroup1)
                .startTime(RollupTestUtils.time(12, 0))
                .build()));
    }

    @Test
    public void testReaderRollupReadyTimesCurHourExcluded() {
        final LocalDateTime curTime = RollupTestUtils.time(13, 30);
        clock.changeInstant(curTime.toInstant(ZoneOffset.UTC));

        final LocalDateTime time2 = RollupTestUtils.time(13, 2);
        final int mgmtSubgroup2 = 2;
        final int mgmtSubgroup3 = 3;
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroup2, ACTION_GROUP_ID, time2);
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroup3, ACTION_GROUP_ID, time2);

        final List<RollupReadyInfo> rollupReadyInfo = baseReader.rollupReadyTimes();
        // The latest time does not get included, because it's still not ready for rollup,
        // since it's in the "current" hour.
        assertThat(rollupReadyInfo.size(), is(0));
    }

    @Test
    public void testReaderRollupReadyAlreadyRolledUp() {
        final LocalDateTime time = RollupTestUtils.time(12, 30);
        final LocalDateTime time1 = RollupTestUtils.time(12, 45);
        final int mgmtSubgroup1 = 1;
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroup1, ACTION_GROUP_ID, time);
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroup1, ACTION_GROUP_ID, time1);

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
    public void testReaderQueryMultipleMatchingTimes() {
        final LocalDateTime startTime = RollupTestUtils.time(12, 0);

        final TimeRange timeRange = TimeRange.newBuilder()
            .setStartTime(startTime.minusMinutes(1).toInstant(ZoneOffset.UTC).toEpochMilli())
            .setEndTime(startTime.plusMinutes(10).toInstant(ZoneOffset.UTC).toEpochMilli())
            .build();

        final int mgmtUnitId = 1;
        final int actionGroupId = 11;
        rollupTestUtils.insertMgmtUnit(mgmtUnitId);
        rollupTestUtils.insertActionGroup(actionGroupId);
        final ActionGroup actionGroup = mock(ActionGroup.class);

        // Insert the snapshot records.
        final ActionSnapshotLatestRecord snapshotRecord1 = new ActionSnapshotLatestRecord();
        snapshotRecord1.setActionSnapshotTime(startTime);
        snapshotRecord1.setSnapshotRecordingTime(startTime.plusMinutes(2));
        snapshotRecord1.setActionsCount(1);
        snapshotRecord1.setTopologyId(1L);

        final ActionSnapshotLatestRecord snapshotRecord2 = snapshotRecord1.copy();
        snapshotRecord2.setActionSnapshotTime(startTime.plusMinutes(6));

        dsl.insertInto(Tables.ACTION_SNAPSHOT_LATEST)
            .set(snapshotRecord1)
            .execute();
        dsl.insertInto(Tables.ACTION_SNAPSHOT_LATEST)
            .set(snapshotRecord2)
            .execute();

        // Insert the stat records.
        final ActionStatsLatestRecord statRecord1 = dsl.newRecord(Tables.ACTION_STATS_LATEST);
        statRecord1.setActionSnapshotTime(snapshotRecord1.getActionSnapshotTime());
        statRecord1.setMgmtUnitSubgroupId(mgmtUnitId);
        statRecord1.setActionGroupId(actionGroupId);
        statRecord1.setTotalActionCount(5);
        statRecord1.setTotalEntityCount(7);
        statRecord1.setTotalInvestment(BigDecimal.ZERO);
        statRecord1.setTotalSavings(BigDecimal.ZERO);

        final ActionStatsLatestRecord statRecord2 = dsl.newRecord(Tables.ACTION_STATS_LATEST);
        statRecord2.setActionSnapshotTime(snapshotRecord2.getActionSnapshotTime());
        statRecord2.setMgmtUnitSubgroupId(mgmtUnitId);
        statRecord2.setActionGroupId(actionGroupId);
        statRecord2.setTotalActionCount(6);
        statRecord2.setTotalEntityCount(8);
        statRecord2.setTotalInvestment(BigDecimal.ZERO);
        statRecord2.setTotalSavings(BigDecimal.ZERO);

        statRecord1.store();
        statRecord2.store();

        final RolledUpActionGroupStat groupStat1 = mock(RolledUpActionGroupStat.class);
        final RolledUpActionGroupStat groupStat2 = mock(RolledUpActionGroupStat.class);
        when(baseReader.recordToGroupStat(any())).thenAnswer(invocation -> {
            final ActionStatsLatestRecord inputRecord =
                invocation.getArgumentAt(0, ActionStatsLatestRecord.class);
            if (inputRecord.getActionSnapshotTime().equals(snapshotRecord1.getActionSnapshotTime())) {
                return groupStat1;
            } else if (inputRecord.getActionSnapshotTime().equals(snapshotRecord2.getActionSnapshotTime())) {
                return groupStat2;
            } else {
                return null;
            }
        });

        final Map<LocalDateTime, Map<ActionGroup, RolledUpActionGroupStat>> queryResult =
            baseReader.query(timeRange, Collections.singleton(mgmtUnitId),
                ImmutableMatchedActionGroups.builder()
                    .allActionGroups(false)
                    .putSpecificActionGroupsById(actionGroupId, actionGroup)
                    .build());
        assertThat(queryResult.keySet(), containsInAnyOrder(
            snapshotRecord1.getActionSnapshotTime(),
            snapshotRecord2.getActionSnapshotTime()));
        assertThat(queryResult.get(snapshotRecord1.getActionSnapshotTime()),
            is(ImmutableMap.of(actionGroup, groupStat1)));
        assertThat(queryResult.get(snapshotRecord2.getActionSnapshotTime()),
            is(ImmutableMap.of(actionGroup, groupStat2)));
    }

    @Test
    public void testReaderQueryAllActionGroupsMatch() {
        final LocalDateTime startTime = RollupTestUtils.time(12, 0);

        final TimeRange timeRange = TimeRange.newBuilder()
            .setStartTime(startTime.minusMinutes(1).toInstant(ZoneOffset.UTC).toEpochMilli())
            .setEndTime(startTime.plusMinutes(10).toInstant(ZoneOffset.UTC).toEpochMilli())
            .build();

        final int mgmtUnitId = 1;
        final int actionGroupId = 11;
        rollupTestUtils.insertMgmtUnit(mgmtUnitId);
        rollupTestUtils.insertActionGroup(actionGroupId);
        final ActionGroup actionGroup = mock(ActionGroup.class);

        // Insert the snapshot record.
        final ActionSnapshotLatestRecord snapshotRecord1 = new ActionSnapshotLatestRecord();
        snapshotRecord1.setActionSnapshotTime(startTime);
        snapshotRecord1.setSnapshotRecordingTime(startTime.plusMinutes(2));
        snapshotRecord1.setActionsCount(1);
        snapshotRecord1.setTopologyId(1L);

        dsl.insertInto(Tables.ACTION_SNAPSHOT_LATEST)
            .set(snapshotRecord1)
            .execute();

        // Insert the stat record.
        final ActionStatsLatestRecord statRecord1 = dsl.newRecord(Tables.ACTION_STATS_LATEST);
        statRecord1.setActionSnapshotTime(snapshotRecord1.getActionSnapshotTime());
        statRecord1.setMgmtUnitSubgroupId(mgmtUnitId);
        statRecord1.setActionGroupId(actionGroupId);
        statRecord1.setTotalActionCount(5);
        statRecord1.setTotalEntityCount(7);
        statRecord1.setTotalInvestment(BigDecimal.ZERO);
        statRecord1.setTotalSavings(BigDecimal.ZERO);

        statRecord1.store();

        final RolledUpActionGroupStat groupStat1 = mock(RolledUpActionGroupStat.class);
        when(baseReader.recordToGroupStat(any())).thenReturn(groupStat1);

        // Act.
        final Map<LocalDateTime, Map<ActionGroup, RolledUpActionGroupStat>> queryResult =
            baseReader.query(timeRange, Collections.singleton(mgmtUnitId),
                ImmutableMatchedActionGroups.builder()
                    // All action groups = true.
                    .allActionGroups(true)
                    .putSpecificActionGroupsById(actionGroupId, actionGroup)
                    .build());

        // Assert..
        assertThat(queryResult.keySet(), containsInAnyOrder(snapshotRecord1.getActionSnapshotTime()));
        assertThat(queryResult.get(snapshotRecord1.getActionSnapshotTime()),
            is(ImmutableMap.of(actionGroup, groupStat1)));
    }

    @Test
    public void testReaderQuerySnapshotOrder() {
        final LocalDateTime earlierTime = RollupTestUtils.time(12, 0);
        final LocalDateTime laterTime = RollupTestUtils.time(12, 30);

        final TimeRange timeRange = TimeRange.newBuilder()
            .setStartTime(earlierTime.minusMinutes(1).toInstant(ZoneOffset.UTC).toEpochMilli())
            .setEndTime(laterTime.plusMinutes(10).toInstant(ZoneOffset.UTC).toEpochMilli())
            .build();

        final int mgmtUnitId = 1;
        final int actionGroupId = 11;
        rollupTestUtils.insertMgmtUnit(mgmtUnitId);
        rollupTestUtils.insertActionGroup(actionGroupId);
        final ActionGroup actionGroup = mock(ActionGroup.class);

        // Insert the snapshot record.
        final ActionSnapshotLatestRecord earlierRecord = dsl.newRecord(Tables.ACTION_SNAPSHOT_LATEST);
        earlierRecord.setActionSnapshotTime(earlierTime);
        earlierRecord.setSnapshotRecordingTime(earlierTime.plusMinutes(2));
        earlierRecord.setActionsCount(1);
        earlierRecord.setTopologyId(1L);
        earlierRecord.store();

        final ActionSnapshotLatestRecord laterRecord = dsl.newRecord(Tables.ACTION_SNAPSHOT_LATEST);
        laterRecord.setActionSnapshotTime(laterTime);
        laterRecord.setSnapshotRecordingTime(laterTime.plusMinutes(2));
        laterRecord.setActionsCount(1);
        laterRecord.setTopologyId(1L);
        laterRecord.store();

        // No stat record (no matching stat record, really).

        // Act.
        final Map<LocalDateTime, Map<ActionGroup, RolledUpActionGroupStat>> queryResult =
            baseReader.query(timeRange, Collections.singleton(mgmtUnitId),
                ImmutableMatchedActionGroups.builder()
                    // All action groups = true.
                    .allActionGroups(true)
                    .putSpecificActionGroupsById(actionGroupId, actionGroup)
                    .build());

        // Assert.
        // Needs to contain the earlier time before the later time.
        assertThat(queryResult.keySet(), contains(earlierTime, laterTime));
    }

    @Test
    public void testReaderQueryReturnsEmptySnapshotTimes() {
        final LocalDateTime startTime = RollupTestUtils.time(12, 0);

        final TimeRange timeRange = TimeRange.newBuilder()
            .setStartTime(startTime.minusMinutes(1).toInstant(ZoneOffset.UTC).toEpochMilli())
            .setEndTime(startTime.plusMinutes(10).toInstant(ZoneOffset.UTC).toEpochMilli())
            .build();

        final int mgmtUnitId = 1;
        final int actionGroupId = 11;
        rollupTestUtils.insertMgmtUnit(mgmtUnitId);
        rollupTestUtils.insertActionGroup(actionGroupId);
        final ActionGroup actionGroup = mock(ActionGroup.class);

        // Insert the snapshot record.
        final ActionSnapshotLatestRecord snapshotRecord1 = new ActionSnapshotLatestRecord();
        snapshotRecord1.setActionSnapshotTime(startTime);
        snapshotRecord1.setSnapshotRecordingTime(startTime.plusMinutes(2));
        snapshotRecord1.setActionsCount(1);
        snapshotRecord1.setTopologyId(1L);

        dsl.insertInto(Tables.ACTION_SNAPSHOT_LATEST)
            .set(snapshotRecord1)
            .execute();

        // No stat record (no matching stat record, really).

        // Act.
        final Map<LocalDateTime, Map<ActionGroup, RolledUpActionGroupStat>> queryResult =
            baseReader.query(timeRange, Collections.singleton(mgmtUnitId),
                ImmutableMatchedActionGroups.builder()
                    // All action groups = true.
                    .allActionGroups(true)
                    .putSpecificActionGroupsById(actionGroupId, actionGroup)
                    .build());

        // Assert..
        assertThat(queryResult.keySet(), containsInAnyOrder(snapshotRecord1.getActionSnapshotTime()));
    }

    @Test
    public void testReaderQueryBeforeTimeRangeEnforcement() {
        final LocalDateTime startTime = RollupTestUtils.time(12, 0);

        final TimeRange timeRange = TimeRange.newBuilder()
            .setStartTime(startTime.toInstant(ZoneOffset.UTC).toEpochMilli())
            .setEndTime(startTime.plusMinutes(10).toInstant(ZoneOffset.UTC).toEpochMilli())
            .build();

        final int mgmtUnitId = 1;
        final int actionGroupId = 11;
        rollupTestUtils.insertMgmtUnit(mgmtUnitId);
        rollupTestUtils.insertActionGroup(actionGroupId);
        final ActionGroup actionGroup = mock(ActionGroup.class);

        // Insert the snapshot records.
        final ActionSnapshotLatestRecord beforeRecord = new ActionSnapshotLatestRecord();
        beforeRecord.setActionSnapshotTime(startTime.minusSeconds(1));
        beforeRecord.setSnapshotRecordingTime(startTime);
        beforeRecord.setActionsCount(1);
        beforeRecord.setTopologyId(1L);

        dsl.insertInto(Tables.ACTION_SNAPSHOT_LATEST)
            .set(beforeRecord)
            .execute();

        // Insert the stat records.
        final ActionStatsLatestRecord statRecord1 = dsl.newRecord(Tables.ACTION_STATS_LATEST);
        statRecord1.setActionSnapshotTime(beforeRecord.getActionSnapshotTime());
        statRecord1.setMgmtUnitSubgroupId(mgmtUnitId);
        statRecord1.setActionGroupId(actionGroupId);
        statRecord1.setTotalActionCount(5);
        statRecord1.setTotalEntityCount(7);
        statRecord1.setTotalInvestment(BigDecimal.ZERO);
        statRecord1.setTotalSavings(BigDecimal.ZERO);

        statRecord1.store();

        // Should be nothing in the time range.
        assertTrue(baseReader.query(timeRange, Collections.singleton(mgmtUnitId),
                ImmutableMatchedActionGroups.builder()
                    .allActionGroups(false)
                    .putSpecificActionGroupsById(actionGroupId, actionGroup)
                    .build()).isEmpty());
    }

    @Test
    public void testReaderQueryAfterTimeRangeEnforcement() {
        final LocalDateTime startTime = RollupTestUtils.time(12, 0);

        final TimeRange timeRange = TimeRange.newBuilder()
            .setStartTime(startTime.toInstant(ZoneOffset.UTC).toEpochMilli())
            .setEndTime(startTime.plusMinutes(10).toInstant(ZoneOffset.UTC).toEpochMilli())
            .build();

        final int mgmtUnitId = 1;
        final int actionGroupId = 11;
        rollupTestUtils.insertMgmtUnit(mgmtUnitId);
        rollupTestUtils.insertActionGroup(actionGroupId);
        final ActionGroup actionGroup = mock(ActionGroup.class);

        // Insert the snapshot records.
        final ActionSnapshotLatestRecord afterRecord = new ActionSnapshotLatestRecord();
        afterRecord.setActionSnapshotTime(startTime.plusMinutes(11));
        afterRecord.setSnapshotRecordingTime(startTime);
        afterRecord.setActionsCount(1);
        afterRecord.setTopologyId(1L);

        dsl.insertInto(Tables.ACTION_SNAPSHOT_LATEST)
            .set(afterRecord)
            .execute();

        // Insert the stat records.
        final ActionStatsLatestRecord statRecord1 = dsl.newRecord(Tables.ACTION_STATS_LATEST);
        statRecord1.setActionSnapshotTime(afterRecord.getActionSnapshotTime());
        statRecord1.setMgmtUnitSubgroupId(mgmtUnitId);
        statRecord1.setActionGroupId(actionGroupId);
        statRecord1.setTotalActionCount(5);
        statRecord1.setTotalEntityCount(7);
        statRecord1.setTotalInvestment(BigDecimal.ZERO);
        statRecord1.setTotalSavings(BigDecimal.ZERO);

        statRecord1.store();

        // Should be nothing in the time range.
        assertTrue(baseReader.query(timeRange, Collections.singleton(mgmtUnitId),
                ImmutableMatchedActionGroups.builder()
                    .allActionGroups(false)
                    .putSpecificActionGroupsById(actionGroupId, actionGroup)
                    .build()).isEmpty());
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

package com.vmturbo.action.orchestrator.stats.rollup;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotDayRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotHourRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotLatestRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotMonthRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByDayRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByHourRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByMonthRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatRollupScheduler.ActionStatRollup;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
    loader = AnnotationConfigContextLoader.class,
    classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=action"})
public class ScheduledRollupIntegrationTest {
    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private DSLContext dsl;

    private RollupTestUtils rollupTestUtils;

    private static final LocalDateTime CLOCK_TIME = RollupTestUtils.time(1, 1);

    private MutableFixedClock clock =
        new MutableFixedClock(CLOCK_TIME.toInstant(ZoneOffset.UTC), ZoneId.from(ZoneOffset.UTC));

    private ExecutorService executorService = mock(ExecutorService.class);

    @Before
    public void setup() {

        // Clean the database and bring it up to the production configuration before running test
        flyway = dbConfig.flyway();
        flyway.clean();
        flyway.migrate();

        // Grab a handle for JOOQ DB operations
        dsl = dbConfig.dsl();

        rollupTestUtils = new RollupTestUtils(dsl);
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    @Test
    public void testLatestToHourRollup() {
        // START ARRANGEMENT ----
        final RolledUpStatCalculator statCalculator = new RolledUpStatCalculator();
        final HourActionStatTable hourTable = new HourActionStatTable(dsl, clock,
            statCalculator, DayActionStatTable.DAY_TABLE_INFO);
        final LatestActionStatTable latestTable = new LatestActionStatTable(dsl, clock,
            statCalculator, HourActionStatTable.HOUR_TABLE_INFO);

        final ActionStatRollupScheduler scheduler = new ActionStatRollupScheduler(
            Collections.singletonList(ImmutableRollupDirection.builder()
                .fromTableReader(latestTable.reader())
                .toTableWriter(hourTable.writer())
                .description("latest to hour")
                .build()), executorService);

        // Insert three "latest" records, two for one mgmt unit subgroup, one for another
        final int mgmtSubgroup1 = 1;
        final int mgmtSubgroup2 = 2;
        final int actionGroup1 = 3;
        final int actionGroup2 = 4;
        rollupTestUtils.insertMgmtUnit(mgmtSubgroup1);
        rollupTestUtils.insertMgmtUnit(mgmtSubgroup2);
        rollupTestUtils.insertActionGroup(actionGroup1);
        rollupTestUtils.insertActionGroup(actionGroup2);

        final LocalDateTime firstTime = RollupTestUtils.time(1, 10);
        final LocalDateTime secondTime = firstTime.plusMinutes(10);

        // Set the clock time to the next hour. We only roll up snapshots when we're out of
        // the last time period.
        final LocalDateTime nextHourTime = firstTime.plusHours(1);
        clock.changeInstant(nextHourTime.toInstant(ZoneOffset.UTC));

        // Insert snapshot and stat records into the latest database.
        // Two snapshots to be rolled up.
        //    The first snapshot contains actions for:
        //       mgmt unit subgroup 1, action group 1
        //       mgmt unit subgroup 1, action group 2
        //       mgmt unit subgroup 2, action group 1
        //    The second snapshot contains actions for:
        //       mgmt unit subgroup 1, action group 1
        //       mgmt unit subgroup 1, action group 2
        //
        // Note - this means that mgmt unit subgroup 2 had 0 actions in the second snapshot.
        {
            // Insert record for the first snapshot within the hour.
            final ActionSnapshotLatestRecord firstSnapshotRecord = dsl.newRecord(Tables.ACTION_SNAPSHOT_LATEST);
            firstSnapshotRecord.setActionSnapshotTime(firstTime);
            // The following three fields don't matter operationally.
            firstSnapshotRecord.setActionsCount(1);
            firstSnapshotRecord.setTopologyId(1L);
            // Add some hours, so that if we accidentally use this operationally the test will fail.
            firstSnapshotRecord.setSnapshotRecordingTime(firstTime.plusHours(10));
            firstSnapshotRecord.store();

            // Insert record for the second snapshot within the hour.
            final ActionSnapshotLatestRecord secondSnapshotRecord = firstSnapshotRecord.copy();
            secondSnapshotRecord.setActionSnapshotTime(secondTime);
            secondSnapshotRecord.store();

            // record 1: Management Subgroup 1, Action Group 1
            storeLatestActionStatsRecord(firstTime, mgmtSubgroup1, actionGroup1,
                /* totalEntityCount*/ 5, /* totalActionCount */10, /* newActionCount */ 1,
                /* totalInvestment */ 2, /* totalSavings */ 1);

            // record 1: Management Subgroup 1, Action Group 2
            storeLatestActionStatsRecord(firstTime, mgmtSubgroup1, actionGroup2,
                /* totalEntityCount*/ 6, /* totalActionCount */10, /* newActionCount */ 2,
                /* totalInvestment */ 2, /* totalSavings */ 1);

            // record 1: Management Subgroup 2, Action Group 1
            storeLatestActionStatsRecord(firstTime, mgmtSubgroup2, actionGroup1,
                /* totalEntityCount*/ 5, /* totalActionCount */10, /* newActionCount */ 2,
                /* totalInvestment */ 2, /* totalSavings */ 1);

            // record 2: Management Subgroup 1, Action Group 1
            storeLatestActionStatsRecord(secondTime, mgmtSubgroup1, actionGroup1,
                /* totalEntityCount*/ 3, /* totalActionCount */8, /* newActionCount */ 1,
                /* totalInvestment */ 4, /* totalSavings */ 3);

            // record 2: Management Subgroup 1, Action Group 2
            storeLatestActionStatsRecord(secondTime, mgmtSubgroup1, actionGroup2,
                /* totalEntityCount*/ 4, /* totalActionCount */5, /* newActionCount */ 1,
                /* totalInvestment */ 4, /* totalSavings */ 3);
        }

        // END ARRANGEMENT ----

        // START ACT -----

        // Schedule rollups, and wait for the rollup to succeed.
        scheduler.scheduleRollups();
        final ArgumentCaptor<ActionStatRollup> rollupCaptor =
            ArgumentCaptor.forClass(ActionStatRollup.class);
        verify(executorService, times(2)).submit(rollupCaptor.capture());

        // Run the scheduled rollup.
        rollupCaptor.getAllValues().forEach(ActionStatRollup::run);

        // END ACT -----

        // START ASSERT -----

        // Verify that the expected values got written to the "hour" stat and snapshot tables.
        List<ActionSnapshotHourRecord> snapshotHourRecords =
            dsl.selectFrom(Tables.ACTION_SNAPSHOT_HOUR)
                .fetch();
        assertThat(snapshotHourRecords.size(), is(1));
        assertThat(snapshotHourRecords.get(0).getHourTime(),
            is(firstTime.truncatedTo(ChronoUnit.HOURS)));
        assertThat(snapshotHourRecords.get(0).getHourRollupTime(),
            is(LocalDateTime.now(clock)));
        assertThat(snapshotHourRecords.get(0).getNumActionSnapshots(), is(2));

        final Map<Integer, Map<Integer, ActionStatsByHourRecord>> hourStatsByMgmtUnitAndActionGroup =
            new HashMap<>();
        dsl.selectFrom(Tables.ACTION_STATS_BY_HOUR)
            .fetch()
            .forEach(actionStatsByHourRecord -> {
                final Map<Integer, ActionStatsByHourRecord> statsByAg =
                    hourStatsByMgmtUnitAndActionGroup.computeIfAbsent(
                        actionStatsByHourRecord.getMgmtUnitSubgroupId(), k -> new HashMap<>());
                final ActionStatsByHourRecord existing =
                    statsByAg.put(actionStatsByHourRecord.getActionGroupId(), actionStatsByHourRecord);
                // Should only have one hour record for each mgmt unit and action group combination.
                assertNull(existing);
            });

        assertThat(hourStatsByMgmtUnitAndActionGroup.keySet(),
            containsInAnyOrder(mgmtSubgroup1, mgmtSubgroup2));
        assertThat(hourStatsByMgmtUnitAndActionGroup.get(mgmtSubgroup1).keySet(),
            containsInAnyOrder(actionGroup1, actionGroup2));

        // Verify the first management subunit's first action group record.
        {
            final ActionStatsByHourRecord mu1A1HourRecord =
                hourStatsByMgmtUnitAndActionGroup.get(mgmtSubgroup1).get(actionGroup1);
            assertThat(mu1A1HourRecord.getHourTime(), is(firstTime.truncatedTo(ChronoUnit.HOURS)));
            // Two snapshots got rolled up into this one.
            assertThat(mu1A1HourRecord.getMgmtUnitSubgroupId(), is(mgmtSubgroup1));
            assertThat(mu1A1HourRecord.getActionGroupId(), is(actionGroup1));

            // Total - New of the first latest record.
            assertThat(mu1A1HourRecord.getPriorActionCount(), is(9));
            assertThat(mu1A1HourRecord.getNewActionCount(), is(2));
            assertThat(mu1A1HourRecord.getMaxActionCount(), is(10));
            assertThat(mu1A1HourRecord.getMinActionCount(), is(8));
            assertThat(mu1A1HourRecord.getAvgActionCount().doubleValue(),
                closeTo((10 + 1) / 2.0, 0.0001));

            assertThat(mu1A1HourRecord.getMaxEntityCount(), is(5));
            assertThat(mu1A1HourRecord.getMinEntityCount(), is(3));
            assertThat(mu1A1HourRecord.getAvgEntityCount().doubleValue(),
                closeTo((5 + 3) / 2.0, 0.0001));

            assertThat(mu1A1HourRecord.getMaxSavings().doubleValue(), is(3.0));
            assertThat(mu1A1HourRecord.getMinSavings().doubleValue(), is(1.0));
            assertThat(mu1A1HourRecord.getAvgSavings().doubleValue(),
                closeTo((1 + 3) / 2.0, 0.0001));

            assertThat(mu1A1HourRecord.getMaxInvestment().doubleValue(), is(4.0));
            assertThat(mu1A1HourRecord.getMinInvestment().doubleValue(), is(2.0));
            assertThat(mu1A1HourRecord.getAvgInvestment().doubleValue(),
                closeTo((4 + 2) / 2.0, 0.0001));
        }

        // Verify the first management subunit's second action group record.
        // This tests that different action groups get rolled up correctly.
        {
            final ActionStatsByHourRecord mu1A2HourRecord =
                hourStatsByMgmtUnitAndActionGroup.get(mgmtSubgroup1).get(actionGroup2);
            assertThat(mu1A2HourRecord.getHourTime(), is(firstTime.truncatedTo(ChronoUnit.HOURS)));
            // Two snapshots got rolled up into this one.
            assertThat(mu1A2HourRecord.getMgmtUnitSubgroupId(), is(mgmtSubgroup1));
            assertThat(mu1A2HourRecord.getActionGroupId(), is(actionGroup2));

            assertThat(mu1A2HourRecord.getMaxActionCount(), is(10));
            assertThat(mu1A2HourRecord.getMinActionCount(), is(5));
            // 10 original, 1 new from second period
            assertThat(mu1A2HourRecord.getAvgActionCount().doubleValue(),
                closeTo((10 + 1) / 2.0, 0.0001));

            assertThat(mu1A2HourRecord.getMaxEntityCount(), is(6));
            assertThat(mu1A2HourRecord.getMinEntityCount(), is(4));
            assertThat(mu1A2HourRecord.getAvgEntityCount().doubleValue(),
                closeTo((6 + 4) / 2.0, 0.0001));

            assertThat(mu1A2HourRecord.getMaxSavings().doubleValue(), is(3.0));
            assertThat(mu1A2HourRecord.getMinSavings().doubleValue(), is(1.0));
            assertThat(mu1A2HourRecord.getAvgSavings().doubleValue(),
                closeTo((1 + 3) / 2.0, 0.0001));

            assertThat(mu1A2HourRecord.getMaxInvestment().doubleValue(), is(4.0));
            assertThat(mu1A2HourRecord.getMinInvestment().doubleValue(), is(2.0));
            assertThat(mu1A2HourRecord.getAvgInvestment().doubleValue(),
                closeTo((4 + 2) / 2.0, 0.0001));
        }

        // Check the second mgmt unit's hourly rollup.
        // This tests that both management units got rolled up correctly.
        {
            final ActionStatsByHourRecord m2HourRecord =
                hourStatsByMgmtUnitAndActionGroup.get(mgmtSubgroup2).get(actionGroup1);
            // The subgroup is different.
            assertThat(m2HourRecord.getMgmtUnitSubgroupId(), is(mgmtSubgroup2));
            assertThat(m2HourRecord.getHourTime(), is(firstTime.truncatedTo(ChronoUnit.HOURS)));

            assertThat(m2HourRecord.getActionGroupId(), is(actionGroup1));

            assertThat(m2HourRecord.getMaxActionCount(), is(10));
            assertThat(m2HourRecord.getMinActionCount(), is(10));
            assertThat(m2HourRecord.getAvgActionCount().doubleValue(),
                closeTo(10 / 2.0, 0.0001));

            assertThat(m2HourRecord.getMaxEntityCount(), is(5));
            assertThat(m2HourRecord.getMinEntityCount(), is(5));
            // The zero-action snapshot affects the average.
            assertThat(m2HourRecord.getAvgEntityCount().doubleValue(),
                closeTo(5 / 2.0, 0.0001));

            assertThat(m2HourRecord.getMaxSavings().doubleValue(), is(1.0));
            assertThat(m2HourRecord.getMinSavings().doubleValue(), is(1.0));
            // The zero-action snapshot affects the average.
            assertThat(m2HourRecord.getAvgSavings().doubleValue(),
                closeTo(1.0 / 2, 0.0001));

            assertThat(m2HourRecord.getMaxInvestment().doubleValue(), is(2.0));
            assertThat(m2HourRecord.getMinInvestment().doubleValue(), is(2.0));
            // The zero-action snapshot affects the average
            assertThat(m2HourRecord.getAvgInvestment().doubleValue(),
                closeTo(2.0 / 2, 0.0001));
        }
    }

    private void storeLatestActionStatsRecord(final LocalDateTime firstTime,
                                              final int mgmtSubgroup,
                                              final int actionGroup,
                                              final int totalEntityCount,
                                              final int totalActionCount,
                                              final int newActionCount,
                                              final int totalInvestment,
                                              final int totalSavings) {
        final ActionStatsLatestRecord newActionStatsLatestRecord = dsl.newRecord(
            Tables.ACTION_STATS_LATEST);
        newActionStatsLatestRecord.setActionSnapshotTime(firstTime);
        newActionStatsLatestRecord.setActionGroupId(actionGroup);
        newActionStatsLatestRecord.setMgmtUnitSubgroupId(mgmtSubgroup);
        newActionStatsLatestRecord.setTotalEntityCount(totalEntityCount);
        newActionStatsLatestRecord.setTotalActionCount(totalActionCount);
        newActionStatsLatestRecord.setNewActionCount(newActionCount);
        newActionStatsLatestRecord.setTotalSavings(BigDecimal.valueOf(totalSavings));
        newActionStatsLatestRecord.setTotalInvestment(BigDecimal.valueOf(totalInvestment));
        newActionStatsLatestRecord.store();
    }

    @Test
    public void testHourToDayRollup() {
        // START ARRANGEMENT ----
        final RolledUpStatCalculator statCalculator = new RolledUpStatCalculator();
        final DayActionStatTable dayTable = new DayActionStatTable(dsl, clock,
            statCalculator, MonthActionStatTable.MONTH_TABLE_INFO);
        final HourActionStatTable hourTable = new HourActionStatTable(dsl, clock,
            statCalculator, DayActionStatTable.DAY_TABLE_INFO);

        final ActionStatRollupScheduler scheduler = new ActionStatRollupScheduler(
            Collections.singletonList(ImmutableRollupDirection.builder()
                .fromTableReader(hourTable.reader())
                .toTableWriter(dayTable.writer())
                .description("hour to day")
                .build()), executorService);

        // Insert three "latest" records, two for one mgmt unit subgroup, one for another
        final int mgmtSubgroup1 = 1;
        final int mgmtSubgroup2 = 2;
        final int actionGroup1 = 3;
        final int actionGroup2 = 4;
        rollupTestUtils.insertMgmtUnit(mgmtSubgroup1);
        rollupTestUtils.insertMgmtUnit(mgmtSubgroup2);
        rollupTestUtils.insertActionGroup(actionGroup1);
        rollupTestUtils.insertActionGroup(actionGroup2);

        final LocalDateTime firstTime = LocalDateTime.of(2018, 9, 7, 1, 0);
        final LocalDateTime secondTime = firstTime.plusHours(2);

        // Set the clock time to the next day. We only roll up snapshots when we're out of
        // the last time period.
        final LocalDateTime nextDayTime = firstTime.plusDays(1);
        clock.changeInstant(nextDayTime.toInstant(ZoneOffset.UTC));

        // Insert snapshot and stat records into the latest database.
        // Two snapshots to be rolled up.
        //    The first snapshot contains actions for:
        //       mgmt unit subgroup 1, action group 1
        //       mgmt unit subgroup 1, action group 2
        //       mgmt unit subgroup 2, action group 1
        //    The second snapshot contains actions for:
        //       mgmt unit subgroup 1, action group 1
        //       mgmt unit subgroup 1, action group 2
        //
        // Note - this means that mgmt unit subgroup 2 had 0 actions in the second snapshot.
        {
            // Insert record for the first snapshot within the hour. This snapshot aggregated
            // two "latest" snapshots.
            final ActionSnapshotHourRecord firstSnapshotRecord = dsl.newRecord(Tables.ACTION_SNAPSHOT_HOUR);
            firstSnapshotRecord.setHourTime(firstTime);
            firstSnapshotRecord.setNumActionSnapshots(2);
            firstSnapshotRecord.store();

            // Insert record for the second snapshot within the hour. This snapshot also aggregated
            // two "latest" snapshots.
            final ActionSnapshotHourRecord secondSnapshotRecord = firstSnapshotRecord.copy();
            secondSnapshotRecord.setHourTime(secondTime);
            firstSnapshotRecord.setNumActionSnapshots(2);
            secondSnapshotRecord.store();

            final ActionStatsByHourRecord m1A1FirstRecord = dsl.newRecord(Tables.ACTION_STATS_BY_HOUR);
            m1A1FirstRecord.setHourTime(firstTime);
            m1A1FirstRecord.setActionGroupId(actionGroup1);
            m1A1FirstRecord.setMgmtUnitSubgroupId(mgmtSubgroup1);
            m1A1FirstRecord.setPriorActionCount(10);
            m1A1FirstRecord.setNewActionCount(100);
            m1A1FirstRecord.setMinActionCount(1);
            m1A1FirstRecord.setAvgActionCount(BigDecimal.valueOf(2));
            m1A1FirstRecord.setMaxActionCount(3);
            m1A1FirstRecord.setMinEntityCount(4);
            m1A1FirstRecord.setAvgEntityCount(BigDecimal.valueOf(5));
            m1A1FirstRecord.setMaxEntityCount(6);
            m1A1FirstRecord.setMinSavings(BigDecimal.valueOf(7));
            m1A1FirstRecord.setAvgSavings(BigDecimal.valueOf(8));
            m1A1FirstRecord.setMaxSavings(BigDecimal.valueOf(9));
            m1A1FirstRecord.setMinInvestment(BigDecimal.valueOf(10));
            m1A1FirstRecord.setAvgInvestment(BigDecimal.valueOf(11));
            m1A1FirstRecord.setMaxInvestment(BigDecimal.valueOf(12));
            m1A1FirstRecord.store();

            final ActionStatsByHourRecord m1A2FirstRecord = m1A1FirstRecord.copy();
            m1A2FirstRecord.setHourTime(m1A1FirstRecord.getHourTime());
            m1A2FirstRecord.setMgmtUnitSubgroupId(m1A1FirstRecord.getMgmtUnitSubgroupId());
            m1A2FirstRecord.setActionGroupId(actionGroup2);
            // Different entity count to distinguish it from the first action group's record.
            m1A2FirstRecord.setMinEntityCount(m1A1FirstRecord.getMinEntityCount() + 10);
            m1A2FirstRecord.setAvgEntityCount(m1A1FirstRecord.getAvgEntityCount().add(BigDecimal.valueOf(10)));
            m1A2FirstRecord.setMaxEntityCount(m1A1FirstRecord.getMaxEntityCount() + 10);
            m1A2FirstRecord.setPriorActionCount(m1A1FirstRecord.getPriorActionCount() + 10);
            m1A2FirstRecord.setNewActionCount(m1A1FirstRecord.getNewActionCount() + 10);
            m1A2FirstRecord.store();

            final ActionStatsByHourRecord m2Record = m1A1FirstRecord.copy();
            m2Record.setHourTime(firstTime);
            m2Record.setMgmtUnitSubgroupId(mgmtSubgroup2);
            m2Record.setActionGroupId(actionGroup1);
            m2Record.store();

            // The second snapshot will have all fields lower by 1.
            // This means the maximum will come from the first snapshot and the minimum from the second.
            final ActionStatsByHourRecord m1A1SecondRecord = dsl.newRecord(Tables.ACTION_STATS_BY_HOUR);
            m1A1SecondRecord.setHourTime(secondTime);
            m1A1SecondRecord.setActionGroupId(m1A1FirstRecord.getActionGroupId());
            m1A1SecondRecord.setMgmtUnitSubgroupId(m1A1FirstRecord.getMgmtUnitSubgroupId());
            m1A1SecondRecord.setMinActionCount(m1A1FirstRecord.getMinActionCount() - 1);
            m1A1SecondRecord.setPriorActionCount(9);
            m1A1SecondRecord.setNewActionCount(99);
            m1A1SecondRecord.setAvgActionCount(m1A1FirstRecord.getAvgActionCount().subtract(BigDecimal.ONE));
            m1A1SecondRecord.setMaxActionCount(m1A1FirstRecord.getMaxActionCount() - 1);
            m1A1SecondRecord.setMinEntityCount(m1A1FirstRecord.getMinEntityCount() - 1);
            m1A1SecondRecord.setAvgEntityCount(m1A1FirstRecord.getAvgEntityCount().subtract(BigDecimal.ONE));
            m1A1SecondRecord.setMaxEntityCount(m1A1FirstRecord.getMaxEntityCount() - 1);
            m1A1SecondRecord.setMinSavings(m1A1FirstRecord.getMinSavings().subtract(BigDecimal.ONE));
            m1A1SecondRecord.setAvgSavings(m1A1FirstRecord.getAvgSavings().subtract(BigDecimal.ONE));
            m1A1SecondRecord.setMaxSavings(m1A1FirstRecord.getMaxSavings().subtract(BigDecimal.ONE));
            m1A1SecondRecord.setMinInvestment(m1A1FirstRecord.getMinInvestment().subtract(BigDecimal.ONE));
            m1A1SecondRecord.setAvgInvestment(m1A1FirstRecord.getAvgInvestment().subtract(BigDecimal.ONE));
            m1A1SecondRecord.setMaxInvestment(m1A1FirstRecord.getMaxInvestment().subtract(BigDecimal.ONE));
            m1A1SecondRecord.store();

            final ActionStatsByHourRecord m1A2SecondRecord = m1A1SecondRecord.copy();
            m1A2SecondRecord.setHourTime(m1A1SecondRecord.getHourTime());
            m1A2SecondRecord.setMgmtUnitSubgroupId(m1A1SecondRecord.getMgmtUnitSubgroupId());
            m1A2SecondRecord.setActionGroupId(actionGroup2);
            // One field to distinguish it from the first AG.
            m1A2SecondRecord.setMinEntityCount(m1A1SecondRecord.getMinEntityCount() + 10);
            m1A2SecondRecord.setAvgEntityCount(m1A1SecondRecord.getAvgEntityCount().add(BigDecimal.valueOf(10)));
            m1A2SecondRecord.setMaxEntityCount(m1A1SecondRecord.getMaxEntityCount() + 10);
            m1A2SecondRecord.store();
        }

        // END ARRANGEMENT ----

        // START ACT -----

        // Schedule rollups, and wait for the rollup to succeed.
        scheduler.scheduleRollups();
        final ArgumentCaptor<ActionStatRollup> rollupCaptor =
            ArgumentCaptor.forClass(ActionStatRollup.class);
        verify(executorService, times(2)).submit(rollupCaptor.capture());

        // Run the scheduled rollup.
        rollupCaptor.getAllValues().forEach(ActionStatRollup::run);

        // END ACT -----

        // START ASSERT -----

        // Verify that the expected values got written to the "hour" stat and snapshot tables.
        List<ActionSnapshotDayRecord> snapshotDayRecords =
            dsl.selectFrom(Tables.ACTION_SNAPSHOT_DAY)
                .fetch();
        assertThat(snapshotDayRecords.size(), is(1));
        assertThat(snapshotDayRecords.get(0).getDayTime(),
            is(firstTime.truncatedTo(ChronoUnit.DAYS)));
        assertThat(snapshotDayRecords.get(0).getDayRollupTime(),
            is(LocalDateTime.now(clock)));
        // Two snapshots, each representing 2 rolled up snapshots.
        assertThat(snapshotDayRecords.get(0).getNumActionSnapshots(), is(4));

        final Map<Integer, Map<Integer, ActionStatsByDayRecord>> dayStatsByMgmtUnitAndActionGroup =
            new HashMap<>();
        dsl.selectFrom(Tables.ACTION_STATS_BY_DAY)
            .fetch()
            .forEach(actionStatsByHourRecord -> {
                final Map<Integer, ActionStatsByDayRecord> statsByAg =
                    dayStatsByMgmtUnitAndActionGroup.computeIfAbsent(
                        actionStatsByHourRecord.getMgmtUnitSubgroupId(), k -> new HashMap<>());
                final ActionStatsByDayRecord existing =
                    statsByAg.put(actionStatsByHourRecord.getActionGroupId(), actionStatsByHourRecord);
                // Should only have one day record for each mgmt unit and action group combination.
                assertNull(existing);
            });

        assertThat(dayStatsByMgmtUnitAndActionGroup.keySet(),
            containsInAnyOrder(mgmtSubgroup1, mgmtSubgroup2));
        assertThat(dayStatsByMgmtUnitAndActionGroup.get(mgmtSubgroup1).keySet(),
            containsInAnyOrder(actionGroup1, actionGroup2));

        // Verify the first management subunit's first action group record.
        {
            final ActionStatsByDayRecord mu1A1DayRecord =
                dayStatsByMgmtUnitAndActionGroup.get(mgmtSubgroup1).get(actionGroup1);
            assertThat(mu1A1DayRecord.getDayTime(), is(firstTime.truncatedTo(ChronoUnit.DAYS)));
            assertThat(mu1A1DayRecord.getMgmtUnitSubgroupId(), is(mgmtSubgroup1));
            assertThat(mu1A1DayRecord.getActionGroupId(), is(actionGroup1));

            // Max should come from the first record.
            // Min should come from the second record.
            assertThat(mu1A1DayRecord.getMaxActionCount(), is(3));
            assertThat(mu1A1DayRecord.getMinActionCount(), is(0));
            assertThat(mu1A1DayRecord.getAvgActionCount().doubleValue(),
                closeTo((2 + 1) / 2.0, 0.0001));

            assertThat(mu1A1DayRecord.getMaxEntityCount(), is(6));
            assertThat(mu1A1DayRecord.getMinEntityCount(), is(3));
            assertThat(mu1A1DayRecord.getAvgEntityCount().doubleValue(),
                closeTo((5 + 4) / 2.0, 0.0001));

            assertThat(mu1A1DayRecord.getMaxSavings().doubleValue(), is(9.0));
            assertThat(mu1A1DayRecord.getMinSavings().doubleValue(), is(6.0));
            assertThat(mu1A1DayRecord.getAvgSavings().doubleValue(),
                closeTo((7 + 8) / 2.0, 0.0001));

            assertThat(mu1A1DayRecord.getMaxInvestment().doubleValue(), is(12.0));
            assertThat(mu1A1DayRecord.getMinInvestment().doubleValue(), is(9.0));
            assertThat(mu1A1DayRecord.getAvgInvestment().doubleValue(),
                closeTo((10 + 11) / 2.0, 0.0001));
        }

        // Verify the first management subunit's second action group record.
        // This tests that different action groups get rolled up correctly.
        {
            final ActionStatsByDayRecord mu1A2DayRecord =
                dayStatsByMgmtUnitAndActionGroup.get(mgmtSubgroup1).get(actionGroup2);
            assertThat(mu1A2DayRecord.getDayTime(), is(firstTime.truncatedTo(ChronoUnit.DAYS)));
            assertThat(mu1A2DayRecord.getMgmtUnitSubgroupId(), is(mgmtSubgroup1));
            assertThat(mu1A2DayRecord.getActionGroupId(), is(actionGroup2));

            // Entities was the different stat for ag2
            assertThat(mu1A2DayRecord.getMaxEntityCount(), is(6 + 10));
            assertThat(mu1A2DayRecord.getMinEntityCount(), is(3 + 10));
            assertThat(mu1A2DayRecord.getAvgEntityCount().doubleValue(),
                closeTo((10 + 5 + 10 + 4) / 2.0, 0.0001));

            // The rest of the comparison is the same as for the first action group.
            assertThat(mu1A2DayRecord.getMaxActionCount(), is(3));
            assertThat(mu1A2DayRecord.getMinActionCount(), is(0));
            assertThat(mu1A2DayRecord.getAvgActionCount().doubleValue(),
                closeTo((2 + 1) / 2.0, 0.0001));

            assertThat(mu1A2DayRecord.getMaxSavings().doubleValue(), is(9.0));
            assertThat(mu1A2DayRecord.getMinSavings().doubleValue(), is(6.0));
            assertThat(mu1A2DayRecord.getAvgSavings().doubleValue(),
                closeTo((7 + 8) / 2.0, 0.0001));

            assertThat(mu1A2DayRecord.getMaxInvestment().doubleValue(), is(12.0));
            assertThat(mu1A2DayRecord.getMinInvestment().doubleValue(), is(9.0));
            assertThat(mu1A2DayRecord.getAvgInvestment().doubleValue(),
                closeTo((10 + 11) / 2.0, 0.0001));
        }

        // Check the second mgmt unit's daily rollup.
        // This tests that both management units got rolled up correctly.
        {
            final ActionStatsByDayRecord m2HourRecord =
                dayStatsByMgmtUnitAndActionGroup.get(mgmtSubgroup2).get(actionGroup1);
            // The subgroup is different.
            assertThat(m2HourRecord.getMgmtUnitSubgroupId(), is(mgmtSubgroup2));
            assertThat(m2HourRecord.getDayTime(), is(firstTime.truncatedTo(ChronoUnit.DAYS)));

            assertThat(m2HourRecord.getActionGroupId(), is(actionGroup1));

            assertThat(m2HourRecord.getMaxActionCount(), is(3));
            assertThat(m2HourRecord.getMinActionCount(), is(1));
            // The zero-action snapshot affects the average.
            assertThat(m2HourRecord.getAvgActionCount().doubleValue(),
                closeTo(2 / 2.0, 0.0001));

            assertThat(m2HourRecord.getMaxEntityCount(), is(6));
            assertThat(m2HourRecord.getMinEntityCount(), is(4));
            // The zero-action snapshot affects the average.
            assertThat(m2HourRecord.getAvgEntityCount().doubleValue(),
                closeTo(5 / 2.0, 0.0001));

            assertThat(m2HourRecord.getMaxSavings().doubleValue(), is(9.0));
            assertThat(m2HourRecord.getMinSavings().doubleValue(), is(7.0));
            // The zero-action snapshot affects the average.
            assertThat(m2HourRecord.getAvgSavings().doubleValue(),
                closeTo(8.0 / 2, 0.0001));

            assertThat(m2HourRecord.getMaxInvestment().doubleValue(), is(12.0));
            assertThat(m2HourRecord.getMinInvestment().doubleValue(), is(10.0));
            // The zero-action snapshot affects the average
            assertThat(m2HourRecord.getAvgInvestment().doubleValue(),
                closeTo(11.0 / 2, 0.0001));
        }
    }

    @Test
    public void testDayToMonthRollup() {
        // START ARRANGEMENT ----
        final RolledUpStatCalculator statCalculator = new RolledUpStatCalculator();
        final MonthActionStatTable monthTable = new MonthActionStatTable(dsl, clock);
        final DayActionStatTable dayTable = new DayActionStatTable(dsl, clock,
            statCalculator, MonthActionStatTable.MONTH_TABLE_INFO);

        final ActionStatRollupScheduler scheduler = new ActionStatRollupScheduler(
            Collections.singletonList(ImmutableRollupDirection.builder()
                .fromTableReader(dayTable.reader())
                .toTableWriter(monthTable.writer())
                .description("day to month")
                .build()), executorService);

        // Insert three "latest" records, two for one mgmt unit subgroup, one for another
        final int mgmtSubgroup1 = 1;
        final int mgmtSubgroup2 = 2;
        final int actionGroup1 = 3;
        final int actionGroup2 = 4;
        rollupTestUtils.insertMgmtUnit(mgmtSubgroup1);
        rollupTestUtils.insertMgmtUnit(mgmtSubgroup2);
        rollupTestUtils.insertActionGroup(actionGroup1);
        rollupTestUtils.insertActionGroup(actionGroup2);

        final LocalDateTime firstTime = LocalDateTime.of(2018, 9, 9, 0, 0);
        final LocalDateTime rollupMonthTime =
            LocalDateTime.of(firstTime.getYear(), firstTime.getMonth(), 1, 0, 0);
        final LocalDateTime secondTime = firstTime.plusDays(2);

        final LocalDateTime nextMonthTime = firstTime.plusMonths(1);
        // Set the clock time to the next month. We only roll up snapshots when we're out of
        // the last time period.
        clock.changeInstant(nextMonthTime.toInstant(ZoneOffset.UTC));

        // Insert snapshot and stat records into the latest database.
        // Two snapshots to be rolled up.
        //    The first snapshot contains actions for:
        //       mgmt unit subgroup 1, action group 1
        //       mgmt unit subgroup 1, action group 2
        //       mgmt unit subgroup 2, action group 1
        //    The second snapshot contains actions for:
        //       mgmt unit subgroup 1, action group 1
        //       mgmt unit subgroup 1, action group 2
        //
        // Note - this means that mgmt unit subgroup 2 had 0 actions in the second snapshot.
        {
            // Insert record for the first snapshot within the hour. This snapshot aggregates over
            // two "latest" action snapshots. That's not realistic for the "day" case, but it makes
            // the testing easier.
            final ActionSnapshotDayRecord firstSnapshotRecord = dsl.newRecord(Tables.ACTION_SNAPSHOT_DAY);
            firstSnapshotRecord.setDayTime(firstTime);
            firstSnapshotRecord.setNumActionSnapshots(2);
            firstSnapshotRecord.store();

            // Insert record for the second snapshot within the hour. This snapshot aggregates over
            // two "latest" action snapshots. That's not realistic for the "day" case, but it makes
            // the testing easier.
            final ActionSnapshotDayRecord secondSnapshotRecord = firstSnapshotRecord.copy();
            secondSnapshotRecord.setDayTime(secondTime);
            secondSnapshotRecord.setNumActionSnapshots(2);
            secondSnapshotRecord.store();

            final ActionStatsByDayRecord m1A1FirstRecord = dsl.newRecord(Tables.ACTION_STATS_BY_DAY);
            m1A1FirstRecord.setDayTime(firstTime);
            m1A1FirstRecord.setActionGroupId(actionGroup1);
            m1A1FirstRecord.setMgmtUnitSubgroupId(mgmtSubgroup1);
            m1A1FirstRecord.setPriorActionCount(10);
            m1A1FirstRecord.setNewActionCount(100);
            m1A1FirstRecord.setMinActionCount(1);
            m1A1FirstRecord.setAvgActionCount(BigDecimal.valueOf(2));
            m1A1FirstRecord.setMaxActionCount(3);
            m1A1FirstRecord.setMinEntityCount(4);
            m1A1FirstRecord.setAvgEntityCount(BigDecimal.valueOf(5));
            m1A1FirstRecord.setMaxEntityCount(6);
            m1A1FirstRecord.setMinSavings(BigDecimal.valueOf(7));
            m1A1FirstRecord.setAvgSavings(BigDecimal.valueOf(8));
            m1A1FirstRecord.setMaxSavings(BigDecimal.valueOf(9));
            m1A1FirstRecord.setMinInvestment(BigDecimal.valueOf(10));
            m1A1FirstRecord.setAvgInvestment(BigDecimal.valueOf(11));
            m1A1FirstRecord.setMaxInvestment(BigDecimal.valueOf(12));
            m1A1FirstRecord.store();

            final ActionStatsByDayRecord m1A2FirstRecord = m1A1FirstRecord.copy();
            m1A2FirstRecord.setDayTime(firstTime);
            m1A2FirstRecord.setMgmtUnitSubgroupId(m1A1FirstRecord.getMgmtUnitSubgroupId());
            m1A2FirstRecord.setActionGroupId(actionGroup2);
            // Different entity count to distinguish it from the first action group's record.
            m1A2FirstRecord.setMinEntityCount(m1A1FirstRecord.getMinEntityCount() + 10);
            m1A2FirstRecord.setAvgEntityCount(m1A1FirstRecord.getAvgEntityCount().add(BigDecimal.valueOf(10)));
            m1A2FirstRecord.setMaxEntityCount(m1A1FirstRecord.getMaxEntityCount() + 10);
            m1A2FirstRecord.setPriorActionCount(m1A1FirstRecord.getPriorActionCount() + 10);
            m1A2FirstRecord.setNewActionCount(m1A1FirstRecord.getNewActionCount() + 10);
            m1A2FirstRecord.store();

            final ActionStatsByDayRecord m2Record = m1A1FirstRecord.copy();
            m2Record.setDayTime(firstTime);
            m2Record.setMgmtUnitSubgroupId(mgmtSubgroup2);
            m2Record.setActionGroupId(actionGroup1);
            m2Record.store();

            // The second snapshot will have all fields lower by 1.
            // This means the maximum will come from the first snapshot and the minimum from the second.
            final ActionStatsByDayRecord m1A1SecondRecord = dsl.newRecord(Tables.ACTION_STATS_BY_DAY);
            m1A1SecondRecord.setDayTime(secondTime);
            m1A1SecondRecord.setActionGroupId(m1A1FirstRecord.getActionGroupId());
            m1A1SecondRecord.setMgmtUnitSubgroupId(m1A1FirstRecord.getMgmtUnitSubgroupId());
            m1A1FirstRecord.setPriorActionCount(9);
            m1A1FirstRecord.setNewActionCount(99);
            m1A1SecondRecord.setMinActionCount(m1A1FirstRecord.getMinActionCount() - 1);
            m1A1SecondRecord.setAvgActionCount(m1A1FirstRecord.getAvgActionCount().subtract(BigDecimal.ONE));
            m1A1SecondRecord.setMaxActionCount(m1A1FirstRecord.getMaxActionCount() - 1);
            m1A1SecondRecord.setMinEntityCount(m1A1FirstRecord.getMinEntityCount() - 1);
            m1A1SecondRecord.setAvgEntityCount(m1A1FirstRecord.getAvgEntityCount().subtract(BigDecimal.ONE));
            m1A1SecondRecord.setMaxEntityCount(m1A1FirstRecord.getMaxEntityCount() - 1);
            m1A1SecondRecord.setMinSavings(m1A1FirstRecord.getMinSavings().subtract(BigDecimal.ONE));
            m1A1SecondRecord.setAvgSavings(m1A1FirstRecord.getAvgSavings().subtract(BigDecimal.ONE));
            m1A1SecondRecord.setMaxSavings(m1A1FirstRecord.getMaxSavings().subtract(BigDecimal.ONE));
            m1A1SecondRecord.setMinInvestment(m1A1FirstRecord.getMinInvestment().subtract(BigDecimal.ONE));
            m1A1SecondRecord.setAvgInvestment(m1A1FirstRecord.getAvgInvestment().subtract(BigDecimal.ONE));
            m1A1SecondRecord.setMaxInvestment(m1A1FirstRecord.getMaxInvestment().subtract(BigDecimal.ONE));
            m1A1SecondRecord.store();

            final ActionStatsByDayRecord m1A2SecondRecord = m1A1SecondRecord.copy();
            m1A2SecondRecord.setDayTime(secondTime);
            m1A2SecondRecord.setMgmtUnitSubgroupId(m1A1SecondRecord.getMgmtUnitSubgroupId());
            m1A2SecondRecord.setActionGroupId(actionGroup2);
            // One field to distinguish it from the first AG.
            m1A2SecondRecord.setMinEntityCount(m1A1SecondRecord.getMinEntityCount() + 10);
            m1A2SecondRecord.setAvgEntityCount(m1A1SecondRecord.getAvgEntityCount().add(BigDecimal.valueOf(10)));
            m1A2SecondRecord.setMaxEntityCount(m1A1SecondRecord.getMaxEntityCount() + 10);
            m1A2SecondRecord.store();
        }

        // END ARRANGEMENT ----

        // START ACT -----

        // Schedule rollups, and wait for the rollup to succeed.
        scheduler.scheduleRollups();
        final ArgumentCaptor<ActionStatRollup> rollupCaptor =
            ArgumentCaptor.forClass(ActionStatRollup.class);
        verify(executorService, times(2)).submit(rollupCaptor.capture());

        // Run the scheduled rollup.
        rollupCaptor.getAllValues().forEach(ActionStatRollup::run);

        // END ACT -----

        // START ASSERT -----

        // Verify that the expected values got written to the "hour" stat and snapshot tables.
        List<ActionSnapshotMonthRecord> snapshotMonthRecords =
            dsl.selectFrom(Tables.ACTION_SNAPSHOT_MONTH)
                .fetch();
        assertThat(snapshotMonthRecords.size(), is(1));
        assertThat(snapshotMonthRecords.get(0).getMonthTime(),
            is(rollupMonthTime));
        assertThat(snapshotMonthRecords.get(0).getMonthRollupTime(),
            is(LocalDateTime.now(clock)));
        // Two snapshots, each representing 2 rolled up snapshots.
        assertThat(snapshotMonthRecords.get(0).getNumActionSnapshots(), is(4));

        final Map<Integer, Map<Integer, ActionStatsByMonthRecord>> monthStatsByMgmtUnitAndActionGroup =
            new HashMap<>();
        dsl.selectFrom(Tables.ACTION_STATS_BY_MONTH)
            .fetch()
            .forEach(actionStatsByMonthRecord -> {
                final Map<Integer, ActionStatsByMonthRecord> statsByAg =
                    monthStatsByMgmtUnitAndActionGroup.computeIfAbsent(
                        actionStatsByMonthRecord.getMgmtUnitSubgroupId(), k -> new HashMap<>());
                final ActionStatsByMonthRecord existing =
                    statsByAg.put(actionStatsByMonthRecord.getActionGroupId(), actionStatsByMonthRecord);
                // Should only have one day record for each mgmt unit and action group combination.
                assertNull(existing);
            });

        assertThat(monthStatsByMgmtUnitAndActionGroup.keySet(),
            containsInAnyOrder(mgmtSubgroup1, mgmtSubgroup2));
        assertThat(monthStatsByMgmtUnitAndActionGroup.get(mgmtSubgroup1).keySet(),
            containsInAnyOrder(actionGroup1, actionGroup2));

        // Verify the first management subunit's first action group record.
        {
            final ActionStatsByMonthRecord mu1A1DayRecord =
                monthStatsByMgmtUnitAndActionGroup.get(mgmtSubgroup1).get(actionGroup1);
            assertThat(mu1A1DayRecord.getMonthTime(), is(rollupMonthTime));
            assertThat(mu1A1DayRecord.getMgmtUnitSubgroupId(), is(mgmtSubgroup1));
            assertThat(mu1A1DayRecord.getActionGroupId(), is(actionGroup1));

            // Max should come from the first record.
            // Min should come from the second record.
            assertThat(mu1A1DayRecord.getMaxActionCount(), is(3));
            assertThat(mu1A1DayRecord.getMinActionCount(), is(0));
            assertThat(mu1A1DayRecord.getAvgActionCount().doubleValue(),
                closeTo((2 + 1) / 2.0, 0.0001));

            assertThat(mu1A1DayRecord.getMaxEntityCount(), is(6));
            assertThat(mu1A1DayRecord.getMinEntityCount(), is(3));
            assertThat(mu1A1DayRecord.getAvgEntityCount().doubleValue(),
                closeTo((5 + 4) / 2.0, 0.0001));

            assertThat(mu1A1DayRecord.getMaxSavings().doubleValue(), is(9.0));
            assertThat(mu1A1DayRecord.getMinSavings().doubleValue(), is(6.0));
            assertThat(mu1A1DayRecord.getAvgSavings().doubleValue(),
                closeTo((7 + 8) / 2.0, 0.0001));

            assertThat(mu1A1DayRecord.getMaxInvestment().doubleValue(), is(12.0));
            assertThat(mu1A1DayRecord.getMinInvestment().doubleValue(), is(9.0));
            assertThat(mu1A1DayRecord.getAvgInvestment().doubleValue(),
                closeTo((10 + 11) / 2.0, 0.0001));
        }

        // Verify the first management subunit's second action group record.
        // This tests that different action groups get rolled up correctly.
        {
            final ActionStatsByMonthRecord mu1A2DayRecord =
                monthStatsByMgmtUnitAndActionGroup.get(mgmtSubgroup1).get(actionGroup2);
            assertThat(mu1A2DayRecord.getMonthTime(), is(rollupMonthTime));
            assertThat(mu1A2DayRecord.getMgmtUnitSubgroupId(), is(mgmtSubgroup1));
            assertThat(mu1A2DayRecord.getActionGroupId(), is(actionGroup2));

            // Entities was the different stat for ag2
            assertThat(mu1A2DayRecord.getMaxEntityCount(), is(6 + 10));
            assertThat(mu1A2DayRecord.getMinEntityCount(), is(3 + 10));
            assertThat(mu1A2DayRecord.getAvgEntityCount().doubleValue(),
                closeTo((10 + 5 + 10 + 4) / 2.0, 0.0001));

            // The rest of the comparison is the same as for the first action group.
            assertThat(mu1A2DayRecord.getMaxActionCount(), is(3));
            assertThat(mu1A2DayRecord.getMinActionCount(), is(0));
            assertThat(mu1A2DayRecord.getAvgActionCount().doubleValue(),
                closeTo((2 + 1) / 2.0, 0.0001));

            assertThat(mu1A2DayRecord.getMaxSavings().doubleValue(), is(9.0));
            assertThat(mu1A2DayRecord.getMinSavings().doubleValue(), is(6.0));
            assertThat(mu1A2DayRecord.getAvgSavings().doubleValue(),
                closeTo((7 + 8) / 2.0, 0.0001));

            assertThat(mu1A2DayRecord.getMaxInvestment().doubleValue(), is(12.0));
            assertThat(mu1A2DayRecord.getMinInvestment().doubleValue(), is(9.0));
            assertThat(mu1A2DayRecord.getAvgInvestment().doubleValue(),
                closeTo((10 + 11) / 2.0, 0.0001));
        }

        // Check the second mgmt unit's daily rollup.
        // This tests that both management units got rolled up correctly.
        //
        // Note that unlike the "latest" case, the hour that didn't have a record for this
        // management unit doesn't affect the averages.
        {
            final ActionStatsByMonthRecord m2DayRecord =
                monthStatsByMgmtUnitAndActionGroup.get(mgmtSubgroup2).get(actionGroup1);
            // The subgroup is different.
            assertThat(m2DayRecord.getMgmtUnitSubgroupId(), is(mgmtSubgroup2));
            assertThat(m2DayRecord.getMonthTime(), is(rollupMonthTime));

            assertThat(m2DayRecord.getActionGroupId(), is(actionGroup1));

            assertThat(m2DayRecord.getMaxActionCount(), is(3));
            assertThat(m2DayRecord.getMinActionCount(), is(1));
            // The zero-action snapshot affects the average.
            assertThat(m2DayRecord.getAvgActionCount().doubleValue(),
                closeTo(2 / 2.0, 0.0001));

            assertThat(m2DayRecord.getMaxEntityCount(), is(6));
            assertThat(m2DayRecord.getMinEntityCount(), is(4));
            // The zero-action snapshot affects the average.
            assertThat(m2DayRecord.getAvgEntityCount().doubleValue(),
                closeTo(5 / 2.0, 0.0001));

            assertThat(m2DayRecord.getMaxSavings().doubleValue(), is(9.0));
            assertThat(m2DayRecord.getMinSavings().doubleValue(), is(7.0));
            // The zero-action snapshot affects the average.
            assertThat(m2DayRecord.getAvgSavings().doubleValue(),
                closeTo(8.0 / 2, 0.0001));

            assertThat(m2DayRecord.getMaxInvestment().doubleValue(), is(12.0));
            assertThat(m2DayRecord.getMinInvestment().doubleValue(), is(10.0));
            // The zero-action snapshot affects the average
            assertThat(m2DayRecord.getAvgInvestment().doubleValue(),
                closeTo(11.0 / 2, 0.0001));
        }
    }
}

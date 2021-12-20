package com.vmturbo.action.orchestrator.stats.rollup;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.action.orchestrator.db.Action;
import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotDayRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotHourRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotLatestRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotMonthRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByDayRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByHourRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByMonthRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatCleanupScheduler.ActionStatCleanup;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Integration-level test for greater confidence that the {@link ActionStatCleanupScheduler}
 * actually removes rows from the proper {@link ActionStatTable}s.
 */
public class ActionStatCleanupIntegrationTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Action.ACTION);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private DSLContext dsl = dbConfig.getDslContext();

    private RollupTestUtils rollupTestUtils;

    private static final LocalDateTime CLOCK_TIME = RollupTestUtils.time(1, 1);

    private MutableFixedClock clock =
        new MutableFixedClock(CLOCK_TIME.toInstant(ZoneOffset.UTC), ZoneId.from(ZoneOffset.UTC));

    private ExecutorService executorService = mock(ExecutorService.class);

    private RetentionPeriodFetcher retentionPeriodFetcher = mock(RetentionPeriodFetcher.class);

    private RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);

    private static final long MIN_SEC_BETWEEN_CLEANUPS = 1;

    private static final int MGMT_SUBGROUP_ID = 12345;
    private static final int ACTION_GROUP_ID = 9273;

    @Before
    public void setup() {
        rollupTestUtils = new RollupTestUtils(dsl);

        rollupTestUtils.insertMgmtUnit(MGMT_SUBGROUP_ID);
        rollupTestUtils.insertActionGroup(ACTION_GROUP_ID);
        when(retentionPeriodFetcher.getRetentionPeriods()).thenReturn(retentionPeriods);
    }

    @Test
    public void testStatCleanupLatest() {
        final RolledUpStatCalculator statCalculator = new RolledUpStatCalculator();
        final LatestActionStatTable latestTable = new LatestActionStatTable(dsl, clock,
            statCalculator, HourActionStatTable.HOUR_TABLE_INFO);

        final ActionStatCleanupScheduler cleanupScheduler = new ActionStatCleanupScheduler(clock,
            Collections.singletonList(latestTable), retentionPeriodFetcher, executorService,
            MIN_SEC_BETWEEN_CLEANUPS, TimeUnit.SECONDS);


        when(retentionPeriods.latestRetentionMinutes()).thenReturn(30);
        // This is the earliest time we keep, according to the (mocked) retention periods.
        final LocalDateTime trimTime = CLOCK_TIME.minusMinutes(30);


        // Insert a record and snapshot that will be cleaned up, and one that won't.
        final ActionSnapshotLatestRecord trimmedSnapshotRecord = dsl.newRecord(Tables.ACTION_SNAPSHOT_LATEST);
        trimmedSnapshotRecord.setActionSnapshotTime(trimTime.minusMinutes(10));
        // The following three fields don't matter operationally.
        trimmedSnapshotRecord.setActionsCount(1);
        trimmedSnapshotRecord.setTopologyId(1L);
        // Add some hours, so that if we accidentally use this operationally the test will fail.
        trimmedSnapshotRecord.setSnapshotRecordingTime(trimTime.plusHours(1));
        trimmedSnapshotRecord.store();

        final ActionSnapshotLatestRecord keptSnapshotRecord = trimmedSnapshotRecord.copy();
        keptSnapshotRecord.setActionSnapshotTime(trimTime);
        keptSnapshotRecord.store();

        final ActionStatsLatestRecord cleanedUpRecord = dsl.newRecord(Tables.ACTION_STATS_LATEST);
        cleanedUpRecord.setActionSnapshotTime(trimTime.minusMinutes(10));
        cleanedUpRecord.setActionGroupId(ACTION_GROUP_ID);
        cleanedUpRecord.setMgmtUnitSubgroupId(MGMT_SUBGROUP_ID);
        cleanedUpRecord.setTotalEntityCount(5);
        cleanedUpRecord.setTotalActionCount(10);
        cleanedUpRecord.setTotalSavings(BigDecimal.valueOf(0));
        cleanedUpRecord.setTotalInvestment(BigDecimal.valueOf(0));
        cleanedUpRecord.store();

        final ActionStatsLatestRecord keptRecord = cleanedUpRecord.copy();
        keptRecord.setActionSnapshotTime(trimTime);
        keptRecord.setActionGroupId(ACTION_GROUP_ID);
        keptRecord.setMgmtUnitSubgroupId(MGMT_SUBGROUP_ID);
        keptRecord.store();

        cleanupScheduler.scheduleCleanups();

        final ArgumentCaptor<ActionStatCleanup> cleanupCaptor =
            ArgumentCaptor.forClass(ActionStatCleanup.class);
        verify(executorService).submit(cleanupCaptor.capture());

        final ActionStatCleanup cleanup = cleanupCaptor.getValue();
        cleanup.run();

        final List<ActionSnapshotLatestRecord> snapshotRecords =
            dsl.selectFrom(Tables.ACTION_SNAPSHOT_LATEST).fetch();
        assertThat(snapshotRecords.size(), is(1));

        assertThat(snapshotRecords.get(0), is(keptSnapshotRecord));

        final List<ActionStatsLatestRecord> statRecords =
            dsl.selectFrom(Tables.ACTION_STATS_LATEST).fetch();
        assertThat(statRecords.size(), is(1));
        assertThat(statRecords.get(0).getActionSnapshotTime(), is(keptRecord.getActionSnapshotTime()));
    }

    @Test
    public void testStatCleanupHourly() {
        final RolledUpStatCalculator statCalculator = new RolledUpStatCalculator();
        final HourActionStatTable hourTable = new HourActionStatTable(dsl, clock,
            statCalculator, DayActionStatTable.DAY_TABLE_INFO);

        final ActionStatCleanupScheduler cleanupScheduler = new ActionStatCleanupScheduler(clock,
            Collections.singletonList(hourTable), retentionPeriodFetcher, executorService,
            MIN_SEC_BETWEEN_CLEANUPS, TimeUnit.SECONDS);


        when(retentionPeriods.hourlyRetentionHours()).thenReturn(1);
        // This is the earliest time we keep, according to the (mocked) retention periods.
        final LocalDateTime trimTime = CLOCK_TIME.minusHours(1);


        // Make one record + snapshot we want to keep, and one to trim.
        final ActionSnapshotHourRecord trimmedSnapshotRecord = dsl.newRecord(Tables.ACTION_SNAPSHOT_HOUR);
        trimmedSnapshotRecord.setHourTime(trimTime.minusHours(1));
        trimmedSnapshotRecord.setNumActionSnapshots(2);
        trimmedSnapshotRecord.store();

        final ActionSnapshotHourRecord keptSnapshotRecord = trimmedSnapshotRecord.copy();
        keptSnapshotRecord.setHourTime(trimTime);
        keptSnapshotRecord.store();

        final ActionStatsByHourRecord trimmedRecord = dsl.newRecord(Tables.ACTION_STATS_BY_HOUR);
        trimmedRecord.setHourTime(trimmedSnapshotRecord.getHourTime());
        trimmedRecord.setActionGroupId(ACTION_GROUP_ID);
        trimmedRecord.setMgmtUnitSubgroupId(MGMT_SUBGROUP_ID);
        trimmedRecord.setMinActionCount(1);
        trimmedRecord.setAvgActionCount(BigDecimal.valueOf(2));
        trimmedRecord.setMaxActionCount(3);
        trimmedRecord.setMinEntityCount(4);
        trimmedRecord.setAvgEntityCount(BigDecimal.valueOf(5));
        trimmedRecord.setMaxEntityCount(6);
        trimmedRecord.setMinSavings(BigDecimal.valueOf(7));
        trimmedRecord.setAvgSavings(BigDecimal.valueOf(8));
        trimmedRecord.setMaxSavings(BigDecimal.valueOf(9));
        trimmedRecord.setMinInvestment(BigDecimal.valueOf(10));
        trimmedRecord.setAvgInvestment(BigDecimal.valueOf(11));
        trimmedRecord.setMaxInvestment(BigDecimal.valueOf(12));
        trimmedRecord.store();

        final ActionStatsByHourRecord keptRecord = trimmedRecord.copy();
        keptRecord.setHourTime(keptSnapshotRecord.getHourTime());
        keptRecord.setActionGroupId(ACTION_GROUP_ID);
        keptRecord.setMgmtUnitSubgroupId(MGMT_SUBGROUP_ID);
        keptRecord.store();

        // Schedule cleanups.
        cleanupScheduler.scheduleCleanups();

        // Capture and run the cleanup - normally this would happen asynchronously.
        final ArgumentCaptor<ActionStatCleanup> cleanupCaptor =
            ArgumentCaptor.forClass(ActionStatCleanup.class);
        verify(executorService).submit(cleanupCaptor.capture());

        final ActionStatCleanup cleanup = cleanupCaptor.getValue();
        cleanup.run();

        // Make sure there is just one snapshot record left - the one we're supposed to keep.
        final List<ActionSnapshotHourRecord> snapshotRecords =
            dsl.selectFrom(Tables.ACTION_SNAPSHOT_HOUR).fetch();
        assertThat(snapshotRecords.size(), is(1));

        assertThat(snapshotRecords.get(0).key(), is(keptSnapshotRecord.key()));

        // Make sure there is just one stat record left - the one we're supposed to keep.
        final List<ActionStatsByHourRecord> statRecords =
            dsl.selectFrom(Tables.ACTION_STATS_BY_HOUR).fetch();
        assertThat(statRecords.size(), is(1));
        assertThat(statRecords.get(0).key(), is(keptRecord.key()));
    }

    @Test
    public void testStatCleanupDaily() {
        final RolledUpStatCalculator statCalculator = new RolledUpStatCalculator();
        final DayActionStatTable dayTable = new DayActionStatTable(dsl, clock,
            statCalculator, MonthActionStatTable.MONTH_TABLE_INFO);

        final ActionStatCleanupScheduler cleanupScheduler = new ActionStatCleanupScheduler(clock,
            Collections.singletonList(dayTable), retentionPeriodFetcher, executorService,
            MIN_SEC_BETWEEN_CLEANUPS, TimeUnit.SECONDS);


        when(retentionPeriods.dailyRetentionDays()).thenReturn(1);
        // This is the earliest time we keep, according to the (mocked) retention periods.
        final LocalDateTime trimTime = CLOCK_TIME.minusDays(1);


        // Make one record + snapshot we want to keep, and one to trim.
        final ActionSnapshotDayRecord trimmedSnapshotRecord = dsl.newRecord(Tables.ACTION_SNAPSHOT_DAY);
        trimmedSnapshotRecord.setDayTime(trimTime.minusDays(1));
        trimmedSnapshotRecord.setNumActionSnapshots(2);
        trimmedSnapshotRecord.store();

        final ActionSnapshotDayRecord keptSnapshotRecord = trimmedSnapshotRecord.copy();
        keptSnapshotRecord.setDayTime(trimTime);
        keptSnapshotRecord.store();

        final ActionStatsByDayRecord trimmedRecord = dsl.newRecord(Tables.ACTION_STATS_BY_DAY);
        trimmedRecord.setDayTime(trimmedSnapshotRecord.getDayTime());
        trimmedRecord.setActionGroupId(ACTION_GROUP_ID);
        trimmedRecord.setMgmtUnitSubgroupId(MGMT_SUBGROUP_ID);
        trimmedRecord.setMinActionCount(1);
        trimmedRecord.setAvgActionCount(BigDecimal.valueOf(2));
        trimmedRecord.setMaxActionCount(3);
        trimmedRecord.setMinEntityCount(4);
        trimmedRecord.setAvgEntityCount(BigDecimal.valueOf(5));
        trimmedRecord.setMaxEntityCount(6);
        trimmedRecord.setMinSavings(BigDecimal.valueOf(7));
        trimmedRecord.setAvgSavings(BigDecimal.valueOf(8));
        trimmedRecord.setMaxSavings(BigDecimal.valueOf(9));
        trimmedRecord.setMinInvestment(BigDecimal.valueOf(10));
        trimmedRecord.setAvgInvestment(BigDecimal.valueOf(11));
        trimmedRecord.setMaxInvestment(BigDecimal.valueOf(12));
        trimmedRecord.store();

        final ActionStatsByDayRecord keptRecord = trimmedRecord.copy();
        keptRecord.setDayTime(keptSnapshotRecord.getDayTime());
        keptRecord.setActionGroupId(ACTION_GROUP_ID);
        keptRecord.setMgmtUnitSubgroupId(MGMT_SUBGROUP_ID);
        keptRecord.store();

        // Schedule cleanups.
        cleanupScheduler.scheduleCleanups();

        // Capture and run the cleanup - normally this would happen asynchronously.
        final ArgumentCaptor<ActionStatCleanup> cleanupCaptor =
            ArgumentCaptor.forClass(ActionStatCleanup.class);
        verify(executorService).submit(cleanupCaptor.capture());

        final ActionStatCleanup cleanup = cleanupCaptor.getValue();
        cleanup.run();

        // Make sure there is just one snapshot record left - the one we're supposed to keep.
        final List<ActionSnapshotDayRecord> snapshotRecords =
            dsl.selectFrom(Tables.ACTION_SNAPSHOT_DAY).fetch();
        assertThat(snapshotRecords.size(), is(1));

        assertThat(snapshotRecords.get(0).key(), is(keptSnapshotRecord.key()));

        // Make sure there is just one stat record left - the one we're supposed to keep.
        final List<ActionStatsByDayRecord> statRecords =
            dsl.selectFrom(Tables.ACTION_STATS_BY_DAY).fetch();
        assertThat(statRecords.size(), is(1));
        assertThat(statRecords.get(0).key(), is(keptRecord.key()));
    }

    @Test
    public void testStatCleanupMonthly() {
        final MonthActionStatTable dayTable = new MonthActionStatTable(dsl, clock);

        final ActionStatCleanupScheduler cleanupScheduler = new ActionStatCleanupScheduler(clock,
            Collections.singletonList(dayTable), retentionPeriodFetcher, executorService,
            MIN_SEC_BETWEEN_CLEANUPS, TimeUnit.SECONDS);


        when(retentionPeriods.monthlyRetentionMonths()).thenReturn(1);
        // This is the earliest time we keep, according to the (mocked) retention periods.
        final LocalDateTime trimTime = CLOCK_TIME.minusMonths(1);


        // Make one record + snapshot we want to keep, and one to trim.
        final ActionSnapshotMonthRecord trimmedSnapshotRecord = dsl.newRecord(Tables.ACTION_SNAPSHOT_MONTH);
        trimmedSnapshotRecord.setMonthTime(trimTime.minusMonths(1));
        trimmedSnapshotRecord.setNumActionSnapshots(2);
        trimmedSnapshotRecord.store();

        final ActionSnapshotMonthRecord keptSnapshotRecord = trimmedSnapshotRecord.copy();
        keptSnapshotRecord.setMonthTime(trimTime);
        keptSnapshotRecord.store();

        final ActionStatsByMonthRecord trimmedRecord = dsl.newRecord(Tables.ACTION_STATS_BY_MONTH);
        trimmedRecord.setMonthTime(trimmedSnapshotRecord.getMonthTime());
        trimmedRecord.setActionGroupId(ACTION_GROUP_ID);
        trimmedRecord.setMgmtUnitSubgroupId(MGMT_SUBGROUP_ID);
        trimmedRecord.setMinActionCount(1);
        trimmedRecord.setAvgActionCount(BigDecimal.valueOf(2));
        trimmedRecord.setMaxActionCount(3);
        trimmedRecord.setMinEntityCount(4);
        trimmedRecord.setAvgEntityCount(BigDecimal.valueOf(5));
        trimmedRecord.setMaxEntityCount(6);
        trimmedRecord.setMinSavings(BigDecimal.valueOf(7));
        trimmedRecord.setAvgSavings(BigDecimal.valueOf(8));
        trimmedRecord.setMaxSavings(BigDecimal.valueOf(9));
        trimmedRecord.setMinInvestment(BigDecimal.valueOf(10));
        trimmedRecord.setAvgInvestment(BigDecimal.valueOf(11));
        trimmedRecord.setMaxInvestment(BigDecimal.valueOf(12));
        trimmedRecord.store();

        final ActionStatsByMonthRecord keptRecord = trimmedRecord.copy();
        keptRecord.setMonthTime(keptSnapshotRecord.getMonthTime());
        keptRecord.setActionGroupId(ACTION_GROUP_ID);
        keptRecord.setMgmtUnitSubgroupId(MGMT_SUBGROUP_ID);
        keptRecord.store();

        // Schedule cleanups.
        cleanupScheduler.scheduleCleanups();

        // Capture and run the cleanup - normally this would happen asynchronously.
        final ArgumentCaptor<ActionStatCleanup> cleanupCaptor =
            ArgumentCaptor.forClass(ActionStatCleanup.class);
        verify(executorService).submit(cleanupCaptor.capture());

        final ActionStatCleanup cleanup = cleanupCaptor.getValue();
        cleanup.run();

        // Make sure there is just one snapshot record left - the one we're supposed to keep.
        final List<ActionSnapshotMonthRecord> snapshotRecords =
            dsl.selectFrom(Tables.ACTION_SNAPSHOT_MONTH).fetch();
        assertThat(snapshotRecords.size(), is(1));

        assertThat(snapshotRecords.get(0).key(), is(keptSnapshotRecord.key()));

        // Make sure there is just one stat record left - the one we're supposed to keep.
        final List<ActionStatsByMonthRecord> statRecords =
            dsl.selectFrom(Tables.ACTION_STATS_BY_MONTH).fetch();
        assertThat(statRecords.size(), is(1));
        assertThat(statRecords.get(0).key(), is(keptRecord.key()));
    }
}

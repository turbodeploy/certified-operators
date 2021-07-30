package com.vmturbo.action.orchestrator.stats.rollup.v2;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.db.Action;
import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotDayRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotHourRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotMonthRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByDayRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByHourRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByMonthRecord;
import com.vmturbo.action.orchestrator.stats.rollup.RollupTestUtils;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Unit tests for the migration.
 */
public class ActionRollupAlgorithmMigratorTest {
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

    private RollupTestUtils rollupTestUtils = new RollupTestUtils(dsl);

    private final int msu1 = 1;
    private final int msu2 = 2;

    private final int ag1 = 3;
    private final int ag2 = 4;

    final LocalDateTime todayH1 = LocalDateTime.of(2018, Month.SEPTEMBER, 9, 1, 0);

    private MutableFixedClock clock = new MutableFixedClock(todayH1);

    /**
     * Common setup before every test.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        rollupTestUtils = new RollupTestUtils(dsl);

        rollupTestUtils.insertActionGroup(ag1);
        rollupTestUtils.insertActionGroup(ag2);
        rollupTestUtils.insertMgmtUnit(msu1);
        rollupTestUtils.insertMgmtUnit(msu2);
    }

    /**
     * hours -> daily rollup.
     */
    @Test
    public void testV1toV2MigrateTodaysHoursToDaily() {
        final LocalDateTime todayH2 = todayH1.plus(1, ChronoUnit.HOURS);
        final LocalDateTime todayDay = todayH2.truncatedTo(ChronoUnit.DAYS);
        final LocalDateTime yesterday = todayH1.minus(1, ChronoUnit.DAYS);

        final List<ActionSnapshotHourRecord> snapshotRecords = Arrays.asList(
                new ActionSnapshotHourRecord(yesterday, 10, yesterday),
                new ActionSnapshotHourRecord(todayH1, 5, todayH1),
                new ActionSnapshotHourRecord(todayH2, 5, todayH2)
        );
        dsl.batchInsert(snapshotRecords).execute();

        final List<ActionStatsByHourRecord> hourRecords = Arrays.asList(
                // Insert a record for yesterday so we can make sure it doesn't get rolled up
                // by the migration.
                hourRecord(yesterday, msu1, ag1, 1),
                hourRecord(todayH1, msu1, ag1, 1),
                hourRecord(todayH2, msu1, ag1, 3),
                hourRecord(todayH1, msu1, ag2, 2),
                hourRecord(todayH2, msu1, ag2, 4),

                hourRecord(todayH1, msu2, ag1, 3),
                hourRecord(todayH2, msu2, ag1, 5),
                hourRecord(todayH1, msu2, ag2, 4),
                hourRecord(todayH2, msu2, ag2, 6)
        );
        final ActionStatsByDayRecord msu1ag1Day = dayRecord(todayDay, msu1, ag1, 1, 2, 3);
        msu1ag1Day.setPriorActionCount(1);
        msu1ag1Day.setNewActionCount(1 + 3);
        final ActionStatsByDayRecord msu1ag2Day = dayRecord(todayDay, msu1, ag2, 2, 3, 4);
        msu1ag2Day.setPriorActionCount(2);
        msu1ag2Day.setNewActionCount(2 + 4);
        final ActionStatsByDayRecord msu2ag1Day = dayRecord(todayDay, msu2, ag1, 3, 4, 5);
        msu2ag1Day.setPriorActionCount(3);
        msu2ag1Day.setNewActionCount(3 + 5);
        final ActionStatsByDayRecord msu2ag2Day = dayRecord(todayDay, msu2, ag2, 4, 5, 6);
        msu2ag2Day.setPriorActionCount(4);
        msu2ag2Day.setNewActionCount(4 + 6);

        dsl.batchInsert(hourRecords).execute();

        ActionRollupAlgorithmMigrator migration = new ActionRollupAlgorithmMigrator(dsl, clock, RollupAlgorithmVersion.V2);
        migration.doMigration();

        final List<ActionSnapshotDayRecord> dayRecords = dsl.selectFrom(Tables.ACTION_SNAPSHOT_DAY).fetch();
        assertThat(dayRecords.size(), is(1));
        assertThat(dayRecords.get(0).getDayTime(), is(todayDay));
        assertThat(dayRecords.get(0).getNumActionSnapshots(), is(10));

        // Only expect one record per msu/ag for this test.
        final Map<Integer, Map<Integer, ActionStatsByDayRecord>> recordsByMuAndAg = new HashMap<>();
        dsl.selectFrom(Tables.ACTION_STATS_BY_DAY).fetch()
            .forEach(record -> {
                assertThat(record.getDayTime(), is(todayDay));
                recordsByMuAndAg.computeIfAbsent(record.getMgmtUnitSubgroupId(), k -> new HashMap<>())
                    .put(record.getActionGroupId(), record);
            });

        rollupTestUtils.compareAggregateRecords(recordsByMuAndAg.get(msu1).get(ag1), msu1ag1Day);
        rollupTestUtils.compareAggregateRecords(recordsByMuAndAg.get(msu1).get(ag2), msu1ag2Day);
        rollupTestUtils.compareAggregateRecords(recordsByMuAndAg.get(msu2).get(ag1), msu2ag1Day);
        rollupTestUtils.compareAggregateRecords(recordsByMuAndAg.get(msu2).get(ag2), msu2ag2Day);
    }

    /**
     * daily -> monthly rollup.
     */
    @Test
    public void testV1toV2MigrateThisMonthsDaysToMonthly() {
        final LocalDateTime lastMonth = LocalDateTime.of(todayH1.getYear(), todayH1.getMonth().minus(1), 1, 0, 0);
        final LocalDateTime thisMonthDay1 = todayH1.truncatedTo(ChronoUnit.DAYS).minusDays(2);
        final LocalDateTime thisMonthDay2 = thisMonthDay1.plusDays(1);
        final LocalDateTime thisMonth = LocalDateTime.of(todayH1.getYear(), todayH1.getMonth(), 1, 0, 0);

        final List<ActionSnapshotDayRecord> snapshotRecords = Arrays.asList(
                new ActionSnapshotDayRecord(lastMonth, 10, lastMonth),
                new ActionSnapshotDayRecord(thisMonthDay1, 5, thisMonthDay1),
                new ActionSnapshotDayRecord(thisMonthDay2, 5, thisMonthDay2)
        );
        dsl.batchInsert(snapshotRecords).execute();

        final List<ActionStatsByDayRecord> dayRecords = Arrays.asList(
                // Insert a record for last month so we can make sure it doesn't get rolled up
                // by the migration.
                dayRecord(lastMonth, msu1, ag1, 1),
                dayRecord(thisMonthDay1, msu1, ag1, 1),
                dayRecord(thisMonthDay2, msu1, ag1, 3),
                dayRecord(thisMonthDay1, msu1, ag2, 2),
                dayRecord(thisMonthDay2, msu1, ag2, 4),

                dayRecord(thisMonthDay1, msu2, ag1, 3),
                dayRecord(thisMonthDay2, msu2, ag1, 5),
                dayRecord(thisMonthDay1, msu2, ag2, 4),
                dayRecord(thisMonthDay2, msu2, ag2, 6)
        );
        final ActionStatsByMonthRecord msu1ag1Day = monthRecord(thisMonth, msu1, ag1, 1, 2, 3);
        msu1ag1Day.setPriorActionCount(1);
        msu1ag1Day.setNewActionCount(1 + 3);
        final ActionStatsByMonthRecord msu1ag2Day = monthRecord(thisMonth, msu1, ag2, 2, 3, 4);
        msu1ag2Day.setPriorActionCount(2);
        msu1ag2Day.setNewActionCount(2 + 4);
        final ActionStatsByMonthRecord msu2ag1Day = monthRecord(thisMonth, msu2, ag1, 3, 4, 5);
        msu2ag1Day.setPriorActionCount(3);
        msu2ag1Day.setNewActionCount(3 + 5);
        final ActionStatsByMonthRecord msu2ag2Day = monthRecord(thisMonth, msu2, ag2, 4, 5, 6);
        msu2ag2Day.setPriorActionCount(4);
        msu2ag2Day.setNewActionCount(4 + 6);

        dsl.batchInsert(dayRecords).execute();

        ActionRollupAlgorithmMigrator migration = new ActionRollupAlgorithmMigrator(dsl, clock, RollupAlgorithmVersion.V2);
        migration.doMigration();

        final List<ActionSnapshotMonthRecord> monthRecords = dsl.selectFrom(Tables.ACTION_SNAPSHOT_MONTH).fetch();
        assertThat(monthRecords.size(), is(1));
        assertThat(monthRecords.get(0).getMonthTime(), is(thisMonth));
        assertThat(monthRecords.get(0).getNumActionSnapshots(), is(10));

        // Only expect one record per msu/ag for this test.
        final Map<Integer, Map<Integer, ActionStatsByMonthRecord>> recordsByMuAndAg = new HashMap<>();
        dsl.selectFrom(Tables.ACTION_STATS_BY_MONTH).fetch()
                .forEach(record -> {
                    assertThat(record.getMonthTime(), is(thisMonth));
                    recordsByMuAndAg.computeIfAbsent(record.getMgmtUnitSubgroupId(), k -> new HashMap<>())
                            .put(record.getActionGroupId(), record);
                });

        rollupTestUtils.compareAggregateRecords(recordsByMuAndAg.get(msu1).get(ag1), msu1ag1Day);
        rollupTestUtils.compareAggregateRecords(recordsByMuAndAg.get(msu1).get(ag2), msu1ag2Day);
        rollupTestUtils.compareAggregateRecords(recordsByMuAndAg.get(msu2).get(ag1), msu2ag1Day);
        rollupTestUtils.compareAggregateRecords(recordsByMuAndAg.get(msu2).get(ag2), msu2ag2Day);
    }

    /**
     * Test that a daily record for today prevents any additional migrations, because we infer
     * that we are already on V2 of the algorithm.
     */
    @Test
    public void testV2toV2MigrationNoop() {
        final LocalDateTime yesterday = todayH1.truncatedTo(ChronoUnit.DAYS).minusDays(1);
        final List<ActionSnapshotDayRecord> snapshotRecords = Arrays.asList(
                // Record from yesterday. If we were doing a V1 to V2 migration this record
                // would get rolled up to monthly.
                new ActionSnapshotDayRecord(yesterday, 10, todayH1),
                // Snapshot record for today. This should prevent any rollup from happening.
                new ActionSnapshotDayRecord(todayH1.truncatedTo(ChronoUnit.DAYS), 10, todayH1));

        dsl.batchInsert(snapshotRecords).execute();

        final List<ActionStatsByDayRecord> dayRecords = Arrays.asList(
                // Stat record from yesterday.
                dayRecord(yesterday, msu1, ag1, 1));
        dsl.batchInsert(dayRecords).execute();

        ActionRollupAlgorithmMigrator migration = new ActionRollupAlgorithmMigrator(dsl, clock, RollupAlgorithmVersion.V2);
        migration.doMigration();

        // Nothing should have gotten rolled up because there is an existing record for the month.
        final List<ActionSnapshotMonthRecord> monthRecords = dsl.selectFrom(Tables.ACTION_SNAPSHOT_MONTH).fetch();
        assertThat(monthRecords.size(), is(0));

        final List<ActionStatsByMonthRecord> statRecords = dsl.selectFrom(Tables.ACTION_STATS_BY_MONTH).fetch();
        assertThat(statRecords.size(), is(0));

    }

    /**
     * Test that downgrading from V2 to V1 of the rollup algorithm drops the days and months
     * rolled up data.
     */
    @Test
    public void testV2toV1MigrationDropsData() {
        LocalDateTime today = todayH1.truncatedTo(ChronoUnit.DAYS);
        LocalDateTime thisMonth = LocalDateTime.of(today.getYear(), today.getMonth(), 1, 0, 0);

        dsl.insertInto(Tables.ACTION_SNAPSHOT_DAY)
            .set(new ActionSnapshotDayRecord(today, 10, todayH1))
            .execute();

        dsl.insertInto(Tables.ACTION_STATS_BY_DAY)
            .set(dayRecord(today, msu1, ag1, 1))
            .execute();

        dsl.insertInto(Tables.ACTION_SNAPSHOT_MONTH)
                .set(new ActionSnapshotMonthRecord(thisMonth, 10, todayH1))
                .execute();

        dsl.insertInto(Tables.ACTION_STATS_BY_MONTH)
                .set(monthRecord(thisMonth, msu1, ag1, 1, 1, 1))
                .execute();

        // We want V1, which should clear out the daily records for today and the monthly records for this month
        ActionRollupAlgorithmMigrator migration = new ActionRollupAlgorithmMigrator(dsl, clock, RollupAlgorithmVersion.V1);
        migration.doMigration();

        assertThat(dsl.fetchCount(Tables.ACTION_SNAPSHOT_DAY), is(0));
        assertThat(dsl.fetchCount(Tables.ACTION_SNAPSHOT_MONTH), is(0));
        assertThat(dsl.fetchCount(Tables.ACTION_STATS_BY_DAY), is(0));
        assertThat(dsl.fetchCount(Tables.ACTION_STATS_BY_MONTH), is(0));
    }


    private ActionStatsByHourRecord hourRecord(LocalDateTime time, int msu, int ag, double value) {
        int intVal = (int)value;
        BigDecimal doubleVal = BigDecimal.valueOf(value);
        return new ActionStatsByHourRecord(time, msu, ag,
                doubleVal, intVal, intVal,
                intVal, intVal, doubleVal,
                intVal, intVal, doubleVal,
                doubleVal, doubleVal, doubleVal, doubleVal, doubleVal);
    }

    private ActionStatsByDayRecord dayRecord(LocalDateTime time, int msu, int ag, double value) {
        ActionStatsByDayRecord rec =  dayRecord(time, msu, ag, (int)value, value, (int)value);
        rec.setNewActionCount((int)value);
        rec.setPriorActionCount((int)value);
        return rec;
    }

    private ActionStatsByDayRecord dayRecord(LocalDateTime day, int msu, int ag,
            int minValue, double avgValue, int maxValue) {
        ActionStatsByDayRecord rec = new ActionStatsByDayRecord();
        rec.setDayTime(day);
        rec.setActionGroupId(ag);
        rec.setMgmtUnitSubgroupId(msu);


        rec.setMinEntityCount(minValue);
        rec.setAvgEntityCount(BigDecimal.valueOf(avgValue));
        rec.setMaxEntityCount(maxValue);

        rec.setMinActionCount(minValue);
        rec.setAvgActionCount(BigDecimal.valueOf(avgValue));
        rec.setMaxActionCount(maxValue);

        rec.setMinInvestment(BigDecimal.valueOf(minValue));
        rec.setAvgInvestment(BigDecimal.valueOf(avgValue));
        rec.setMaxInvestment(BigDecimal.valueOf(maxValue));

        rec.setMinSavings(BigDecimal.valueOf(minValue));
        rec.setAvgSavings(BigDecimal.valueOf(avgValue));
        rec.setMaxSavings(BigDecimal.valueOf(maxValue));
        return rec;
    }

    private ActionStatsByMonthRecord monthRecord(LocalDateTime month, int msu, int ag,
            int minValue, double avgValue, int maxValue) {
        ActionStatsByMonthRecord rec = new ActionStatsByMonthRecord();
        rec.setMonthTime(month);
        rec.setActionGroupId(ag);
        rec.setMgmtUnitSubgroupId(msu);


        rec.setMinEntityCount(minValue);
        rec.setAvgEntityCount(BigDecimal.valueOf(avgValue));
        rec.setMaxEntityCount(maxValue);

        rec.setMinActionCount(minValue);
        rec.setAvgActionCount(BigDecimal.valueOf(avgValue));
        rec.setMaxActionCount(maxValue);

        rec.setMinInvestment(BigDecimal.valueOf(minValue));
        rec.setAvgInvestment(BigDecimal.valueOf(avgValue));
        rec.setMaxInvestment(BigDecimal.valueOf(maxValue));

        rec.setMinSavings(BigDecimal.valueOf(minValue));
        rec.setAvgSavings(BigDecimal.valueOf(avgValue));
        rec.setMaxSavings(BigDecimal.valueOf(maxValue));
        return rec;
    }
}

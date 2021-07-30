package com.vmturbo.action.orchestrator.stats.rollup.v2;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.db.Action;
import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotDayRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotHourRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotMonthRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByDayRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByHourRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByMonthRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat;
import com.vmturbo.action.orchestrator.stats.rollup.ImmutableRolledUpActionGroupStat;
import com.vmturbo.action.orchestrator.stats.rollup.RolledUpStatCalculator;
import com.vmturbo.action.orchestrator.stats.rollup.RollupTestUtils;
import com.vmturbo.action.orchestrator.stats.rollup.export.RollupExporter;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Unit tests for {@link HourActionStatRollup}.
 */
public class HourActionStatRollupTest {
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

    private RollupExporter rollupExporter = mock(RollupExporter.class);

    // We use a real one instead of mocking for simplicity.
    private RolledUpStatCalculator statCalculator = mock(RolledUpStatCalculator.class);

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    @Captor
    private ArgumentCaptor<List<ActionStatsLatestRecord>> recordsCaptor;

    private final int msu1 = 1;

    private final int ag1 = 3;

    private final LocalDateTime t1 = LocalDateTime.of(2018, Month.SEPTEMBER, 30, 1, 10);

    private final LocalDateTime t1Month = LocalDateTime.of(2018, Month.SEPTEMBER, 1, 0, 0);

    private final LocalDateTime t2 = LocalDateTime.of(2018, Month.SEPTEMBER, 30, 1, 20);

    private final ActionStatsLatestRecord msu1ag1t1 = new ActionStatsLatestRecord(t1, msu1, ag1, 5, 2, 3, BigDecimal.valueOf(4), BigDecimal.valueOf(5));

    private final ActionStatsLatestRecord msu1ag1t2 = new ActionStatsLatestRecord(t2, msu1, ag1, 7, 2, 5, BigDecimal.valueOf(6), BigDecimal.valueOf(7));

    private final RolledUpActionGroupStat groupStat = ImmutableRolledUpActionGroupStat.builder()
            .avgActionCount(1)
            .minActionCount(0)
            .maxActionCount(2)
            .avgEntityCount(4)
            .minEntityCount(3)
            .maxEntityCount(5)
            .avgInvestment(7)
            .minInvestment(6)
            .maxInvestment(8)
            .avgSavings(10)
            .minSavings(9)
            .maxSavings(11)
            .priorActionCount(18)
            .newActionCount(17)
            .build();

    RolledUpActionGroupStat groupStat2 = ImmutableRolledUpActionGroupStat.builder()
            .avgActionCount(3)
            .minActionCount(2)
            .maxActionCount(4)
            .avgEntityCount(6)
            .minEntityCount(5)
            .maxEntityCount(7)
            .avgInvestment(9)
            .minInvestment(8)
            .maxInvestment(10)
            .avgSavings(12)
            .minSavings(11)
            .maxSavings(13)
            .priorActionCount(20)
            .newActionCount(19)
            .build();

    /**
     * Common setup before every test.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        rollupTestUtils = new RollupTestUtils(dsl);
    }

    /**
     * Test that rolling up on a new hour adds data to hourly, daily, and monthly tables.
     */
    @Test
    public void testRollupInserts() {
        // ARRANGE
        insertRecords(Arrays.asList(msu1ag1t1, msu1ag1t2));

        // The mocked output of the rollup calculator.
        when(statCalculator.rollupLatestRecords(anyInt(), any()))
                .thenReturn(Optional.of(groupStat));

        // ACT
        HourActionStatRollup rollup = new HourActionStatRollup(t1.truncatedTo(ChronoUnit.HOURS), 2, rollupExporter, statCalculator, dsl, clock);
        rollup.run();

        // ASSERT
        // Verify that the inserted latest records got passed to the rollup stats calculator
        verify(statCalculator).rollupLatestRecords(eq(2), recordsCaptor.capture());
        Map<LocalDateTime, ActionStatsLatestRecord> recByTime = recordsCaptor.getValue().stream()
            .collect(Collectors.toMap(ActionStatsLatestRecord::getActionSnapshotTime, Function.identity()));
        rollupTestUtils.compareRecords(recByTime.get(t1), msu1ag1t1);
        rollupTestUtils.compareRecords(recByTime.get(t2), msu1ag1t2);

        // Verify that the hourly, daily, and monthly tables have the right data.
        ActionSnapshotHourRecord hourSnapshot = dsl.selectFrom(Tables.ACTION_SNAPSHOT_HOUR)
            .where(Tables.ACTION_SNAPSHOT_HOUR.HOUR_TIME.eq(t1.truncatedTo(ChronoUnit.HOURS)))
            .fetchOne();
        assertThat(hourSnapshot.getNumActionSnapshots(), is(2));

        ActionStatsByHourRecord hourStats = dsl.selectFrom(Tables.ACTION_STATS_BY_HOUR)
            .where(Tables.ACTION_STATS_BY_HOUR.HOUR_TIME.eq(t1.truncatedTo(ChronoUnit.HOURS)))
            .fetchOne();
        compareRecord(hourStats, ag1, msu1, groupStat);

        ActionSnapshotDayRecord daySnapshot = dsl.selectFrom(Tables.ACTION_SNAPSHOT_DAY)
                .where(Tables.ACTION_SNAPSHOT_DAY.DAY_TIME.eq(t1.truncatedTo(ChronoUnit.DAYS)))
                .fetchOne();
        assertThat(daySnapshot.getNumActionSnapshots(), is(2));

        ActionStatsByDayRecord dayStats = dsl.selectFrom(Tables.ACTION_STATS_BY_DAY)
                .where(Tables.ACTION_STATS_BY_DAY.DAY_TIME.eq(t1.truncatedTo(ChronoUnit.DAYS)))
                .fetchOne();
        compareRecord(dayStats, ag1, msu1, groupStat);

        ActionSnapshotMonthRecord monthSnapshot = dsl.selectFrom(Tables.ACTION_SNAPSHOT_MONTH)
                .where(Tables.ACTION_SNAPSHOT_MONTH.MONTH_TIME.eq(t1Month))
                .fetchOne();
        assertThat(monthSnapshot.getNumActionSnapshots(), is(2));

        ActionStatsByMonthRecord monthStats = dsl.selectFrom(Tables.ACTION_STATS_BY_MONTH)
                .where(Tables.ACTION_STATS_BY_MONTH.MONTH_TIME.eq(t1Month))
                .fetchOne();
        compareRecord(monthStats, ag1, msu1, groupStat);
    }

    /**
     * Test that rollups increment the existing counts in the daily and monthly tables.
     */
    @Test
    public void testRollupDailyAndMonthlyUpdates() {
        // ARRANGE
        // Insert and roll up the first hour of data. Mock the stat calculator to return groupStat1
        // when aggregating the two records.
        final int hr1Snapshots = 2;
        final int hr2Snapshots = 1;
        final int totalSnapshots = hr1Snapshots + hr2Snapshots;
        when(statCalculator.rollupLatestRecords(anyInt(), any()))
                .thenReturn(Optional.of(groupStat));
        insertRecords(Arrays.asList(msu1ag1t1, msu1ag1t2));
        new HourActionStatRollup(t1.truncatedTo(ChronoUnit.HOURS), hr1Snapshots, rollupExporter, statCalculator, dsl, clock)
            .run();

        // Insert and roll up the second hour of data. Mock the stat calculator to return groupStat2
        // when aggregating the two records.
        LocalDateTime hour2t1 = t1.plusHours(1);

        // Just one snapshot for this hour, to add some asymmetry to the weighted average calculation.
        msu1ag1t1.setActionSnapshotTime(hour2t1);
        when(statCalculator.rollupLatestRecords(anyInt(), any()))
                .thenReturn(Optional.of(groupStat2));
        insertRecords(Arrays.asList(msu1ag1t1));

        // Roll up the second hour.
        // This should update the daily and monthly tables with the aggregated data.
        // Only 1 snapshot for this hour.
        new HourActionStatRollup(hour2t1.truncatedTo(ChronoUnit.HOURS), hr2Snapshots, rollupExporter, statCalculator, dsl, clock)
                .run();

        // This is the expected rollup when combining groupStat with 2 action snapshots and
        // groupStat2 with 1 action snapshot.
        final RolledUpActionGroupStat expectedAvg = ImmutableRolledUpActionGroupStat.builder()
                .avgActionCount((groupStat.avgActionCount() * hr1Snapshots + groupStat2.avgActionCount()) / totalSnapshots)
                .minActionCount(Math.min(groupStat.minActionCount(), groupStat2.minActionCount()))
                .maxActionCount(Math.max(groupStat.maxActionCount(), groupStat2.maxActionCount()))
                .avgEntityCount((groupStat.avgEntityCount() * hr1Snapshots + groupStat2.avgEntityCount()) / totalSnapshots)
                .minEntityCount(Math.min(groupStat.minEntityCount(), groupStat2.minEntityCount()))
                .maxEntityCount(Math.max(groupStat.maxEntityCount(), groupStat2.maxEntityCount()))
                .avgInvestment((groupStat.avgInvestment() * hr1Snapshots + groupStat2.avgInvestment()) / totalSnapshots)
                .minInvestment(Math.min(groupStat.minInvestment(), groupStat2.minInvestment()))
                .maxInvestment(Math.max(groupStat.maxInvestment(), groupStat2.maxInvestment()))
                .avgSavings((groupStat.avgSavings() * hr1Snapshots + groupStat2.avgSavings()) / totalSnapshots)
                .minSavings(Math.min(groupStat.minSavings(), groupStat2.minSavings()))
                .maxSavings(Math.max(groupStat.maxSavings(), groupStat2.maxSavings()))
                .priorActionCount(groupStat.priorActionCount())
                .newActionCount(groupStat.newActionCount() + groupStat2.newActionCount())
                .build();

        // Verify that the hourly, daily, and monthly tables have the right data.
        // The hourly stats are just groupStat2
        ActionSnapshotHourRecord hourSnapshot = dsl.selectFrom(Tables.ACTION_SNAPSHOT_HOUR)
                .where(Tables.ACTION_SNAPSHOT_HOUR.HOUR_TIME.eq(hour2t1.truncatedTo(ChronoUnit.HOURS)))
                .fetchOne();
        assertThat(hourSnapshot.getNumActionSnapshots(), is(hr2Snapshots));

        ActionStatsByHourRecord hourStats = dsl.selectFrom(Tables.ACTION_STATS_BY_HOUR)
                .where(Tables.ACTION_STATS_BY_HOUR.HOUR_TIME.eq(hour2t1.truncatedTo(ChronoUnit.HOURS)))
                .fetchOne();
        compareRecord(hourStats, ag1, msu1, groupStat2);

        // The daily and monthly stats are the averaged record
        ActionSnapshotDayRecord daySnapshot = dsl.selectFrom(Tables.ACTION_SNAPSHOT_DAY)
                .where(Tables.ACTION_SNAPSHOT_DAY.DAY_TIME.eq(t1.truncatedTo(ChronoUnit.DAYS)))
                .fetchOne();
        // 2 from the first hour, 1 from the second hour
        assertThat(daySnapshot.getNumActionSnapshots(), is(totalSnapshots));

        ActionStatsByDayRecord dayStats = dsl.selectFrom(Tables.ACTION_STATS_BY_DAY)
                .where(Tables.ACTION_STATS_BY_DAY.DAY_TIME.eq(t1.truncatedTo(ChronoUnit.DAYS)))
                .fetchOne();
        compareRecord(dayStats, ag1, msu1, expectedAvg);

        ActionSnapshotMonthRecord monthSnapshot = dsl.selectFrom(Tables.ACTION_SNAPSHOT_MONTH)
                .where(Tables.ACTION_SNAPSHOT_MONTH.MONTH_TIME.eq(t1Month))
                .fetchOne();
        // 2 from the first hour, 1 from the second hour
        assertThat(monthSnapshot.getNumActionSnapshots(), is(totalSnapshots));

        ActionStatsByMonthRecord monthStats = dsl.selectFrom(Tables.ACTION_STATS_BY_MONTH)
                .where(Tables.ACTION_STATS_BY_MONTH.MONTH_TIME.eq(t1Month))
                .fetchOne();
        compareRecord(monthStats, ag1, msu1, expectedAvg);
    }

    private void compareRecord(Record record, int expectedAgId, int expectedMsuId, RolledUpActionGroupStat groupStat) {
        assertThat(record.get("action_group_id", Integer.class), is(expectedAgId));
        assertThat(record.get("mgmt_unit_subgroup_id", Integer.class), is(expectedMsuId));
        compareValues(record, "savings", groupStat.minSavings(), groupStat.avgSavings(), groupStat.maxSavings());
        compareValues(record, "investment", groupStat.minInvestment(), groupStat.avgInvestment(), groupStat.maxInvestment());
        compareValues(record, "action_count", groupStat.minActionCount(), groupStat.avgActionCount(), groupStat.maxActionCount());
        compareValues(record, "entity_count", groupStat.minEntityCount(), groupStat.avgEntityCount(), groupStat.maxEntityCount());

    }

    private void compareValues(Record record, String field, double expectedMin, double expectedAvg, double expectedMax) {
        assertThat(record.get("avg_" + field, BigDecimal.class).doubleValue(), closeTo(expectedAvg, 0.0001));
        if (field.equals("investment") || field.equals("savings")) {
            assertThat(record.get("min_" + field, BigDecimal.class).doubleValue(), closeTo(expectedMin, 0.0001));
            assertThat(record.get("max_" + field, BigDecimal.class).doubleValue(), closeTo(expectedMax, 0.0001));
        } else {
            assertThat(record.get("min_" + field, Integer.class), is((int)expectedMin));
            assertThat(record.get("max_" + field, Integer.class), is((int)expectedMax));
        }
    }

    private void insertRecords(List<ActionStatsLatestRecord> records) {
        records.forEach(r -> {
            rollupTestUtils.insertLatestSnapshotOnly(r.getActionSnapshotTime());
            rollupTestUtils.insertMgmtUnit(r.getMgmtUnitSubgroupId());
            rollupTestUtils.insertActionGroup(r.getActionGroupId());
        });
        dsl.batchInsert(records).execute();
    }
}

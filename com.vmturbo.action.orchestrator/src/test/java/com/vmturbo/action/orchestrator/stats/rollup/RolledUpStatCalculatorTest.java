package com.vmturbo.action.orchestrator.stats.rollup;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;

import org.junit.Test;

import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByDayRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByHourRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat;
import com.vmturbo.action.orchestrator.stats.rollup.BaseActionStatTableReader.StatWithSnapshotCnt;
import com.vmturbo.action.orchestrator.stats.rollup.RolledUpStatCalculator.AverageSummary;

public class RolledUpStatCalculatorTest {

    private RolledUpStatCalculator calculator = new RolledUpStatCalculator();

    private final int ACTION_GROUP_ID = 1;
    private final int MGMT_SUBUNIT_ID = 1;

    @Test
    public void testRollupLatest() {
        final StatWithSnapshotCnt<ActionStatsLatestRecord> latestRecord1 =
            RollupTestUtils.statRecordWithActionCount(1, ActionStatsLatestRecord.class);
        latestRecord1.record().setTotalEntityCount(10);
        // The first of the "latest" snapshots has 10 total actions, but only 5 of them are
        // new (e.g. 5 are inherited from the previous record, which is not in this hour).
        latestRecord1.record().setTotalActionCount(10);
        latestRecord1.record().setNewActionCount(5);
        latestRecord1.record().setTotalSavings(BigDecimal.valueOf(7));
        latestRecord1.record().setTotalInvestment(BigDecimal.valueOf(6));
        latestRecord1.record().setActionGroupId(ACTION_GROUP_ID);
        latestRecord1.record().setMgmtUnitSubgroupId(MGMT_SUBUNIT_ID);
        latestRecord1.record().setActionSnapshotTime(LocalDateTime.MAX);

        final StatWithSnapshotCnt<ActionStatsLatestRecord> latestRecord2 =
            RollupTestUtils.statRecordWithActionCount(1, ActionStatsLatestRecord.class);
        latestRecord2.record().setTotalEntityCount(8);
        latestRecord2.record().setTotalActionCount(4);
        latestRecord2.record().setNewActionCount(1);
        latestRecord2.record().setTotalSavings(BigDecimal.valueOf(8));
        latestRecord2.record().setTotalInvestment(BigDecimal.valueOf(6));
        latestRecord2.record().setActionGroupId(ACTION_GROUP_ID);
        latestRecord2.record().setMgmtUnitSubgroupId(MGMT_SUBUNIT_ID);
        latestRecord2.record().setActionSnapshotTime(LocalDateTime.MAX);

        // Suppose there is also an empty snapshot that doesn't apply to this action group.
        final RolledUpActionGroupStat rolledUp = calculator.rollupLatestRecords(3,
            Arrays.asList(latestRecord1, latestRecord2)).orElseThrow(RuntimeException::new);
        assertThat(rolledUp.avgEntityCount(), closeTo((10.0 + 8) / 3, 0.0001));
        assertThat(rolledUp.avgActionCount(), closeTo((10.0 + 1) / 3, 0.0001));
        assertThat(rolledUp.priorActionCount(), is(5));
        assertThat(rolledUp.newActionCount(), is(5 + 1));

        assertThat(rolledUp.avgSavings(), closeTo((7.0 + 8) / 3, 0.0001));
        assertThat(rolledUp.avgInvestment(), closeTo((6.0 + 6) / 3, 0.0001));

        assertThat(rolledUp.minEntityCount(), is(8));
        assertThat(rolledUp.minActionCount(), is(4));
        assertThat(rolledUp.minSavings(), closeTo(7.0, 0.0001));
        assertThat(rolledUp.minInvestment(), closeTo(6.0, 0.0001));

        assertThat(rolledUp.maxEntityCount(), is(10));
        // The first snapshot had 10 actions.
        assertThat(rolledUp.maxActionCount(), is(10));
        assertThat(rolledUp.maxSavings(), closeTo(8.0, 0.0001));
        assertThat(rolledUp.maxInvestment(), closeTo(6.0, 0.0001));
    }

    @Test
    public void testRollupLatestIgnoreDifferentMgmtGroupId() {
        final StatWithSnapshotCnt<ActionStatsLatestRecord> latestRecord1 =
            RollupTestUtils.statRecordWithActionCount(1, ActionStatsLatestRecord.class);
        latestRecord1.record().setTotalEntityCount(10);
        latestRecord1.record().setTotalActionCount(5);
        latestRecord1.record().setTotalSavings(BigDecimal.valueOf(7));
        latestRecord1.record().setTotalInvestment(BigDecimal.valueOf(6));
        latestRecord1.record().setActionGroupId(ACTION_GROUP_ID);
        latestRecord1.record().setMgmtUnitSubgroupId(MGMT_SUBUNIT_ID);
        latestRecord1.record().setActionSnapshotTime(LocalDateTime.MAX);

        final StatWithSnapshotCnt<ActionStatsLatestRecord> latestRecord2 =
            RollupTestUtils.copyRecord(latestRecord1);
        // Different management sub-unit
        latestRecord2.record().setMgmtUnitSubgroupId(MGMT_SUBUNIT_ID + 1);

        assertFalse(calculator.rollupLatestRecords(2,
            Arrays.asList(latestRecord1, latestRecord2)).isPresent());
    }

    @Test
    public void testRollupLatestIgnoreDifferentActionGroupId() {
        final StatWithSnapshotCnt<ActionStatsLatestRecord> latestRecord1 =
            RollupTestUtils.statRecordWithActionCount(1, ActionStatsLatestRecord.class);
        latestRecord1.record().setTotalEntityCount(10);
        latestRecord1.record().setTotalActionCount(5);
        latestRecord1.record().setTotalSavings(BigDecimal.valueOf(7));
        latestRecord1.record().setTotalInvestment(BigDecimal.valueOf(6));
        latestRecord1.record().setActionGroupId(ACTION_GROUP_ID);
        latestRecord1.record().setMgmtUnitSubgroupId(MGMT_SUBUNIT_ID);
        latestRecord1.record().setActionSnapshotTime(LocalDateTime.MAX);

        final StatWithSnapshotCnt<ActionStatsLatestRecord> latestRecord2 =
            RollupTestUtils.copyRecord(latestRecord1);
        // Different action group ID.
        latestRecord2.record().setActionGroupId(ACTION_GROUP_ID + 1);

        assertFalse(calculator.rollupLatestRecords(2,
            Arrays.asList(latestRecord1, latestRecord2)).isPresent());
    }

    @Test
    public void testRollupHourly() {
        final StatWithSnapshotCnt<ActionStatsByHourRecord> hourRecord1 =
            RollupTestUtils.statRecordWithActionCount(2, ActionStatsByHourRecord.class);
        hourRecord1.record().setActionGroupId(ACTION_GROUP_ID);
        hourRecord1.record().setMgmtUnitSubgroupId(MGMT_SUBUNIT_ID);
        hourRecord1.record().setHourTime(LocalDateTime.MAX);

        hourRecord1.record().setAvgActionCount(BigDecimal.valueOf(5));
        hourRecord1.record().setAvgEntityCount(BigDecimal.valueOf(6));
        hourRecord1.record().setAvgInvestment(BigDecimal.valueOf(7));
        hourRecord1.record().setAvgSavings(BigDecimal.valueOf(8));

        hourRecord1.record().setMinActionCount(3);
        hourRecord1.record().setMinEntityCount(4);
        hourRecord1.record().setMinInvestment(BigDecimal.valueOf(5));
        hourRecord1.record().setMinSavings(BigDecimal.valueOf(6));

        hourRecord1.record().setPriorActionCount(12);
        hourRecord1.record().setNewActionCount(5);
        hourRecord1.record().setMaxActionCount(7);
        hourRecord1.record().setMaxEntityCount(8);
        hourRecord1.record().setMaxInvestment(BigDecimal.valueOf(9));
        hourRecord1.record().setMaxSavings(BigDecimal.valueOf(10));

        final StatWithSnapshotCnt<ActionStatsByHourRecord> hourRecord2 =
            RollupTestUtils.statRecordWithActionCount(2, ActionStatsByHourRecord.class);
        hourRecord2.record().setActionGroupId(ACTION_GROUP_ID);
        hourRecord2.record().setMgmtUnitSubgroupId(MGMT_SUBUNIT_ID);
        hourRecord2.record().setHourTime(LocalDateTime.MAX);

        hourRecord2.record().setAvgActionCount(BigDecimal.valueOf(10));
        hourRecord2.record().setAvgEntityCount(BigDecimal.valueOf(12));
        hourRecord2.record().setAvgInvestment(BigDecimal.valueOf(14));
        hourRecord2.record().setAvgSavings(BigDecimal.valueOf(16));

        hourRecord2.record().setMinActionCount(6);
        hourRecord2.record().setMinEntityCount(8);
        hourRecord2.record().setMinInvestment(BigDecimal.valueOf(10));
        hourRecord2.record().setMinSavings(BigDecimal.valueOf(12));

        hourRecord2.record().setPriorActionCount(7);
        hourRecord2.record().setNewActionCount(2);
        hourRecord2.record().setMaxActionCount(14);
        hourRecord2.record().setMaxEntityCount(16);
        hourRecord2.record().setMaxInvestment(BigDecimal.valueOf(18));
        hourRecord2.record().setMaxSavings(BigDecimal.valueOf(20));

        // Suppose there is also an empty hour consisting of four snapshots,
        // so there are 8 rolled up action plans in total - 4 with stats, and 4 without.
        final RolledUpActionGroupStat groupStat = calculator.rollupHourRecords(2 + 2 + 4,
            Arrays.asList(hourRecord1, hourRecord2)).orElseThrow(RuntimeException::new);

        // Divisor is 4, because each record has 2 snapshots and there are 8 total snapshots in
        // range, so each record contributes "1/4" of the value to the total average.
        assertThat(groupStat.avgActionCount(), closeTo((5.0 + 10) / 4, 0.0001));
        assertThat(groupStat.avgEntityCount(), closeTo((6.0 + 12) / 4, 0.0001));
        assertThat(groupStat.avgInvestment(), closeTo((7.0 + 14) / 4, 0.0001));
        assertThat(groupStat.avgSavings(), closeTo((8.0 + 16) / 4, 0.00001));

        assertThat(groupStat.minActionCount(), is(3));
        assertThat(groupStat.minEntityCount(), is(4));
        assertThat(groupStat.minInvestment(), closeTo(5, 0.00001));
        assertThat(groupStat.minSavings(), closeTo(6, 0.00001));

        assertThat(groupStat.priorActionCount(), is(12));
        assertThat(groupStat.newActionCount(), is(7));
        assertThat(groupStat.maxActionCount(), is(14));
        assertThat(groupStat.maxEntityCount(), is(16));
        assertThat(groupStat.maxInvestment(), closeTo(18, 0.00001));
        assertThat(groupStat.maxSavings(), closeTo(20, 0.00001));
    }

    @Test
    public void testRollupHourlyIgnoreDifferentMgmtGroupId() {
        final StatWithSnapshotCnt<ActionStatsByHourRecord> hourRecord1 =
            RollupTestUtils.statRecordWithActionCount(2, ActionStatsByHourRecord.class);
        hourRecord1.record().setActionGroupId(ACTION_GROUP_ID);
        hourRecord1.record().setMgmtUnitSubgroupId(MGMT_SUBUNIT_ID);
        hourRecord1.record().setHourTime(LocalDateTime.MAX);

        final StatWithSnapshotCnt<ActionStatsByHourRecord> hourRecord2 =
            RollupTestUtils.copyRecord(hourRecord1);
        // Different management sub-unit
        hourRecord2.record().setMgmtUnitSubgroupId(MGMT_SUBUNIT_ID + 1);

        assertFalse(calculator.rollupHourRecords(2, Arrays.asList(hourRecord1, hourRecord2))
            .isPresent());
    }

    @Test
    public void testRollupHourlyIgnoreDifferentActionGroupId() {
        final StatWithSnapshotCnt<ActionStatsByHourRecord> hourRecord1 =
            RollupTestUtils.statRecordWithActionCount(2, ActionStatsByHourRecord.class);
        hourRecord1.record().setActionGroupId(ACTION_GROUP_ID);
        hourRecord1.record().setMgmtUnitSubgroupId(MGMT_SUBUNIT_ID);
        hourRecord1.record().setHourTime(LocalDateTime.MAX);

        final StatWithSnapshotCnt<ActionStatsByHourRecord> hourRecord2 =
            RollupTestUtils.copyRecord(hourRecord1);
        // Different action group
        hourRecord2.record().setActionGroupId(ACTION_GROUP_ID + 1);

        assertFalse(calculator.rollupHourRecords(2, Arrays.asList(hourRecord1, hourRecord2))
            .isPresent());
    }

    @Test
    public void testRollupDaily() {
        final StatWithSnapshotCnt<ActionStatsByDayRecord> dayRecord1 =
            RollupTestUtils.statRecordWithActionCount(2, ActionStatsByDayRecord.class);
        dayRecord1.record().setActionGroupId(ACTION_GROUP_ID);
        dayRecord1.record().setMgmtUnitSubgroupId(MGMT_SUBUNIT_ID);
        dayRecord1.record().setDayTime(LocalDateTime.MAX);

        dayRecord1.record().setPriorActionCount(10);
        dayRecord1.record().setNewActionCount(7);
        dayRecord1.record().setAvgActionCount(BigDecimal.valueOf(5));
        dayRecord1.record().setAvgEntityCount(BigDecimal.valueOf(6));
        dayRecord1.record().setAvgInvestment(BigDecimal.valueOf(7));
        dayRecord1.record().setAvgSavings(BigDecimal.valueOf(8));

        dayRecord1.record().setMinActionCount(3);
        dayRecord1.record().setMinEntityCount(4);
        dayRecord1.record().setMinInvestment(BigDecimal.valueOf(5));
        dayRecord1.record().setMinSavings(BigDecimal.valueOf(6));

        dayRecord1.record().setMaxActionCount(7);
        dayRecord1.record().setMaxEntityCount(8);
        dayRecord1.record().setMaxInvestment(BigDecimal.valueOf(9));
        dayRecord1.record().setMaxSavings(BigDecimal.valueOf(10));

        final StatWithSnapshotCnt<ActionStatsByDayRecord> dayRecord2 =
            RollupTestUtils.statRecordWithActionCount(2, ActionStatsByDayRecord.class);
        dayRecord2.record().setActionGroupId(ACTION_GROUP_ID);
        dayRecord2.record().setMgmtUnitSubgroupId(MGMT_SUBUNIT_ID);
        dayRecord2.record().setDayTime(LocalDateTime.MAX);

        dayRecord2.record().setPriorActionCount(10);
        dayRecord2.record().setNewActionCount(5);
        dayRecord2.record().setAvgActionCount(BigDecimal.valueOf(10));
        dayRecord2.record().setAvgEntityCount(BigDecimal.valueOf(12));
        dayRecord2.record().setAvgInvestment(BigDecimal.valueOf(14));
        dayRecord2.record().setAvgSavings(BigDecimal.valueOf(16));

        dayRecord2.record().setMinActionCount(6);
        dayRecord2.record().setMinEntityCount(8);
        dayRecord2.record().setMinInvestment(BigDecimal.valueOf(10));
        dayRecord2.record().setMinSavings(BigDecimal.valueOf(12));

        dayRecord2.record().setMaxActionCount(14);
        dayRecord2.record().setMaxEntityCount(16);
        dayRecord2.record().setMaxInvestment(BigDecimal.valueOf(18));
        dayRecord2.record().setMaxSavings(BigDecimal.valueOf(20));

        // Suppose there is also an empty day consisting of four snapshots,
        // so there are 8 rolled up action plans in total - 4 with stats, and 4 without.
        final RolledUpActionGroupStat groupStat = calculator.rollupDayRecords(2 + 2 + 4,
            Arrays.asList(dayRecord1, dayRecord2)).orElseThrow(RuntimeException::new);

        // Divisor is 4, because each record has 2 snapshots and there are 8 total snapshots in
        // range, so each record contributes "1/4" of the value to the total average.
        assertThat(groupStat.avgActionCount(), closeTo((5.0 + 10) / 4, 0.0001));
        assertThat(groupStat.avgEntityCount(), closeTo((6.0 + 12) / 4, 0.0001));
        assertThat(groupStat.avgInvestment(), closeTo((7.0 + 14) / 4, 0.0001));
        assertThat(groupStat.avgSavings(), closeTo((8.0 + 16) / 4, 0.00001));

        assertThat(groupStat.priorActionCount(), is(dayRecord1.record().getPriorActionCount()));
        assertThat(groupStat.newActionCount(), is(dayRecord1.record().getNewActionCount() + dayRecord2.record().getNewActionCount()));
        assertThat(groupStat.minActionCount(), is(3));
        assertThat(groupStat.minEntityCount(), is(4));
        assertThat(groupStat.minInvestment(), closeTo(5, 0.00001));
        assertThat(groupStat.minSavings(), closeTo(6, 0.00001));

        assertThat(groupStat.maxActionCount(), is(14));
        assertThat(groupStat.maxEntityCount(), is(16));
        assertThat(groupStat.maxInvestment(), closeTo(18, 0.00001));
        assertThat(groupStat.maxSavings(), closeTo(20, 0.00001));
    }

    @Test
    public void testRollupDailyIgnoreDifferentMgmtGroupId() {
        final StatWithSnapshotCnt<ActionStatsByDayRecord> dayRecord1 =
            RollupTestUtils.statRecordWithActionCount(2, ActionStatsByDayRecord.class);
        dayRecord1.record().setActionGroupId(ACTION_GROUP_ID);
        dayRecord1.record().setMgmtUnitSubgroupId(MGMT_SUBUNIT_ID);
        dayRecord1.record().setDayTime(LocalDateTime.MAX);

        final StatWithSnapshotCnt<ActionStatsByDayRecord> dayRecord2 =
            RollupTestUtils.copyRecord(dayRecord1);
        // Different management sub-unit
        dayRecord2.record().setMgmtUnitSubgroupId(MGMT_SUBUNIT_ID + 1);

        assertFalse(calculator.rollupDayRecords(2, Arrays.asList(dayRecord1, dayRecord2))
            .isPresent());
    }

    @Test
    public void testRollupDailyIgnoreDifferentActionGroupId() {
        final StatWithSnapshotCnt<ActionStatsByDayRecord> dayRecord1 =
            RollupTestUtils.statRecordWithActionCount(2, ActionStatsByDayRecord.class);
        dayRecord1.record().setActionGroupId(ACTION_GROUP_ID);
        dayRecord1.record().setMgmtUnitSubgroupId(MGMT_SUBUNIT_ID);
        dayRecord1.record().setDayTime(LocalDateTime.MAX);

        final StatWithSnapshotCnt<ActionStatsByDayRecord> dayRecord2 =
            RollupTestUtils.statRecordWithActionCount(2, ActionStatsByDayRecord.class);
        // Different action group
        dayRecord2.record().setActionGroupId(ACTION_GROUP_ID + 1);

        assertFalse(calculator.rollupDayRecords(2, Arrays.asList(dayRecord1, dayRecord2))
            .isPresent());
    }

    @Test
    public void testCombineAveragesSameWeights() {
        AverageSummary avgSummary1 = ImmutableAverageSummary.builder()
            .numSnapshots(2)
            .avgActionCount(5)
            .avgEntityCount(6)
            .avgInvestment(7)
            .avgSavings(8)
            .build();
        AverageSummary avgSummary2 = ImmutableAverageSummary.builder()
            .numSnapshots(2)
            .avgActionCount(10)
            .avgEntityCount(12)
            .avgInvestment(14)
            .avgSavings(16)
            .build();
        AverageSummary combined = AverageSummary.combine(4, Arrays.asList(avgSummary1, avgSummary2));
        assertThat(combined.numSnapshots(), is(4));
        assertThat(combined.avgEntityCount(), is(9.0));
        assertThat(combined.avgActionCount(), closeTo((5 + 10) / 2.0, 0.0001));
        assertThat(combined.avgInvestment(), closeTo(10.5, 0.0001));
        assertThat(combined.avgSavings(), closeTo(12.0, 0.0001));
    }

    @Test
    public void testCombineAveragesDiffWeights() {
        final AverageSummary avgSummary1 = ImmutableAverageSummary.builder()
            .numSnapshots(2)
            .avgActionCount(5)
            .avgEntityCount(6)
            .avgInvestment(7)
            .avgSavings(8)
            .build();
        final AverageSummary avgSummary2 = ImmutableAverageSummary.builder()
            .numSnapshots(3)
            .avgActionCount(10)
            .avgEntityCount(12)
            .avgInvestment(14)
            .avgSavings(16)
            .build();
        final AverageSummary avgSummary3 = ImmutableAverageSummary.builder()
            .numSnapshots(5)
            .avgActionCount(2)
            .avgEntityCount(2)
            .avgInvestment(4)
            .avgSavings(6)
            .build();
        final AverageSummary combined = AverageSummary.combine(10,
            Arrays.asList(avgSummary1, avgSummary2, avgSummary3));
        // 2 + 3 + 5
        assertThat(combined.numSnapshots(), is(10));
        // avg of 5 over the first 2
        // avg of 10 over the next 3
        // avg of 2 over the next 5
        // total avg: ((5 * 2) + (10 * 3) + (2 * 5)) / (2 + 3 + 5)
        //       ^- total (rough) count over total num snapshots
        //   = 5
        assertThat(combined.avgActionCount(), is(5.0));

        // avg of 6 over the first 2
        // avg of 12 over the next 3
        // avg of 2 over the next 5
        // total avg: ((6 * 2) + (12 * 3) + (2 * 5)) / (2 + 3 + 5)
        //    = 58 / 10 = ~5.8 (rounded up)
        assertThat(combined.avgEntityCount(), closeTo(5.8, 0.0001));

        // avg of 7 over the first 2
        // avg of 14 over the next 3
        // avg of 4 over the next 5
        // total avg: ((7 * 2) + (14 * 3) + (4 * 5)) / (2 + 3 + 5)
        //    = 76 / 10 = ~7.6
        assertThat(combined.avgInvestment(), closeTo(7.6, 0.0001));

        // avg of 8 over the first 2
        // avg of 16 over the next 3
        // avg of 6 over the next 5
        // total avg: ((8 * 2) + (16 * 3) + (6 * 5)) / (2 + 3 + 5)
        //    = 94 / 10 = ~9.4
        assertThat(combined.avgSavings(), closeTo(9.4, 0.0001));
    }

    @Test
    public void testCombineAveragesDiffWeightsAndZeroVals() {
        AverageSummary avgSummary1 = ImmutableAverageSummary.builder()
            // Summary 1 has 2 snapshots
            .numSnapshots(2)
            .avgActionCount(5)
            .avgEntityCount(6)
            .avgInvestment(7)
            .avgSavings(8)
            .build();
        AverageSummary avgSummary2 = ImmutableAverageSummary.builder()
            // Summary 2 has 3 snapshots
            .numSnapshots(3)
            .avgActionCount(10)
            .avgEntityCount(12)
            .avgInvestment(14)
            .avgSavings(16)
            .build();
        // Suppose there are 5 "empty" snapshots, so the total number of snapshots is 10.
        AverageSummary combined = AverageSummary.combine(10, Arrays.asList(avgSummary1, avgSummary2));
        assertThat(combined.numSnapshots(), is(10));

        // avg of 5 over the first 2
        // avg of 10 over the next 3
        // avg of 0 over the 5 empty snapshots
        // total avg: ((5 * 2) + (10 * 3)) / 10 = 40 / 10 = 4
        assertThat(combined.avgActionCount(), closeTo(4.0, 0.0001));

        // avg of 6 over the first 2
        // avg of 12 over the next 3
        // avg of 0 over the 5 empty snapshots
        // total avg: ((6 * 2) + (12 * 3)) / (2 + 3 + 5) = 48 / 10 = ~4.8
        assertThat(combined.avgEntityCount(), closeTo(4.8, 0.0001));

        // avg of 7 over the first 2
        // avg of 14 over the next 3
        // avg of 0 over the 5 empty snapshots
        // total avg: ((7 * 2) + (14 * 3)) / 10 = 56 / 10 = ~5.6
        assertThat(combined.avgInvestment(), closeTo(5.6, 0.0001));

        // avg of 8 over the first 2
        // avg of 16 over the next 3
        // avg of 0 over the 5 empty snapshots
        // total avg: ((8 * 2) + (16 * 3)) / 10 = 64 / 10 = ~6.4
        assertThat(combined.avgSavings(), closeTo(6.4, 0.0001));
    }

}

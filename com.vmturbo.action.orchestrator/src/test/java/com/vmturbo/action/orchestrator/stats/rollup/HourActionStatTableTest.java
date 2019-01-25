package com.vmturbo.action.orchestrator.stats.rollup;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jooq.DSLContext;
import org.junit.Test;

import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotHourRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByHourRecord;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat;
import com.vmturbo.action.orchestrator.stats.rollup.BaseActionStatTableReader.StatWithSnapshotCnt;
import com.vmturbo.action.orchestrator.stats.rollup.HourActionStatTable.HourlyReader;
import com.vmturbo.action.orchestrator.stats.rollup.HourActionStatTable.HourlyWriter;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;

public class HourActionStatTableTest {

    /**
     * We don't actually need the database, because the base reader/writer classes handle
     * database interaction.
     */
    private DSLContext dslContext = mock(DSLContext.class);

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private RolledUpStatCalculator calculator =
        mock(RolledUpStatCalculator.class);

    private HourActionStatTable hourActionStatTable =
        new HourActionStatTable(dslContext, clock, calculator, DayActionStatTable.DAY_TABLE_INFO);

    @Test
    public void testReaderIsPresent() {
        assertTrue(hourActionStatTable.reader() instanceof HourlyReader);
    }

    @Test
    public void testWriterIsPresent() {
        assertTrue(hourActionStatTable.writer() instanceof HourlyWriter);
    }

    @Test
    public void testTrimTime() {
        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.hourlyRetentionHours()).thenReturn(1);
        final LocalDateTime trimmedTime = hourActionStatTable.getTrimTime(retentionPeriods);
        assertThat(trimmedTime, is(LocalDateTime.now(clock).minusHours(1).truncatedTo(ChronoUnit.HOURS)));
    }

    @Test
    public void testReaderSummarize() {
        final HourlyReader reader = (HourlyReader)hourActionStatTable.reader();
        final int ag1Id = 1;
        final int ag2Id = 2;
        final StatWithSnapshotCnt<ActionStatsByHourRecord> ag1Record =
            RollupTestUtils.statRecordWithActionCount(1, ActionStatsByHourRecord.class);
        ag1Record.record().setMaxEntityCount(1);
        final StatWithSnapshotCnt<ActionStatsByHourRecord> ag2Record =
            RollupTestUtils.statRecordWithActionCount(1, ActionStatsByHourRecord.class);
        ag2Record.record().setMaxEntityCount(2);
        final Map<Integer, List<StatWithSnapshotCnt<ActionStatsByHourRecord>>> recordsByActionGroup = new HashMap<>();
        recordsByActionGroup.put(ag1Id, Collections.singletonList(ag1Record));
        recordsByActionGroup.put(ag2Id, Collections.singletonList(ag2Record));

        final RolledUpActionGroupStat ag1Stat = mock(RolledUpActionGroupStat.class);
        final RolledUpActionGroupStat ag2Stat = mock(RolledUpActionGroupStat.class);
        when(calculator.rollupHourRecords(1, Collections.singletonList(ag1Record)))
            .thenReturn(Optional.of(ag1Stat));
        when(calculator.rollupHourRecords(1, Collections.singletonList(ag2Record)))
            .thenReturn(Optional.of(ag2Stat));

        final Map<Integer, RolledUpActionGroupStat> statsByGroupId =
            reader.rollupRecords(1, recordsByActionGroup);
        assertThat(statsByGroupId.keySet(), containsInAnyOrder(ag1Id, ag2Id));
        assertThat(statsByGroupId.get(ag1Id), is(ag1Stat));
        assertThat(statsByGroupId.get(ag2Id), is(ag2Stat));
    }

    @Test
    public void testReaderNumSnapshots() {
        final HourlyReader reader = (HourlyReader) hourActionStatTable.reader();
        final int numSnapshots = 11;
        final ActionSnapshotHourRecord record = new ActionSnapshotHourRecord();
        record.setNumActionSnapshots(numSnapshots);
        assertThat(reader.numSnapshotsInSnapshotRecord(record), is(numSnapshots));
    }

    @Test
    public void testWriterSummaryToRecord() {
        final LocalDateTime time = LocalDateTime.of(2018, Month.SEPTEMBER, 1, 1, 0);
        final HourlyWriter writer = (HourlyWriter)hourActionStatTable.writer();
        final int mgmtSubgroupId = 1;
        final int actionGroupId = 2;
        final ActionStatsByHourRecord record =
            writer.statRecord(mgmtSubgroupId, actionGroupId, time,
                ImmutableRolledUpActionGroupStat.builder()
                    .avgActionCount(3)
                    .avgEntityCount(4)
                    .avgInvestment(5.0)
                    .avgSavings(6.0)
                    .minActionCount(7)
                    .minEntityCount(8)
                    .minInvestment(9.0)
                    .minSavings(10.0)
                    .maxActionCount(11)
                    .maxEntityCount(12)
                    .maxInvestment(13.0)
                    .maxSavings(14.0)
                    .build());
        assertThat(record.getHourTime(), is(time));
        assertThat(record.getActionGroupId(), is(actionGroupId));
        assertThat(record.getMgmtUnitSubgroupId(), is(mgmtSubgroupId));
        assertThat(record.getAvgActionCount().doubleValue(), closeTo(3.0, 0.0001));
        assertThat(record.getAvgEntityCount().doubleValue(), closeTo(4.0, 0.0001));
        assertThat(record.getAvgInvestment().doubleValue(), closeTo(5.0, 0.0001));
        assertThat(record.getAvgSavings().doubleValue(), closeTo(6.0, 0.0001));
        assertThat(record.getMinActionCount(), is(7));
        assertThat(record.getMinEntityCount(), is(8));
        assertThat(record.getMinInvestment().doubleValue(), closeTo(9.0, 0.0001));
        assertThat(record.getMinSavings().doubleValue(), closeTo(10.0, 0.0001));
        assertThat(record.getMaxActionCount(), is(11));
        assertThat(record.getMaxEntityCount(), is(12));
        assertThat(record.getMaxInvestment().doubleValue(), closeTo(13.0, 0.0001));
        assertThat(record.getMaxSavings().doubleValue(), closeTo(14.0, 0.0001));
    }

    @Test
    public void testWriterStatRecord() {
        final HourlyWriter writer = (HourlyWriter)hourActionStatTable.writer();
        final LocalDateTime time =
            LocalDateTime.ofEpochSecond(100000, 10000, ZoneOffset.UTC);
        final int numActionSnapshots = 10;
        final ActionSnapshotHourRecord statRecord = writer.snapshotRecord(time, numActionSnapshots);
        assertThat(statRecord.getHourTime(), is(time));
        assertThat(statRecord.getHourRollupTime(), is(LocalDateTime.now(clock)));
        assertThat(statRecord.getNumActionSnapshots(), is(numActionSnapshots));
    }

    @Test
    public void testReaderToGroupStatRoundTrip() {
        final HourlyWriter writer = (HourlyWriter) hourActionStatTable.writer();
        final LocalDateTime time = LocalDateTime.of(2018, Month.SEPTEMBER, 1, 0, 0);
        final int mgmtSubgroupId = 1;
        final int actionGroupId = 2;
        final RolledUpActionGroupStat rolledUpStat = ImmutableRolledUpActionGroupStat.builder()
            .avgActionCount(3)
            .avgEntityCount(4)
            .avgInvestment(5.0)
            .avgSavings(6.0)
            .minActionCount(7)
            .minEntityCount(8)
            .minInvestment(9.0)
            .minSavings(10.0)
            .maxActionCount(11)
            .maxEntityCount(12)
            .maxInvestment(13.0)
            .maxSavings(14.0)
            .build();
        final ActionStatsByHourRecord record =
            writer.statRecord(mgmtSubgroupId, actionGroupId, time, rolledUpStat);

        final HourlyReader reader = (HourlyReader) hourActionStatTable.reader();
        assertThat(reader.recordToGroupStat(record), is(rolledUpStat));
    }
}

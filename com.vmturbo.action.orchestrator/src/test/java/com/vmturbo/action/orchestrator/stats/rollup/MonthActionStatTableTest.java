package com.vmturbo.action.orchestrator.stats.rollup;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.Collections;

import org.jooq.DSLContext;
import org.junit.Test;

import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotMonthRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsByMonthRecord;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat;
import com.vmturbo.action.orchestrator.stats.rollup.MonthActionStatTable.MonthlyReader;
import com.vmturbo.action.orchestrator.stats.rollup.MonthActionStatTable.MonthlyWriter;
import com.vmturbo.components.api.test.MutableFixedClock;

public class MonthActionStatTableTest {

    /**
     * We don't actually need the database, because the base reader/writer classes handle
     * database interaction.
     */
    private DSLContext dslContext = mock(DSLContext.class);

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private MonthActionStatTable monthActionStatTable =
        new MonthActionStatTable(dslContext, clock);

    @Test
    public void testReaderIsPresent() {
        assertTrue(monthActionStatTable.reader() instanceof MonthlyReader);
    }

    @Test
    public void testWriterIsPresent() {
        assertTrue(monthActionStatTable.writer().isPresent());
        assertTrue(monthActionStatTable.writer().get() instanceof MonthlyWriter);
    }

    @Test
    public void testWriterSummaryToRecord() {
        final LocalDateTime time = LocalDateTime.of(2018, Month.SEPTEMBER, 1, 0, 0);
        final MonthlyWriter writer = (MonthlyWriter) monthActionStatTable.writer().get();
        final int mgmtSubgroupId = 1;
        final int actionGroupId = 2;
        final ActionStatsByMonthRecord record =
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
        assertThat(record.getMonthTime(), is(time));
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
        final MonthlyWriter writer = (MonthlyWriter) monthActionStatTable.writer().get();
        final LocalDateTime time =
            LocalDateTime.ofEpochSecond(100000, 10000, ZoneOffset.UTC);
        final int numActionSnapshots = 10;
        final ActionSnapshotMonthRecord statRecord = writer.snapshotRecord(time, numActionSnapshots);
        assertThat(statRecord.getMonthTime(), is(time));
        assertThat(statRecord.getMonthRollupTime(), is(LocalDateTime.now(clock)));
        assertThat(statRecord.getNumActionSnapshots(), is(numActionSnapshots));
    }

    @Test
    public void testReaderToGroupStatRoundTrip() {
        final MonthlyWriter writer = (MonthlyWriter) monthActionStatTable.writer().get();
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
        final ActionStatsByMonthRecord record =
            writer.statRecord(mgmtSubgroupId, actionGroupId, time, rolledUpStat);

        final MonthlyReader reader = (MonthlyReader) monthActionStatTable.reader();
        assertThat(reader.recordToGroupStat(record), is(rolledUpStat));
    }

    @Test(expected = IllegalStateException.class)
    public void testReaderRollupRecordsException() {
        final MonthlyReader reader = (MonthlyReader) monthActionStatTable.reader();
        reader.rollupRecords(1, Collections.emptyMap());
    }

    @Test(expected = IllegalStateException.class)
    public void testReaderNumSnapshotsInRecordException() {
        final MonthlyReader reader = (MonthlyReader) monthActionStatTable.reader();
        reader.numSnapshotsInSnapshotRecord(new ActionSnapshotMonthRecord());
    }
}

package com.vmturbo.action.orchestrator.stats.rollup;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jooq.DSLContext;
import org.junit.Test;

import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotLatestRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat;
import com.vmturbo.action.orchestrator.stats.rollup.BaseActionStatTableReader.StatWithSnapshotCnt;
import com.vmturbo.action.orchestrator.stats.rollup.LatestActionStatTable.LatestReader;
import com.vmturbo.components.api.test.MutableFixedClock;

public class LatestActionStatTableTest {

    private DSLContext dsl = mock(DSLContext.class);

    private static final int ACTION_GROUP_ID = 1123;

    private RolledUpStatCalculator calculator = mock(RolledUpStatCalculator.class);

    private Clock clock = new MutableFixedClock(1_000_000);

    private RollupTestUtils rollupTestUtils;

    private final LatestActionStatTable latestActionStatTable =
        new LatestActionStatTable(dsl, clock, calculator, HourActionStatTable.HOUR_TABLE_INFO);

    @Test
    public void testNoWriter() {
        assertFalse(latestActionStatTable.writer().isPresent());
    }

    @Test
    public void testReaderSummarize() {
        final LatestReader reader = (LatestReader) latestActionStatTable.reader();
        final int ag1Id = 1;
        final int ag2Id = 2;
        final StatWithSnapshotCnt<ActionStatsLatestRecord> ag1Record =
            RollupTestUtils.statRecordWithActionCount(1, ActionStatsLatestRecord.class);
        ag1Record.record().setTotalEntityCount(1);
        final StatWithSnapshotCnt<ActionStatsLatestRecord> ag2Record =
            RollupTestUtils.statRecordWithActionCount(1, ActionStatsLatestRecord.class);
        ag2Record.record().setTotalEntityCount(2);
        final Map<Integer, List<StatWithSnapshotCnt<ActionStatsLatestRecord>>> recordsByActionGroup = new HashMap<>();
        recordsByActionGroup.put(ag1Id, Collections.singletonList(ag1Record));
        recordsByActionGroup.put(ag2Id, Collections.singletonList(ag2Record));

        final RolledUpActionGroupStat ag1Stat = mock(RolledUpActionGroupStat.class);
        final RolledUpActionGroupStat ag2Stat = mock(RolledUpActionGroupStat.class);
        when(calculator.rollupLatestRecords(1, Collections.singletonList(ag1Record)))
            .thenReturn(Optional.of(ag1Stat));
        when(calculator.rollupLatestRecords(1, Collections.singletonList(ag2Record)))
            .thenReturn(Optional.of(ag2Stat));

        final Map<Integer, RolledUpActionGroupStat> statsByGroupId =
            reader.rollupRecords(1, recordsByActionGroup);
        assertThat(statsByGroupId.keySet(), containsInAnyOrder(ag1Id, ag2Id));
        assertThat(statsByGroupId.get(ag1Id), is(ag1Stat));
        assertThat(statsByGroupId.get(ag2Id), is(ag2Stat));
    }

    @Test
    public void testReaderNumSnapshots() {
        final LatestReader reader = (LatestReader) latestActionStatTable.reader();
        final ActionSnapshotLatestRecord record = new ActionSnapshotLatestRecord();
        assertThat(reader.numSnapshotsInSnapshotRecord(record), is(1));
    }

    @Test
    public void testReaderToGroupStatRoundTrip() {
        final LocalDateTime time = RollupTestUtils.time(10, 7);
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
        final ActionStatsLatestRecord record = new ActionStatsLatestRecord();
        record.setActionSnapshotTime(time);
        record.setMgmtUnitSubgroupId(mgmtSubgroupId);
        record.setActionGroupId(actionGroupId);

        record.setTotalActionCount(1);
        record.setTotalEntityCount(2);
        record.setTotalInvestment(BigDecimal.valueOf(3));
        record.setTotalSavings(BigDecimal.valueOf(4));

        final LatestReader reader = (LatestReader) latestActionStatTable.reader();
        assertThat(reader.recordToGroupStat(record), is(ImmutableRolledUpActionGroupStat.builder()
            .avgActionCount(1.0)
            .avgEntityCount(2.0)
            .avgInvestment(3.0)
            .avgSavings(4.0)
            .minActionCount(1)
            .minEntityCount(2)
            .minInvestment(3.0)
            .minSavings(4.0)
            .maxActionCount(1)
            .maxEntityCount(2)
            .maxInvestment(3.0)
            .maxSavings(4.0)
            .build()));
    }

}

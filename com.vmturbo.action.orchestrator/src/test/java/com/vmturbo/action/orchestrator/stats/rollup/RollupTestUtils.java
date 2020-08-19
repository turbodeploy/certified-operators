package com.vmturbo.action.orchestrator.stats.rollup;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.number.IsCloseTo.closeTo;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.temporal.ChronoUnit;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.UpdatableRecord;

import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.ActionGroupRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotHourRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotLatestRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.db.tables.records.MgmtUnitSubgroupRecord;
import com.vmturbo.action.orchestrator.db.tables.records.RelatedRiskForActionRecord;
import com.vmturbo.action.orchestrator.stats.rollup.BaseActionStatTableReader.StatWithSnapshotCnt;

public class RollupTestUtils {

    private final DSLContext dsl;

    public RollupTestUtils(final DSLContext dsl) {
        this.dsl = dsl;
    }

    public static LocalDateTime time(final int hourOfDay, final int minuteOfHour) {
        return LocalDateTime.of(2018, Month.SEPTEMBER, 30, hourOfDay, minuteOfHour);
    }

    public void insertHourlySnapshotOnly(@Nonnull final LocalDateTime snapshotTime) {
        final ActionSnapshotHourRecord record = new ActionSnapshotHourRecord();
        record.setHourRollupTime(LocalDateTime.MIN);
        record.setHourTime(snapshotTime.truncatedTo(ChronoUnit.HOURS));
        dsl.insertInto(Tables.ACTION_SNAPSHOT_HOUR)
                .set(record)
                .onDuplicateKeyIgnore()
                .execute();
    }

    public void insertLatestSnapshotOnly(@Nonnull final LocalDateTime snapshotTime) {
        final ActionSnapshotLatestRecord record = new ActionSnapshotLatestRecord();
        record.setTopologyId(1L);
        record.setActionsCount(1);
        record.setSnapshotRecordingTime(LocalDateTime.MIN);
        record.setActionSnapshotTime(snapshotTime);
        dsl.insertInto(Tables.ACTION_SNAPSHOT_LATEST)
                .set(record)
                .onDuplicateKeyIgnore()
                .execute();
    }

    public void insertMgmtUnit(final int mgmtUnitSubgroupId) {
        final MgmtUnitSubgroupRecord mgmtUnitRecord = new MgmtUnitSubgroupRecord();
        mgmtUnitRecord.setMgmtUnitType((short)1);
        mgmtUnitRecord.setMgmtUnitId(1L);
        mgmtUnitRecord.setEnvironmentType((short)1);
        mgmtUnitRecord.setId(mgmtUnitSubgroupId);
        // Set entity type to something different, because it's part of a key.
        mgmtUnitRecord.setEntityType((short)mgmtUnitSubgroupId);
        dsl.insertInto(Tables.MGMT_UNIT_SUBGROUP)
                .set(mgmtUnitRecord)
                .onDuplicateKeyIgnore()
                .execute();
    }

    public void insertActionGroup(final int actionGroupId) {
        final RelatedRiskForActionRecord relatedRiskForActionRecord = new RelatedRiskForActionRecord();
        relatedRiskForActionRecord.setId(1);
        relatedRiskForActionRecord.setRiskDescription("Mem congestion");
        dsl.insertInto(Tables.RELATED_RISK_FOR_ACTION)
                .set(relatedRiskForActionRecord)
                .onDuplicateKeyIgnore()
                .execute();
        final ActionGroupRecord actionGroupRecord = new ActionGroupRecord();
        actionGroupRecord.setId(actionGroupId);
        actionGroupRecord.setActionType((short)actionGroupId);
        actionGroupRecord.setActionState((short)actionGroupId);
        actionGroupRecord.setActionMode((short)actionGroupId);
        actionGroupRecord.setActionCategory((short)actionGroupId);
        actionGroupRecord.setActionRelatedRisk(1);
        dsl.insertInto(Tables.ACTION_GROUP)
                .set(actionGroupRecord)
                .onDuplicateKeyIgnore()
                .execute();
    }

    public void insertMgmtUnitStatRecord(final int mgmtUnitSubgroupId,
                                         final int actionGroupId,
                                         @Nonnull final LocalDateTime snapshotTime) {
        insertLatestSnapshotOnly(snapshotTime);
        insertMgmtUnit(mgmtUnitSubgroupId);
        insertActionGroup(actionGroupId);


        dsl.insertInto(Tables.ACTION_STATS_LATEST)
                .set(dummyRecord(mgmtUnitSubgroupId, actionGroupId, snapshotTime))
                .onDuplicateKeyIgnore()
                .execute();
    }

    public ActionStatsLatestRecord dummyRecord(final int mgmtUnitSubgroupId,
                                               final int actionGroupId,
                                               @Nonnull final LocalDateTime snapshotTime) {
        final ActionStatsLatestRecord statsRecord = new ActionStatsLatestRecord();
        statsRecord.setActionSnapshotTime(snapshotTime);
        statsRecord.setMgmtUnitSubgroupId(mgmtUnitSubgroupId);
        statsRecord.setActionGroupId(actionGroupId);
        statsRecord.setTotalInvestment(BigDecimal.valueOf(1));
        statsRecord.setTotalSavings(BigDecimal.valueOf(1));
        statsRecord.setTotalActionCount(1);
        statsRecord.setTotalEntityCount(1);
        return statsRecord;
    }

    public void compareRecords(final ActionStatsLatestRecord got, final ActionStatsLatestRecord expected) {
        assertThat(got.getActionSnapshotTime(), is(expected.getActionSnapshotTime()));
        assertThat(got.getMgmtUnitSubgroupId(), is(expected.getMgmtUnitSubgroupId()));
        assertThat(got.getActionGroupId(), is(expected.getActionGroupId()));
        assertThat(got.getTotalEntityCount(), is(expected.getTotalEntityCount()));
        assertThat(got.getTotalActionCount(), is(expected.getTotalActionCount()));
        assertThat(got.getTotalSavings().doubleValue(), closeTo(expected.getTotalSavings().doubleValue(), 0.0001));
        assertThat(got.getTotalInvestment().doubleValue(), closeTo(expected.getTotalInvestment().doubleValue(), 0.0001));
    }

    public void compareSnapshotRecords(final ActionSnapshotLatestRecord got, final ActionSnapshotLatestRecord expected) {
        assertThat(got.getTopologyId(), is(expected.getTopologyId()));
        assertThat(got.getSnapshotRecordingTime(), is(expected.getSnapshotRecordingTime()));
        assertThat(got.getActionSnapshotTime(), is(expected.getActionSnapshotTime()));
        assertThat(got.getActionsCount(), is(expected.getActionsCount()));
    }

    public static <STAT_RECORD extends Record> StatWithSnapshotCnt<STAT_RECORD>
            statRecordWithActionCount(final int actionCount,
                                      @Nonnull final Class<STAT_RECORD> recordClass) {
        try {
            return ImmutableStatWithSnapshotCnt.<STAT_RECORD>builder()
                .record(recordClass.newInstance())
                .numActionSnapshots(actionCount)
                .build();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new IllegalStateException("Unexpected exception when creating instance of record class!", e);
        }
    }

    public static <STAT_RECORD extends UpdatableRecord> StatWithSnapshotCnt<STAT_RECORD>
            copyRecord(StatWithSnapshotCnt<STAT_RECORD> record) {
        return ImmutableStatWithSnapshotCnt.<STAT_RECORD>builder()
            .record((STAT_RECORD)record.record().copy())
            .numActionSnapshots(record.numActionSnapshots())
            .build();
    }
}

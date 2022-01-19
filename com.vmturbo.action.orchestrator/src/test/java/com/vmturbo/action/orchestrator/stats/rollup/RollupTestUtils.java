package com.vmturbo.action.orchestrator.stats.rollup;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.number.IsCloseTo.closeTo;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.temporal.ChronoUnit;

import javax.annotation.Nonnull;

import org.apache.commons.codec.digest.DigestUtils;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.UpdatableRecord;

import com.vmturbo.action.orchestrator.db.Tables;
import com.vmturbo.action.orchestrator.db.tables.records.ActionGroupRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotHourRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotLatestRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.db.tables.records.MgmtUnitSubgroupRecord;
import com.vmturbo.action.orchestrator.db.tables.records.RelatedRiskDescriptionRecord;
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
        // Earliest timestamp supported by PostgreSQL is 0001-01-01T00:00.
        dsl.insertInto(Tables.ACTION_SNAPSHOT_HOUR, Tables.ACTION_SNAPSHOT_HOUR.HOUR_TIME,
                   Tables.ACTION_SNAPSHOT_HOUR.NUM_ACTION_SNAPSHOTS,
                   Tables.ACTION_SNAPSHOT_HOUR.HOUR_ROLLUP_TIME)
           .values(snapshotTime.truncatedTo(ChronoUnit.HOURS), 2, LocalDateTime.of(1, 1, 1, 0, 0))
           .onDuplicateKeyIgnore()
           .execute();
    }

    public void insertLatestSnapshotOnly(@Nonnull final LocalDateTime snapshotTime) {
        final ActionSnapshotLatestRecord record = new ActionSnapshotLatestRecord();
        record.setTopologyId(1L);
        record.setActionsCount(1);
        // Earliest timestamp supported by PostgreSQL is 0001-01-01T00:00.
        record.setSnapshotRecordingTime(LocalDateTime.of(1, 1, 1, 0, 0));
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
        relatedRiskForActionRecord.setChecksum(DigestUtils.md5Hex("Mem congestion"));
        dsl.insertInto(Tables.RELATED_RISK_FOR_ACTION)
                .set(relatedRiskForActionRecord)
                .onDuplicateKeyIgnore()
                .execute();
        final RelatedRiskDescriptionRecord relatedRiskDescriptionsRecord = new RelatedRiskDescriptionRecord();
        relatedRiskDescriptionsRecord.setId(1);
        relatedRiskDescriptionsRecord.setRiskDescription("Mem congestion");
        dsl.insertInto(Tables.RELATED_RISK_DESCRIPTION)
                .set(relatedRiskDescriptionsRecord)
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

    public void compareAggregateRecords(final Record got, final Record expected) {
        assertThat(got.getClass(), is(expected.getClass()));
        assertThat(got.get("mgmt_unit_subgroup_id", Integer.class), is(expected.get("mgmt_unit_subgroup_id", Integer.class)));
        assertThat(got.get("action_group_id", Integer.class), is(expected.get("action_group_id", Integer.class)));
        assertThat(got.get("new_action_count", Integer.class), is(expected.get("new_action_count", Integer.class)));
        assertThat(got.get("prior_action_count", Integer.class), is(expected.get("prior_action_count", Integer.class)));
        assertThat(got.get("avg_action_count", BigDecimal.class).doubleValue(),
                closeTo(expected.get("avg_action_count", BigDecimal.class).doubleValue(), 0.0001f));
        assertThat(got.get("min_action_count", Integer.class), is(expected.get("min_action_count", Integer.class)));
        assertThat(got.get("max_action_count", Integer.class), is(expected.get("max_action_count", Integer.class)));
        assertThat(got.get("avg_entity_count", BigDecimal.class).doubleValue(),
                closeTo(expected.get("avg_entity_count", BigDecimal.class).doubleValue(), 0.0001f));
        assertThat(got.get("min_entity_count", Integer.class), is(expected.get("min_entity_count", Integer.class)));
        assertThat(got.get("max_entity_count", Integer.class), is(expected.get("max_entity_count", Integer.class)));
        assertThat(got.get("min_savings", BigDecimal.class).doubleValue(),
                closeTo(expected.get("min_savings", BigDecimal.class).doubleValue(), 0.0001f));
        assertThat(got.get("max_savings", BigDecimal.class).doubleValue(),
                closeTo(expected.get("max_savings", BigDecimal.class).doubleValue(), 0.0001f));
        assertThat(got.get("avg_savings", BigDecimal.class).doubleValue(),
                closeTo(expected.get("avg_savings", BigDecimal.class).doubleValue(), 0.0001f));
        assertThat(got.get("min_investment", BigDecimal.class).doubleValue(),
                closeTo(expected.get("min_investment", BigDecimal.class).doubleValue(), 0.0001f));
        assertThat(got.get("max_investment", BigDecimal.class).doubleValue(),
                closeTo(expected.get("max_investment", BigDecimal.class).doubleValue(), 0.0001f));
        assertThat(got.get("avg_investment", BigDecimal.class).doubleValue(),
                closeTo(expected.get("avg_investment", BigDecimal.class).doubleValue(), 0.0001f));
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

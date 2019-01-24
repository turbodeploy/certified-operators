package com.vmturbo.action.orchestrator.stats;

import static com.vmturbo.action.orchestrator.db.Tables.ACTION_GROUP;
import static com.vmturbo.action.orchestrator.db.Tables.ACTION_SNAPSHOT_LATEST;
import static com.vmturbo.action.orchestrator.db.Tables.MGMT_UNIT_SUBGROUP;
import static com.vmturbo.action.orchestrator.db.tables.ActionStatsLatest.ACTION_STATS_LATEST;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.db.tables.records.ActionGroupRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotLatestRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.db.tables.records.MgmtUnitSubgroupRecord;
import com.vmturbo.action.orchestrator.stats.SingleActionSnapshotFactory.SingleActionSnapshot;
import com.vmturbo.action.orchestrator.stats.aggregator.ActionAggregatorFactory;
import com.vmturbo.action.orchestrator.stats.aggregator.ActionAggregatorFactory.ActionAggregator;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup.ActionGroupKey;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroupStore;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup.MgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroupStore;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatCleanupScheduler;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatRollupScheduler;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=action"})
public class LiveActionsStatisticianTest {

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private DSLContext dsl;

    private LiveActionsStatistician statistician;

    private SingleActionSnapshotFactory snapshotFactory = mock(SingleActionSnapshotFactory.class);

    private ActionAggregatorFactory<ActionAggregator> aggregatorFactory =
            mock(ActionAggregatorFactory.class);

    private ActionTranslator actionTranslator = mock(ActionTranslator.class);

    private ActionGroupStore actionGroupStore = mock(ActionGroupStore.class);

    private MgmtUnitSubgroupStore mgmtUnitSubgroupStore = mock(MgmtUnitSubgroupStore.class);

    private ActionStatRollupScheduler rollupScheduler = mock(ActionStatRollupScheduler.class);

    private ActionStatCleanupScheduler cleanupScheduler = mock(ActionStatCleanupScheduler.class);

    /**
     * The clock can't start at too small of a number because TIMESTAMP starts in 1970, but
     * epoch millis starts in 1969.
     */
    private MutableFixedClock clock = new MutableFixedClock(Instant.ofEpochMilli(1_000_000_000), ZoneId.systemDefault());

    @Before
    public void setup() throws Exception {
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();


        when(actionTranslator.translate(any(Stream.class))).thenAnswer(invocation -> invocation.getArgumentAt(0, Stream.class));

        statistician = new LiveActionsStatistician(dsl, 2, actionGroupStore,
            mgmtUnitSubgroupStore, snapshotFactory, Collections.singletonList(aggregatorFactory),
            clock, actionTranslator, rollupScheduler, cleanupScheduler);
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    @Test
    public void testRecordActions() throws UnsupportedActionException {
        final ActionView action1 = mock(ActionView.class);
        final ActionView action2 = mock(ActionView.class);
        final ActionView action3 = mock(ActionView.class);
        final SingleActionSnapshot snapshot1 = mock(SingleActionSnapshot.class);
        final SingleActionSnapshot snapshot2 = mock(SingleActionSnapshot.class);
        final SingleActionSnapshot snapshot3 = mock(SingleActionSnapshot.class);
        when(snapshotFactory.newSnapshot(eq(action1))).thenReturn(snapshot1);
        when(snapshotFactory.newSnapshot(eq(action2))).thenReturn(snapshot2);
        when(snapshotFactory.newSnapshot(eq(action3))).thenReturn(snapshot3);

        final ActionAggregator aggregator = mock(ActionAggregator.class);
        when(aggregatorFactory.newAggregator(LocalDateTime.now(clock))).thenReturn(aggregator);

        final Map<Integer, ActionStatsLatestRecord> aggregatedRecordsByMgmtUnit = ImmutableMap.of(
                1, newRecord(1, 1),
                2, newRecord(2, 2),
                3, newRecord(3, 3));

        // Fake the management unit subgroups and action groups that the mock aggregator needs to
        // return to the statistician.
        final Set<MgmtUnitSubgroupKey> aggregateSubgroupKeys = Collections.emptySet();
        final Set<ActionGroupKey> aggregateActionGroupKeys = Collections.emptySet();
        doReturn(aggregateSubgroupKeys).when(aggregator).getMgmtUnitSubgroupKeys();
        doReturn(aggregateActionGroupKeys).when(aggregator).getActionGroupKeys();

        // Fake the results of upserting the subgroups/groups to their respective stores.
        final Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> upsertedSubgroup = Collections.emptyMap();
        final Map<ActionGroupKey, ActionGroup> upsertedActionGroup = Collections.emptyMap();
        when(actionGroupStore.ensureExist(aggregateActionGroupKeys)).thenReturn(upsertedActionGroup);
        when(mgmtUnitSubgroupStore.ensureExist(aggregateSubgroupKeys)).thenReturn(upsertedSubgroup);

        doReturn(aggregatedRecordsByMgmtUnit.values().stream()).when(aggregator)
                .createRecords(upsertedSubgroup, upsertedActionGroup);

        final long topologyId = 1;
        statistician.recordActionStats(topologyId, Stream.of(action1, action2, action3));

        // Verify that the statistician interacts with the aggregator in the right order.
        final InOrder inOrder = Mockito.inOrder(aggregator);
        inOrder.verify(aggregator).start();
        inOrder.verify(aggregator).processAction(snapshot1);
        inOrder.verify(aggregator).processAction(snapshot2);
        inOrder.verify(aggregator).processAction(snapshot3);
        inOrder.verify(aggregator).createRecords(upsertedSubgroup, upsertedActionGroup);

        // Verify that the statistician schedules rollups.
        verify(rollupScheduler).scheduleRollups();
        // Verify that the statistician schedules cleanups.
        verify(cleanupScheduler).scheduleCleanups();

        final Map<Integer, ActionStatsLatestRecord> recordsByMgmtUnit = dsl.selectFrom(ACTION_STATS_LATEST)
                .fetch()
                .into(ActionStatsLatestRecord.class)
                .stream()
                .collect(Collectors.toMap(ActionStatsLatestRecord::getMgmtUnitSubgroupId, Function.identity()));

        assertThat(recordsByMgmtUnit.keySet(), containsInAnyOrder(1, 2, 3));
        compareRecords(recordsByMgmtUnit.get(1), aggregatedRecordsByMgmtUnit.get(1));
        compareRecords(recordsByMgmtUnit.get(2), aggregatedRecordsByMgmtUnit.get(2));
        compareRecords(recordsByMgmtUnit.get(3), aggregatedRecordsByMgmtUnit.get(3));

        List<ActionSnapshotLatestRecord> snapshotRecords =
            dsl.selectFrom(ACTION_SNAPSHOT_LATEST)
                .fetch()
                .into(ActionSnapshotLatestRecord.class);
        assertThat(snapshotRecords.size(), is(1));
        ActionSnapshotLatestRecord snapshotRecord = snapshotRecords.get(0);
        assertThat(snapshotRecord.getSnapshotRecordingTime(), is(LocalDateTime.now(clock)));
        assertThat(snapshotRecord.getActionSnapshotTime(), is(LocalDateTime.now(clock)));
        assertThat(snapshotRecord.getTopologyId(), is(1L));
        assertThat(snapshotRecord.getActionsCount(), is(3));
    }

    @Test
    public void testActionAggregatorStartFail() throws UnsupportedActionException {
        final ActionView action1 = mock(ActionView.class);
        final SingleActionSnapshot snapshot1 = mock(SingleActionSnapshot.class);
        when(snapshotFactory.newSnapshot(eq(action1))).thenReturn(snapshot1);

        final ActionAggregator aggregator = mock(ActionAggregator.class);
        when(aggregatorFactory.newAggregator(LocalDateTime.now(clock))).thenReturn(aggregator);

        doThrow(new RuntimeException("foo")).when(aggregator).start();

        final long topologyId = 1;
        statistician.recordActionStats(topologyId, Stream.of(action1));

        verify(aggregator, times(0)).processAction(any());

        // No stats aggregated, so stats table should be empty.
        assertTrue(dsl.selectFrom(ACTION_STATS_LATEST)
            .fetch()
            .into(ActionStatsLatestRecord.class)
            .isEmpty());

        // Should still make a record for the snapshot.
        final List<ActionSnapshotLatestRecord> snapshotRecords =
            dsl.selectFrom(ACTION_SNAPSHOT_LATEST)
                .fetch()
                .into(ActionSnapshotLatestRecord.class);
        assertThat(snapshotRecords.size(), is(1));
        ActionSnapshotLatestRecord snapshotRecord = snapshotRecords.get(0);
        assertThat(snapshotRecord.getSnapshotRecordingTime(), is(LocalDateTime.now(clock)));
        assertThat(snapshotRecord.getActionSnapshotTime(), is(LocalDateTime.now(clock)));
        assertThat(snapshotRecord.getTopologyId(), is(1L));
        assertThat(snapshotRecord.getActionsCount(), is(1));
    }

    @Test
    public void testActionAggregatorProcessFail() throws UnsupportedActionException {
        final ActionView action1 = mock(ActionView.class);
        final SingleActionSnapshot snapshot1 = mock(SingleActionSnapshot.class);
        when(snapshotFactory.newSnapshot(eq(action1))).thenReturn(snapshot1);

        final ActionAggregator aggregator = mock(ActionAggregator.class);
        when(aggregatorFactory.newAggregator(LocalDateTime.now(clock))).thenReturn(aggregator);

        doThrow(new RuntimeException("foo")).when(aggregator).processAction(any());

        final long topologyId = 1;
        statistician.recordActionStats(topologyId, Stream.of(action1));

        // Started properly
        verify(aggregator).start();

        // No stats aggregated, so stats table should be empty.
        assertTrue(dsl.selectFrom(ACTION_STATS_LATEST)
            .fetch()
            .into(ActionStatsLatestRecord.class)
            .isEmpty());

        // Should still make a record for the snapshot.
        final List<ActionSnapshotLatestRecord> snapshotRecords =
            dsl.selectFrom(ACTION_SNAPSHOT_LATEST)
                .fetch()
                .into(ActionSnapshotLatestRecord.class);
        assertThat(snapshotRecords.size(), is(1));
        ActionSnapshotLatestRecord snapshotRecord = snapshotRecords.get(0);
        assertThat(snapshotRecord.getSnapshotRecordingTime(), is(LocalDateTime.now(clock)));
        assertThat(snapshotRecord.getActionSnapshotTime(), is(LocalDateTime.now(clock)));
        assertThat(snapshotRecord.getTopologyId(), is(1L));
        assertThat(snapshotRecord.getActionsCount(), is(1));
    }

    private ActionStatsLatestRecord newRecord(final int mgmtSubgroupId, final int actionGroupId) {
        // Insert a fake management subgroup and action group to avoid violating integrity constraints.
        insertFakeMgmtSubgroup(mgmtSubgroupId);
        insertFakeActionGroup(actionGroupId);

        final ActionStatsLatestRecord record1 = new ActionStatsLatestRecord();
        record1.setActionSnapshotTime(LocalDateTime.now(clock));
        record1.setMgmtUnitSubgroupId(mgmtSubgroupId);
        record1.setActionGroupId(actionGroupId);
        record1.setTotalEntityCount(1);
        record1.setTotalActionCount(1);
        record1.setTotalSavings(BigDecimal.valueOf(0));
        record1.setTotalInvestment(BigDecimal.valueOf(0));
        return record1;
    }

    private void insertFakeMgmtSubgroup(final int mgmtSubgroupId) {
        final MgmtUnitSubgroupRecord subgroupRecord = new MgmtUnitSubgroupRecord();
        subgroupRecord.setId(mgmtSubgroupId);
        subgroupRecord.setEntityType((short)mgmtSubgroupId);
        subgroupRecord.setEnvironmentType((short)mgmtSubgroupId);
        subgroupRecord.setMgmtUnitId((long)mgmtSubgroupId);
        subgroupRecord.setMgmtUnitType((short)mgmtSubgroupId);
        dsl.insertInto(MGMT_UNIT_SUBGROUP)
                .set(subgroupRecord)
                .onDuplicateKeyIgnore()
                .execute();
    }

    private void insertFakeActionGroup(final int actionGroupId) {
        final ActionGroupRecord actionGroupRecord = new ActionGroupRecord();
        actionGroupRecord.setId(actionGroupId);
        actionGroupRecord.setActionCategory((short)actionGroupId);
        actionGroupRecord.setActionMode((short)actionGroupId);
        actionGroupRecord.setActionState((short)actionGroupId);
        actionGroupRecord.setActionType((short)actionGroupId);
        dsl.insertInto(ACTION_GROUP)
                .set(actionGroupRecord)
                .onDuplicateKeyIgnore()
                .execute();
    }

    private void compareRecords(final ActionStatsLatestRecord got, final ActionStatsLatestRecord expected) {
        assertThat(got.getActionSnapshotTime(), is(expected.getActionSnapshotTime()));
        assertThat(got.getMgmtUnitSubgroupId(), is(expected.getMgmtUnitSubgroupId()));
        assertThat(got.getActionGroupId(), is(expected.getActionGroupId()));
        assertThat(got.getTotalEntityCount(), is(expected.getTotalEntityCount()));
        assertThat(got.getTotalActionCount(), is(expected.getTotalActionCount()));
        assertThat(got.getTotalSavings().doubleValue(), is(expected.getTotalSavings().doubleValue()));
        assertThat(got.getTotalInvestment().doubleValue(), is(expected.getTotalInvestment().doubleValue()));
    }
}

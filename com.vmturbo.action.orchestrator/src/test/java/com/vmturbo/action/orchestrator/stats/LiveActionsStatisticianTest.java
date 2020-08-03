package com.vmturbo.action.orchestrator.stats;

import static com.vmturbo.action.orchestrator.db.Tables.ACTION_GROUP;
import static com.vmturbo.action.orchestrator.db.Tables.ACTION_SNAPSHOT_LATEST;
import static com.vmturbo.action.orchestrator.db.Tables.MGMT_UNIT_SUBGROUP;
import static com.vmturbo.action.orchestrator.db.tables.ActionStatsLatest.ACTION_STATS_LATEST;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.jooq.DSLContext;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.db.Action;
import com.vmturbo.action.orchestrator.db.tables.records.ActionGroupRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotLatestRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.db.tables.records.MgmtUnitSubgroupRecord;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician.PreviousBroadcastActions;
import com.vmturbo.action.orchestrator.stats.StatsActionViewFactory.StatsActionView;
import com.vmturbo.action.orchestrator.stats.aggregator.ActionAggregatorFactory;
import com.vmturbo.action.orchestrator.stats.aggregator.ActionAggregatorFactory.ActionAggregator;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup.ActionGroupKey;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroupStore;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableActionGroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup.MgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroupStore;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatCleanupScheduler;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatRollupScheduler;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

public class LiveActionsStatisticianTest {
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

    private StatsActionViewFactory snapshotFactory = mock(StatsActionViewFactory.class);

    private ActionAggregatorFactory<ActionAggregator> aggregatorFactory =
            mock(ActionAggregatorFactory.class);

    private ActionGroupStore actionGroupStore = mock(ActionGroupStore.class);

    private MgmtUnitSubgroupStore mgmtUnitSubgroupStore = mock(MgmtUnitSubgroupStore.class);

    private ActionStatRollupScheduler rollupScheduler = mock(ActionStatRollupScheduler.class);

    private ActionStatCleanupScheduler cleanupScheduler = mock(ActionStatCleanupScheduler.class);

    /**
     * The clock can't start at too small of a number because TIMESTAMP starts in 1970, but
     * epoch millis starts in 1969.
     */
    private MutableFixedClock clock = new MutableFixedClock(Instant.ofEpochMilli(1_000_000_000), ZoneId.systemDefault());

    private LiveActionsStatistician statistician = new LiveActionsStatistician(dsl, 2, actionGroupStore,
           mgmtUnitSubgroupStore, snapshotFactory, Collections.singletonList(aggregatorFactory),
            clock, rollupScheduler, cleanupScheduler);

    @Test
    public void testRecordActions() throws UnsupportedActionException {
        final long actionId1 = 10;
        final long actionId2 = 11;
        final long actionId3 = 12;
        final ActionView action1 = mock(ActionView.class);
        final ActionView action2 = mock(ActionView.class);
        final ActionView action3 = mock(ActionView.class);
        final StatsActionView snapshot1 = statsActionView(actionId1, ActionState.READY);
        final StatsActionView snapshot2 = statsActionView(actionId2, ActionState.READY);
        final StatsActionView snapshot3 = statsActionView(actionId3, ActionState.READY);
        when(snapshotFactory.newStatsActionView(eq(action1))).thenReturn(snapshot1);
        when(snapshotFactory.newStatsActionView(eq(action2))).thenReturn(snapshot2);
        when(snapshotFactory.newStatsActionView(eq(action3))).thenReturn(snapshot3);

        final ActionAggregator aggregator = spy(new ActionAggregator(LocalDateTime.now(clock)) {
            @Override
            public void processAction(@Nonnull final StatsActionView action,
                                      @Nonnull final PreviousBroadcastActions previousBroadcastActions) {
                return;
            }

            @Nonnull
            @Override
            protected ManagementUnitType getManagementUnitType() {
                return null;
            }
        });
        when(aggregatorFactory.newAggregator(any())).thenReturn(aggregator);

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

        when(aggregator.createRecords(upsertedSubgroup, upsertedActionGroup))
            .thenAnswer(mock -> aggregatedRecordsByMgmtUnit.values().stream());

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyId(1)
            .setCreationTime(clock.millis())
            .setTopologyType(TopologyType.REALTIME)
            .build();
        statistician.recordActionStats(topologyInfo, Stream.of(action1, action2, action3));

        // Verify that the statistician interacts with the aggregator in the right order.
        final InOrder inOrder = Mockito.inOrder(aggregator);
        inOrder.verify(aggregator).start();
        inOrder.verify(aggregator).processAction(snapshot1, statistician.getPreviousBroadcastActions());
        inOrder.verify(aggregator).processAction(snapshot2, statistician.getPreviousBroadcastActions());
        inOrder.verify(aggregator).processAction(snapshot3, statistician.getPreviousBroadcastActions());
        inOrder.verify(aggregator).createRecords(upsertedSubgroup, upsertedActionGroup);

        // Verify that the statistician schedules rollups.
        verify(rollupScheduler).scheduleRollups();
        // Verify that the statistician schedules cleanups.
        verify(cleanupScheduler).scheduleCleanups();

        Map<Integer, ActionStatsLatestRecord> recordsByMgmtUnit = dsl.selectFrom(ACTION_STATS_LATEST)
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
        assertThat(statistician.getPreviousBroadcastActions().size(), is(3));
        assertThat(statistician.getPreviousBroadcastActions().getActionGroupKey(actionId1),
            is(snapshot1.actionGroupKey()));
        assertThat(statistician.getPreviousBroadcastActions().getActionGroupKey(actionId2),
            is(snapshot2.actionGroupKey()));
        assertThat(statistician.getPreviousBroadcastActions().getActionGroupKey(actionId3),
            is(snapshot3.actionGroupKey()));

        // Next broadcast.
        clock.addTime(10, ChronoUnit.HOURS);

        // Update records because clock is updated.
        final Map<Integer, ActionStatsLatestRecord> aggregatedRecordsByMgmtUnit1 = ImmutableMap.of(
            1, newRecord(1, 1),
            2, newRecord(2, 2),
            3, newRecord(3, 3));

        when(aggregator.createRecords(upsertedSubgroup, upsertedActionGroup))
            .thenAnswer(mock -> aggregatedRecordsByMgmtUnit1.values().stream());

        // action1 and action2 are still the same. ActionState of action3 is changed to SUCCEED.
        // action3 will be considered as a new action.
        final StatsActionView snapshot4 = statsActionView(12, ActionState.SUCCEEDED);
        when(snapshotFactory.newStatsActionView(eq(action3))).thenReturn(snapshot4);

        assertFalse(aggregator.actionIsNew(snapshot1, statistician.getPreviousBroadcastActions()));
        assertFalse(aggregator.actionIsNew(snapshot2, statistician.getPreviousBroadcastActions()));
        assertTrue(aggregator.actionIsNew(snapshot4, statistician.getPreviousBroadcastActions()));

        final TopologyInfo topologyInfo1 = TopologyInfo.newBuilder()
            .setTopologyId(1)
            .setCreationTime(clock.millis())
            .setTopologyType(TopologyType.REALTIME)
            .build();
        statistician.recordActionStats(topologyInfo1, Stream.of(action1, action2, action3));

        assertThat(statistician.getPreviousBroadcastActions().size(), is(3));
        assertThat(statistician.getPreviousBroadcastActions().getActionGroupKey(actionId1),
            is(snapshot1.actionGroupKey()));
        assertThat(statistician.getPreviousBroadcastActions().getActionGroupKey(actionId2),
            is(snapshot2.actionGroupKey()));
        // ActionGroupKey of action3 was updated.
        assertThat(statistician.getPreviousBroadcastActions().getActionGroupKey(actionId3),
            is(snapshot4.actionGroupKey()));
    }

    @Test
    public void testActionAggregatorStartFail() throws UnsupportedActionException {
        final ActionView action1 = mock(ActionView.class);
        when(snapshotFactory.newStatsActionView(eq(action1))).thenReturn(statsActionView(10, ActionState.READY));

        final ActionAggregator aggregator = mock(ActionAggregator.class);
        when(aggregatorFactory.newAggregator(LocalDateTime.now(clock))).thenReturn(aggregator);

        doThrow(new RuntimeException("foo")).when(aggregator).start();

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyId(1)
            .setCreationTime(clock.millis())
            .setTopologyType(TopologyType.REALTIME)
            .build();
        statistician.recordActionStats(topologyInfo, Stream.of(action1));

        verify(aggregator, times(0)).processAction(any(), any());

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
        when(snapshotFactory.newStatsActionView(eq(action1))).thenReturn(statsActionView(10, ActionState.READY));

        final ActionAggregator aggregator = mock(ActionAggregator.class);
        when(aggregatorFactory.newAggregator(LocalDateTime.now(clock))).thenReturn(aggregator);

        doThrow(new RuntimeException("foo")).when(aggregator).processAction(any(), any());

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyId(1)
            .setCreationTime(clock.millis())
            .setTopologyType(TopologyType.REALTIME)
            .build();
        statistician.recordActionStats(topologyInfo, Stream.of(action1));

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
        record1.setNewActionCount(1);
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
        assertThat(got.getNewActionCount(), is(expected.getNewActionCount()));
        assertThat(got.getTotalSavings().doubleValue(), is(expected.getTotalSavings().doubleValue()));
        assertThat(got.getTotalInvestment().doubleValue(), is(expected.getTotalInvestment().doubleValue()));
    }

    private StatsActionView statsActionView(long actionId, ActionState actionState) {
        return ImmutableStatsActionView.builder()
            .actionGroupKey(ImmutableActionGroupKey.builder()
                .actionType(ActionType.MOVE)
                .actionMode(ActionMode.RECOMMEND)
                .category(ActionCategory.PERFORMANCE_ASSURANCE)
                .actionState(actionState).build())
            .recommendation(ActionDTO.Action.newBuilder()
                .setId(actionId)
                .setInfo(ActionInfo.newBuilder())
                .setExplanation(Explanation.newBuilder())
                .setDeprecatedImportance(0).build()).build();
    }
}

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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

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
import com.vmturbo.action.orchestrator.stats.StatsActionViewFactory.StatsActionView;
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
        final ActionView action1 = mock(ActionView.class);
        final ActionView action2 = mock(ActionView.class);
        final ActionView action3 = mock(ActionView.class);
        final StatsActionView snapshot1 = mock(StatsActionView.class);
        final StatsActionView snapshot2 = mock(StatsActionView.class);
        final StatsActionView snapshot3 = mock(StatsActionView.class);
        when(snapshotFactory.newStatsActionView(eq(action1))).thenReturn(snapshot1);
        when(snapshotFactory.newStatsActionView(eq(action2))).thenReturn(snapshot2);
        when(snapshotFactory.newStatsActionView(eq(action3))).thenReturn(snapshot3);

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

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyId(1)
            .setCreationTime(clock.millis())
            .setTopologyType(TopologyType.REALTIME)
            .build();
        statistician.recordActionStats(topologyInfo, Stream.of(action1, action2, action3),
            ImmutableSet.of());

        // Verify that the statistician interacts with the aggregator in the right order.
        final InOrder inOrder = Mockito.inOrder(aggregator);
        inOrder.verify(aggregator).start();
        inOrder.verify(aggregator).processAction(snapshot1, ImmutableSet.of());
        inOrder.verify(aggregator).processAction(snapshot2, ImmutableSet.of());
        inOrder.verify(aggregator).processAction(snapshot3, ImmutableSet.of());
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
        final StatsActionView snapshot1 = mock(StatsActionView.class);
        when(snapshotFactory.newStatsActionView(eq(action1))).thenReturn(snapshot1);

        final ActionAggregator aggregator = mock(ActionAggregator.class);
        when(aggregatorFactory.newAggregator(LocalDateTime.now(clock))).thenReturn(aggregator);

        doThrow(new RuntimeException("foo")).when(aggregator).start();

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyId(1)
            .setCreationTime(clock.millis())
            .setTopologyType(TopologyType.REALTIME)
            .build();
        statistician.recordActionStats(topologyInfo, Stream.of(action1), ImmutableSet.of());

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
        final StatsActionView snapshot1 = mock(StatsActionView.class);
        when(snapshotFactory.newStatsActionView(eq(action1))).thenReturn(snapshot1);

        final ActionAggregator aggregator = mock(ActionAggregator.class);
        when(aggregatorFactory.newAggregator(LocalDateTime.now(clock))).thenReturn(aggregator);

        doThrow(new RuntimeException("foo")).when(aggregator).processAction(any(), any());

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyId(1)
            .setCreationTime(clock.millis())
            .setTopologyType(TopologyType.REALTIME)
            .build();
        statistician.recordActionStats(topologyInfo, Stream.of(action1), ImmutableSet.of());

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

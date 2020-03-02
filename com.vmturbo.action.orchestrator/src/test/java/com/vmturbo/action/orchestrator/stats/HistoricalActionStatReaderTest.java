package com.vmturbo.action.orchestrator.stats;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.action.orchestrator.stats.HistoricalActionStatReader.CombinedStatsBuckets;
import com.vmturbo.action.orchestrator.stats.HistoricalActionStatReader.CombinedStatsBucketsFactory;
import com.vmturbo.action.orchestrator.stats.HistoricalActionStatReader.CombinedStatsBucketsFactory.DefaultBucketsFactory;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup.ActionGroupKey;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroupStore;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroupStore.MatchedActionGroups;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMatchedActionGroups;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroup;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableQueryResult;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroupStore;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.QueryResultsFromSnapshot;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat;
import com.vmturbo.action.orchestrator.stats.rollup.ImmutableQueryResultsFromSnapshot;
import com.vmturbo.action.orchestrator.stats.rollup.ImmutableRolledUpActionGroupStat;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStat.StatGroup;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStat.Value;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStats;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStats.ActionStatSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.ActionGroupFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.MgmtUnitSubgroupFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.TimeRange;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.TimeFrameCalculator;

public class HistoricalActionStatReaderTest {

    private final ActionGroupStore actionGroupStore =
        mock(ActionGroupStore.class);

    private final MgmtUnitSubgroupStore mgmtUnitSubgroupStore =
        mock(MgmtUnitSubgroupStore.class);

    private final TimeFrameCalculator timeFrameCalculator =
        mock(TimeFrameCalculator.class);

    private final CombinedStatsBucketsFactory statsBucketsFactory =
        mock(CombinedStatsBucketsFactory.class);

    private final ActionStatTable.Reader hourReader = mock(ActionStatTable.Reader.class);

    private final Map<TimeFrame, ActionStatTable.Reader> tablesForTimeFrame =
        ImmutableMap.of(TimeFrame.HOUR, hourReader);

    @Test
    public void testReadActionStats() {
        final HistoricalActionStatReader reader = new HistoricalActionStatReader(actionGroupStore,
            mgmtUnitSubgroupStore, timeFrameCalculator, tablesForTimeFrame, statsBucketsFactory);

        final MgmtUnitSubgroupFilter muFilter = MgmtUnitSubgroupFilter.newBuilder()
            .setMgmtUnitId(1)
            .build();
        final ActionGroupFilter agFilter = ActionGroupFilter.newBuilder()
            .addActionCategory(ActionCategory.PERFORMANCE_ASSURANCE)
            .build();
        final TimeRange timeRange = TimeRange.newBuilder()
            .setStartTime(1)
            .setEndTime(2)
            .build();
        final LocalDateTime retStatTime = LocalDateTime.ofEpochSecond(1, 0, ZoneOffset.UTC);
        final GroupBy groupBy = GroupBy.ACTION_CATEGORY;
        final HistoricalActionStatsQuery query = HistoricalActionStatsQuery.newBuilder()
            .setMgmtUnitSubgroupFilter(muFilter)
            .setActionGroupFilter(agFilter)
            .setTimeRange(timeRange)
            .setGroupBy(groupBy)
            .build();

        final ActionGroup actionGroup = mock(ActionGroup.class);
        final RolledUpActionGroupStat actionGroupStat = mock(RolledUpActionGroupStat.class);

        final ActionDTO.ActionStat actionStat = ActionDTO.ActionStat.newBuilder()
            // Something to distinguish it from the default instance.
            .setStatGroup(ActionDTO.ActionStat.StatGroup.newBuilder()
                .setActionCategory(ActionCategory.PERFORMANCE_ASSURANCE))
            .build();

        final CombinedStatsBuckets buckets = mock(CombinedStatsBuckets.class);

        final Map<Integer, MgmtUnitSubgroup> targetMgmtSubunit = Collections.singletonMap(7, mock(MgmtUnitSubgroup.class));
        final MatchedActionGroups matchedActionGroups = ImmutableMatchedActionGroups.builder()
            .allActionGroups(false)
            .putSpecificActionGroupsById(9, actionGroup)
            .build();

        when(mgmtUnitSubgroupStore.query(eq(muFilter), any())).thenReturn(Optional.of(ImmutableQueryResult.builder()
            .mgmtUnit(1)
            .putAllMgmtUnitSubgroups(targetMgmtSubunit)
            .build()));
        when(actionGroupStore.query(agFilter)).thenReturn(Optional.of(matchedActionGroups));
        when(timeFrameCalculator.millis2TimeFrame(timeRange.getStartTime())).thenReturn(TimeFrame.HOUR);


        final Map<ActionGroup, Map<Integer, RolledUpActionGroupStat>> statsByGroupAndMu =
            ImmutableMap.of(actionGroup, ImmutableMap.of(7, actionGroupStat));
        final QueryResultsFromSnapshot qResult = ImmutableQueryResultsFromSnapshot.builder()
            .time(retStatTime)
            .numActionSnapshots(2)
            .putAllStatsByGroupAndMu(statsByGroupAndMu)
            .build();

        when(hourReader.query(any(), any(), any())).thenReturn(Collections.singletonList(qResult));
        when(statsBucketsFactory.arrangeIntoBuckets(groupBy,
            qResult.numActionSnapshots(),
            ImmutableMap.of(actionGroup, ImmutableMap.of(7, actionGroupStat)),
            targetMgmtSubunit)).thenReturn(buckets);

        when(buckets.toActionStats()).thenReturn(Stream.of(actionStat));

        final ActionStats stats = reader.readActionStats(query);

        verify(mgmtUnitSubgroupStore).query(eq(muFilter), eq(groupBy));
        verify(actionGroupStore).query(agFilter);
        verify(timeFrameCalculator).millis2TimeFrame(timeRange.getStartTime());
        verify(hourReader).query(timeRange, targetMgmtSubunit.keySet(), matchedActionGroups);
        verify(statsBucketsFactory).arrangeIntoBuckets(
            groupBy, qResult.numActionSnapshots(), statsByGroupAndMu, targetMgmtSubunit);

        when(buckets.toActionStats()).thenReturn(Stream.of(actionStat));

        assertThat(stats.getMgmtUnitId(), is(1L));
        assertThat(stats.getStatSnapshotsList(), contains(ActionStatSnapshot.newBuilder()
            .setTime(retStatTime.toInstant(ZoneOffset.UTC).toEpochMilli())
            .addStats(actionStat)
            .build()));
    }

    @Test
    public void testReadActionStatsZeroValues() {
        final HistoricalActionStatReader reader = new HistoricalActionStatReader(actionGroupStore,
            mgmtUnitSubgroupStore, timeFrameCalculator, tablesForTimeFrame, statsBucketsFactory);

        final MgmtUnitSubgroupFilter muFilter = MgmtUnitSubgroupFilter.newBuilder()
            .setMgmtUnitId(1)
            .build();
        final ActionGroupFilter agFilter = ActionGroupFilter.newBuilder()
            .addActionCategory(ActionCategory.PERFORMANCE_ASSURANCE)
            .build();
        final TimeRange timeRange = TimeRange.newBuilder()
            .setStartTime(1)
            .setEndTime(2)
            .build();
        final LocalDateTime retStatTime = LocalDateTime.ofEpochSecond(1, 0, ZoneOffset.UTC);
        final GroupBy groupBy = GroupBy.ACTION_CATEGORY;
        final HistoricalActionStatsQuery query = HistoricalActionStatsQuery.newBuilder()
            .setMgmtUnitSubgroupFilter(muFilter)
            .setActionGroupFilter(agFilter)
            .setTimeRange(timeRange)
            .setGroupBy(groupBy)
            .build();

        final ActionGroup actionGroup = mock(ActionGroup.class);
        final CombinedStatsBuckets buckets = mock(CombinedStatsBuckets.class);

        final Map<Integer, MgmtUnitSubgroup> targetMgmtSubunit = Collections.singletonMap(7, mock(MgmtUnitSubgroup.class));
        final MatchedActionGroups matchedActionGroups = ImmutableMatchedActionGroups.builder()
            .allActionGroups(false)
            .putSpecificActionGroupsById(9, actionGroup)
            .build();

        final QueryResultsFromSnapshot qResult = ImmutableQueryResultsFromSnapshot.builder()
            .time(retStatTime)
            .numActionSnapshots(2)
            .build();

        when(mgmtUnitSubgroupStore.query(eq(muFilter), any())).thenReturn(Optional.of(ImmutableQueryResult.builder()
            .mgmtUnit(1)
            .putAllMgmtUnitSubgroups(targetMgmtSubunit)
            .build()));
        when(actionGroupStore.query(agFilter)).thenReturn(Optional.of(matchedActionGroups));
        when(timeFrameCalculator.millis2TimeFrame(timeRange.getStartTime())).thenReturn(TimeFrame.HOUR);


        when(hourReader.query(any(), any(), any())).thenReturn(Collections.singletonList(qResult));
        when(statsBucketsFactory.arrangeIntoBuckets(groupBy, 2, Collections.emptyMap(), targetMgmtSubunit))
            .thenReturn(buckets);

        when(buckets.toActionStats()).thenReturn(Stream.empty());

        final ActionStats stats = reader.readActionStats(query);

        verify(mgmtUnitSubgroupStore).query(eq(muFilter), eq(groupBy));
        verify(actionGroupStore).query(agFilter);
        verify(timeFrameCalculator).millis2TimeFrame(timeRange.getStartTime());
        verify(hourReader).query(timeRange, targetMgmtSubunit.keySet(), matchedActionGroups);
        verify(statsBucketsFactory).arrangeIntoBuckets(groupBy, 2, Collections.emptyMap(), targetMgmtSubunit);
        verify(buckets).toActionStats();

        assertThat(stats.getMgmtUnitId(), is(1L));
        assertThat(stats.getStatSnapshotsList(), contains(ActionStatSnapshot.newBuilder()
            .setTime(retStatTime.toInstant(ZoneOffset.UTC).toEpochMilli())
            .build()));
    }

    @Test
    public void testBucketsNoSplit() {
        final int mgmtUnitSubgroupId = 1;
        final RolledUpActionGroupStat stat1 = mock(RolledUpActionGroupStat.class);
        when(stat1.maxEntityCount()).thenReturn(5);
        final RolledUpActionGroupStat stat2 = mock(RolledUpActionGroupStat.class);
        when(stat2.maxEntityCount()).thenReturn(4);
        final Map<ActionGroup, Map<Integer, RolledUpActionGroupStat>> statsByGroup = ImmutableMap.of(
            mock(ActionGroup.class), ImmutableMap.of(mgmtUnitSubgroupId, stat1),
            mock(ActionGroup.class), ImmutableMap.of(mgmtUnitSubgroupId, stat2));
        final CombinedStatsBuckets buckets =
            new DefaultBucketsFactory().arrangeIntoBuckets(GroupBy.NONE, 1, statsByGroup, Collections.emptyMap());
        final List<ActionDTO.ActionStat> stats = buckets.toActionStats().collect(Collectors.toList());
        assertThat(stats.size(), is(1));
        assertThat(stats.get(0), is(ActionDTO.ActionStat.newBuilder()
            .setStatGroup(StatGroup.getDefaultInstance())
            .setEntityCount(Value.newBuilder()
                .setMax(9)
                .setMin(0)
                .setAvg(0)
                .setTotal(0))
            .build()));
    }

    @Test
    public void testBucketsEmpty() {
        final CombinedStatsBuckets buckets =
            new DefaultBucketsFactory().arrangeIntoBuckets(GroupBy.ACTION_CATEGORY, 1, Collections.emptyMap(), Collections.emptyMap());
        assertThat(buckets.toActionStats().count(), is(0L));
    }

    private RolledUpActionGroupStat increasingGroupStat() {
        return ImmutableRolledUpActionGroupStat.builder()
            .priorActionCount(10)
            .newActionCount(12)
            .minActionCount(1)
            .avgActionCount(2)
            .maxActionCount(3)
            .minEntityCount(4)
            .avgEntityCount(5)
            .maxEntityCount(6)
            .minSavings(7)
            .avgSavings(8)
            .maxSavings(9)
            .minInvestment(10)
            .avgInvestment(11)
            .maxInvestment(12)
            .build();
    }

    private ActionDTO.ActionStat combinedIncreasingStats(final int numGroupStats,
                                                 final ActionDTO.ActionStat.Builder template) {
        final RolledUpActionGroupStat increasingGroupStat = increasingGroupStat();
        final int actionCount = increasingGroupStat.priorActionCount()
            + increasingGroupStat.newActionCount();
        return template
            .setActionCount(Value.newBuilder()
                .setMin(numGroupStats * increasingGroupStat.minActionCount())
                .setAvg(numGroupStats * increasingGroupStat.avgActionCount())
                .setMax(numGroupStats * increasingGroupStat.maxActionCount())
                .setTotal(numGroupStats * actionCount))
            .setEntityCount(Value.newBuilder()
                .setMin(numGroupStats * increasingGroupStat.minEntityCount())
                .setAvg(numGroupStats * increasingGroupStat.avgEntityCount())
                .setMax(numGroupStats * increasingGroupStat.maxEntityCount())
                .setTotal(numGroupStats * increasingGroupStat.avgEntityCount()))
            .setSavings(Value.newBuilder()
                .setMin(numGroupStats * increasingGroupStat.minSavings())
                .setAvg(numGroupStats * increasingGroupStat.avgSavings())
                .setMax(numGroupStats * increasingGroupStat.maxSavings())
                .setTotal(numGroupStats * increasingGroupStat.avgSavings()))
            .setInvestments(Value.newBuilder()
                .setMin(numGroupStats * increasingGroupStat.minInvestment())
                .setAvg(numGroupStats * increasingGroupStat.avgInvestment())
                .setMax(numGroupStats * increasingGroupStat.maxInvestment())
                .setTotal(numGroupStats * increasingGroupStat.avgInvestment()))
            .build();
    }

    @Test
    public void testBucketsGroupByCategory() {
        final RolledUpActionGroupStat perfAssuranceStat1 = increasingGroupStat();
        final RolledUpActionGroupStat perfAssuranceStat2 = increasingGroupStat();
        final RolledUpActionGroupStat efficiencyImprovementStat = increasingGroupStat();
        final int mgmtUnitSubgroupId = 1;

        final ActionGroup perfAssuranceAg1 = mock(ActionGroup.class);
        final ActionGroupKey key1 = mock(ActionGroupKey.class);
        when(key1.getCategory()).thenReturn(ActionCategory.PERFORMANCE_ASSURANCE);
        when(perfAssuranceAg1.key()).thenReturn(key1);

        final ActionGroup perfAssuranceAg2 = mock(ActionGroup.class);
        final ActionGroupKey key2 = mock(ActionGroupKey.class);
        when(key2.getCategory()).thenReturn(ActionCategory.PERFORMANCE_ASSURANCE);
        when(perfAssuranceAg2.key()).thenReturn(key2);

        final ActionGroup efficiencyImprovementAg = mock(ActionGroup.class);
        final ActionGroupKey key3 = mock(ActionGroupKey.class);
        when(key3.getCategory()).thenReturn(ActionCategory.EFFICIENCY_IMPROVEMENT);
        when(efficiencyImprovementAg.key()).thenReturn(key3);

        final Map<ActionGroup, Map<Integer, RolledUpActionGroupStat>> statsByGroup = ImmutableMap.of(
            perfAssuranceAg1, ImmutableMap.of(mgmtUnitSubgroupId, perfAssuranceStat1),
            perfAssuranceAg2, ImmutableMap.of(mgmtUnitSubgroupId, perfAssuranceStat2),
            efficiencyImprovementAg, ImmutableMap.of(mgmtUnitSubgroupId, efficiencyImprovementStat));

        final CombinedStatsBuckets buckets =
            new DefaultBucketsFactory().arrangeIntoBuckets(GroupBy.ACTION_CATEGORY, 1, statsByGroup, Collections.emptyMap());
        final List<ActionDTO.ActionStat> stats = buckets.toActionStats()
            .collect(Collectors.toList());
        assertThat(stats.size(), is(2));
        assertThat(stats, containsInAnyOrder(
            combinedIncreasingStats(2, ActionDTO.ActionStat.newBuilder()
                    .setStatGroup(ActionDTO.ActionStat.StatGroup.newBuilder()
                .setActionCategory(ActionCategory.PERFORMANCE_ASSURANCE))),
            combinedIncreasingStats(1, ActionDTO.ActionStat.newBuilder()
                .setStatGroup(ActionDTO.ActionStat.StatGroup.newBuilder()
                .setActionCategory(ActionCategory.EFFICIENCY_IMPROVEMENT)))));
    }

    @Test
    public void testBucketsGroupByState() {
        final RolledUpActionGroupStat readyStat1 = increasingGroupStat();
        final RolledUpActionGroupStat readyStat2 = increasingGroupStat();
        final RolledUpActionGroupStat queuedStat = increasingGroupStat();
        final int mgmtUnitSubgroup = 1;

        final ActionGroup readyAg1 = mock(ActionGroup.class);
        final ActionGroupKey key1 = mock(ActionGroupKey.class);
        when(key1.getActionState()).thenReturn(ActionState.READY);
        when(readyAg1.key()).thenReturn(key1);

        final ActionGroup readyAg2 = mock(ActionGroup.class);
        final ActionGroupKey key2 = mock(ActionGroupKey.class);
        when(key2.getActionState()).thenReturn(ActionState.READY);
        when(readyAg2.key()).thenReturn(key2);

        final ActionGroup queuedAg = mock(ActionGroup.class);
        final ActionGroupKey key3 = mock(ActionGroupKey.class);
        when(key3.getActionState()).thenReturn(ActionState.QUEUED);
        when(queuedAg.key()).thenReturn(key3);

        final Map<ActionGroup, Map<Integer, RolledUpActionGroupStat>> statsByGroup = ImmutableMap.of(
            readyAg1, ImmutableMap.of(mgmtUnitSubgroup, readyStat1),
            readyAg2, ImmutableMap.of(mgmtUnitSubgroup, readyStat2),
            queuedAg, ImmutableMap.of(mgmtUnitSubgroup, queuedStat));

        final CombinedStatsBuckets buckets =
            new DefaultBucketsFactory().arrangeIntoBuckets(GroupBy.ACTION_STATE, 1, statsByGroup, Collections.emptyMap());
        final List<ActionDTO.ActionStat> stats = buckets.toActionStats()
            .collect(Collectors.toList());
        assertThat(stats.size(), is(2));
        assertThat(stats, containsInAnyOrder(
            combinedIncreasingStats(2, ActionDTO.ActionStat.newBuilder()
                .setStatGroup(ActionDTO.ActionStat.StatGroup.newBuilder()
                    .setActionState(ActionState.READY))),
            combinedIncreasingStats(1, ActionDTO.ActionStat.newBuilder()
                .setStatGroup(ActionDTO.ActionStat.StatGroup.newBuilder()
                    .setActionState(ActionState.QUEUED)))));
    }

    private MgmtUnitSubgroup makeBaSubgroup(final long baId, final int subgroupId) {
        return ImmutableMgmtUnitSubgroup.builder()
            .id(subgroupId)
            .key(ImmutableMgmtUnitSubgroupKey.builder()
                .mgmtUnitId(baId)
                .mgmtUnitType(ManagementUnitType.BUSINESS_ACCOUNT)
                .environmentType(EnvironmentType.CLOUD)
                .build())
            .build();
    }

    /**
     * Test grouping by business accounts.
     */
    @Test
    public void testBucketsGroupByBusinessAccount() {
        final long ba1Id = 182;
        final long ba2Id = 282;
        MgmtUnitSubgroup ba1Subgroup = makeBaSubgroup(ba1Id, 1);
        MgmtUnitSubgroup ba2Subgroup = makeBaSubgroup(ba2Id, 2);
        final ActionGroup ag1 = mock(ActionGroup.class);
        final ActionGroup ag2 = mock(ActionGroup.class);
        final RolledUpActionGroupStat increasingStat = increasingGroupStat();
        final Map<ActionGroup, Map<Integer, RolledUpActionGroupStat>> statsByGroup = ImmutableMap.of(
            ag1, ImmutableMap.of(
                ba1Subgroup.id(), increasingStat,
                ba2Subgroup.id(), increasingStat),
            ag2, ImmutableMap.of(
                ba1Subgroup.id(), increasingStat
            ));
        final CombinedStatsBuckets buckets =
            new DefaultBucketsFactory().arrangeIntoBuckets(GroupBy.BUSINESS_ACCOUNT_ID, 1,
                statsByGroup, ImmutableMap.of(ba1Subgroup.id(), ba1Subgroup, ba2Subgroup.id(), ba2Subgroup));
        final Map<Long, ActionDTO.ActionStat> statsByBa = buckets.toActionStats()
            .collect(Collectors.toMap(stat -> stat.getStatGroup().getBusinessAccountId(), Function.identity()));
        assertThat(statsByBa.keySet(), containsInAnyOrder(ba1Id, ba2Id));
        assertThat(statsByBa.get(ba1Id), is(combinedIncreasingStats(2, ActionDTO.ActionStat.newBuilder()
            .setStatGroup(ActionDTO.ActionStat.StatGroup.newBuilder()
                .setBusinessAccountId(ba1Id)))));
        assertThat(statsByBa.get(ba2Id), is(combinedIncreasingStats(1, ActionDTO.ActionStat.newBuilder()
            .setStatGroup(ActionDTO.ActionStat.StatGroup.newBuilder()
                .setBusinessAccountId(ba2Id)))));
    }

    @Test
    public void testBucketsCombineMgmtUnitSubgroups() {
        final ActionGroup ag1 = mock(ActionGroup.class);
        final RolledUpActionGroupStat increasingStat = increasingGroupStat();
        final int mgmtUnitSubgroup1 = 1;
        final int mgmtUnitSubgroup2 = 2;
        final Map<ActionGroup, Map<Integer, RolledUpActionGroupStat>> statsByGroup = ImmutableMap.of(
            ag1, ImmutableMap.of(
                mgmtUnitSubgroup1, increasingStat,
                mgmtUnitSubgroup2, increasingStat));
        final CombinedStatsBuckets buckets =
            new DefaultBucketsFactory().arrangeIntoBuckets(GroupBy.NONE, 1, statsByGroup, Collections.emptyMap());
        final List<ActionDTO.ActionStat> stats = buckets.toActionStats()
            .collect(Collectors.toList());
        assertThat(stats.size(), is(1));
        assertThat(stats.get(0), is(ActionDTO.ActionStat.newBuilder()
            .setStatGroup(StatGroup.getDefaultInstance())
            .setActionCount(
                Value.newBuilder()
                .setMin(increasingStat.minActionCount() * 2)
                .setAvg(increasingStat.avgActionCount() * 2)
                .setMax(increasingStat.maxActionCount() * 2)
                .setTotal((increasingStat.newActionCount() + increasingStat.priorActionCount()) * 2)
            )
            .setEntityCount(
                Value.newBuilder()
                    .setMin(increasingStat.minEntityCount() * 2)
                    .setAvg(increasingStat.avgEntityCount() * 2)
                    .setMax(increasingStat.maxEntityCount() * 2)
                    // Total is avg * num snapshots, which is 1
                    .setTotal(increasingStat.avgEntityCount() * 2)
            )
            .setSavings(
                Value.newBuilder()
                    .setMin(increasingStat.minSavings() * 2)
                    .setAvg(increasingStat.avgSavings() * 2)
                    .setMax(increasingStat.maxSavings() * 2)
                    // Total is avg * num snapshots, which is 1
                    .setTotal(increasingStat.avgSavings() * 2)
            )
            .setInvestments(
                Value.newBuilder()
                    .setMin(increasingStat.minInvestment() * 2)
                    .setAvg(increasingStat.avgInvestment() * 2)
                    .setMax(increasingStat.maxInvestment() * 2)
                    // Total is avg * num snapshots, which is 1
                    .setTotal(increasingStat.avgInvestment() * 2)
            )
            .build()));
    }

    @Test
    public void testBucketsTotal() {
        final ActionGroup ag1 = mock(ActionGroup.class);
        final RolledUpActionGroupStat increasingStat = increasingGroupStat();
        final int mgmtUnitSubgroup1 = 1;
        final int numSnapshots = 3;
        final Map<ActionGroup, Map<Integer, RolledUpActionGroupStat>> statsByGroup = ImmutableMap.of(
            ag1, ImmutableMap.of(
                mgmtUnitSubgroup1, increasingStat));
        final CombinedStatsBuckets buckets =
            new DefaultBucketsFactory().arrangeIntoBuckets(GroupBy.NONE, numSnapshots, statsByGroup, Collections.emptyMap());
        final List<ActionDTO.ActionStat> stats = buckets.toActionStats()
            .collect(Collectors.toList());
        assertThat(stats.size(), is(1));
        ActionDTO.ActionStat stat = stats.get(0);
        assertThat(stat.getActionCount().getTotal(), is(Double.valueOf(increasingStat.newActionCount()
            + increasingStat.priorActionCount())));
        assertThat(stat.getEntityCount().getTotal(), is(increasingStat.avgEntityCount() * 3));
        assertThat(stat.getSavings().getTotal(), is(increasingStat.avgSavings() * 3));
        assertThat(stat.getInvestments().getTotal(), is(increasingStat.avgInvestment() * 3));
    }
}

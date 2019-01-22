package com.vmturbo.action.orchestrator.stats;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.action.orchestrator.stats.LiveActionStatReader.CombinedStatsBuckets;
import com.vmturbo.action.orchestrator.stats.LiveActionStatReader.CombinedStatsBucketsFactory;
import com.vmturbo.action.orchestrator.stats.LiveActionStatReader.CombinedStatsBucketsFactory.DefaultBucketsFactory;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup.ActionGroupKey;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroupStore;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroupStore.MatchedActionGroups;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableMatchedActionGroups;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableQueryResult;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroupStore;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat;
import com.vmturbo.action.orchestrator.stats.rollup.ImmutableRolledUpActionGroupStat;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStat.Value;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStats;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStats.ActionStatSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionCountsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionCountsQuery.ActionGroupFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionCountsQuery.GroupBy;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionCountsQuery.MgmtUnitSubgroupFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionCountsQuery.TimeRange;
import com.vmturbo.components.api.TimeFrameCalculator;
import com.vmturbo.components.api.TimeFrameCalculator.TimeFrame;

public class LiveActionStatReaderTest {

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
        final LiveActionStatReader reader = new LiveActionStatReader(actionGroupStore,
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
        final HistoricalActionCountsQuery query = HistoricalActionCountsQuery.newBuilder()
            .setMgmtUnitSubgroupFilter(muFilter)
            .setActionGroupFilter(agFilter)
            .setTimeRange(timeRange)
            .setGroupBy(groupBy)
            .build();

        final ActionGroup actionGroup = mock(ActionGroup.class);
        final RolledUpActionGroupStat actionGroupStat = mock(RolledUpActionGroupStat.class);
        final Map<LocalDateTime, Map<ActionGroup, RolledUpActionGroupStat>> retStats = new HashMap<>();
        retStats.put(retStatTime, ImmutableMap.of(actionGroup, actionGroupStat));

        final ActionDTO.ActionStat actionStat = ActionDTO.ActionStat.newBuilder()
            // Something to distinguish it from the default instance.
            .setActionCategory(ActionCategory.PERFORMANCE_ASSURANCE)
            .build();

        final CombinedStatsBuckets buckets = mock(CombinedStatsBuckets.class);

        final Set<Integer> targetMgmtSubunit = Collections.singleton(7);
        final MatchedActionGroups matchedActionGroups = ImmutableMatchedActionGroups.builder()
            .allActionGroups(false)
            .putSpecificActionGroupsById(9, actionGroup)
            .build();

        when(mgmtUnitSubgroupStore.query(muFilter)).thenReturn(Optional.of(ImmutableQueryResult.builder()
            .mgmtUnit(1)
            .addAllMgmtUnitSubgroups(targetMgmtSubunit)
            .build()));
        when(actionGroupStore.query(agFilter)).thenReturn(Optional.of(matchedActionGroups));
        when(timeFrameCalculator.millis2TimeFrame(timeRange.getStartTime())).thenReturn(TimeFrame.HOUR);


        when(hourReader.query(any(), any(), any())).thenReturn(retStats);
        when(statsBucketsFactory.arrangeIntoBuckets(groupBy, ImmutableMap.of(actionGroup, actionGroupStat)))
            .thenReturn(buckets);

        when(buckets.toActionStats()).thenReturn(Stream.of(actionStat));

        final ActionStats stats = reader.readActionStats(query);

        verify(mgmtUnitSubgroupStore).query(muFilter);
        verify(actionGroupStore).query(agFilter);
        verify(timeFrameCalculator).millis2TimeFrame(timeRange.getStartTime());
        verify(hourReader).query(timeRange, targetMgmtSubunit, matchedActionGroups);
        verify(statsBucketsFactory).arrangeIntoBuckets(groupBy, ImmutableMap.of(actionGroup, actionGroupStat));

        when(buckets.toActionStats()).thenReturn(Stream.of(actionStat));

        assertThat(stats.getMgmtUnitId(), is(1L));
        assertThat(stats.getStatSnapshotsList(), contains(ActionStatSnapshot.newBuilder()
            .setTime(retStatTime.toInstant(ZoneOffset.UTC).toEpochMilli())
            .addStats(actionStat)
            .build()));
    }

    @Test
    public void testReadActionStatsZeroValues() {
        final LiveActionStatReader reader = new LiveActionStatReader(actionGroupStore,
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
        final HistoricalActionCountsQuery query = HistoricalActionCountsQuery.newBuilder()
            .setMgmtUnitSubgroupFilter(muFilter)
            .setActionGroupFilter(agFilter)
            .setTimeRange(timeRange)
            .setGroupBy(groupBy)
            .build();

        final ActionGroup actionGroup = mock(ActionGroup.class);
        final Map<LocalDateTime, Map<ActionGroup, RolledUpActionGroupStat>> retStats = new HashMap<>();
        retStats.put(retStatTime, Collections.emptyMap());
        final CombinedStatsBuckets buckets = mock(CombinedStatsBuckets.class);

        final Set<Integer> targetMgmtSubunit = Collections.singleton(7);
        final MatchedActionGroups matchedActionGroups = ImmutableMatchedActionGroups.builder()
            .allActionGroups(false)
            .putSpecificActionGroupsById(9, actionGroup)
            .build();

        when(mgmtUnitSubgroupStore.query(muFilter)).thenReturn(Optional.of(ImmutableQueryResult.builder()
            .mgmtUnit(1)
            .addAllMgmtUnitSubgroups(targetMgmtSubunit)
            .build()));
        when(actionGroupStore.query(agFilter)).thenReturn(Optional.of(matchedActionGroups));
        when(timeFrameCalculator.millis2TimeFrame(timeRange.getStartTime())).thenReturn(TimeFrame.HOUR);


        when(hourReader.query(any(), any(), any())).thenReturn(retStats);
        when(statsBucketsFactory.arrangeIntoBuckets(groupBy, Collections.emptyMap()))
            .thenReturn(buckets);

        when(buckets.toActionStats()).thenReturn(Stream.empty());

        final ActionStats stats = reader.readActionStats(query);

        verify(mgmtUnitSubgroupStore).query(muFilter);
        verify(actionGroupStore).query(agFilter);
        verify(timeFrameCalculator).millis2TimeFrame(timeRange.getStartTime());
        verify(hourReader).query(timeRange, targetMgmtSubunit, matchedActionGroups);
        verify(statsBucketsFactory).arrangeIntoBuckets(groupBy, Collections.emptyMap());
        verify(buckets).toActionStats();

        assertThat(stats.getMgmtUnitId(), is(1L));
        assertThat(stats.getStatSnapshotsList(), contains(ActionStatSnapshot.newBuilder()
            .setTime(retStatTime.toInstant(ZoneOffset.UTC).toEpochMilli())
            .build()));
    }

    @Test
    public void testBucketsNoSplit() {
        final RolledUpActionGroupStat stat1 = mock(RolledUpActionGroupStat.class);
        when(stat1.maxEntityCount()).thenReturn(5);
        final RolledUpActionGroupStat stat2 = mock(RolledUpActionGroupStat.class);
        when(stat2.maxEntityCount()).thenReturn(4);
        final Map<ActionGroup, RolledUpActionGroupStat> statsByGroup = ImmutableMap.of(
            mock(ActionGroup.class), stat1,
            mock(ActionGroup.class), stat2);
        final CombinedStatsBuckets buckets =
            new DefaultBucketsFactory().arrangeIntoBuckets(GroupBy.NONE, statsByGroup);
        final List<ActionDTO.ActionStat> stats = buckets.toActionStats().collect(Collectors.toList());
        assertThat(stats.size(), is(1));
        assertThat(stats.get(0), is(ActionDTO.ActionStat.newBuilder()
            .setEntityCount(Value.newBuilder()
                .setMax(9)
                .setMin(0)
                .setAvg(0))
            .build()));
    }

    @Test
    public void testBucketsEmpty() {
        final CombinedStatsBuckets buckets =
            new DefaultBucketsFactory().arrangeIntoBuckets(GroupBy.ACTION_CATEGORY, Collections.emptyMap());
        assertThat(buckets.toActionStats().count(), is(0L));
    }

    private RolledUpActionGroupStat increasingGroupStat() {
        return ImmutableRolledUpActionGroupStat.builder()
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
        return template
            .setActionCount(Value.newBuilder()
                .setMin(numGroupStats * increasingGroupStat.minActionCount())
                .setAvg(numGroupStats * increasingGroupStat.avgActionCount())
                .setMax(numGroupStats * increasingGroupStat.maxActionCount()))
            .setEntityCount(Value.newBuilder()
                .setMin(numGroupStats * increasingGroupStat.minEntityCount())
                .setAvg(numGroupStats * increasingGroupStat.avgEntityCount())
                .setMax(numGroupStats * increasingGroupStat.maxEntityCount()))
            .setSavings(Value.newBuilder()
                .setMin(numGroupStats * increasingGroupStat.minSavings())
                .setAvg(numGroupStats * increasingGroupStat.avgSavings())
                .setMax(numGroupStats * increasingGroupStat.maxSavings()))
            .setInvestments(Value.newBuilder()
                .setMin(numGroupStats * increasingGroupStat.minInvestment())
                .setAvg(numGroupStats * increasingGroupStat.avgInvestment())
                .setMax(numGroupStats * increasingGroupStat.maxInvestment()))
            .build();
    }

    @Test
    public void testBucketsGroupByCategory() {
        final RolledUpActionGroupStat perfAssuranceStat1 = increasingGroupStat();
        final RolledUpActionGroupStat perfAssuranceStat2 = increasingGroupStat();
        final RolledUpActionGroupStat efficiencyImprovementStat = increasingGroupStat();

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

        final Map<ActionGroup, RolledUpActionGroupStat> statsByGroup = ImmutableMap.of(
            perfAssuranceAg1, perfAssuranceStat1,
            perfAssuranceAg2, perfAssuranceStat2,
            efficiencyImprovementAg, efficiencyImprovementStat);

        final CombinedStatsBuckets buckets =
            new DefaultBucketsFactory().arrangeIntoBuckets(GroupBy.ACTION_CATEGORY, statsByGroup);
        final List<ActionDTO.ActionStat> stats = buckets.toActionStats()
            .collect(Collectors.toList());
        assertThat(stats.size(), is(2));
        assertThat(stats, containsInAnyOrder(
            combinedIncreasingStats(2, ActionDTO.ActionStat.newBuilder()
                .setActionCategory(ActionCategory.PERFORMANCE_ASSURANCE)),
            combinedIncreasingStats(1, ActionDTO.ActionStat.newBuilder()
                .setActionCategory(ActionCategory.EFFICIENCY_IMPROVEMENT))));
    }

    @Test
    public void testBucketsGroupByState() {
        final RolledUpActionGroupStat readyStat1 = increasingGroupStat();
        final RolledUpActionGroupStat readyStat2 = increasingGroupStat();
        final RolledUpActionGroupStat queuedStat = increasingGroupStat();

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

        final Map<ActionGroup, RolledUpActionGroupStat> statsByGroup = ImmutableMap.of(
            readyAg1, readyStat1,
            readyAg2, readyStat2,
            queuedAg, queuedStat);

        final CombinedStatsBuckets buckets =
            new DefaultBucketsFactory().arrangeIntoBuckets(GroupBy.ACTION_STATE, statsByGroup);
        final List<ActionDTO.ActionStat> stats = buckets.toActionStats()
            .collect(Collectors.toList());
        assertThat(stats.size(), is(2));
        assertThat(stats, containsInAnyOrder(
            combinedIncreasingStats(2, ActionDTO.ActionStat.newBuilder()
                .setActionState(ActionState.READY)),
            combinedIncreasingStats(1, ActionDTO.ActionStat.newBuilder()
                .setActionState(ActionState.QUEUED))));
    }
}

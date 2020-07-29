package com.vmturbo.api.component.external.api.util.action;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Test;

import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor.ActionStatsQuery;
import com.vmturbo.api.component.external.api.util.action.GroupByFilters.GroupByFiltersFactory;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.ActionCostType;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStat;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStat.Value;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStats;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStats.ActionStatSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStat;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStat.StatGroup;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.common.protobuf.utils.StringConstants;

public class ActionStatsMapperTest {

    private GroupByFiltersFactory groupByFiltersFactory = mock(GroupByFiltersFactory.class);

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    @Test
    public void testMapCurrentActionStat() {
        final CurrentActionStat stat1 = CurrentActionStat.newBuilder()
            .setActionCount(3)
            .setEntityCount(4)
            .build();

        final GroupByFilters stat1Filters = mock(GroupByFilters.class);
        final List<StatFilterApiDTO> stat1ApiFilters = Collections.singletonList(new StatFilterApiDTO());
        when(stat1Filters.getFilters()).thenReturn(stat1ApiFilters);

        final ActionStatsQuery query = mock(ActionStatsQuery.class);
        when(query.getCostType()).thenReturn(Optional.empty());
        when(query.currentTimeStamp()).thenReturn(Optional.empty());
        when(groupByFiltersFactory.filtersForQuery(query)).thenReturn(stat1Filters);

        final StatSnapshotApiDTO snapshots = new ActionStatsMapper(clock, groupByFiltersFactory)
            .currentActionStatsToApiSnapshot(Arrays.asList(stat1), query, Maps.newHashMap(), Maps.newHashMap());

        assertThat(snapshots.getDate(), is(DateTimeUtil.toString(clock.millis())));

        final Map<String, StatApiDTO> statsByName = snapshots.getStatistics().stream()
            .collect(Collectors.toMap(StatApiDTO::getName, Function.identity()));

        verify(stat1Filters, times(2)).getFilters();
        verifyNoMoreInteractions(stat1Filters);

        assertThat(statsByName.keySet(), containsInAnyOrder(StringConstants.NUM_ACTIONS,
            StringConstants.NUM_ENTITIES));

        final StatApiDTO actions = statsByName.get(StringConstants.NUM_ACTIONS);
        assertThat(actions.getFilters(), is(stat1ApiFilters));
        assertThat(actions.getName(), is(StringConstants.NUM_ACTIONS));
        assertThat(actions.getValue(), is(3.0f));
        assertThat(actions.getValues().getAvg(), is(3.0f));
        assertThat(actions.getValues().getMin(), is(3.0f));
        assertThat(actions.getValues().getMax(), is(3.0f));
        assertThat(actions.getValues().getTotal(), is(3.0f));

        final StatApiDTO entities = statsByName.get(StringConstants.NUM_ENTITIES);
        assertThat(entities.getFilters(), is(stat1ApiFilters));
        assertThat(entities.getName(), is(StringConstants.NUM_ENTITIES));
        assertThat(entities.getValue(), is(4.0f));
        assertThat(entities.getValues().getAvg(), is(4.0f));
        assertThat(entities.getValues().getMin(), is(4.0f));
        assertThat(entities.getValues().getMax(), is(4.0f));
        assertThat(entities.getValues().getTotal(), is(4.0f));
    }

    @Test
    public void testMapCurrentActionStatFilters() {

        final CurrentActionStat stat1 = CurrentActionStat.newBuilder()
            .setStatGroup(StatGroup.newBuilder()
                .setTargetEntityType(10)
                .setReasonCommodityBaseType(1)
                .setActionCategory(ActionCategory.PERFORMANCE_ASSURANCE)
                .setActionState(ActionState.READY)
                .setActionType(ActionType.ACTIVATE)
                .setCostType(ActionDTO.ActionCostType.SAVINGS)
                .setSeverity(ActionDTO.Severity.CRITICAL)
                .setActionRelatedRisk("Mem congestion")
                .setTargetEntityId(111))
            .setActionCount(3)
            .setEntityCount(4)
            .build();

        final CurrentActionStat stat2 = CurrentActionStat.newBuilder()
            .setInvestments(1.0)
            .setSavings(2.0)
            .setActionCount(3)
            .setEntityCount(4)
            .build();

        final ActionStatsQuery query = mock(ActionStatsQuery.class);
        when(query.getCostType()).thenReturn(Optional.empty());
        when(query.currentTimeStamp()).thenReturn(Optional.empty());
        ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        inputDTO.setGroupBy(new ArrayList<>());
        when(query.actionInput()).thenReturn(inputDTO);

        final GroupByFilters stat1Filters = mock(GroupByFilters.class);
        final List<StatFilterApiDTO> stat1ApiFilters = Lists.newArrayList(new StatFilterApiDTO());
        when(stat1Filters.getFilters()).thenReturn(stat1ApiFilters);

        final GroupByFilters stat2Filters = mock(GroupByFilters.class);
        final List<StatFilterApiDTO> stat2ApiFilters = Lists.newArrayList(new StatFilterApiDTO());
        when(stat2Filters.getFilters()).thenReturn(stat2ApiFilters);

        when(groupByFiltersFactory.filtersForQuery(query)).thenReturn(stat1Filters, stat2Filters);

        new ActionStatsMapper(clock, groupByFiltersFactory)
            .currentActionStatsToApiSnapshot(Arrays.asList(stat1, stat2), query, Maps.newHashMap(), Maps.newHashMap());

        // Should have been called twice - one for each action stat.
        verify(groupByFiltersFactory, times(2)).filtersForQuery(any());
        verify(stat1Filters).setCategory(ActionCategory.PERFORMANCE_ASSURANCE);
        verify(stat1Filters).setState(ActionState.READY);
        verify(stat1Filters).setType(ActionType.ACTIVATE);
        verify(stat1Filters).setReasonCommodity(1);
        verify(stat1Filters).setTargetEntityType(10);
        verify(stat1Filters).setTargetEntityId(111);
        verify(stat1Filters).setActionCostType(ActionDTO.ActionCostType.SAVINGS);
        verify(stat1Filters).setActionRiskSeverity(ActionDTO.Severity.CRITICAL);
        verify(stat1Filters).setRelatedRisk("Mem congestion");
        // We get the filters for each StatApiDTO.
        verify(stat1Filters, times(2)).getFilters();
        verifyNoMoreInteractions(stat1Filters);

        // Stat2 has investments and savings.
        verify(stat2Filters, times(4)).getFilters();
        verifyNoMoreInteractions(stat2Filters);
    }

    @Test
    public void tempNoSavingsOrInvestmentsShouldNotAddStats() {
        final CurrentActionStat stat = CurrentActionStat.newBuilder()
            .setInvestments(0.0)
            .setSavings(0.0)
            .build();

        final ActionStatsQuery query = mock(ActionStatsQuery.class);
        when(query.getCostType()).thenReturn(Optional.empty());
        when(query.currentTimeStamp()).thenReturn(Optional.empty());
        ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        inputDTO.setGroupBy(new ArrayList<>());
        when(query.actionInput()).thenReturn(inputDTO);

        StatSnapshotApiDTO stats = new ActionStatsMapper(clock, groupByFiltersFactory)
            .currentActionStatsToApiSnapshot(Arrays.asList(stat), query, Maps.newHashMap(), Maps.newHashMap());

        List<StatApiDTO> statsList = stats.getStatistics();

        // Because we we set the investment and savings to 0, there shouldn't be any stats.
        assertEquals(statsList.size(),0);
    }

    @Test
    public void testMapHistoricalActionStat() {
        final long emptyTime = 100;
        final long statTime = 700;
        final ActionStats actionStats = ActionStats.newBuilder()
            .setMgmtUnitId(7)
            .addStatSnapshots(ActionStatSnapshot.newBuilder()
                .setTime(emptyTime)
                .build())
            .addStatSnapshots(ActionStatSnapshot.newBuilder()
                .setTime(statTime)
                .addStats(ActionStat.newBuilder()
                    .setStatGroup(ActionStat.StatGroup.newBuilder()
                        .setActionState(ActionDTO.ActionState.READY)
                        .setActionCategory(ActionCategory.EFFICIENCY_IMPROVEMENT))
                    .setActionCount(Value.newBuilder()
                        .setMin(1)
                        .setAvg(2)
                        .setMax(3)
                        .setTotal(7))
                    .setEntityCount(Value.newBuilder()
                        .setMin(4)
                        .setAvg(5)
                        .setMax(6)
                        .setTotal(8))))
            .build();

        final ActionStatsQuery query = mock(ActionStatsQuery.class);
        final ActionApiInputDTO actionInput = mock(ActionApiInputDTO.class);
        when(actionInput.getGroupBy())
            .thenReturn(Arrays.asList(StringConstants.RISK_SUB_CATEGORY, StringConstants.ACTION_STATES));
        when(query.actionInput()).thenReturn(actionInput);
        when(query.getCostType()).thenReturn(Optional.empty());

        final GroupByFilters groupByFilters = mock(GroupByFilters.class);
        final List<StatFilterApiDTO> apiFilters = Collections.singletonList(new StatFilterApiDTO());
        when(groupByFilters.getFilters()).thenReturn(apiFilters);
        when(groupByFiltersFactory.filtersForQuery(query)).thenReturn(groupByFilters);


        final List<StatSnapshotApiDTO> snapshots =
            new ActionStatsMapper(clock, groupByFiltersFactory).historicalActionStatsToApiSnapshots(actionStats, query);

        verify(groupByFilters).setState(ActionState.READY);
        verify(groupByFilters).setCategory(ActionCategory.EFFICIENCY_IMPROVEMENT);

        assertThat(snapshots.size(), is(2));
        final StatSnapshotApiDTO emptySnapshot = snapshots.get(0);
        assertThat(emptySnapshot.getDate(), is(DateTimeUtil.toString(emptyTime)));
        assertThat(emptySnapshot.getStatistics(), is(Collections.emptyList()));

        final StatSnapshotApiDTO statSnapshot = snapshots.get(1);
        assertThat(statSnapshot.getDate(), is(DateTimeUtil.toString(statTime)));
        assertThat(statSnapshot.getStatistics().size(), is(2));
        final Map<String, StatApiDTO> statsByName = statSnapshot.getStatistics().stream()
            .collect(Collectors.toMap(StatApiDTO::getName, Function.identity()));
        final StatApiDTO numActions = statsByName.get(StringConstants.NUM_ACTIONS);
        assertNotNull(numActions);
        assertThat(numActions.getFilters(), is(apiFilters));
        assertThat(numActions.getValue(), is(2.0f));
        assertThat(numActions.getValues().getMin(), is(1.0f));
        assertThat(numActions.getValues().getAvg(), is(2.0f));
        assertThat(numActions.getValues().getMax(), is(3.0f));

        final StatApiDTO numEntities = statsByName.get(StringConstants.NUM_ENTITIES);
        assertNotNull(numEntities);
        assertThat(numEntities.getFilters(), is(apiFilters));
        assertThat(numEntities.getValue(), is(5.0f));

        assertThat(numEntities.getValues().getMin(), is(4.0f));
        assertThat(numEntities.getValues().getAvg(), is(5.0f));
        assertThat(numEntities.getValues().getMax(), is(6.0f));
    }

    @Test
    public void testMapHistoricalActionStatSavings() {
        final long statTime = 700;
        final ActionStats actionStats = ActionStats.newBuilder()
            .setMgmtUnitId(7)
            .addStatSnapshots(ActionStatSnapshot.newBuilder()
                .setTime(statTime)
                .addStats(ActionStat.newBuilder()
                    .setStatGroup(ActionStat.StatGroup.newBuilder()
                        .setActionState(ActionDTO.ActionState.READY)
                        .setActionCategory(ActionCategory.EFFICIENCY_IMPROVEMENT))
                    .setSavings(Value.newBuilder()
                        .setMin(1)
                        .setAvg(2)
                        .setMax(3)
                        .setTotal(4))))
            .build();

        final ActionStatsQuery query = mock(ActionStatsQuery.class);
        final ActionApiInputDTO actionInput = mock(ActionApiInputDTO.class);
        when(actionInput.getGroupBy()).thenReturn(Collections.emptyList());
        when(query.actionInput()).thenReturn(actionInput);
        when(query.getCostType()).thenReturn(Optional.of(ActionCostType.SAVING));

        final GroupByFilters groupByFilters = mock(GroupByFilters.class);
        final List<StatFilterApiDTO> apiFilters = Lists.newArrayList(new StatFilterApiDTO());
        when(groupByFilters.getFilters()).thenReturn(apiFilters);
        when(groupByFiltersFactory.filtersForQuery(query)).thenReturn(groupByFilters);


        final List<StatSnapshotApiDTO> snapshots =
            new ActionStatsMapper(clock, groupByFiltersFactory).historicalActionStatsToApiSnapshots(actionStats, query);

        assertThat(snapshots.size(), is(1));
        final StatSnapshotApiDTO snapshot = snapshots.get(0);
        assertThat(snapshot.getDate(), is(DateTimeUtil.toString(statTime)));
        assertThat(snapshot.getStatistics().size(), is(1));

        final StatApiDTO savingsStat = snapshot.getStatistics().get(0);
        assertThat(savingsStat.getName(), is(StringConstants.COST_PRICE));
        assertThat(savingsStat.getFilters(), is(apiFilters));
        assertThat(savingsStat.getValue(), is(2.0f));
        assertThat(savingsStat.getValues().getMin(), is(1.0f));
        assertThat(savingsStat.getValues().getAvg(), is(2.0f));
        assertThat(savingsStat.getValues().getMax(), is(3.0f));
        assertThat(savingsStat.getValues().getTotal(), is(4.0f));
    }

    @Test
    public void testMapHistoricalActionStatInvestment() {
        final long statTime = 700;
        final ActionStats actionStats = ActionStats.newBuilder()
            .setMgmtUnitId(7)
            .addStatSnapshots(ActionStatSnapshot.newBuilder()
                .setTime(statTime)
                .addStats(ActionStat.newBuilder()
                    .setStatGroup(ActionStat.StatGroup.newBuilder()
                        .setActionState(ActionDTO.ActionState.READY)
                        .setActionCategory(ActionCategory.EFFICIENCY_IMPROVEMENT))
                    .setInvestments(Value.newBuilder()
                        .setMin(1)
                        .setAvg(2)
                        .setMax(3)
                        .setTotal(4))))
            .build();

        final ActionStatsQuery query = mock(ActionStatsQuery.class);
        final ActionApiInputDTO actionInput = mock(ActionApiInputDTO.class);
        when(actionInput.getGroupBy()).thenReturn(Collections.emptyList());
        when(query.actionInput()).thenReturn(actionInput);
        when(query.getCostType()).thenReturn(Optional.of(ActionCostType.INVESTMENT));

        final GroupByFilters groupByFilters = mock(GroupByFilters.class);
        final List<StatFilterApiDTO> apiFilters = Lists.newArrayList(new StatFilterApiDTO());
        when(groupByFilters.getFilters()).thenReturn(apiFilters);
        when(groupByFiltersFactory.filtersForQuery(query)).thenReturn(groupByFilters);


        final List<StatSnapshotApiDTO> snapshots =
            new ActionStatsMapper(clock, groupByFiltersFactory).historicalActionStatsToApiSnapshots(actionStats, query);

        assertThat(snapshots.size(), is(1));
        final StatSnapshotApiDTO snapshot = snapshots.get(0);
        assertThat(snapshot.getDate(), is(DateTimeUtil.toString(statTime)));
        assertThat(snapshot.getStatistics().size(), is(1));

        final StatApiDTO savingsStat = snapshot.getStatistics().get(0);
        assertThat(savingsStat.getName(), is(StringConstants.COST_PRICE));
        assertThat(savingsStat.getFilters(), is(apiFilters));
        assertThat(savingsStat.getValue(), is(2.0f));
        assertThat(savingsStat.getValues().getMin(), is(1.0f));
        assertThat(savingsStat.getValues().getAvg(), is(2.0f));
        assertThat(savingsStat.getValues().getMax(), is(3.0f));
        assertThat(savingsStat.getValues().getTotal(), is(4.0f));
    }

    @Test
    public void testMapCurrentActionStatSavings() {
        final CurrentActionStat stat1 = CurrentActionStat.newBuilder()
            .setSavings(2.0)
            .build();

        final GroupByFilters groupByFilters = mock(GroupByFilters.class);
        final List<StatFilterApiDTO> apiFilters = Lists.newArrayList(new StatFilterApiDTO());
        when(groupByFilters.getFilters()).thenReturn(apiFilters);

        final ActionStatsQuery query = mock(ActionStatsQuery.class);
        when(query.getCostType()).thenReturn(Optional.of(ActionCostType.SAVING));
        when(query.currentTimeStamp()).thenReturn(Optional.empty());
        when(groupByFiltersFactory.filtersForQuery(query)).thenReturn(groupByFilters);

        final StatSnapshotApiDTO snapshot = new ActionStatsMapper(clock, groupByFiltersFactory)
            .currentActionStatsToApiSnapshot(Arrays.asList(stat1), query, Maps.newHashMap(), Maps.newHashMap());

        assertThat(snapshot.getDate(), is(DateTimeUtil.toString(clock.millis())));
        assertThat(snapshot.getStatistics().size(), is(1));

        final StatApiDTO savingsStat = snapshot.getStatistics().get(0);
        assertThat(savingsStat.getName(), is(StringConstants.COST_PRICE));
        assertThat(savingsStat.getFilters(), is(apiFilters));
        assertThat(savingsStat.getValue(), is(2.0f));
        assertThat(savingsStat.getValues().getMin(), is(2.0f));
        assertThat(savingsStat.getValues().getAvg(), is(2.0f));
        assertThat(savingsStat.getValues().getMax(), is(2.0f));
        assertThat(savingsStat.getValues().getTotal(), is(2.0f));
    }

    @Test
    public void testMapCurrentActionStatInvestment() {
        final CurrentActionStat stat1 = CurrentActionStat.newBuilder()
            .setInvestments(2.0)
            .build();

        final GroupByFilters groupByFilters = mock(GroupByFilters.class);
        final List<StatFilterApiDTO> apiFilters = Lists.newArrayList(new StatFilterApiDTO());
        when(groupByFilters.getFilters()).thenReturn(apiFilters);

        final ActionStatsQuery query = mock(ActionStatsQuery.class);
        when(query.getCostType()).thenReturn(Optional.of(ActionCostType.INVESTMENT));
        when(query.currentTimeStamp()).thenReturn(Optional.empty());
        when(groupByFiltersFactory.filtersForQuery(query)).thenReturn(groupByFilters);

        final StatSnapshotApiDTO snapshot = new ActionStatsMapper(clock, groupByFiltersFactory)
            .currentActionStatsToApiSnapshot(Arrays.asList(stat1), query, Maps.newHashMap(), Maps.newHashMap());

        assertThat(snapshot.getDate(), is(DateTimeUtil.toString(clock.millis())));
        assertThat(snapshot.getStatistics().size(), is(1));

        final StatApiDTO savingsStat = snapshot.getStatistics().get(0);
        assertThat(savingsStat.getName(), is(StringConstants.COST_PRICE));
        assertThat(savingsStat.getFilters(), is(apiFilters));
        assertThat(savingsStat.getValue(), is(2.0f));
        assertThat(savingsStat.getValues().getMin(), is(2.0f));
        assertThat(savingsStat.getValues().getAvg(), is(2.0f));
        assertThat(savingsStat.getValues().getMax(), is(2.0f));
        assertThat(savingsStat.getValues().getTotal(), is(2.0f));
    }
}

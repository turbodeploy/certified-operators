package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.isNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.ActionStatsQueryExecutor.ActionStatsMapper;
import com.vmturbo.api.component.external.api.util.ActionStatsQueryExecutor.ActionStatsQuery;
import com.vmturbo.api.component.external.api.util.ActionStatsQueryExecutor.QueryMapper;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.ActionMode;
import com.vmturbo.api.enums.ActionState;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStat;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStat.Value;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStats;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStats.ActionStatSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO.GetHistoricalActionStatsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetHistoricalActionStatsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionCountsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionCountsQuery.GroupBy;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class ActionStatsQueryExecutorTest {

    private ActionsServiceMole actionsServiceMole = spy(new ActionsServiceMole());

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(actionsServiceMole);

    @Test
    public void testExecuteQuery() {
        final QueryMapper queryMapper = mock(QueryMapper.class);
        final ActionStatsMapper actionStatsMapper = mock(ActionStatsMapper.class);
        final ActionStatsQueryExecutor executor = new ActionStatsQueryExecutor(
            ActionsServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            queryMapper, actionStatsMapper);
        final ActionStatsQuery actionStatsQuery = mock(ActionStatsQuery.class);
        final HistoricalActionCountsQuery grpcQuery = HistoricalActionCountsQuery.newBuilder()
            .setGroupBy(GroupBy.ACTION_CATEGORY)
            .build();

        final ActionStats actionStats = ActionStats.newBuilder()
            .setMgmtUnitId(1)
            .build();

        when(queryMapper.mapToHistoricalQuery(actionStatsQuery)).thenReturn(grpcQuery);
        when(actionsServiceMole.getHistoricalActionStats(GetHistoricalActionStatsRequest.newBuilder()
            .setQuery(grpcQuery)
            .build())).thenReturn(GetHistoricalActionStatsResponse.newBuilder()
                .setActionStats(actionStats)
                .build());
        final StatSnapshotApiDTO mappedSnapshot = new StatSnapshotApiDTO();
        mappedSnapshot.setDate("foo");
        final List<StatSnapshotApiDTO> expectedRetStats =
            Collections.singletonList(mappedSnapshot);
        when(actionStatsMapper.actionStatsToApiSnapshot(actionStats)).thenReturn(expectedRetStats);

        final List<StatSnapshotApiDTO> retStats = executor.retrieveActionStats(actionStatsQuery);

        assertThat(retStats, is(expectedRetStats));
    }

    @Test
    public void testMapToHistoricalQuery() {
        // Arrange
        final long startTime = 1_000_000;
        final long endTime = 2_000_000;
        final long mgmtUnitId = 128;
        final ApiId apiId = mock(ApiId.class);
        when(apiId.isRealtimeMarket()).thenReturn(false);
        when(apiId.oid()).thenReturn(mgmtUnitId);

        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        inputDto.setStartTime(DateTimeUtil.toString(startTime));
        inputDto.setEndTime(DateTimeUtil.toString(endTime));

        inputDto.setActionModeList(Arrays.asList(ActionMode.AUTOMATIC, ActionMode.MANUAL));
        inputDto.setActionStateList(Arrays.asList(ActionState.ACCEPTED, ActionState.IN_PROGRESS));
        inputDto.setActionTypeList(Arrays.asList(ActionType.MOVE, ActionType.RESIZE));
        inputDto.setRiskSubCategoryList(Arrays.asList("effImpr", "compliance"));

        final ActionSpecMapper actionSpecMapper = mock(ActionSpecMapper.class);
        when(actionSpecMapper.mapApiModeToXl(ActionMode.AUTOMATIC))
            .thenReturn(Optional.of(ActionDTO.ActionMode.AUTOMATIC));
        when(actionSpecMapper.mapApiModeToXl(ActionMode.MANUAL))
            .thenReturn(Optional.of(ActionDTO.ActionMode.MANUAL));
        when(actionSpecMapper.mapApiStateToXl(ActionState.ACCEPTED))
            .thenReturn(Optional.of(ActionDTO.ActionState.QUEUED));
        when(actionSpecMapper.mapApiStateToXl(ActionState.IN_PROGRESS))
            .thenReturn(Optional.of(ActionDTO.ActionState.IN_PROGRESS));

        when(actionSpecMapper.mapApiActionCategoryToXl("effImpr"))
            .thenReturn(Optional.of(ActionCategory.EFFICIENCY_IMPROVEMENT));
        when(actionSpecMapper.mapApiActionCategoryToXl("compliance"))
            .thenReturn(Optional.of(ActionCategory.COMPLIANCE));

        inputDto.setEnvironmentType(EnvironmentType.CLOUD);
        inputDto.setRelatedEntityTypes(Collections.singletonList("PhysicalMachine"));
        inputDto.setGroupBy(Collections.singletonList(StringConstants.RISK_SUB_CATEGORY));

        final ActionStatsQuery query = ImmutableActionStatsQuery.builder()
            .scope(apiId)
            .entityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .actionInput(inputDto)
            .build();

        // Act
        final HistoricalActionCountsQuery grpcQuery =
            new QueryMapper(actionSpecMapper).mapToHistoricalQuery(query);

        // Assert
        assertThat(grpcQuery.getTimeRange().getStartTime(), is(startTime));
        assertThat(grpcQuery.getTimeRange().getEndTime(), is(endTime));

        assertThat(grpcQuery.getActionGroupFilter().getActionCategoryList(),
            containsInAnyOrder(ActionCategory.EFFICIENCY_IMPROVEMENT, ActionCategory.COMPLIANCE));
        assertThat(grpcQuery.getActionGroupFilter().getActionModeList(),
            containsInAnyOrder(ActionDTO.ActionMode.AUTOMATIC, ActionDTO.ActionMode.MANUAL));
        assertThat(grpcQuery.getActionGroupFilter().getActionStateList(),
            containsInAnyOrder(ActionDTO.ActionState.QUEUED, ActionDTO.ActionState.IN_PROGRESS));
        assertThat(grpcQuery.getActionGroupFilter().getActionTypeList(),
            containsInAnyOrder(ActionDTO.ActionType.MOVE, ActionDTO.ActionType.RESIZE));

        assertThat(grpcQuery.getMgmtUnitSubgroupFilter().getMgmtUnitId(), is(mgmtUnitId));
        assertThat(grpcQuery.getMgmtUnitSubgroupFilter().getEntityTypeList(),
            containsInAnyOrder(EntityType.VIRTUAL_MACHINE_VALUE, EntityType.PHYSICAL_MACHINE_VALUE));
        assertThat(grpcQuery.getMgmtUnitSubgroupFilter().getEnvironmentType(),
            is(EnvironmentTypeEnum.EnvironmentType.CLOUD));

        assertThat(grpcQuery.getGroupBy(), is(GroupBy.ACTION_CATEGORY));
    }

    @Test
    public void testActionStatToApiSnapshot() {
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
                    .setActionState(ActionDTO.ActionState.READY)
                    .setActionCategory(ActionCategory.EFFICIENCY_IMPROVEMENT)
                    .setActionCount(Value.newBuilder()
                        .setMin(1)
                        .setAvg(2)
                        .setMax(3))
                    .setEntityCount(Value.newBuilder()
                        .setMin(4)
                        .setAvg(5)
                        .setMax(6))
                    .setInvestments(Value.newBuilder()
                        .setMin(7)
                        .setAvg(8)
                        .setMax(9))
                    .setSavings(Value.newBuilder()
                        .setMin(10)
                        .setAvg(11)
                        .setMax(12))))
            .build();

        final ActionSpecMapper actionSpecMapper = mock(ActionSpecMapper.class);
        when(actionSpecMapper.mapXlActionStateToApi(ActionDTO.ActionState.READY))
            .thenReturn(ActionState.RECOMMENDED);

        final Consumer<StatApiDTO> checkExpectedFilters = statApiDTO -> {
            final Map<String, String> typeToValue = statApiDTO.getFilters().stream()
                .collect(Collectors.toMap(StatFilterApiDTO::getType, StatFilterApiDTO::getValue));
            assertThat(typeToValue.keySet(), containsInAnyOrder(
                StringConstants.RISK_SUB_CATEGORY, StringConstants.ACTION_STATES));
            // This is kind of ugly, but we depend on the result of ActionSpecMapper.
            assertThat(typeToValue.get(StringConstants.RISK_SUB_CATEGORY), is("Efficiency Improvement"));
            assertThat(typeToValue.get(StringConstants.ACTION_STATES), is(ActionState.RECOMMENDED.name()));
        };

        final List<StatSnapshotApiDTO> snapshots =
            new ActionStatsMapper(actionSpecMapper).actionStatsToApiSnapshot(actionStats);
        assertThat(snapshots.size(), is(2));
        final StatSnapshotApiDTO emptySnapshot = snapshots.get(0);
        assertThat(emptySnapshot.getDate(), is(DateTimeUtil.toString(emptyTime)));
        assertThat(emptySnapshot.getStatistics(), is(Collections.emptyList()));


        final StatSnapshotApiDTO statSnapshot = snapshots.get(1);
        assertThat(statSnapshot.getDate(), is(DateTimeUtil.toString(statTime)));
        // Note - investments and savings currently not handled.
        assertThat(statSnapshot.getStatistics().size(), is(2));
        final Map<String, StatApiDTO> statsByName = statSnapshot.getStatistics().stream()
            .collect(Collectors.toMap(StatApiDTO::getName, Function.identity()));
        final StatApiDTO numActions = statsByName.get(StringConstants.NUM_ACTIONS);
        assertNotNull(numActions);
        assertThat(numActions.getValue(), is(2.0f));
        checkExpectedFilters.accept(numActions);

        assertThat(numActions.getValues().getMin(), is(1.0f));
        assertThat(numActions.getValues().getAvg(), is(2.0f));
        assertThat(numActions.getValues().getMax(), is(3.0f));

        final StatApiDTO numEntities = statsByName.get(StringConstants.NUM_ENTITIES);
        assertNotNull(numEntities);
        assertThat(numEntities.getValue(), is(5.0f));

        assertThat(numEntities.getValues().getMin(), is(4.0f));
        assertThat(numEntities.getValues().getAvg(), is(5.0f));
        assertThat(numEntities.getValues().getMax(), is(6.0f));
        checkExpectedFilters.accept(numEntities);
    }
}

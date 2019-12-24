package com.vmturbo.api.component.external.api.util.action;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedGroupInfo;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor.ActionStatsQuery;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.enums.ActionMode;
import com.vmturbo.api.enums.ActionState;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.MgmtUnitSubgroupFilter;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class HistoricalQueryMapperTest {

    @Test
    public void testMapToHistoricalQuery() {
        // Arrange
        final long startTime = 1_000_000;
        final long endTime = 2_000_000;
        final long mgmtUnitId = 128;
        final ApiId apiId = mock(ApiId.class);
        when(apiId.isRealtimeMarket()).thenReturn(false);
        when(apiId.oid()).thenReturn(mgmtUnitId);
        when(apiId.getCachedGroupInfo()).thenReturn(Optional.empty());

        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        inputDto.setStartTime(DateTimeUtil.toString(startTime));
        inputDto.setEndTime(DateTimeUtil.toString(endTime));

        inputDto.setActionModeList(Arrays.asList(ActionMode.AUTOMATIC, ActionMode.MANUAL));
        inputDto.setActionStateList(Arrays.asList(ActionState.ACCEPTED, ActionState.IN_PROGRESS));
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

        final BuyRiScopeHandler buyRiScopeHandler = mock(BuyRiScopeHandler.class);
        when(buyRiScopeHandler.extractActionTypes(any(), any()))
                .thenReturn(ImmutableSet.of(ActionDTO.ActionType.MOVE, ActionDTO.ActionType.RESIZE));

        inputDto.setEnvironmentType(EnvironmentType.CLOUD);
        inputDto.setRelatedEntityTypes(Collections.singletonList("PhysicalMachine"));
        inputDto.setGroupBy(Collections.singletonList(StringConstants.RISK_SUB_CATEGORY));

        final ActionStatsQuery query = ImmutableActionStatsQuery.builder()
            .scopes(Collections.singleton(apiId))
            .entityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .actionInput(inputDto)
            .build();

        // Act
        final Map<ApiId, HistoricalActionStatsQuery> grpcQueries =
            new HistoricalQueryMapper(actionSpecMapper, buyRiScopeHandler)
                    .mapToHistoricalQueries(query);

        // Assert
        assertThat(grpcQueries.keySet(), contains(apiId));

        final HistoricalActionStatsQuery grpcQuery = grpcQueries.get(apiId);
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
    public void testExtractRealtimeMktFilter() {
        final ApiId mktScope = mock(ApiId.class);
        when(mktScope.isRealtimeMarket()).thenReturn(true);
        ActionStatsQuery query = ImmutableActionStatsQuery.builder()
            .addScopes(mktScope)
            .entityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
            .actionInput(new ActionApiInputDTO())
            .build();
        final Map<ApiId, MgmtUnitSubgroupFilter> filters = new HistoricalQueryMapper(
                mock(ActionSpecMapper.class), mock(BuyRiScopeHandler.class))
            .extractMgmtUnitSubgroupFilter(query);
        assertTrue(filters.get(mktScope).getMarket());
        assertThat(filters.get(mktScope).getEntityTypeList(),
            containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.typeNumber()));
    }

    @Test
    public void testExtractRealtimeGlobalGroupFilter() {
        final CachedGroupInfo globalVmGroup = mock(CachedGroupInfo.class);
        when(globalVmGroup.getEntityTypes()).thenReturn(Collections.singleton(
                        UIEntityType.VIRTUAL_MACHINE));
        when(globalVmGroup.isGlobalTempGroup()).thenReturn(true);

        final ApiId mktScope = mock(ApiId.class);
        when(mktScope.isRealtimeMarket()).thenReturn(false);
        when(mktScope.isGlobalTempGroup()).thenReturn(true);
        when(mktScope.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(
                        UIEntityType.VIRTUAL_MACHINE)));

        ActionStatsQuery query = ImmutableActionStatsQuery.builder()
            .addScopes(mktScope)
            .actionInput(new ActionApiInputDTO())
            .build();
        final Map<ApiId, MgmtUnitSubgroupFilter> filters = new HistoricalQueryMapper(
                mock(ActionSpecMapper.class), mock(BuyRiScopeHandler.class))
            .extractMgmtUnitSubgroupFilter(query);
        assertTrue(filters.get(mktScope).getMarket());

        // By default, the entity type of the global group is used.
        assertThat(filters.get(mktScope).getEntityTypeList(),
            containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.typeNumber()));
    }

}
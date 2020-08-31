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
import org.mockito.internal.util.collections.Sets;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedGroupInfo;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor.ActionStatsQuery;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.enums.ActionMode;
import com.vmturbo.api.enums.ActionState;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.MgmtUnitSubgroupFilter;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link HistoricalQueryMapper}.
 */
public class HistoricalQueryMapperTest {

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

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
        final GroupExpander groupExpander = mock(GroupExpander.class);
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
            new HistoricalQueryMapper(actionSpecMapper, buyRiScopeHandler, groupExpander, clock)
                    .mapToHistoricalQueries(query);

        // Assert
        assertThat(grpcQueries.keySet(), contains(apiId));

        final HistoricalActionStatsQuery grpcQuery = grpcQueries.get(apiId);
        assertThat(grpcQuery.getTimeRange().getStartTime(), is(startTime));
        assertThat(grpcQuery.getTimeRange().getEndTime(), is(endTime));
        assertThat(grpcQuery.getActionGroupFilter().getActionModeList(),
            containsInAnyOrder(ActionDTO.ActionMode.AUTOMATIC, ActionDTO.ActionMode.MANUAL));
        assertThat(grpcQuery.getActionGroupFilter().getActionStateList(),
            containsInAnyOrder(ActionDTO.ActionState.ACCEPTED, ActionDTO.ActionState.IN_PROGRESS));
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
            .entityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
            .actionInput(new ActionApiInputDTO())
            .build();
        final Map<ApiId, MgmtUnitSubgroupFilter> filters = new HistoricalQueryMapper(
                mock(ActionSpecMapper.class), mock(BuyRiScopeHandler.class), mock(GroupExpander.class),
                clock)
            .extractMgmtUnitSubgroupFilter(query, Optional.empty());
        assertTrue(filters.get(mktScope).getMarket());
        assertThat(filters.get(mktScope).getEntityTypeList(),
            containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber()));
    }

    @Test
    public void testExtractRealtimeGlobalGroupFilter() {
        final CachedGroupInfo globalVmGroup = mock(CachedGroupInfo.class);
        when(globalVmGroup.getEntityTypes()).thenReturn(Collections.singleton(
                        ApiEntityType.VIRTUAL_MACHINE));
        when(globalVmGroup.isGlobalTempGroup()).thenReturn(true);

        final ApiId mktScope = mock(ApiId.class);
        when(mktScope.isRealtimeMarket()).thenReturn(false);
        when(mktScope.isGlobalTempGroup()).thenReturn(true);
        when(mktScope.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(
                        ApiEntityType.VIRTUAL_MACHINE)));

        ActionStatsQuery query = ImmutableActionStatsQuery.builder()
            .addScopes(mktScope)
            .actionInput(new ActionApiInputDTO())
            .build();
        final Map<ApiId, MgmtUnitSubgroupFilter> filters = new HistoricalQueryMapper(
                mock(ActionSpecMapper.class), mock(BuyRiScopeHandler.class), mock(GroupExpander.class), clock)
            .extractMgmtUnitSubgroupFilter(query, Optional.empty());
        assertTrue(filters.get(mktScope).getMarket());

        // By default, the entity type of the global group is used.
        assertThat(filters.get(mktScope).getEntityTypeList(),
            containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber()));
    }

    /**
     * Test the case that we are in scope of business account and grouping by resource group.
     */
    @Test
    public void testMappingAccountGroupByResourceGroup() {
        // ARRANGE
        long accountOid = 1002L;
        long rgOid = 2001L;
        GroupExpander groupExpander = mock(GroupExpander.class);
        when(groupExpander.getResourceGroupsForAccounts(Collections.singleton(accountOid)))
            .thenReturn(Collections.singletonList(GroupDTO.Grouping.newBuilder().setId(rgOid).build()));
        HistoricalQueryMapper mapper = new HistoricalQueryMapper(
            mock(ActionSpecMapper.class), mock(BuyRiScopeHandler.class), groupExpander, clock);

        final ApiId scope = mock(ApiId.class);
        when(scope.isRealtimeMarket()).thenReturn(false);
        when(scope.isGlobalTempGroup()).thenReturn(false);
        when(scope.isEntity()).thenReturn(true);
        when(scope.getClassName()).thenReturn(ApiEntityType.BUSINESS_ACCOUNT.apiStr());
        when(scope.oid()).thenReturn(accountOid);

        ActionStatsQuery query = ImmutableActionStatsQuery.builder()
            .addScopes(scope)
            .actionInput(new ActionApiInputDTO())
            .build();

        Optional<GroupBy> groupbyOpt = Optional.of(GroupBy.RESOURCE_GROUP_ID);

        // ACT
        final Map<ApiId, MgmtUnitSubgroupFilter> filters = mapper.extractMgmtUnitSubgroupFilter(query,
            groupbyOpt);

        // ASSERT
        assertThat(filters.size(), is(1));
        assertTrue(filters.get(scope).hasMgmtUnits());
        assertThat(filters.get(scope).getMgmtUnits().getMgmtUnitIdsList(),
            is(Collections.singletonList(rgOid)));
    }

    /**
     * Tests when we are querying in scope of group of resource groups.
     */
    @Test
    public void testGroupOfResourceGroup() {
        // ARRANGE
        long groupOfRgsOids = 1001L;
        long rg1Oid = 2001L;
        long rg2Oid = 2002L;

        GroupExpander groupExpander = mock(GroupExpander.class);
        HistoricalQueryMapper mapper = new HistoricalQueryMapper(
            mock(ActionSpecMapper.class), mock(BuyRiScopeHandler.class), groupExpander, clock);

        final CachedGroupInfo cachedGroupInfo = mock(CachedGroupInfo.class);
        when(cachedGroupInfo.getEntityTypes()).thenReturn(Collections.singleton(ApiEntityType.VIRTUAL_MACHINE));
        when(cachedGroupInfo.getNestedGroupTypes())
            .thenReturn(Collections.singleton(CommonDTO.GroupDTO.GroupType.RESOURCE));
        when(cachedGroupInfo.getEntityIds()).thenReturn(Sets.newSet(rg1Oid, rg2Oid));
        when(cachedGroupInfo.getGroupType()).thenReturn(CommonDTO.GroupDTO.GroupType.REGULAR);

        final ApiId scope = mock(ApiId.class);
        when(scope.isRealtimeMarket()).thenReturn(false);
        when(scope.isGlobalTempGroup()).thenReturn(false);
        when(scope.isEntity()).thenReturn(false);
        when(scope.isGroup()).thenReturn(true);
        when(scope.oid()).thenReturn(groupOfRgsOids);
        when(scope.getCachedGroupInfo()).thenReturn(Optional.of(cachedGroupInfo));


        ActionStatsQuery query = ImmutableActionStatsQuery.builder()
            .addScopes(scope)
            .actionInput(new ActionApiInputDTO())
            .build();

        // ACT
        final Map<ApiId, MgmtUnitSubgroupFilter> filters = mapper.extractMgmtUnitSubgroupFilter(query,
            Optional.empty());

        // ASSERT
        assertThat(filters.size(), is(1));
        assertTrue(filters.get(scope).hasMgmtUnits());
        assertThat(filters.get(scope).getMgmtUnits().getMgmtUnitIdsList(),
            containsInAnyOrder(rg1Oid, rg2Oid));
    }

    /**
     * Tests when we are querying in scope of group of accounts grouped by resource groups.
     */
    @Test
    public void testGroupOfAccountsGroupByResourceGroup() {
        // ARRANGE
        long groupOfAccountsOid = 3000L;
        long accountOid1 = 1001L;
        long accountOid2 = 1002L;
        long rg1Oid = 2001L;
        long rg2Oid = 2002L;

        GroupExpander groupExpander = mock(GroupExpander.class);
        when(groupExpander.getResourceGroupsForAccounts(Sets.newSet(accountOid1, accountOid2)))
            .thenReturn(Arrays.asList(GroupDTO.Grouping.newBuilder().setId(rg1Oid).build(),
                GroupDTO.Grouping.newBuilder().setId(rg2Oid).build()));
        HistoricalQueryMapper mapper = new HistoricalQueryMapper(
            mock(ActionSpecMapper.class), mock(BuyRiScopeHandler.class), groupExpander, clock);

        final CachedGroupInfo cachedGroupInfo = mock(CachedGroupInfo.class);
        when(cachedGroupInfo.getEntityTypes()).thenReturn(Collections.singleton(ApiEntityType.BUSINESS_ACCOUNT));
        when(cachedGroupInfo.getEntityIds()).thenReturn(Sets.newSet(accountOid1, accountOid2));
        when(cachedGroupInfo.getGroupType()).thenReturn(CommonDTO.GroupDTO.GroupType.REGULAR);

        final ApiId scope = mock(ApiId.class);
        when(scope.isRealtimeMarket()).thenReturn(false);
        when(scope.isGlobalTempGroup()).thenReturn(false);
        when(scope.isEntity()).thenReturn(false);
        when(scope.isGroup()).thenReturn(true);
        when(scope.oid()).thenReturn(groupOfAccountsOid);
        when(scope.getCachedGroupInfo()).thenReturn(Optional.of(cachedGroupInfo));


        ActionStatsQuery query = ImmutableActionStatsQuery.builder()
            .addScopes(scope)
            .actionInput(new ActionApiInputDTO())
            .build();

        Optional<GroupBy> groupbyOpt = Optional.of(GroupBy.RESOURCE_GROUP_ID);

        // ACT
        final Map<ApiId, MgmtUnitSubgroupFilter> filters = mapper.extractMgmtUnitSubgroupFilter(query,
            groupbyOpt);

        // ASSERT
        assertThat(filters.size(), is(1));
        assertTrue(filters.get(scope).hasMgmtUnits());
        assertThat(filters.get(scope).getMgmtUnits().getMgmtUnitIdsList(),
            containsInAnyOrder(rg1Oid, rg2Oid));
    }

}

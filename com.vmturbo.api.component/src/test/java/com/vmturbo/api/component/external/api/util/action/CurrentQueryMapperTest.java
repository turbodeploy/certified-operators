package com.vmturbo.api.component.external.api.util.action;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.junit.Test;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor.ActionStatsQuery;
import com.vmturbo.api.component.external.api.util.action.CurrentQueryMapper.ActionGroupFilterExtractor;
import com.vmturbo.api.component.external.api.util.action.CurrentQueryMapper.EntityScopeFactory;
import com.vmturbo.api.component.external.api.util.action.CurrentQueryMapper.GroupByExtractor;
import com.vmturbo.api.component.external.api.util.action.CurrentQueryMapper.ScopeFilterExtractor;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.enums.ActionMode;
import com.vmturbo.api.enums.ActionState;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ActionGroupFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter.EntityScope;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStatsQuery.ScopeFilter.GlobalScope;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.identity.ArrayOidSet;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class CurrentQueryMapperTest {
    private ActionSpecMapper actionSpecMapper = mock(ActionSpecMapper.class);

    private GroupExpander groupExpander = mock(GroupExpander.class);

    private SupplyChainFetcherFactory supplyChainFetcherFactory = mock(SupplyChainFetcherFactory.class);

    private UserSessionContext userSessionContext = mock(UserSessionContext.class);

    @Test
    public void testMapToLiveQueries() throws OperationFailedException {
        // ARRANGE
        final ActionGroupFilterExtractor actionGroupFilterExtractor = mock(ActionGroupFilterExtractor.class);
        final GroupByExtractor groupByExtractor = mock(GroupByExtractor.class);
        final ScopeFilterExtractor scopeFilterExtractor = mock(ScopeFilterExtractor.class);

        final CurrentQueryMapper queryMapper = new CurrentQueryMapper(actionGroupFilterExtractor,
            groupByExtractor, scopeFilterExtractor);

        final ActionStatsQuery query = mock(ActionStatsQuery.class);

        final ActionGroupFilter actionGroupFilter = ActionGroupFilter.newBuilder()
            .addActionCategory(ActionCategory.PERFORMANCE_ASSURANCE)
            .build();
        when(actionGroupFilterExtractor.extractActionGroupFilter(query))
            .thenReturn(actionGroupFilter);
        final List<GroupBy> groupByList = Arrays.asList(GroupBy.ACTION_CATEGORY, GroupBy.ACTION_STATE);
        when(groupByExtractor.extractGroupByCriteria(query))
            .thenReturn(groupByList);
        final ScopeFilter scopeFilter = ScopeFilter.newBuilder()
            .setTopologyContextId(123L)
            .build();
        final ApiId scopeId = ApiTestUtils.mockEntityId("1");
        when(scopeFilterExtractor.extractScopeFilters(query))
            .thenReturn(ImmutableMap.of(scopeId, scopeFilter));


        // ACT
        final Map<ApiId, CurrentActionStatsQuery> result = queryMapper.mapToCurrentQueries(query);

        // ASSERT
        assertThat(result.keySet(), contains(scopeId));
        assertThat(result.get(scopeId), is(CurrentActionStatsQuery.newBuilder()
            .setActionGroupFilter(actionGroupFilter)
            .setScopeFilter(scopeFilter)
            .addAllGroupBy(groupByList)
            .build()));
    }

    @Test
    public void testEntityScopeFactoryNoRelatedTypes() throws OperationFailedException {
        final EntityAccessScope userScope = mock(EntityAccessScope.class);
        when(userScope.filter(anySet()))
            .thenAnswer(invocation -> invocation.getArgumentAt(0, Set.class));

        final EntityScopeFactory scopeFactory = new EntityScopeFactory(groupExpander, supplyChainFetcherFactory);
        final Set<Long> originalScope = Sets.newHashSet(1L, 2L);
        final Set<Long> expandedScope = Sets.newHashSet(3L, 4L);

        when(groupExpander.expandOids(originalScope)).thenReturn(expandedScope);
        when(supplyChainFetcherFactory.expandGroupingServiceEntities(expandedScope)).thenReturn(expandedScope);

        final EntityScope entityScope = scopeFactory.createEntityScope(
            originalScope, Collections.emptySet(), Optional.empty(), userScope);

        verify(groupExpander).expandOids(originalScope);
        verify(userScope).filter(expandedScope);
        assertThat(entityScope.getOidsList(), containsInAnyOrder(expandedScope.toArray()));
    }

    @Test
    public void testEntityScopeFactoryRelatedTypes() throws OperationFailedException {
        final EntityAccessScope userScope = mock(EntityAccessScope.class);
        when(userScope.filter(anySet()))
            .thenAnswer(invocation -> invocation.getArgumentAt(0, Set.class));

        final EntityScopeFactory scopeFactory = new EntityScopeFactory(groupExpander, supplyChainFetcherFactory);

        final Set<Long> originalScope = Sets.newHashSet(1L, 2L);
        final Set<Long> relatedVms = Sets.newHashSet(11L, 12L);
        final Set<Integer> relatedTypes = Sets.newHashSet(EntityType.VIRTUAL_MACHINE_VALUE);

        final SupplyChainNodeFetcherBuilder nodeFetcherBuilder = ApiTestUtils.mockNodeFetcherBuilder(
            ImmutableMap.of(UIEntityType.VIRTUAL_MACHINE.apiStr(), SupplyChainNode.newBuilder()
                .putMembersByState(EntityState.POWERED_ON_VALUE, MemberList.newBuilder()
                    .addAllMemberOids(relatedVms)
                    .build())
                .build()));
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(nodeFetcherBuilder);
        when(supplyChainFetcherFactory.expandGroupingServiceEntities(relatedVms)).thenReturn(relatedVms);

        final EntityScope entityScope = scopeFactory.createEntityScope(originalScope,
            relatedTypes,
            Optional.of(EnvironmentType.ON_PREM),
            userScope);

        verify(nodeFetcherBuilder).entityTypes(Collections.singletonList(
            UIEntityType.VIRTUAL_MACHINE.apiStr()));
        verify(nodeFetcherBuilder).addSeedUuid("1");
        verify(nodeFetcherBuilder).addSeedUuid("2");
        verify(nodeFetcherBuilder).environmentType(EnvironmentType.ON_PREM);

        assertThat(entityScope.getOidsList(), containsInAnyOrder(relatedVms.toArray()));

    }

    @Test
    public void testScopeFilterExtractorRealtimeMktAdminScopeNoEnvType() throws OperationFailedException {
        final EntityAccessScope entityAccessScope = mock(EntityAccessScope.class);
        when(userSessionContext.getUserAccessScope()).thenReturn(entityAccessScope);
        when(entityAccessScope.containsAll()).thenReturn(true);

        final EntityScopeFactory entityScopeFactory = mock(EntityScopeFactory.class);
        final ScopeFilterExtractor scopeFilterExtractor =
            new ScopeFilterExtractor(userSessionContext, entityScopeFactory);

        final ActionStatsQuery query = mock(ActionStatsQuery.class);
        final ApiId mktId = ApiTestUtils.mockRealtimeId("7", 7);
        when(query.scopes()).thenReturn(Collections.singleton(mktId));
        when(query.getRelatedEntityTypes()).thenReturn(Collections.singleton(1));
        when(query.getEnvironmentType()).thenReturn(Optional.empty());

        final Map<ApiId, ScopeFilter> result = scopeFilterExtractor.extractScopeFilters(query);
        assertThat(result.keySet(), contains(mktId));
        assertThat(result.get(mktId), is(ScopeFilter.newBuilder()
            .setTopologyContextId(7)
            .setGlobal(GlobalScope.newBuilder()
                .addEntityType(1)
                .build())
            .build()));
    }

    @Test
    public void testScopeFilterExtractorRealtimeMarketAdminScopeEnvType() throws OperationFailedException {
        final EntityAccessScope entityAccessScope = mock(EntityAccessScope.class);
        when(userSessionContext.getUserAccessScope()).thenReturn(entityAccessScope);
        when(entityAccessScope.containsAll()).thenReturn(true);

        final EntityScopeFactory entityScopeFactory = mock(EntityScopeFactory.class);
        final ScopeFilterExtractor scopeFilterExtractor =
            new ScopeFilterExtractor(userSessionContext, entityScopeFactory);

        final ActionStatsQuery query = mock(ActionStatsQuery.class);
        final ApiId mktId = ApiTestUtils.mockRealtimeId("7", 7);
        when(query.scopes()).thenReturn(Collections.singleton(mktId));
        when(query.getRelatedEntityTypes()).thenReturn(Collections.singleton(1));
        // Environment type set.
        when(query.getEnvironmentType()).thenReturn(Optional.of(EnvironmentType.ON_PREM));

        final Map<ApiId, ScopeFilter> result = scopeFilterExtractor.extractScopeFilters(query);
        assertThat(result.keySet(), contains(mktId));
        assertThat(result.get(mktId), is(ScopeFilter.newBuilder()
            .setTopologyContextId(7)
            .setGlobal(GlobalScope.newBuilder()
                .addEntityType(1)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .build())
            .build()));
    }

    @Test
    public void testScopeFilterExtractorRealtimeMarketLimitedScope() throws OperationFailedException {
        final EntityAccessScope entityAccessScope = mock(EntityAccessScope.class);
        when(userSessionContext.getUserAccessScope()).thenReturn(entityAccessScope);
        when(entityAccessScope.containsAll()).thenReturn(false);
        final List<Long> accessibleOids = Arrays.asList(123L, 345L);
        when(entityAccessScope.accessibleOids()).thenReturn(new ArrayOidSet(accessibleOids));

        final EntityScopeFactory entityScopeFactory = mock(EntityScopeFactory.class);
        final ScopeFilterExtractor scopeFilterExtractor =
            new ScopeFilterExtractor(userSessionContext, entityScopeFactory);

        final ActionStatsQuery query = mock(ActionStatsQuery.class);
        final ApiId mktId = ApiTestUtils.mockRealtimeId("7", 7);
        when(query.scopes()).thenReturn(Collections.singleton(mktId));
        final Set<Integer> relatedEntityTypes = Collections.singleton(1);
        when(query.getRelatedEntityTypes()).thenReturn(relatedEntityTypes);
        when(query.getEnvironmentType()).thenReturn(Optional.of(EnvironmentType.ON_PREM));

        final EntityScope entityScope = EntityScope.newBuilder()
            .addOids(1029)
            .build();

        when(entityScopeFactory.createEntityScope(any(), any(), any(), any()))
            .thenReturn(entityScope);

        final Map<ApiId, ScopeFilter> result = scopeFilterExtractor.extractScopeFilters(query);

        verify(entityScopeFactory).createEntityScope(
            Sets.newHashSet(accessibleOids),
            relatedEntityTypes,
            Optional.of(EnvironmentType.ON_PREM),
            entityAccessScope);

        assertThat(result.keySet(), contains(mktId));
        assertThat(result.get(mktId), is(ScopeFilter.newBuilder()
            .setTopologyContextId(7)
            .setEntityList(entityScope)
            .build()));
    }

    @Test
    public void testScopeFilterEntityScope() throws OperationFailedException {
        final EntityAccessScope entityAccessScope = mock(EntityAccessScope.class);
        when(userSessionContext.getUserAccessScope()).thenReturn(entityAccessScope);

        final EntityScopeFactory entityScopeFactory = mock(EntityScopeFactory.class);
        final ScopeFilterExtractor scopeFilterExtractor =
            new ScopeFilterExtractor(userSessionContext, entityScopeFactory);
        final ActionStatsQuery query = mock(ActionStatsQuery.class);
        when(query.getEnvironmentType()).thenReturn(Optional.of(EnvironmentType.ON_PREM));
        final Set<Integer> relatedTypes = Sets.newHashSet(10, 11);
        when(query.getRelatedEntityTypes()).thenReturn(relatedTypes);

        final ApiId e1Id = ApiTestUtils.mockEntityId("1");
        final ApiId e2Id = ApiTestUtils.mockEntityId("2");
        when(query.scopes()).thenReturn(Sets.newHashSet(e1Id, e2Id));

        final EntityScope entityScope = EntityScope.newBuilder()
            .addOids(1029)
            .build();

        when(entityScopeFactory.createEntityScope(any(), any(), any(), any()))
            .thenReturn(entityScope);

        final Map<ApiId, ScopeFilter> filters = scopeFilterExtractor.extractScopeFilters(query);
        verify(entityScopeFactory).createEntityScope(
            Collections.singleton(1L),
            relatedTypes,
            Optional.of(EnvironmentType.ON_PREM),
            entityAccessScope);
        verify(entityScopeFactory).createEntityScope(
            Collections.singleton(2L),
            relatedTypes,
            Optional.of(EnvironmentType.ON_PREM),
            entityAccessScope);

        assertThat(filters.keySet(), containsInAnyOrder(e1Id, e2Id));
        assertThat(filters.get(e1Id), is(ScopeFilter.newBuilder()
            .setEntityList(entityScope)
            .build()));
        assertThat(filters.get(e2Id), is(ScopeFilter.newBuilder()
            .setEntityList(entityScope)
            .build()));
    }

    @Test
    public void testActionGroupFilterExtractorMode() {
        final ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        inputDTO.setActionModeList(Arrays.asList(ActionMode.AUTOMATIC, ActionMode.MANUAL, ActionMode.COLLECTION));
        when(actionSpecMapper.mapApiModeToXl(ActionMode.AUTOMATIC))
            .thenReturn(Optional.of(ActionDTO.ActionMode.AUTOMATIC));
        when(actionSpecMapper.mapApiModeToXl(ActionMode.MANUAL))
            .thenReturn(Optional.of(ActionDTO.ActionMode.MANUAL));
        when(actionSpecMapper.mapApiModeToXl(ActionMode.COLLECTION))
            .thenReturn(Optional.empty());

        final ActionGroupFilterExtractor groupFilterExtractor = new ActionGroupFilterExtractor(actionSpecMapper);
        final ActionGroupFilter groupFilter = groupFilterExtractor.extractActionGroupFilter(
            ImmutableActionStatsQuery.builder()
                .actionInput(inputDTO)
                .build());
        assertThat(groupFilter.getActionModeList(),
            containsInAnyOrder(ActionDTO.ActionMode.AUTOMATIC, ActionDTO.ActionMode.MANUAL));
    }

    @Test
    public void testActionGroupFilterExtractorState() {
        final ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        inputDTO.setActionStateList(Arrays.asList(ActionState.IN_PROGRESS,
            ActionState.RECOMMENDED,
            ActionState.ACCOUNTING));
        when(actionSpecMapper.mapApiStateToXl(ActionState.IN_PROGRESS))
            .thenReturn(Optional.of(ActionDTO.ActionState.IN_PROGRESS));
        when(actionSpecMapper.mapApiStateToXl(ActionState.RECOMMENDED))
            .thenReturn(Optional.of(ActionDTO.ActionState.READY));
        when(actionSpecMapper.mapApiStateToXl(ActionState.ACCOUNTING))
            .thenReturn(Optional.empty());

        final ActionGroupFilterExtractor groupFilterExtractor = new ActionGroupFilterExtractor(actionSpecMapper);
        final ActionGroupFilter groupFilter = groupFilterExtractor.extractActionGroupFilter(
            ImmutableActionStatsQuery.builder()
                .actionInput(inputDTO)
                .build());
        assertThat(groupFilter.getActionStateList(),
            containsInAnyOrder(ActionDTO.ActionState.IN_PROGRESS, ActionDTO.ActionState.READY));
    }

    @Test
    public void testActionGroupFilterExtractorType() {
        final ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        inputDTO.setActionTypeList(Arrays.asList(ActionType.SUSPEND,
            ActionType.MOVE, ActionType.RESERVE_ON_DS));
        final ActionGroupFilterExtractor groupFilterExtractor = new ActionGroupFilterExtractor(actionSpecMapper);
        final ActionGroupFilter groupFilter = groupFilterExtractor.extractActionGroupFilter(
            ImmutableActionStatsQuery.builder()
                .actionInput(inputDTO)
                .build());
        assertThat(groupFilter.getActionTypeList(),
            containsInAnyOrder(ActionDTO.ActionType.MOVE, ActionDTO.ActionType.SUSPEND,
                ActionDTO.ActionType.DEACTIVATE,ActionDTO.ActionType.NONE));
    }

    @Test
    public void testActionGroupFilterExtractorRiskSubCategory() {
        final ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        inputDTO.setRiskSubCategoryList(Arrays.asList("Performance Assurance", "Compliance", "foo"));
        when(actionSpecMapper.mapApiActionCategoryToXl("Performance Assurance"))
            .thenReturn(Optional.of(ActionCategory.PERFORMANCE_ASSURANCE));
        when(actionSpecMapper.mapApiActionCategoryToXl("Compliance"))
            .thenReturn(Optional.of(ActionCategory.COMPLIANCE));
        when(actionSpecMapper.mapApiActionCategoryToXl("foo"))
            .thenReturn(Optional.empty());

        final ActionGroupFilterExtractor groupFilterExtractor = new ActionGroupFilterExtractor(actionSpecMapper);
        final ActionGroupFilter groupFilter = groupFilterExtractor.extractActionGroupFilter(
            ImmutableActionStatsQuery.builder()
                .actionInput(inputDTO)
                .build());
        assertThat(groupFilter.getActionCategoryList(),
            containsInAnyOrder(ActionCategory.PERFORMANCE_ASSURANCE, ActionCategory.COMPLIANCE));
    }

    @Test
    public void testGroupByExtractor() {
        final ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        inputDTO.setGroupBy(Arrays.asList( StringConstants.RISK_SUB_CATEGORY,
            StringConstants.ACTION_STATES,
            StringConstants.ACTION_TYPE,
            StringConstants.TARGET_TYPE,
            StringConstants.REASON_COMMODITY));
        GroupByExtractor groupByExtractor = new GroupByExtractor();
        final ActionStatsQuery actionStatsQuery = ImmutableActionStatsQuery.builder()
            .actionInput(inputDTO)
            .build();
        final List<GroupBy> mappedGroupBy = groupByExtractor.extractGroupByCriteria(actionStatsQuery);
        assertThat(mappedGroupBy, contains(
            GroupBy.ACTION_CATEGORY,
            GroupBy.ACTION_STATE,
            GroupBy.ACTION_TYPE,
            GroupBy.TARGET_ENTITY_TYPE,
            GroupBy.REASON_COMMODITY));
    }

    @Test
    public void testGroupByExtractorNoMapping() {
        final ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        inputDTO.setGroupBy(Arrays.asList(StringConstants.RISK_SUB_CATEGORY,
            "foo", "bar"));
        GroupByExtractor groupByExtractor = new GroupByExtractor();
        final ActionStatsQuery actionStatsQuery = ImmutableActionStatsQuery.builder()
            .actionInput(inputDTO)
            .build();
        final List<GroupBy> mappedGroupBy = groupByExtractor.extractGroupByCriteria(actionStatsQuery);
        assertThat(mappedGroupBy, contains(GroupBy.ACTION_CATEGORY));
    }


}
package com.vmturbo.api.component.external.api.util.stats;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.identity.ArrayOidSet;

public class StatsQueryScopeExpanderTest {

    private final GroupExpander groupExpander = mock(GroupExpander.class);

    private final RepositoryApi repositoryApi = mock(RepositoryApi.class);

    private final SupplyChainFetcherFactory supplyChainFetcherFactory = mock(SupplyChainFetcherFactory.class);

    private final UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private StatsQueryScopeExpander scopeExpander = new StatsQueryScopeExpander(groupExpander,
        repositoryApi, supplyChainFetcherFactory, userSessionContext);

    private static final MinimalEntity DC = MinimalEntity.newBuilder()
        .setOid(123123)
        .setEntityType(UIEntityType.DATACENTER.typeNumber())
        .setDisplayName("dc")
        .build();

    @Before
    public void setup() {
        // Doing this here because we make this RPC in every call.
        final SearchRequest req = ApiTestUtils.mockSearchMinReq(Collections.singletonList(DC));
        when(repositoryApi.newSearchRequest(SearchProtoUtil.makeSearchParameters(
            SearchProtoUtil.entityTypeFilter(UIEntityType.DATACENTER)).build())).thenReturn(req);
    }

    @Test
    public void testExpandUnscopedMarket() throws OperationFailedException {
        ApiId scope = mock(ApiId.class);
        when(scope.isRealtimeMarket()).thenReturn(true);
        when(userSessionContext.isUserScoped()).thenReturn(false);

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());
        assertThat(expandedScope.isAll(), is(true));

    }

    @Test
    public void testExpandScopedMarket() throws OperationFailedException {
        ApiId scope = mock(ApiId.class);
        when(scope.isRealtimeMarket()).thenReturn(true);
        when(userSessionContext.isUserScoped()).thenReturn(true);

        final EntityAccessScope accessScope = mock(EntityAccessScope.class);
        when(accessScope.accessibleOids()).thenReturn(new ArrayOidSet(Collections.singletonList(1L)));

        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());
        assertThat(expandedScope.isAll(), is(false));
        assertThat(expandedScope.getEntities(), is(Collections.singleton(1L)));
    }

    @Test
    public void testExpandScopeGroup() throws OperationFailedException {
        ApiId scope = mock(ApiId.class);
        when(scope.oid()).thenReturn(7L);
        when(scope.isRealtimeMarket()).thenReturn(false);
        when(scope.isGroup()).thenReturn(true);

        when(groupExpander.expandOids(Collections.singleton(7L))).thenReturn(Collections.singleton(1L));

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());

        assertThat(expandedScope.isAll(), is(false));
        assertThat(expandedScope.getEntities(), is(Collections.singleton(1L)));
    }

    @Test
    public void testExpandScopeTarget() throws OperationFailedException {
        ApiId scope = mock(ApiId.class);
        when(scope.oid()).thenReturn(7L);
        when(scope.isRealtimeMarket()).thenReturn(false);
        when(scope.isGroup()).thenReturn(false);
        when(scope.isTarget()).thenReturn(true);

        final SearchRequest discoveredReq = ApiTestUtils.mockSearchIdReq(Collections.singleton(1L));
        when(repositoryApi.newSearchRequest(SearchProtoUtil.makeSearchParameters(
            SearchProtoUtil.discoveredBy(7L)).build())).thenReturn(discoveredReq);

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());

        assertThat(expandedScope.isAll(), is(false));
        assertThat(expandedScope.getEntities(), is(Collections.singleton(1L)));
    }

    @Test
    public void testExpandScopePlan() throws OperationFailedException {
        ApiId scope = mock(ApiId.class);
        when(scope.oid()).thenReturn(7L);
        when(scope.isRealtimeMarket()).thenReturn(false);
        when(scope.isGroup()).thenReturn(false);
        when(scope.isTarget()).thenReturn(false);
        when(scope.isPlan()).thenReturn(true);

        PlanInstance planInstance = PlanInstance.newBuilder()
            .setPlanId(121)
            .setStatus(PlanStatus.READY)
            .setScenario(Scenario.newBuilder()
                .setId(111)
                .setScenarioInfo(ScenarioInfo.newBuilder()
                    .setScope(PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder()
                            .setScopeObjectOid(1L)))))
            .build();
        when(scope.getPlanInstance()).thenReturn(Optional.of(planInstance));

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());

        assertThat(expandedScope.isAll(), is(false));
        assertThat(expandedScope.getEntities(), is(Collections.singleton(1L)));
    }

    @Test
    public void testExpandScopeEntity() throws OperationFailedException {
        ApiId scope = mock(ApiId.class);
        when(scope.oid()).thenReturn(7L);
        when(scope.isRealtimeMarket()).thenReturn(false);
        when(scope.isGroup()).thenReturn(false);
        when(scope.isTarget()).thenReturn(false);
        when(scope.isPlan()).thenReturn(false);
        when(scope.isEntity()).thenReturn(true);

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());

        assertThat(expandedScope.isAll(), is(false));
        assertThat(expandedScope.getEntities(), is(Collections.singleton(7L)));
    }

    @Test
    public void testExpandDcToPm() throws OperationFailedException {
        ApiId scope = mock(ApiId.class);
        when(scope.oid()).thenReturn(DC.getOid());
        when(scope.isRealtimeMarket()).thenReturn(false);
        when(scope.isGroup()).thenReturn(false);
        when(scope.isTarget()).thenReturn(false);
        when(scope.isPlan()).thenReturn(false);
        when(scope.isEntity()).thenReturn(true);

        final SupplyChainNodeFetcherBuilder fetcherBuilder = ApiTestUtils.mockNodeFetcherBuilder(
            ImmutableMap.of(UIEntityType.PHYSICAL_MACHINE.apiStr(),
                SupplyChainNode.newBuilder()
                    .putMembersByState(UIEntityState.ACTIVE.toEntityState().getNumber(), MemberList.newBuilder()
                        .addMemberOids(1L)
                        .build())
                    .build()));
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());

        assertThat(expandedScope.isAll(), is(false));
        assertThat(expandedScope.getEntities(), is(Collections.singleton(1L)));
    }

    @Test
    public void testExpandDcToPmFailedQuery() throws OperationFailedException {
        ApiId scope = mock(ApiId.class);
        when(scope.oid()).thenReturn(DC.getOid());
        when(scope.isRealtimeMarket()).thenReturn(false);
        when(scope.isGroup()).thenReturn(false);
        when(scope.isTarget()).thenReturn(false);
        when(scope.isPlan()).thenReturn(false);
        when(scope.isEntity()).thenReturn(true);

        final SupplyChainNodeFetcherBuilder fetcherBuilder = ApiTestUtils.mockNodeFetcherBuilder(Collections.emptyMap());
        when(fetcherBuilder.fetch()).thenThrow(new OperationFailedException("foo"));
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());

        assertThat(expandedScope.isAll(), is(false));
        assertThat(expandedScope.getEntities(), is(Collections.singleton(DC.getOid())));
    }

    @Test
    public void testExpandGetRelatedTypes() throws OperationFailedException {
        ApiId scope = mock(ApiId.class);
        when(scope.oid()).thenReturn(7L);
        when(scope.isRealtimeMarket()).thenReturn(false);
        when(scope.isGroup()).thenReturn(false);
        when(scope.isTarget()).thenReturn(false);
        when(scope.isPlan()).thenReturn(false);
        when(scope.isEntity()).thenReturn(true);

        final StatApiInputDTO inputStat = new StatApiInputDTO();
        inputStat.setName("foo");
        inputStat.setRelatedEntityType(UIEntityType.PHYSICAL_MACHINE.apiStr());

        when(supplyChainFetcherFactory.expandScope(Collections.singleton(7L),
            Collections.singletonList(UIEntityType.PHYSICAL_MACHINE.apiStr()))).thenReturn(Collections.singleton(1L));

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.singletonList(inputStat));

        verify(supplyChainFetcherFactory).expandScope(Collections.singleton(7L),
            Collections.singletonList(UIEntityType.PHYSICAL_MACHINE.apiStr()));

        assertThat(expandedScope.isAll(), is(false));
        assertThat(expandedScope.getEntities(), is(Collections.singleton(1L)));
    }

    @Test
    public void testExpandDcRelatedToDc() throws OperationFailedException {
        // The UI sometimes requests stats for DC entities w/related entity "DataCenter". And even
        // though the related entity type is "DataCenter", we expect the final entity type to be
        // that of a PhysicalMachine.
        ApiId scope = mock(ApiId.class);
        when(scope.oid()).thenReturn(DC.getOid());
        when(scope.isRealtimeMarket()).thenReturn(false);
        when(scope.isGroup()).thenReturn(false);
        when(scope.isTarget()).thenReturn(false);
        when(scope.isPlan()).thenReturn(false);
        when(scope.isEntity()).thenReturn(true);

        final SupplyChainNodeFetcherBuilder fetcherBuilder = ApiTestUtils.mockNodeFetcherBuilder(
                ImmutableMap.of(
                        UIEntityType.DATACENTER.apiStr(),
                        SupplyChainNode.newBuilder()
                                .putMembersByState(UIEntityState.ACTIVE.toEntityState().getNumber(), MemberList.newBuilder()
                                        .addMemberOids(DC.getOid())
                                        .build())
                                .build()));
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);
        when(supplyChainFetcherFactory.expandScope(Collections.singleton(DC.getOid()),
                Collections.singletonList(UIEntityType.DATACENTER.apiStr()))).thenReturn(Collections.singleton(1L));

        final StatApiInputDTO inputStat = new StatApiInputDTO();
        inputStat.setName("foo");
        inputStat.setRelatedEntityType(UIEntityType.DATACENTER.apiStr());

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.singletonList(inputStat));

        assertThat(expandedScope.isAll(), is(false));
        // We should still land on a PM oid even though the related type "DC" was requested.
        assertThat(expandedScope.getEntities(), is(Collections.singleton(1L)));
    }

}
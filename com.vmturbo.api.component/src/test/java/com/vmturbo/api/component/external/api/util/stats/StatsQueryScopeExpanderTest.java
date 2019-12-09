package com.vmturbo.api.component.external.api.util.stats;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedGroupInfo;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.action.EntitySeverityDTOMoles.EntitySeverityServiceMole;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.identity.ArrayOidSet;

public class StatsQueryScopeExpanderTest {

    private final GroupExpander groupExpander = mock(GroupExpander.class);

    private final RepositoryApi repositoryApi = mock(RepositoryApi.class);

    private final EntitySeverityServiceMole severityServiceBackend = Mockito.spy(new EntitySeverityServiceMole());

    private final SupplyChainServiceMole supplyChainServiceBackend = Mockito.spy(new SupplyChainServiceMole());

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(supplyChainServiceBackend, severityServiceBackend);

    private final UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private SupplyChainFetcherFactory supplyChainFetcherFactory;

    private StatsQueryScopeExpander scopeExpander;

    private static final MinimalEntity DC = MinimalEntity.newBuilder()
        .setOid(123123)
        .setEntityType(UIEntityType.DATACENTER.typeNumber())
        .setDisplayName("dc")
        .build();

    @Before
    public void setup() {
        supplyChainFetcherFactory = spy(new SupplyChainFetcherFactory(
            SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            EntitySeverityServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            repositoryApi,
            groupExpander,
            7));
        scopeExpander = new StatsQueryScopeExpander(groupExpander, repositoryApi,
            supplyChainFetcherFactory, userSessionContext);
        // Doing this here because we make this RPC in every call.
        final SearchRequest req = ApiTestUtils.mockSearchMinReq(Collections.singletonList(DC));
        final SearchRequest emptyReq = ApiTestUtils.mockSearchMinReq(Collections.emptyList());
        when(repositoryApi.newSearchRequest(any())).thenReturn(req);
    }

    @Test
    public void testExpandUnscopedMarket() throws OperationFailedException {
        final ApiId scope = mock(ApiId.class);
        when(scope.isRealtimeMarket()).thenReturn(true);
        when(userSessionContext.isUserScoped()).thenReturn(false);

        final StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());
        assertThat(expandedScope.getGlobalScope(), is(Optional.of(ImmutableGlobalScope.builder()
            .build())));
    }

    @Test
    public void testExpandMarketRelatedEntityType() throws OperationFailedException {
        final ApiId scope = mock(ApiId.class);
        when(scope.isRealtimeMarket()).thenReturn(true);
        when(userSessionContext.isUserScoped()).thenReturn(false);

        List<StatApiInputDTO> input = Collections.singletonList(new StatApiInputDTO());
        input.get(0).setRelatedEntityType(UIEntityType.VIRTUAL_MACHINE.apiStr());
        final StatsQueryScope expandedScope = scopeExpander.expandScope(scope, input);
        assertThat(expandedScope.getEntities(), is(Collections.emptySet()));
        assertThat(expandedScope.getGlobalScope(), is(Optional.of(ImmutableGlobalScope.builder()
            .addEntityTypes(UIEntityType.VIRTUAL_MACHINE)
            .build())));
    }

    @Test
    public void testExpandMarketGlobalTempGroup() throws OperationFailedException {
        final ApiId scope = mock(ApiId.class);
        when(scope.isRealtimeMarket()).thenReturn(false);
        when(scope.isGlobalTempGroup()).thenReturn(true);

        final CachedGroupInfo groupInfo = mock(CachedGroupInfo.class);
        when(groupInfo.isGlobalTempGroup()).thenReturn(true);
        when(groupInfo.getEntityTypes()).thenReturn(Collections.singleton(UIEntityType.VIRTUAL_MACHINE));
        when(groupInfo.getGlobalEnvType()).thenReturn(Optional.of(EnvironmentType.CLOUD));

        when(scope.getCachedGroupInfo()).thenReturn(Optional.of(groupInfo));

        final StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());
        assertThat(expandedScope.getEntities(), is(Collections.emptySet()));
        assertThat(expandedScope.getGlobalScope(), is(Optional.of(ImmutableGlobalScope.builder()
            .addEntityTypes(UIEntityType.VIRTUAL_MACHINE)
            .environmentType(EnvironmentType.CLOUD)
            .build())));
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
        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
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

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
        assertThat(expandedScope.getEntities(), is(Collections.singleton(1L)));
    }

    /**
     * Tests expanding a group without members.
     */
    @Test
    public void testExpandEmptyScopeGroup() throws OperationFailedException {
        ApiId scope = mock(ApiId.class);
        when(scope.oid()).thenReturn(7L);
        when(scope.isRealtimeMarket()).thenReturn(false);
        when(scope.isGroup()).thenReturn(true);

        when(groupExpander.expandOids(Collections.singleton(7L))).thenReturn(Collections.emptySet());

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
        assertThat(expandedScope.getEntities(), empty());
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

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
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

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
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

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
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

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
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

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
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
        when(scope.getScopeTypes()).thenReturn(Optional.empty());

        final StatApiInputDTO inputStat = new StatApiInputDTO();
        inputStat.setName("foo");
        inputStat.setRelatedEntityType(UIEntityType.PHYSICAL_MACHINE.apiStr());

        doReturn(Collections.singleton(1L)).when(supplyChainFetcherFactory).expandScope(
            Collections.singleton(7L),
            Collections.singletonList(UIEntityType.PHYSICAL_MACHINE.apiStr()));

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.singletonList(inputStat));

        verify(supplyChainFetcherFactory).expandScope(Collections.singleton(7L),
            Collections.singletonList(UIEntityType.PHYSICAL_MACHINE.apiStr()));

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
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
        when(scope.getScopeTypes()).thenReturn(Optional.empty());

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

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
        // We should still land on a PM oid even though the related type "DC" was requested.
        assertThat(expandedScope.getEntities(), is(Collections.singleton(1L)));
    }

}

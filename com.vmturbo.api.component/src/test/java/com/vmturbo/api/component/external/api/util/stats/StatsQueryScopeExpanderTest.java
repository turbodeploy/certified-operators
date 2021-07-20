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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import io.grpc.Status;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedEntityInfo;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedGroupInfo;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.StatsQueryScope;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.action.EntitySeverityDTOMoles.EntitySeverityServiceMole;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.identity.ArrayOidSet;

public class StatsQueryScopeExpanderTest {

    private final GroupExpander groupExpander = mock(GroupExpander.class);

    private final RepositoryApi repositoryApi = mock(RepositoryApi.class);

    private final EntitySeverityServiceMole severityServiceBackend = Mockito.spy(new EntitySeverityServiceMole());

    private final SupplyChainServiceMole supplyChainServiceBackend = Mockito.spy(new SupplyChainServiceMole());

    private final CostServiceMole costServiceMole = Mockito.spy(new CostServiceMole());

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(supplyChainServiceBackend,
            severityServiceBackend, costServiceMole);

    private final UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private SupplyChainFetcherFactory supplyChainFetcherFactory;

    private StatsQueryScopeExpander scopeExpander;

    private static final MinimalEntity DC = MinimalEntity.newBuilder()
        .setOid(123123)
        .setEntityType(ApiEntityType.DATACENTER.typeNumber())
        .setDisplayName("dc")
        .build();

    private UuidMapper uuidMapper = mock(UuidMapper.class);

    @Before
    public void setup() {
        supplyChainFetcherFactory = spy(new SupplyChainFetcherFactory(
            SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            EntitySeverityServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            repositoryApi,
            groupExpander,
            mock(EntityAspectMapper.class),
            CostServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                7));
        supplyChainFetcherFactory.setUuidMapper(uuidMapper);
        scopeExpander = new StatsQueryScopeExpander(supplyChainFetcherFactory, userSessionContext);
        // Doing this here because we make this RPC in every call.
        final SearchRequest req = ApiTestUtils.mockSearchMinReq(Collections.singletonList(DC));
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
        input.get(0).setRelatedEntityType(ApiEntityType.VIRTUAL_MACHINE.apiStr());
        final StatsQueryScope expandedScope = scopeExpander.expandScope(scope, input);
        assertThat(expandedScope.getExpandedOids(), is(Collections.emptySet()));
        assertThat(expandedScope.getGlobalScope(), is(Optional.of(ImmutableGlobalScope.builder()
            .addEntityTypes(ApiEntityType.VIRTUAL_MACHINE)
            .build())));
    }

    @Test
    public void testExpandMarketGlobalTempGroup() throws OperationFailedException {
        final ApiId scope = mock(ApiId.class);
        when(scope.isRealtimeMarket()).thenReturn(false);
        when(scope.isGlobalTempGroup()).thenReturn(true);

        final CachedGroupInfo groupInfo = mock(CachedGroupInfo.class);
        when(groupInfo.isGlobalTempGroup()).thenReturn(true);
        when(groupInfo.getEntityTypes()).thenReturn(Collections.singleton(ApiEntityType.VIRTUAL_MACHINE));
        when(groupInfo.getGlobalEnvType()).thenReturn(Optional.of(EnvironmentType.CLOUD));

        when(scope.getCachedGroupInfo()).thenReturn(Optional.of(groupInfo));

        final StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());
        assertThat(expandedScope.getExpandedOids(), is(Collections.emptySet()));
        assertThat(expandedScope.getGlobalScope(), is(Optional.of(ImmutableGlobalScope.builder()
            .addEntityTypes(ApiEntityType.VIRTUAL_MACHINE)
            .environmentType(EnvironmentType.CLOUD)
            .build())));
    }

    @Test
    public void testExpandScopedMarket() throws OperationFailedException {
        ApiId scope = mock(ApiId.class);
        when(scope.isRealtimeMarket()).thenReturn(true);
        when(scope.getScopeOids(userSessionContext, Collections.emptyList())).thenReturn(Collections.singleton(1L));
        when(userSessionContext.isUserScoped()).thenReturn(true);

        final EntityAccessScope accessScope = mock(EntityAccessScope.class);
        when(accessScope.accessibleOids()).thenReturn(new ArrayOidSet(Collections.singletonList(1L)));

        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());
        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
        assertThat(expandedScope.getExpandedOids(), is(Collections.singleton(1L)));
    }

    @Test
    public void testExpandScopeGroup() throws OperationFailedException {
        ApiId scope = ApiTestUtils.mockGroupId("7", uuidMapper);
        ApiTestUtils.mockGroupId("1", uuidMapper);
        when(scope.getScopeOids(userSessionContext, Collections.emptyList())).thenReturn(Collections.singleton(1L));

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
        assertThat(expandedScope.getExpandedOids(), is(Collections.singleton(1L)));
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

        when(groupExpander.expandOids(Collections.singleton(scope))).thenReturn(Collections.emptySet());

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
        assertThat(expandedScope.getExpandedOids(), empty());
    }

    @Test
    public void testExpandScopeTarget() throws OperationFailedException {
        ApiId scope = mock(ApiId.class);
        when(scope.oid()).thenReturn(7L);
        when(scope.isRealtimeMarket()).thenReturn(false);
        when(scope.isGroup()).thenReturn(false);
        when(scope.isTarget()).thenReturn(true);
        when(uuidMapper.fromOid(7L)).thenReturn(scope);
        ApiTestUtils.mockEntityId("1", ApiEntityType.VIRTUAL_MACHINE, uuidMapper);
        when(scope.getScopeOids(userSessionContext, Collections.emptyList())).thenReturn(Collections.singleton(1L));

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
        assertThat(expandedScope.getExpandedOids(), is(Collections.singleton(1L)));
    }

    @Test
    public void testExpandScopePlan() throws OperationFailedException {
        ApiId scope = ApiTestUtils.mockPlanId("7", uuidMapper);
        when(scope.getScopeOids(userSessionContext, Collections.emptyList())).thenReturn(Collections.singleton(1L));

        final UuidMapper.CachedPlanInfo planInfo = mock(UuidMapper.CachedPlanInfo.class);
        when(planInfo.getPlanScopeIds()).thenReturn(Sets.newHashSet(1L));
        when(scope.getCachedPlanInfo()).thenReturn(Optional.of(planInfo));

        ApiId entityId = ApiTestUtils.mockEntityId("1", uuidMapper);
        CachedEntityInfo c = mock(CachedEntityInfo.class);
        when(c.getEntityType()).thenReturn(ApiEntityType.VIRTUAL_MACHINE);
        when(entityId.getCachedEntityInfo()).thenReturn(Optional.of(c));

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
        assertThat(expandedScope.getExpandedOids(), is(Collections.singleton(1L)));
    }

    @Test
    public void testExpandScopeEntity() throws OperationFailedException {
        ApiId scope = ApiTestUtils.mockEntityId("7", ApiEntityType.VIRTUAL_MACHINE, uuidMapper);
        when(scope.getScopeOids(userSessionContext, Collections.emptyList())).thenReturn(Collections.singleton(7L));

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
        assertThat(expandedScope.getExpandedOids(), is(Collections.singleton(7L)));
    }

    @Test
    public void testExpandDcToPm() throws OperationFailedException {
        ApiId scope = ApiTestUtils.mockEntityId(Long.toString(DC.getOid()), uuidMapper);
        CachedEntityInfo i = mock(CachedEntityInfo.class);
        when(i.getEntityType()).thenReturn(ApiEntityType.DATACENTER);
        when(scope.getCachedEntityInfo()).thenReturn(Optional.of(i));
        when(scope.getScopeOids(userSessionContext, Collections.emptyList())).thenReturn(Collections.singleton(DC.getOid()));

        final SupplyChainNodeFetcherBuilder fetcherBuilder = ApiTestUtils.mockNodeFetcherBuilder(
            ImmutableMap.of(ApiEntityType.PHYSICAL_MACHINE.apiStr(),
                SupplyChainNode.newBuilder()
                    .putMembersByState(UIEntityState.ACTIVE.toEntityState().getNumber(), MemberList.newBuilder()
                        .addMemberOids(1L)
                        .build())
                    .build()));
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);
        when(supplyChainServiceBackend.getMultiSupplyChains(any()))
            .thenReturn(Collections.singletonList(GetMultiSupplyChainsResponse.newBuilder()
                    // The 1 here is hard-coded in the single-scope implementation of
                    // SupplyChainFetcherFactory.
                    .setSeedOid(1L)
                    .setSupplyChain(SupplyChain.newBuilder()
                        .addSupplyChainNodes(SupplyChainNode.newBuilder()
                            .putMembersByState(UIEntityState.ACTIVE.toEntityState().getNumber(), MemberList.newBuilder()
                                    .addMemberOids(1L)
                                    .build())))
                    .build()));

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
        assertThat(expandedScope.getExpandedOids(), is(Collections.singleton(1L)));
    }

    @Test
    public void testExpandDcToPmFailedQuery() throws OperationFailedException {
        ApiId scope = ApiTestUtils.mockEntityId(Long.toString(DC.getOid()), ApiEntityType.DATACENTER, uuidMapper);
        when(scope.getScopeOids(userSessionContext, Collections.emptyList())).thenReturn(Collections.singleton(DC.getOid()));

        doReturn(Optional.of(Status.INTERNAL.asException())).when(supplyChainServiceBackend).getMultiSupplyChainsError(any());

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
        assertThat(expandedScope.getExpandedOids(), is(Collections.singleton(DC.getOid())));
    }

    @Test
    public void testExpandGetRelatedTypes() throws OperationFailedException {
        ApiId scope = ApiTestUtils.mockEntityId("7", ApiEntityType.VIRTUAL_MACHINE, uuidMapper);
        ApiTestUtils.mockEntityId("1", ApiEntityType.PHYSICAL_MACHINE, uuidMapper);

        final StatApiInputDTO inputStat = new StatApiInputDTO();
        inputStat.setName("foo");
        inputStat.setRelatedEntityType(ApiEntityType.PHYSICAL_MACHINE.apiStr());

        when(scope.getScopeOids(userSessionContext, Collections.singletonList(inputStat))).thenReturn(Collections.singleton(7L));

        doReturn(Collections.singleton(1L)).when(supplyChainFetcherFactory).expandScope(
            Collections.singleton(7L),
            Collections.singletonList(ApiEntityType.PHYSICAL_MACHINE.apiStr()),
            0L);

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.singletonList(inputStat));

        expandedScope.getExpandedOids();

        verify(supplyChainFetcherFactory).expandScope(Collections.singleton(7L),
            Collections.singletonList(ApiEntityType.PHYSICAL_MACHINE.apiStr()),
                0L);

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
        assertThat(expandedScope.getExpandedOids(), is(Collections.singleton(1L)));
    }

    /**
     * Test to check that the scope for an observer role gets expanded properly.
     */
    @Test
    public void testExpandEmptyRelatedTypesForObserver() {
        ApiId scope = ApiTestUtils.mockEntityId("7", ApiEntityType.VIRTUAL_MACHINE, uuidMapper);
        final StatApiInputDTO inputStat = new StatApiInputDTO();
        EntityAccessScope accessScope = new EntityAccessScope(null, null,
            new ArrayOidSet(Arrays.asList(7L)), null);

        when(userSessionContext.getUserAccessScope()).thenReturn(accessScope);
        when(userSessionContext.isUserObserver()).thenReturn(true);
        when(scope.oid()).thenReturn(7L);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(scope.getScopeOids(userSessionContext, Collections.singletonList(inputStat))).thenReturn(Collections.singleton(7L));

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.singletonList(inputStat));
        assertThat(expandedScope.getExpandedOids(), is(Collections.singleton(7L)));
    }

    @Test
    public void testExpandDcRelatedToDc() throws OperationFailedException {
        // The UI sometimes requests stats for DC entities w/related entity "DataCenter". And even
        // though the related entity type is "DataCenter", we expect the final entity type to be
        // that of a PhysicalMachine.
        ApiId scope = ApiTestUtils.mockEntityId(Long.toString(DC.getOid()), ApiEntityType.DATACENTER, uuidMapper);
        when(scope.getScopeTypes()).thenReturn(Optional.empty());

        ApiTestUtils.mockEntityId("1", ApiEntityType.PHYSICAL_MACHINE, uuidMapper);

        final SupplyChainNodeFetcherBuilder fetcherBuilder = ApiTestUtils.mockNodeFetcherBuilder(
                ImmutableMap.of(
                        ApiEntityType.DATACENTER.apiStr(),
                        SupplyChainNode.newBuilder()
                                .putMembersByState(UIEntityState.ACTIVE.toEntityState().getNumber(), MemberList.newBuilder()
                                        .addMemberOids(DC.getOid())
                                        .build())
                                .build()));
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);
        when(supplyChainFetcherFactory.expandScope(Collections.singleton(DC.getOid()),
                Collections.singletonList(ApiEntityType.DATACENTER.apiStr()))).thenReturn(Collections.singleton(1L));

        final StatApiInputDTO inputStat = new StatApiInputDTO();
        inputStat.setName("foo");
        inputStat.setRelatedEntityType(ApiEntityType.DATACENTER.apiStr());

        when(scope.getScopeOids(userSessionContext, Collections.singletonList(inputStat))).thenReturn(Collections.singleton(DC.getOid()));

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.singletonList(inputStat));

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
        // We should still land on a PM oid even though the related type "DC" was requested.
        assertThat(expandedScope.getExpandedOids(), is(Collections.singleton(1L)));
    }
}

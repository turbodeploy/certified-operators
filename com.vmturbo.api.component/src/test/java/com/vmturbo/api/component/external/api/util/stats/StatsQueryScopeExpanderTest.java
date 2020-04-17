package com.vmturbo.api.component.external.api.util.stats;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
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
        scopeExpander = new StatsQueryScopeExpander(groupExpander, repositoryApi,
            supplyChainFetcherFactory, userSessionContext);
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
        ApiId scope = mock(ApiId.class);
        when(scope.oid()).thenReturn(7L);
        when(scope.isRealtimeMarket()).thenReturn(false);
        when(scope.isGroup()).thenReturn(true);
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

        when(groupExpander.expandOids(Collections.singleton(7L))).thenReturn(Collections.emptySet());

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
        when(scope.getScopeOids(userSessionContext, Collections.emptyList())).thenReturn(Collections.singleton(1L));

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
        assertThat(expandedScope.getExpandedOids(), is(Collections.singleton(1L)));
    }

    @Test
    public void testExpandScopePlan() throws OperationFailedException {
        ApiId scope = mock(ApiId.class);
        when(scope.oid()).thenReturn(7L);
        when(scope.isRealtimeMarket()).thenReturn(false);
        when(scope.isGroup()).thenReturn(false);
        when(scope.isTarget()).thenReturn(false);
        when(scope.isPlan()).thenReturn(true);
        when(scope.getScopeOids(userSessionContext, Collections.emptyList())).thenReturn(Collections.singleton(1L));

        final UuidMapper.CachedPlanInfo planInfo = mock(UuidMapper.CachedPlanInfo.class);
        when(planInfo.getPlanScopeIds()).thenReturn(Sets.newHashSet(1L));
        when(scope.getCachedPlanInfo()).thenReturn(Optional.of(planInfo));

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());

        assertTrue(expandedScope.getGlobalScope().isPresent());
        assertTrue(expandedScope.getExpandedOids().isEmpty());
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
        when(scope.getScopeOids(userSessionContext, Collections.emptyList())).thenReturn(Collections.singleton(7L));

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
        assertThat(expandedScope.getExpandedOids(), is(Collections.singleton(7L)));
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
        when(scope.getScopeOids(userSessionContext, Collections.emptyList())).thenReturn(Collections.singleton(DC.getOid()));

        final SupplyChainNodeFetcherBuilder fetcherBuilder = ApiTestUtils.mockNodeFetcherBuilder(
            ImmutableMap.of(ApiEntityType.PHYSICAL_MACHINE.apiStr(),
                SupplyChainNode.newBuilder()
                    .putMembersByState(UIEntityState.ACTIVE.toEntityState().getNumber(), MemberList.newBuilder()
                        .addMemberOids(1L)
                        .build())
                    .build()));
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
        assertThat(expandedScope.getExpandedOids(), is(Collections.singleton(1L)));
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
        when(scope.getScopeOids(userSessionContext, Collections.emptyList())).thenReturn(Collections.singleton(DC.getOid()));

        final SupplyChainNodeFetcherBuilder fetcherBuilder = ApiTestUtils.mockNodeFetcherBuilder(Collections.emptyMap());
        when(fetcherBuilder.fetch()).thenThrow(new OperationFailedException("foo"));
        when(supplyChainFetcherFactory.newNodeFetcher()).thenReturn(fetcherBuilder);

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.emptyList());

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
        assertThat(expandedScope.getExpandedOids(), is(Collections.singleton(DC.getOid())));
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
        inputStat.setRelatedEntityType(ApiEntityType.PHYSICAL_MACHINE.apiStr());

        when(scope.getScopeOids(userSessionContext, Collections.singletonList(inputStat))).thenReturn(Collections.singleton(7L));

        doReturn(Collections.singleton(1L)).when(supplyChainFetcherFactory).expandScope(
            Collections.singleton(7L),
            Collections.singletonList(ApiEntityType.PHYSICAL_MACHINE.apiStr()));

        StatsQueryScope expandedScope = scopeExpander.expandScope(scope, Collections.singletonList(inputStat));

        expandedScope.getExpandedOids();

        verify(supplyChainFetcherFactory).expandScope(Collections.singleton(7L),
            Collections.singletonList(ApiEntityType.PHYSICAL_MACHINE.apiStr()));

        assertThat(expandedScope.getGlobalScope(), is(Optional.empty()));
        assertThat(expandedScope.getExpandedOids(), is(Collections.singleton(1L)));
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

    /**
     * Test for finding expanded oids with and without related types.
     */
    @Test
    public void testExpandConnectedVMs() throws OperationFailedException {
        final ApiId mockScope = mock(ApiId.class);
        final long originalOid = 1L;
        when(mockScope.getScopeOids(userSessionContext, Collections.emptyList()))
            .thenReturn(Collections.singleton(originalOid));

        final long expandedFromOriginalId = 2L;
        doReturn(Collections.singleton(expandedFromOriginalId)).when(supplyChainFetcherFactory)
            .expandAggregatedEntities(eq(Collections.singleton(originalOid)));

        final long relatedOid = 3L;
        doReturn(Collections.singleton(relatedOid)).when(supplyChainFetcherFactory)
            .expandScope(any(), any());
        final long expandedFromRelatedId = 4L;
        doReturn(Collections.singleton(expandedFromRelatedId)).when(supplyChainFetcherFactory)
            .expandAggregatedEntities(eq(Collections.singleton(relatedOid)));

        final StatApiInputDTO stat = new StatApiInputDTO();
        stat.setRelatedEntityType(ApiEntityType.STORAGE_TIER.apiStr());
        final List<StatApiInputDTO> relatedTypeStats = Collections.singletonList(stat);
        when(mockScope.getScopeOids(userSessionContext, relatedTypeStats))
                .thenReturn(Collections.singleton(originalOid));

        // no related types - result comes from original id
        final StatsQueryScope result1 = scopeExpander.expandScope(mockScope, Collections.emptyList());
        assertEquals(Collections.singleton(expandedFromOriginalId), result1.getExpandedOids());

        // has related types - entities of related type are expanded first
        final StatsQueryScope result2 = scopeExpander.expandScope(mockScope, relatedTypeStats);
        assertEquals(Collections.singleton(expandedFromRelatedId), result2.getExpandedOids());

        // connected VM should not be added separately if the scope type doesn't have volume
        stat.setRelatedEntityType(ApiEntityType.VIRTUAL_MACHINE.apiStr());
        when(mockScope.getScopeTypes()).thenReturn(Optional.empty());
        final StatsQueryScope result3 = scopeExpander.expandScope(mockScope, relatedTypeStats);
        assertEquals(Collections.singleton(expandedFromRelatedId), result3.getExpandedOids());

        // if connected VM should be added separately but there are no connected VMs, original oid is used.
        final SearchRequest emptyReq = ApiTestUtils.mockSearchMinReq(Collections.emptyList());
        when(repositoryApi.newSearchRequest(any())).thenReturn(emptyReq);
        when(mockScope.getScopeTypes())
            .thenReturn(Optional.of(Collections.singleton(ApiEntityType.VIRTUAL_VOLUME)));
        final StatsQueryScope result4 = scopeExpander.expandScope(mockScope, relatedTypeStats);
        assertEquals(Collections.singleton(expandedFromOriginalId), result4.getExpandedOids());

        // connected VM is added separately
        final long vmId = 5;
        final long expandedFromVmAndOrigId = 6L;
        doReturn(Collections.singleton(expandedFromVmAndOrigId)).when(supplyChainFetcherFactory)
            .expandAggregatedEntities(eq(ImmutableSet.of(vmId, originalOid)));
        final SearchRequest goodRec = ApiTestUtils.mockSearchIdReq(Collections.singleton(vmId));
        when(repositoryApi.newSearchRequest(any())).thenReturn(goodRec);

        final StatsQueryScope result5 = scopeExpander.expandScope(mockScope, relatedTypeStats);
        assertEquals(Collections.singleton(expandedFromVmAndOrigId), result5.getExpandedOids());


        // related type expansion throws an exception (connected VMs not added)
        stat.setRelatedEntityType(ApiEntityType.STORAGE_TIER.apiStr());
        doThrow(new OperationFailedException("failed!!!!!!")).when(supplyChainFetcherFactory)
            .expandScope(any(), any());
        final StatsQueryScope result6 = scopeExpander.expandScope(mockScope, relatedTypeStats);
        assertEquals(Collections.singleton(originalOid), result6.getExpandedOids());
    }
}

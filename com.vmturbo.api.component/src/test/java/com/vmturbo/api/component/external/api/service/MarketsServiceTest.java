package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.api.MarketNotificationDTO.MarketNotification;
import com.vmturbo.api.MarketNotificationDTO.StatusNotification;
import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.MarketMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.PolicyMapper;
import com.vmturbo.api.component.external.api.mapper.PriceIndexPopulator;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedPlanInfo;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.ServiceProviderExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.stats.PlanEntityStatsFetcher;
import com.vmturbo.api.component.external.api.websocket.UINotificationChannel;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.market.MarketApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.enums.MergePolicyType;
import com.vmturbo.api.enums.PolicyType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.EntityPaginationRequest;
import com.vmturbo.api.pagination.EntityPaginationRequest.EntityPaginationResponse;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.EntitySeverityDTOMoles.EntitySeverityServiceMole;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyDeleteResponse;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyEditResponse;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.AtMostNPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MergePolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MergePolicy.MergeType;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyRequest;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles.PolicyServiceMole;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.UpdatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTOMoles.PlanServiceMole;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.common.protobuf.plan.ScenarioMoles.ScenarioServiceMole;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.ScenarioServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Unit test for {@link MarketsService}.
 */
public class MarketsServiceTest {

    private static final Logger logger = LogManager.getLogger();

    private static final long REALTIME_CONTEXT_ID = 777777;

    private static final long REALTIME_PLAN_ID = 0;
    private static final long TEST_PLAN_OVER_PLAN_ID = REALTIME_PLAN_ID + 1;
    private static final long TEST_SCENARIO_ID = 12;
    private static final List<Long> MERGE_GROUP_IDS = Arrays.asList(1L, 2L, 3L);
    private static final Map<MergePolicyType, EntityType> MERGE_PROVIDER_ENTITY_TYPE =
            ImmutableMap.of(MergePolicyType.DesktopPool, EntityType.DESKTOP_POOL,
                    MergePolicyType.DataCenter, EntityType.DATACENTER);
    private static final long POLICY_ID = 5;
    private static final long MERGE_GROUP_ID = 4;
    private static final String MARKET_UUID = "Market";

    private final PlanInstance planDefault = PlanInstance.newBuilder()
        .setPlanId(111)
                .addActionPlanId(222)
                .setStartTime(System.currentTimeMillis() - 10000)
        .setEndTime(System.currentTimeMillis() - 10)
        .setStatus(PlanStatus.QUEUED)
                .setScenario(Scenario.newBuilder()
                        .setId(555)
                        .setScenarioInfo(ScenarioInfo.newBuilder().setName("Some scenario")))
        .build();

    /**
     * JUnit rule to help represent expected exceptions in tests.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private ActionSpecMapper actionSpecMapper = mock(ActionSpecMapper.class);
    private UuidMapper uuidMapper = mock(UuidMapper.class);
    private PolicyMapper policyMapper = mock(PolicyMapper.class);
    private MarketMapper marketMapper = mock(MarketMapper.class);
    private PaginationMapper paginationMapper = mock(PaginationMapper.class);

    private PoliciesService policiesService = mock(PoliciesService.class);

    private UINotificationChannel uiNotificationChannel = mock(UINotificationChannel.class);

    private ActionStatsQueryExecutor actionStatsQueryExecutor = mock(ActionStatsQueryExecutor.class);

    private ThinTargetCache thinTargetCache = mock(ThinTargetCache.class);

    private StatsService statsService = mock(StatsService.class);

    private RepositoryApi repositoryApi = mock(RepositoryApi.class);

    private ServiceEntityMapper serviceEntityMapper = mock(ServiceEntityMapper.class);

    private SeverityPopulator severityPopulator = mock(SeverityPopulator.class);
    private PriceIndexPopulator priceIndexPopulator = mock(PriceIndexPopulator.class);
    private PlanEntityStatsFetcher planEntityStatsFetcher = mock(PlanEntityStatsFetcher.class);


    private ActionsServiceMole actionsBackend = spy(ActionsServiceMole.class);
    private PolicyServiceMole policiesBackend = spy(PolicyServiceMole.class);
    private PlanServiceMole planBackend = spy(PlanServiceMole.class);
    private ScenarioServiceMole scenarioBackend = spy(ScenarioServiceMole.class);
    private RepositoryServiceMole repositoryBackend = spy(RepositoryServiceMole.class);

    private EntitySeverityServiceMole entitySeverityBackend = spy(EntitySeverityServiceMole.class);

    private GroupServiceMole groupBackend = spy(GroupServiceMole.class);

    private SearchServiceMole searchBackend = spy(SearchServiceMole.class);

    private final ServiceProviderExpander serviceProviderExpander = mock(ServiceProviderExpander.class);

    /**
     * Test gRPC server to mock out gRPC dependencies.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(actionsBackend, policiesBackend,
        planBackend, scenarioBackend, repositoryBackend, entitySeverityBackend,
        groupBackend, searchBackend);

    private MarketsService marketsService;

    /**
     * Startup method to run before every test.
     */
    @Before
    public void startup() {
        final ActionsServiceBlockingStub actionsRpcService = ActionsServiceGrpc.newBlockingStub(
                grpcTestServer.getChannel());
        marketsService = new MarketsService(actionSpecMapper, uuidMapper,
            ActionsServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            policiesService,
            PolicyServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            PlanServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            ScenarioServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            policyMapper,
            marketMapper,
            paginationMapper,
            GroupServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            RepositoryServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            uiNotificationChannel,
            actionStatsQueryExecutor,
            thinTargetCache,
            statsService,
            repositoryApi,
            serviceEntityMapper,
            severityPopulator,
            priceIndexPopulator,
            actionsRpcService,
            planEntityStatsFetcher,
            SearchServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            new ActionSearchUtil(actionsRpcService, actionSpecMapper, paginationMapper,
                                 Mockito.mock(SupplyChainFetcherFactory.class),
                                 Mockito.mock(GroupExpander.class),
                                 serviceProviderExpander,
                                 REALTIME_CONTEXT_ID),
            REALTIME_CONTEXT_ID
        );

    }

    /**
     * Tests getting all the plan instances (markets in API terminology).
     *
     * @throws Exception if exception occurs
     */
    @Test
    public void testGetAllMarkets() throws Exception {
        final PlanInstance plan1 = PlanInstance.newBuilder(planDefault).setPlanId(1).build();
        final PlanInstance plan2 = PlanInstance.newBuilder(planDefault).setPlanId(2).build();

        final MarketApiDTO mappedPlan1 = new MarketApiDTO();
        final MarketApiDTO mappedPlan2 = new MarketApiDTO();
        when(marketMapper.dtoFromPlanInstance(plan1)).thenReturn(mappedPlan1);
        when(marketMapper.dtoFromPlanInstance(plan2)).thenReturn(mappedPlan2);

        doReturn(Arrays.asList(plan1, plan2)).when(planBackend).getAllPlans(any());

        final List<MarketApiDTO> resp = marketsService.getMarkets(null);

        // Three markets are expected because the results include the realtime market
        assertEquals(3, resp.size());
        assertTrue(resp.contains(mappedPlan1));
        assertTrue(resp.contains(mappedPlan2));
        // Also check that the realtime market was returned.
        final MarketApiDTO realtimeMarket =
                resp.stream().filter(market -> market.getUuid().equals("777777")).findFirst().get();
        assertNotNull(realtimeMarket);
    }

    /**
     * Test that successfully deleting a "market" (i.e. plan) causes a DELETE notification
     * to be sent to the {@link UINotificationChannel}.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testMarketDeleteNotification() throws Exception {
        final PlanInstance plan1 = PlanInstance.newBuilder(planDefault).setPlanId(1).build();

        doReturn(plan1).when(planBackend).deletePlan(any());

        marketsService.deleteMarketByUuid("1");

        final ArgumentCaptor<MarketNotification> notificationCaptor =
                ArgumentCaptor.forClass(MarketNotification.class);
        Mockito.verify(uiNotificationChannel).broadcastMarketNotification(notificationCaptor.capture());

        final MarketNotification notification = notificationCaptor.getValue();
        assertEquals("1", notification.getMarketId());
        assertEquals(StatusNotification.Status.DELETED,
                notification.getStatusNotification().getStatus());
    }

    /**
     * Tests getting a plan instance (markets in API terminology) by its id.
     *
     * @throws Exception if exception occurs
     */
    @Test
    public void testGetMarketById() throws Exception {
        final PlanInstance plan2 = PlanInstance.newBuilder(planDefault).setPlanId(2).build();
        ApiTestUtils.mockPlanId("1", uuidMapper);
        ApiTestUtils.mockPlanId("2", uuidMapper);
        doReturn(OptionalPlanInstance.newBuilder()
            .setPlanInstance(plan2)
            .build()).when(planBackend).getPlan(PlanId.newBuilder()
            .setPlanId(2)
            .build());

        final MarketApiDTO mappedDto = new MarketApiDTO();
        when(marketMapper.dtoFromPlanInstance(plan2)).thenReturn(mappedDto);
        MarketApiDTO response = marketsService.getMarketByUuid("2");
        assertThat(response, is(mappedDto));
    }

    /**
     * Test that running plan over main topology makes the appropriate RPC calls.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRunPlanOnMainMarket() throws Exception {
        ApiTestUtils.mockRealtimeId(MARKET_UUID, REALTIME_PLAN_ID, uuidMapper);

        final PlanInstance newPlan = PlanInstance.newBuilder(planDefault)
            .build();

        doReturn(newPlan).when(planBackend).runPlan(any());

        MarketApiDTO mappedNewPlan = new MarketApiDTO();
        when(marketMapper.dtoFromPlanInstance(newPlan)).thenReturn(mappedNewPlan);

        final MarketApiDTO resp = marketsService.applyAndRunScenario(
            MARKET_UUID, 1L, false, null);

        assertThat(resp, is(mappedNewPlan));
    }

    /**
     * Test that running plan over plan makes the appropriate RPC calls.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRunPlanOnPlan() throws Exception {

        final PlanInstance plan1 = PlanInstance.newBuilder(planDefault)
            .setPlanId(TEST_PLAN_OVER_PLAN_ID)
            .setStatus(PlanStatus.SUCCEEDED)
            .build();
        final PlanInstance newPlan = PlanInstance.newBuilder(planDefault)
            .build();
        ApiTestUtils.mockPlanId(Long.toString(TEST_PLAN_OVER_PLAN_ID), uuidMapper);

        doReturn(OptionalPlanInstance.newBuilder()
            .setPlanInstance(plan1)
            .build()).when(planBackend).getPlan(PlanId.newBuilder()
                .setPlanId(TEST_PLAN_OVER_PLAN_ID)
                .build());
        doReturn(newPlan).when(planBackend).runPlan(any());

        MarketApiDTO mappedNewPlan = new MarketApiDTO();
        when(marketMapper.dtoFromPlanInstance(newPlan)).thenReturn(mappedNewPlan);

        // the market UUID is the ID of the Plan Spec to start from
        String runPlanUri = "/markets/" + TEST_PLAN_OVER_PLAN_ID + "/scenarios/"
                + TEST_SCENARIO_ID;

        final MarketApiDTO resp = marketsService.applyAndRunScenario(
            Long.toString(TEST_PLAN_OVER_PLAN_ID), TEST_SCENARIO_ID, true, null);

        assertThat(resp, is(mappedNewPlan));
    }

    /**
     * Test that getting entities by real market Id causes a searchEntities call
     * to the search rpc service.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testRetrieveTopologyEntities() throws Exception {
        ApiTestUtils.mockRealtimeId(MARKET_UUID, REALTIME_PLAN_ID, uuidMapper);

        ServiceEntityApiDTO se1 = new ServiceEntityApiDTO();
        se1.setClassName(ApiEntityType.VIRTUAL_MACHINE.apiStr());
        se1.setUuid("1");
        ApiPartialEntity entity = ApiPartialEntity.newBuilder()
            .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
            .setOid(1)
            .build();
        doReturn(SearchEntitiesResponse.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setApi(entity))
            .build()).when(searchBackend).searchEntities(any());
        when(serviceEntityMapper.toServiceEntityApiDTO(Collections.singletonList(entity)))
            .thenReturn(Collections.singletonList(se1));

        when(paginationMapper.toProtoParams(any())).thenReturn(PaginationParameters.getDefaultInstance());
        // act
        EntityPaginationResponse response = marketsService.getEntitiesByMarketUuid(MARKET_UUID,
            new EntityPaginationRequest(null, null, true, null));

        // verify
        assertThat(response.getRawResults(), containsInAnyOrder(se1));
        verify(searchBackend).searchEntities(any());
        verify(priceIndexPopulator).populateRealTimeEntities(Collections.singletonList(se1));
        verify(severityPopulator).populate(REALTIME_CONTEXT_ID, Collections.singletonList(se1));
    }

    /**
     * Test that getting entities by plan market Id causes a retrieveTopologyEntities call
     * to the repository rpc service and then properly paginates that response.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testRetrievePaginatedPlanEntities() throws Exception {
        final String planUuid = Long.toString(REALTIME_PLAN_ID);
        final long projectedPlanTopologyId = 56;
        ApiTestUtils.mockPlanId(planUuid, uuidMapper);

        ServiceEntityApiDTO se1 = new ServiceEntityApiDTO();
        se1.setClassName(ApiEntityType.VIRTUAL_MACHINE.apiStr());
        se1.setDisplayName("SE1");
        se1.setUuid("1");
        ServiceEntityApiDTO se2 = new ServiceEntityApiDTO();
        se2.setClassName(ApiEntityType.VIRTUAL_MACHINE.apiStr());
        se2.setDisplayName("SE2");
        se2.setUuid("2");
        ServiceEntityApiDTO se3 = new ServiceEntityApiDTO();
        se3.setClassName(ApiEntityType.VIRTUAL_MACHINE.apiStr());
        se3.setDisplayName("SE3");
        se3.setUuid("3");
        ServiceEntityApiDTO se4 = new ServiceEntityApiDTO();
        se4.setClassName(ApiEntityType.VIRTUAL_MACHINE.apiStr());
        se4.setDisplayName("SE4");
        se4.setUuid("4");
        final List<PartialEntity> partialEntities = Stream.of(1, 2, 3, 4)
            .map(id -> PartialEntity.newBuilder()
                .setApi(ApiPartialEntity.newBuilder()
                    .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                    .setOid(id))
                .build())
            .collect(Collectors.toList());
        doReturn(OptionalPlanInstance.newBuilder()
            .setPlanInstance(PlanInstance.newBuilder()
                .setPlanId(123)
                .setProjectedTopologyId(projectedPlanTopologyId)
                .setStatus(PlanStatus.SUCCEEDED))
            .build())
            .when(planBackend).getPlan(any());
        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addAllEntities(partialEntities)
            .build())).when(repositoryBackend).retrieveTopologyEntities(any());
        when(serviceEntityMapper.toServiceEntityApiDTO(any(TopologyEntityDTO.class)))
            .thenReturn(se1).thenReturn(se2).thenReturn(se3).thenReturn(se4);

        when(paginationMapper.toProtoParams(any())).thenReturn(PaginationParameters.getDefaultInstance());

        // Pagination parameters - asks for records 2-3
        final String cursor = "1";
        final Integer limit = 2;
        // act
        EntityPaginationResponse response = marketsService.getEntitiesByMarketUuid(
            planUuid,
            new EntityPaginationRequest(cursor, limit, true, null));

        // verify
        assertThat(response.getRawResults(), containsInAnyOrder(se2, se3));
        Assert.assertEquals("3", response.getRestResponse().getHeaders().get("X-Next-Cursor").get(0));
        Assert.assertEquals("4", response.getRestResponse().getHeaders().get("X-Total-Record-Count").get(0));
        verify(planBackend).getPlan(any());
        verify(repositoryBackend).retrieveTopologyEntities(any());
        verify(priceIndexPopulator)
            .populatePlanEntities(projectedPlanTopologyId, Arrays.asList(se2, se3));
        verify(severityPopulator).populate(REALTIME_PLAN_ID, Arrays.asList(se2, se3));
    }

    /**
     * call the MarketsService group stats API and check the results.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testGetStatsByMarketAndGroup() throws Exception {
        // Setup the test
        final Set<Long> expandedUids = new HashSet<>();
        expandedUids.add(1L);
        expandedUids.add(2L);
        expandedUids.add(3L);

        // Mock expected members response
        GetMembersResponse membersResponse = GetMembersResponse.newBuilder()
                .setGroupId(1L)
                .addAllMemberId(expandedUids)
                .build();
        Mockito.when(groupBackend.getMembers(Mockito.any()))
                .thenReturn(Collections.singletonList(membersResponse));

        // Mock call to stats service which is called from MarketService
        final EntityStatsPaginationRequest paginationRequest = new EntityStatsPaginationRequest(null, 100, true, null);

        ArgumentCaptor<StatScopesApiInputDTO> scopeApiArgument = ArgumentCaptor.forClass(StatScopesApiInputDTO.class);

        ApiTestUtils.mockRealtimeId(StatsService.MARKET, REALTIME_PLAN_ID, uuidMapper);

        // Now execute the test
        // Test the most basic case where a group uuid is specified and no other data in the input
        StatScopesApiInputDTO inputDTO = new StatScopesApiInputDTO();
        // Invoke the service and verify that it results in calling getStatsByUuidsQuery with a scope size of 3
        marketsService.getStatsByEntitiesInGroupInMarketQuery(StatsService.MARKET, "5", inputDTO, paginationRequest);
        Mockito.verify(statsService).getStatsByUuidsQuery(scopeApiArgument.capture(), any());
        assertEquals(3, scopeApiArgument.getValue().getScopes().size());

        // Set the input scope to a subset of entities in the group membership
        final StatScopesApiInputDTO statScopesApiInputDTO = new StatScopesApiInputDTO();
        List<String> scopes = new ArrayList<>();
        scopes.add("1");
        scopes.add("2");
        scopes.add("4");
        statScopesApiInputDTO.setScopes(scopes);
        // Setting relatedType to VirtualMachine should have no impact on the results
        statScopesApiInputDTO.setRelatedType(ApiEntityType.VIRTUAL_MACHINE.apiStr());
        scopeApiArgument = ArgumentCaptor.forClass(StatScopesApiInputDTO.class);
        // Invoke the service and then verify that the service calls getStatsByUuidsQuery with a scope size of 2, since this is the overlap of the
        // group and the scope.
        marketsService.getStatsByEntitiesInGroupInMarketQuery(StatsService.MARKET, "5", statScopesApiInputDTO, paginationRequest);
        Mockito.verify(statsService, Mockito.times(2)).getStatsByUuidsQuery(scopeApiArgument.capture(), any());
        assertEquals(2, scopeApiArgument.getValue().getScopes().size());

        // Set the input related entity type to PhysicalMachine
        // This should be the same as the first test, but just verifying that the addition of the related entity
        // type does not change the scope sent to the getStatsByUuidsQuery
        inputDTO = new StatScopesApiInputDTO();
        inputDTO.setRelatedType(ApiEntityType.PHYSICAL_MACHINE.apiStr());
        marketsService.getStatsByEntitiesInGroupInMarketQuery(StatsService.MARKET, "5", inputDTO, paginationRequest);
        Mockito.verify(statsService, Mockito.times(3)).getStatsByUuidsQuery(scopeApiArgument.capture(), any());
        assertEquals(3, scopeApiArgument.getValue().getScopes().size());
    }

    /**
     * call the MarketsService group stats API with a group uuid
     * and a scope, such that the scope does not overlap the group
     * members.  So, the scope is effectively invalid.  This will
     * throw an illegal argument exception.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testGetStatsByMarketAndGroupError() throws Exception {
        expectedException.expect(IllegalArgumentException.class);

        // SETUP THE TEST
        final Set<Long> expandedUids = new HashSet<>();
        expandedUids.add(1L);
        expandedUids.add(2L);
        expandedUids.add(3L);

        // Mock expected members response
        GetMembersResponse membersResponse = GetMembersResponse.newBuilder()
                .setGroupId(1L)
                .addAllMemberId(expandedUids)
                .build();
        Mockito.when(groupBackend.getMembers(Mockito.any()))
                .thenReturn(Collections.singletonList(membersResponse));

        ApiTestUtils.mockRealtimeId(StatsService.MARKET, REALTIME_PLAN_ID, uuidMapper);

        // Set the input scope to a subset of entities in the group membership
        final StatScopesApiInputDTO inputDTO = new StatScopesApiInputDTO();
        final EntityStatsPaginationRequest paginationRequest = new EntityStatsPaginationRequest(null, 100, true, null);

        // Set the input scope to a set of uuids, none of the uuids overlap with the group uuids
        List<String> testUuids = new ArrayList<>();
        testUuids.add("6");
        testUuids.add("7");
        inputDTO.setScopes(testUuids);
        // This should throw an exception since there are no members in the overlap of the group and the input scope
        marketsService.getStatsByEntitiesInGroupInMarketQuery(StatsService.MARKET, "5", inputDTO, paginationRequest);
    }

    /**
     * Test for {@link MarketsService#addPolicy(String, PolicyApiInputDTO)}.
     * When merge policy required hidden group.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testAddMergePolicyWhenNeedCreateHiddenGroup() throws Exception {
        testMergePolicyRequiredHiddenGroupAddOrEdit((policyApiInputDTO, groupDefinition) -> {
            final CreateGroupRequest createGroupRequest = CreateGroupRequest.newBuilder()
                    .setGroupDefinition(groupDefinition)
                    .setOrigin(Origin.newBuilder()
                            .setSystem(Origin.System.newBuilder()
                                    .setDescription(String.format(
                                            "Hidden group to support merge with type %s.",
                                            policyApiInputDTO.getMergeType()))))
                    .build();
            final CreateGroupResponse createGroupResponse = CreateGroupResponse.newBuilder()
                    .setGroup(Grouping.newBuilder().setId(MERGE_GROUP_ID))
                    .build();
            doReturn(createGroupResponse).when(groupBackend).createGroup(createGroupRequest);
            // Act
            try {
                marketsService.addPolicy(MARKET_UUID, policyApiInputDTO);
            } catch (UnknownObjectException e) {
                // this should never happen
                logger.error("UnknownObjectException caught while creating policy", e);
                fail("Policy creation failed");
            }
        });
    }

    /**
     * Validate that {@link MarketsService#editPolicy(String, String, PolicyApiInputDTO)} returns a
     * dto with values populated when editing a policy.
     *
     * @throws InterruptedException on exceptions occurred
     * @throws ConversionException on exceptions occurred
     * @throws OperationFailedException on exceptions occurred
     */
    @Test
    public void validateExistenceOfBasicFieldsInEditPolicyResponse()
            throws InterruptedException, ConversionException, OperationFailedException {
        PolicyApiInputDTO policyApiInputDTO = new PolicyApiInputDTO();
        policyApiInputDTO.setPolicyName("Policy1");
        policyApiInputDTO.setType(PolicyType.AT_MOST_N);
        policyApiInputDTO.setSellerUuid("5");
        policyApiInputDTO.setBuyerUuid("8");
        policyApiInputDTO.setCapacity(50);
        policyApiInputDTO.setEnabled(true);
        PolicyApiDTO responsePolicyApiDTO = new PolicyApiDTO();
        responsePolicyApiDTO.setUuid("1");
        responsePolicyApiDTO.setName("Policy1");
        responsePolicyApiDTO.setDisplayName("Policy1");
        responsePolicyApiDTO.setType(PolicyType.AT_MOST_N);
        BaseApiDTO provider = new BaseApiDTO();
        provider.setUuid("5");
        responsePolicyApiDTO.setProviderGroup(provider);
        BaseApiDTO consumer = new BaseApiDTO();
        consumer.setUuid("8");
        responsePolicyApiDTO.setConsumerGroup(consumer);
        responsePolicyApiDTO.setCapacity(52); //changed value
        responsePolicyApiDTO.setEnabled(true);

        when(policyMapper.policyApiInputDtoToProto(policyApiInputDTO)).thenReturn(
                PolicyInfo.newBuilder().build());
        when(policiesBackend.editPolicy(any())).thenReturn(
                PolicyEditResponse.newBuilder().setPolicy(Policy.newBuilder()
                        .setId(1)
                        .setPolicyInfo(PolicyInfo.newBuilder()
                                .setName("Policy1")
                                .setAtMostN(AtMostNPolicy.newBuilder()
                                        .setCapacity((float)52)
                                        .setConsumerGroupId(8)
                                        .setProviderGroupId(5))
                        )
                ).build()
        );
        when(policiesService.toPolicyApiDTO(any())).thenReturn(responsePolicyApiDTO);

        PolicyApiDTO result = marketsService.editPolicy("Market", "1", policyApiInputDTO);

        assertEquals("1", result.getUuid());
        assertEquals("Policy1", result.getName());
        assertEquals("Policy1", result.getDisplayName());
        assertEquals(PolicyType.AT_MOST_N, result.getType());
        assertTrue(result.isEnabled());
        assertEquals(52, result.getCapacity().intValue());
        assertEquals("5", result.getProviderGroup().getUuid());
        assertEquals("8", result.getConsumerGroup().getUuid());
    }

    /**
     * Test for {@link MarketsService#editPolicy(String, String, PolicyApiInputDTO)}.
     * When merge policy required hidden group.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testEditMergePolicyWhenNeedUpdateHiddenGroup() throws Exception {
        testMergePolicyRequiredHiddenGroupAddOrEdit((policyApiInputDTO, groupDefinition) -> {
            doReturn(PolicyResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                    .setId(POLICY_ID)
                    .setPolicyInfo(PolicyInfo.newBuilder()
                        .setMerge(MergePolicy.newBuilder()
                            .setMergeType(
                                PolicyMapper.MERGE_TYPE_API_TO_PROTO.get(
                                    policyApiInputDTO.getMergeType()))
                            .addMergeGroupIds(MERGE_GROUP_ID))))
                .build()).when(policiesBackend).getPolicy(PolicyRequest.newBuilder()
                    .setPolicyId(POLICY_ID)
                    .build());
            final UpdateGroupRequest updateGroupRequest = UpdateGroupRequest.newBuilder()
                    .setId(MERGE_GROUP_ID)
                    .setNewDefinition(groupDefinition)
                    .build();
            final UpdateGroupResponse updateGroupResponse = UpdateGroupResponse.newBuilder()
                    .setUpdatedGroup(Grouping.newBuilder().setId(MERGE_GROUP_ID))
                    .build();
            doReturn(updateGroupResponse).when(groupBackend).updateGroup(updateGroupRequest);
            final PolicyEditResponse policyEditResponse = PolicyEditResponse.newBuilder()
                    .setPolicy(Policy.newBuilder()
                            .setId(POLICY_ID)
                            .setPolicyInfo(PolicyInfo.newBuilder()
                                    .setMerge(MergePolicy.newBuilder()
                                            .setMergeType(PolicyMapper.MERGE_TYPE_API_TO_PROTO.get(
                                                            policyApiInputDTO.getMergeType()))
                                            .addMergeGroupIds(MERGE_GROUP_ID)
                                            .build())
                                    .build())
                            .build())
                    .build();
            doReturn(policyEditResponse).when(policiesBackend).editPolicy(any());

            // Act
            try {
                marketsService.editPolicy(MARKET_UUID, Long.toString(POLICY_ID), policyApiInputDTO);
            } catch (OperationFailedException e) {
                // this should never happen
                logger.error("OperationFailedException caught while editing policy", e);
                fail("Editing policy failed");
            }
        });
    }

    private void testMergePolicyRequiredHiddenGroupAddOrEdit(final EditOrCreateOperation addOrEdit)
            throws ConversionException, InterruptedException {
        for (MergePolicyType mergePolicyType : Arrays.asList(MergePolicyType.DesktopPool,
                MergePolicyType.DataCenter)) {
                    final PolicyApiInputDTO policyApiInputDTO = new PolicyApiInputDTO();
                    policyApiInputDTO.setType(PolicyType.MERGE);
                    policyApiInputDTO.setMergeType(mergePolicyType);
                    policyApiInputDTO.setMergeUuids(MERGE_GROUP_IDS.stream()
                            .map(String::valueOf)
                            .collect(Collectors.toList()));
                    Mockito.when(policyMapper.policyApiInputDtoToProto(policyApiInputDTO))
                            .thenReturn(PolicyInfo.newBuilder().build());
                    final EntityType entityType =
                            MERGE_PROVIDER_ENTITY_TYPE.get(policyApiInputDTO.getMergeType());
                    final List<MinimalEntity> entities = MERGE_GROUP_IDS.stream()
                            .map(oid -> MinimalEntity.newBuilder()
                                    .setOid(oid)
                                    .setEntityType(entityType.getNumber())
                                    .setDisplayName(String.format("%s %d", entityType, oid))
                                    .build())
                            .collect(Collectors.toList());
                    final MultiEntityRequest multiEntityRequest =
                            ApiTestUtils.mockMultiMinEntityReq(entities);
                    Mockito.when(repositoryApi
                            .entitiesRequest(new HashSet<>(MERGE_GROUP_IDS)))
                            .thenReturn(multiEntityRequest);
                    final GroupDefinition.Builder groupDefinition = GroupDefinition.newBuilder()
                            .setDisplayName(entities.stream()
                                    .map(MinimalEntity::getDisplayName)
                                    .collect(Collectors.joining(",", "Merge: ", "")))
                            .setIsHidden(true)
                            .setStaticGroupMembers(StaticMembers.newBuilder()
                                    .addMembersByType(StaticMembersByType.newBuilder()
                                            .setType(MemberType.newBuilder()
                                                    .setEntity(entityType.getNumber()))
                                            .addAllMembers(MERGE_GROUP_IDS)));
                    addOrEdit.createOrEdit(policyApiInputDTO, groupDefinition);
                    // Assert
                    Assert.assertEquals(1, policyApiInputDTO.getMergeUuids().size());
                    Assert.assertEquals(String.valueOf(MERGE_GROUP_ID),
                            policyApiInputDTO.getMergeUuids().iterator().next());
                }
    }

    /**
     * Test for {@link MarketsService#deletePolicy(String, String)}.
     * When merge policy required hidden group.
     */
    @Test
    public void testDeleteMergePolicyWhenNeedDeleteHiddenGroup() {
        Stream.of(MergeType.DESKTOP_POOL, MergeType.DATACENTER).forEach(mergeType -> {
            Mockito.reset(groupBackend);
            doReturn(PolicyDeleteResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                    .setId(POLICY_ID)
                    .setPolicyInfo(PolicyInfo.newBuilder()
                        .setMerge(MergePolicy.newBuilder()
                            .setMergeType(mergeType)
                            .addAllMergeGroupIds(MERGE_GROUP_IDS))))
                .build())
                .when(policiesBackend).deletePolicy(PolicyDTO.PolicyDeleteRequest.newBuilder()
                    .setPolicyId(POLICY_ID)
                    .build());
            // Act
            marketsService.deletePolicy(MARKET_UUID, Long.toString(POLICY_ID));
            // Assert
            final ArgumentCaptor<GroupID> captor = ArgumentCaptor.forClass(GroupID.class);
            Mockito.verify(groupBackend, Mockito.times(MERGE_GROUP_IDS.size()))
                    .deleteGroup(captor.capture());
            MERGE_GROUP_IDS.forEach(groupId -> Assert.assertTrue(
                    captor.getAllValues().contains(GroupID.newBuilder().setId(groupId).build())));
        });
    }

    /**
     * Tests expected behavior when renaming plan.
     *
     * @throws Exception thrown if no plan matches marketUuid or user does not have plan access
     */
    @Test
    public void testRenamePlan() throws Exception {
        //GIVEN
        final String planUuid = Long.toString(REALTIME_PLAN_ID);
        final ApiId mockApi = ApiTestUtils.mockPlanId(planUuid, uuidMapper);
        final PlanInstance plan = PlanInstance.newBuilder(planDefault)
                .setPlanId(REALTIME_PLAN_ID)
                .build();
        final CachedPlanInfo planInfo = mock(CachedPlanInfo.class);
        doReturn(plan).when(planInfo).getPlanInstance();
        doReturn(Optional.of(planInfo)).when(mockApi).getCachedPlanInfo();

        final ArgumentCaptor<UpdatePlanRequest> argument = ArgumentCaptor.forClass(UpdatePlanRequest.class);
        doReturn(planDefault).when(planBackend).updatePlan(argument.capture());
        final String displayName = "newPlanName";

        //WHEN
        marketsService.renameMarket(planUuid, displayName);

        //THEN
        assertEquals(argument.getValue().getName(), displayName);
        assertEquals(argument.getValue().getPlanId(), REALTIME_PLAN_ID);
        final ArgumentCaptor<PlanInstance> updatedPlanArgument = ArgumentCaptor.forClass(PlanInstance.class);
        verify(marketMapper).dtoFromPlanInstance(updatedPlanArgument.capture());
        assertEquals(updatedPlanArgument.getValue(), planDefault);
    }

    /**
     * Test Exception thrown when no plan matches marketUuid.
     *
     * @throws Exception thrown if no plan matches marketUuid
     */
    @Test (expected = InvalidOperationException.class)
    public void testRenamePlanInvalidMarketUuid() throws Exception {
        //GIVEN
        final String planUuid = Long.toString(REALTIME_PLAN_ID);
        final ApiId mockApi = ApiTestUtils.mockPlanId(planUuid, uuidMapper);
        doReturn(Optional.empty()).when(mockApi).getCachedPlanInfo();

        //WHEN
        marketsService.renameMarket(planUuid, "");
    }


    /**
     * Function to trigger creation or update of a policy.
     */
    @FunctionalInterface
    private interface EditOrCreateOperation {
        void createOrEdit(PolicyApiInputDTO policy, GroupDefinition.Builder groupBuilder)
                throws ConversionException, InterruptedException;
    }
}

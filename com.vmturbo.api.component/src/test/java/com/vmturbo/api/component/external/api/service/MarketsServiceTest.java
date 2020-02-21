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
import java.util.Set;
import java.util.function.BiConsumer;
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
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.stats.PlanEntityStatsFetcher;
import com.vmturbo.api.component.external.api.websocket.UINotificationChannel;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.market.MarketApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.enums.MergePolicyType;
import com.vmturbo.api.enums.PolicyType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.EntityPaginationRequest;
import com.vmturbo.api.pagination.EntityPaginationRequest.EntityPaginationResponse;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityDTOMoles.EntitySeverityServiceMole;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse.Members;
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
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
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
            ActionsServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            planEntityStatsFetcher,
            SearchServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
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
     * Test that getting actions by market UUID fails if given future start time.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetActionsByMarketUuidShouldFailGivenFutureStartTime() throws Exception {
        ActionApiInputDTO actionDTO = new ActionApiInputDTO();
        Long currentDateTime = DateTimeUtil.parseTime(DateTimeUtil.getNow());
        String futureDate = DateTimeUtil.addDays(currentDateTime, 2);
        actionDTO.setStartTime(futureDate);
        marketsService.getActionsByMarketUuid(MARKET_UUID, actionDTO, null);
    }

    /**
     * Test that getting actions by market UUID fails if given start time after end time.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetActionsByMarketUuidShouldFailGivenStartTimeAfterEndTime() throws Exception {
        ActionApiInputDTO actionDTO = new ActionApiInputDTO();
        String currentDateTime = DateTimeUtil.getNow();
        String futureDate = DateTimeUtil.addDays(DateTimeUtil.parseTime(currentDateTime), 2);
        actionDTO.setStartTime(futureDate);
        actionDTO.setEndTime(currentDateTime);
        marketsService.getActionsByMarketUuid(MARKET_UUID, actionDTO, null);
    }

    /**
     * Test that getting actions by market UUID fails if given end time and no start time.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetActionsByMarketUuidShouldFailGivenEndTimeOnly() throws Exception {
        ActionApiInputDTO actionDTO = new ActionApiInputDTO();
        actionDTO.setEndTime(DateTimeUtil.getNow());
        marketsService.getActionsByMarketUuid(MARKET_UUID, actionDTO, null);
    }

    /**
     * Test that getting entities by real market Id causes a retrieveTopologyEntities call
     * to the repository rpc service.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testRetrieveTopologyEntities() throws Exception {
        ApiTestUtils.mockRealtimeId(MARKET_UUID, REALTIME_PLAN_ID, uuidMapper);

        ServiceEntityApiDTO se1 = new ServiceEntityApiDTO();
        se1.setClassName(UIEntityType.VIRTUAL_MACHINE.apiStr());
        se1.setUuid("1");
        ApiPartialEntity entity = ApiPartialEntity.newBuilder()
            .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
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
                .setMembers(Members.newBuilder().addAllIds(expandedUids).build())
                .build();
        doReturn(membersResponse).when(groupBackend).getMembers(any());

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
        statScopesApiInputDTO.setRelatedType(UIEntityType.VIRTUAL_MACHINE.apiStr());
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
        inputDTO.setRelatedType(UIEntityType.PHYSICAL_MACHINE.apiStr());
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
                .setMembers(Members.newBuilder().addAllIds(expandedUids).build())
                .build();
        doReturn(membersResponse).when(groupBackend).getMembers(any());

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
     */
    @Test
    public void testAddMergePolicyWhenNeedCreateHiddenGroup() {
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
     * Test for {@link MarketsService#editPolicy(String, String, PolicyApiInputDTO)}.
     * When merge policy required hidden group.
     */
    @Test
    public void testEditMergePolicyWhenNeedUpdateHiddenGroup() {
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

            // Act
           marketsService.editPolicy(MARKET_UUID, Long.toString(POLICY_ID), policyApiInputDTO);
        });
    }

    private void testMergePolicyRequiredHiddenGroupAddOrEdit(
            final BiConsumer<PolicyApiInputDTO, GroupDefinition.Builder> addOrEdit) {
        Stream.of(MergePolicyType.DesktopPool, MergePolicyType.DataCenter)
                .forEach(mergePolicyType -> {
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
                    addOrEdit.accept(policyApiInputDTO, groupDefinition);
                    // Assert
                    Assert.assertEquals(1, policyApiInputDTO.getMergeUuids().size());
                    Assert.assertEquals(String.valueOf(MERGE_GROUP_ID),
                            policyApiInputDTO.getMergeUuids().iterator().next());
                });
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
}

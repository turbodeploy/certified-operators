package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.grpc.Status;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.PolicyMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.ServiceProviderExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor.ActionStatsQuery;
import com.vmturbo.api.dto.QueryInputApiDTO;
import com.vmturbo.api.dto.RangeInputApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.action.ActionDetailsApiDTO;
import com.vmturbo.api.dto.action.ActionExecutionApiDTO;
import com.vmturbo.api.dto.action.ActionScopesApiInputDTO;
import com.vmturbo.api.dto.action.CloudResizeActionDetailsApiDTO;
import com.vmturbo.api.dto.action.NoDetailsApiDTO;
import com.vmturbo.api.dto.action.ScopeUuidsApiInputDTO;
import com.vmturbo.api.dto.action.SkippedActionApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.settingspolicy.SettingsPolicyApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.ActionDetailLevel;
import com.vmturbo.api.enums.PolicyType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.api.pagination.EntityActionPaginationRequest;
import com.vmturbo.api.pagination.EntityActionPaginationRequest.EntityActionPaginationResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionExecution;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionExecution.SkippedAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionExecutionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.AllActionExecutionsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.AllActionExecutionsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.GetInstanceIdsForRecommendationIdsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetInstanceIdsForRecommendationIdsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.MultiActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.BindToGroupAndLicencePolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyRequest;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles.PolicyServiceMole;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingPoliciesResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Test the service methods for the /actions endpoint.
 */
public class ActionsServiceTest {

    public static final String[] ALL_ACTION_MODES = {
            "RECOMMEND", "DISABLED", "MANUAL", "AUTOMATIC", "EXTERNAL_APPROVAL"
    };

    private static final long FIRST_ACTION_ID = 10L;
    private static final long SECOND_ACTION_ID = 20L;
    private static final long FIRST_ACTION_RECOMMENDATION_ID = 101L;
    private static final long SECOND_ACTION_RECOMMENDATION_ID = 201L;
    private static final long FIRST_ACTION_INSTANCE_ID = 1001L;

    private static final long ACTION_EXECUTION_ID = 1111L;
    private static final List<Long> ACTION_EXECUTION_ACTION_IDS = ImmutableList.of(1L, 2L);
    private static final Long ACTION_EXECUTION_SKIPPED_ACTION_ID = 3L;
    private static final String ACTION_EXECUTION_SKIPPED_ACTION_REASON = "Action is in the wrong state";

    private static final long VM_ID = 10001L;
    private static final long POLICY_ID = 20001L;

    /**
     * The backend the API forwards calls to (i.e. the part that's in the plan orchestrator).
     */
    private final ActionsServiceMole actionsServiceBackend =
            spy(new ActionsServiceMole());
    private final RepositoryServiceMole repositoryServiceBackend = spy(new RepositoryServiceMole());
    private final PolicyServiceMole policyServiceBackend = spy(new PolicyServiceMole());
    private final GroupServiceMole groupServiceBackend = spy(new GroupServiceMole());
    private final SettingPolicyServiceMole settingPolicyServiceBackend = spy(new SettingPolicyServiceMole());
    private ActionsService actionsServiceUnderTest;

    private ActionsService actionsServiceUsingStableId;

    private RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);

    private ActionStatsQueryExecutor actionStatsQueryExecutor = mock(ActionStatsQueryExecutor.class);

    private UuidMapper uuidMapper = mock(UuidMapper.class);

    private final ServiceProviderExpander serviceProviderExpander = mock(ServiceProviderExpander.class);

    ActionsServiceGrpc.ActionsServiceBlockingStub actionsRpcService;

    private ActionSpecMapper actionSpecMapper;

    private ActionSearchUtil actionSearchUtil;

    private ActionSearchUtil actionSearchUtilWithStableIdEnabled;

    private ActionSearchUtil actionSearchUtilSpy;

    private MarketsService marketsService;

    private SupplyChainFetcherFactory supplyChainFetcherFactory;

    private RepositoryServiceBlockingStub repositoryService;

    private PolicyServiceBlockingStub policyRpcService;

    private PolicyMapper policyMapper;

    private SettingPolicyServiceBlockingStub settingPolicyService;

    private SettingsMapper settingsMapper;

    private GroupServiceBlockingStub groupsService;

    private static final long REALTIME_TOPOLOGY_ID = 777777L;

    private static final String UUID = "12345";

    private final int maxInnerPagination = 500;

    private ArgumentCaptor<MultiActionRequest> multiActionRequestCaptor;
    private ArgumentCaptor<Collection> orchestratorActionCaptor;
    private ArgumentCaptor<GetInstanceIdsForRecommendationIdsRequest> instanceIdRequest;

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(actionsServiceBackend,
            repositoryServiceBackend, policyServiceBackend, groupServiceBackend, settingPolicyServiceBackend);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws IOException {
        // set up a mock Actions RPC server
        actionSpecMapper = Mockito.mock(ActionSpecMapper.class);
        marketsService = Mockito.mock(MarketsService.class);
        supplyChainFetcherFactory = Mockito.mock(SupplyChainFetcherFactory.class);
        actionsRpcService = ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel());
        repositoryService = RepositoryServiceGrpc.newBlockingStub(grpcServer.getChannel());
        policyRpcService = PolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());
        policyMapper = Mockito.mock(PolicyMapper.class);
        settingPolicyService = SettingPolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());
        settingsMapper = Mockito.mock(SettingsMapper.class);
        groupsService = GroupServiceGrpc.newBlockingStub(grpcServer.getChannel());


        actionSearchUtil = new ActionSearchUtil(actionsRpcService, actionSpecMapper, mock(PaginationMapper.class),
                supplyChainFetcherFactory, mock(GroupExpander.class), mock(ServiceProviderExpander.class),
                REALTIME_TOPOLOGY_ID, false);

        actionSearchUtilWithStableIdEnabled = new ActionSearchUtil(actionsRpcService, actionSpecMapper,
                mock(PaginationMapper.class), supplyChainFetcherFactory, mock(GroupExpander.class),
                mock(ServiceProviderExpander.class), REALTIME_TOPOLOGY_ID, true);

        actionSearchUtilSpy = spy(actionSearchUtil);

        ActionsService.Builder builder = ActionsService.newBuilder()
                .withActionOrchestratorRpc(actionsRpcService)
                .withActionSpecMapper(actionSpecMapper)
                .withRealtimeTopologyContextId(REALTIME_TOPOLOGY_ID)
                .withActionStatsQueryExecutor(actionStatsQueryExecutor)
                .withUuidMapper(uuidMapper)
                .withServiceProviderExpander(serviceProviderExpander)
                .withActionSearchUtil(actionSearchUtilSpy)
                .withMarketsService(marketsService)
                .withSupplyChainFetcherFactory(supplyChainFetcherFactory)
                .withRepositoryService(repositoryService)
                .withPolicyRpcService(policyRpcService)
                .withPolicyMapper(policyMapper)
                .withSettingPolicyServiceBlockingStub(settingPolicyService)
                .withSettingsMapper(settingsMapper)
                .withGroupService(groupsService)
                .withApiPaginationMaxLimit(maxInnerPagination);

        // set up the ActionsService to test
        actionsServiceUnderTest = builder.build();

        builder.withActionSearchUtil(actionSearchUtilWithStableIdEnabled);
        actionsServiceUsingStableId = builder.build();

        multiActionRequestCaptor = ArgumentCaptor.forClass(MultiActionRequest.class);
        orchestratorActionCaptor = ArgumentCaptor.forClass(Collection.class);
        instanceIdRequest = ArgumentCaptor.forClass(GetInstanceIdsForRecommendationIdsRequest.class);
    }

    /**
     * Test the return of available action modes for a given actionType and seType.
     * Note that the actionType and seType are currently ignored in XL, pending implementation
     * of user permissions, etc.
     */
    @Test
    public void testGetAvailActionModes() throws Exception {
        // Arrange

        // note that the getAvailableActions()
        String actionType = "ignored";
        String seType = "ignored";
        // Act
        List<String> modes = actionsServiceUnderTest.getAvailActionModes(actionType, seType);
        // Assert
        assertThat(modes, containsInAnyOrder(ALL_ACTION_MODES));
    }

    /**
     * Test execute action throw correct exception when RPC call return errors
     * @throws Exception
     */
    @Test
    public void testExecuteActionThrowException() throws Exception {
        expectedException.expect(UnknownObjectException.class);
        when(actionsServiceBackend.acceptActionsError(any())).thenReturn(Optional.of(Status.NOT_FOUND
            .withDescription("test").asException()));
        actionsServiceUnderTest.executeAction(UUID, true, false);
    }

    /**
     * Test creating a new action execution to execute multiple actions in one request.
     *
     * @throws Exception On any error.
     */
    @Test
    public void testCreateActionExecution() throws Exception {
        // ARRANGE
        final MultiActionRequest request = MultiActionRequest.newBuilder()
                .setTopologyContextId(REALTIME_TOPOLOGY_ID)
                .addAllActionIds(ACTION_EXECUTION_ACTION_IDS)
                .build();
        final ActionExecution actionExecution = createActionExecution();
        when(actionsServiceBackend.acceptActions(request)).thenReturn(actionExecution);

        // ACT
        final ActionExecutionApiDTO actionExecutionDto = actionsServiceUnderTest
                .createActionExecution(ACTION_EXECUTION_ACTION_IDS.stream()
                        .map(String::valueOf)
                        .collect(Collectors.toList()));

        // ASSERT
        checkActionExecution(actionExecutionDto);
    }

    /**
     * Test getting action execution by ID.
     */
    @Test
    public void testGetActionExecution() {
        // ARRANGE
        final ActionExecutionRequest request = ActionExecutionRequest.newBuilder()
                .setExecutionId(ACTION_EXECUTION_ID)
                .build();
        final ActionExecution actionExecution = createActionExecution();
        when(actionsServiceBackend.getActionExecution(request)).thenReturn(actionExecution);

        // ACT
        final ActionExecutionApiDTO actionExecutionDto = actionsServiceUnderTest
                .getActionExecution(ACTION_EXECUTION_ID);

        // ASSERT
        checkActionExecution(actionExecutionDto);
    }

    /**
     * Test getting a list all action executions.
     */
    @Test
    public void testGetActionExecutions() throws Exception {
        // ARRANGE
        final AllActionExecutionsRequest request = AllActionExecutionsRequest.newBuilder()
                .build();
        final ActionExecution actionExecution = createActionExecution();
        final AllActionExecutionsResponse response = AllActionExecutionsResponse.newBuilder()
                .addExecutions(actionExecution)
                .build();
        when(actionsServiceBackend.getAllActionExecutions(request)).thenReturn(response);

        // ACT
        final List<ActionExecutionApiDTO> actionExecutionDtoList = actionsServiceUnderTest
                .getActionExecutions();

        // ASSERT
        assertEquals(1, actionExecutionDtoList.size());
        checkActionExecution(actionExecutionDtoList.get(0));
    }

    private static ActionExecution createActionExecution() {
        return ActionExecution.newBuilder()
                .setId(ACTION_EXECUTION_ID)
                .addAllActionId(ACTION_EXECUTION_ACTION_IDS)
                .addSkippedAction(SkippedAction.newBuilder()
                        .setActionId(ACTION_EXECUTION_SKIPPED_ACTION_ID)
                        .setReason(ACTION_EXECUTION_SKIPPED_ACTION_REASON)
                        .build())
                .build();
    }

    private static void checkActionExecution(ActionExecutionApiDTO actionExecutionDto) {
        assertEquals(String.valueOf(ACTION_EXECUTION_ID), actionExecutionDto.getId());
        final Set<String> expectedActionIds = ACTION_EXECUTION_ACTION_IDS.stream()
                .map(String::valueOf)
                .collect(Collectors.toSet());
        final Set<String> actualActionIds = new HashSet<>(actionExecutionDto.getActionIds());
        assertEquals(expectedActionIds, actualActionIds);
        assertEquals(1, actionExecutionDto.getSkippedActions().size());
        final SkippedActionApiDTO skippedActionDto = actionExecutionDto.getSkippedActions().get(0);
        assertEquals(String.valueOf(ACTION_EXECUTION_SKIPPED_ACTION_ID), skippedActionDto.getActionId());
        assertEquals(ACTION_EXECUTION_SKIPPED_ACTION_REASON, skippedActionDto.getReason());
    }

    @Test
    public void testGetActionTypeCountStatsByUuidsQuery() throws Exception {

        final ActionScopesApiInputDTO actionScopesApiInputDTO = new ActionScopesApiInputDTO();
        final ActionApiInputDTO actionApiInputDTO = new ActionApiInputDTO();
        actionScopesApiInputDTO.setScopes(Lists.newArrayList("1", "3"));

        final ApiId scope1 = ApiTestUtils.mockEntityId("1", uuidMapper);
        final ApiId scope2 = ApiTestUtils.mockGroupId("3", uuidMapper);
        when(scope1.getDisplayName()).thenReturn("First Entity");
        when(scope1.getClassName()).thenReturn(ApiEntityType.VIRTUAL_MACHINE.apiStr());
        actionScopesApiInputDTO.setActionInput(actionApiInputDTO);
        actionScopesApiInputDTO.setRelatedType(ApiEntityType.PHYSICAL_MACHINE.apiStr());

        final List<StatSnapshotApiDTO> scope1Snapshots = Arrays.asList(new StatSnapshotApiDTO());
        final List<StatSnapshotApiDTO> scope2Snapshots = Arrays.asList(new StatSnapshotApiDTO(), new StatSnapshotApiDTO());

        final ArgumentCaptor<ActionStatsQuery> queryArgumentCaptor = ArgumentCaptor.forClass(ActionStatsQuery.class);

        when(actionStatsQueryExecutor.retrieveActionStats(queryArgumentCaptor.capture()))
            .thenReturn(ImmutableMap.of(scope1, scope1Snapshots, scope2, scope2Snapshots));

        MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Lists.newArrayList(MinimalEntity.newBuilder()
            .setOid(1)
            .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
            .setDisplayName("First Entity")
            .build()));
        when(repositoryApi.entitiesRequest(Collections.singleton(1L))).thenReturn(req);

        when(serviceProviderExpander.expand(any())).thenReturn(ImmutableSet.of(3L, 1L));

        final Map<String, EntityStatsApiDTO> statsByUuid =
            actionsServiceUnderTest.getActionStatsByUuidsQuery(actionScopesApiInputDTO).stream()
                .collect(Collectors.toMap(EntityStatsApiDTO::getUuid, Function.identity()));

        assertThat(queryArgumentCaptor.getValue().entityType().get(), equalTo(EntityType.PHYSICAL_MACHINE_VALUE));
        assertThat(queryArgumentCaptor.getValue().actionInput(), equalTo(actionApiInputDTO));
        assertThat(queryArgumentCaptor.getValue().scopes(), containsInAnyOrder(scope1, scope2));

        assertThat(statsByUuid.keySet(), containsInAnyOrder("1", "3"));
        assertThat(statsByUuid.get("1").getStats(), is(scope1Snapshots));
        assertThat(statsByUuid.get("1").getDisplayName(), is("First Entity"));
        assertThat(statsByUuid.get("1").getClassName(), is(ApiEntityType.VIRTUAL_MACHINE.apiStr()));

        assertThat(statsByUuid.get("3").getStats(), is(scope2Snapshots));
    }

    /**
     * Tests getting action details for an empty collection of uuids.
     */
    @Test
    public void testGetActionDetailsByUuidsEmptyUuids()
            throws OperationFailedException, IllegalArgumentException {
        ScopeUuidsApiInputDTO inputDTO = new ScopeUuidsApiInputDTO();
        inputDTO.setUuids(Collections.emptyList());

        Map<String, ActionDetailsApiDTO> actionDetails = actionsServiceUnderTest.getActionDetailsByUuids(inputDTO);
        assertEquals(0, actionDetails.size());
    }

    /**
     * Tests getting action details for an empty collection of uuids when the api uses action recommendation
     * (stable) id.
     */
    @Test
    public void testGetActionDetailsByUuidsEmptyUuidsStableIdsActive()
            throws OperationFailedException, IllegalArgumentException {
        ScopeUuidsApiInputDTO inputDTO = new ScopeUuidsApiInputDTO();
        inputDTO.setUuids(Collections.emptyList());

        Map<String, ActionDetailsApiDTO> actionDetails = actionsServiceUsingStableId.getActionDetailsByUuids(inputDTO);
        assertEquals(0, actionDetails.size());
    }

    /**
     * Tests getting action details for actions in a specific topology context.
     */
    @Test
    public void testGetActionDetailsByUuidsInTopologyContext()
            throws OperationFailedException, IllegalArgumentException {
        // ARRANGE
        final long topologyContextId = 20;

        setupGetActionDetailsByUuids(false);

        final ScopeUuidsApiInputDTO inputDTO = new ScopeUuidsApiInputDTO();
        inputDTO.setUuids(Arrays.asList(Long.toString(FIRST_ACTION_ID), Long.toString(SECOND_ACTION_ID)));
        inputDTO.setMarketId(Long.toString(topologyContextId));

        ApiTestUtils.mockEntityId("20", uuidMapper);

        // ACT
        Map<String, ActionDetailsApiDTO> resultDetailMap = actionsServiceUnderTest.getActionDetailsByUuids(inputDTO);

        // ASSERT
        verifyCallsAndResults(resultDetailMap, false, topologyContextId);
    }

    /**
     * Tests getting action details for actions in a specific topology context.
     */
    @Test
    public void testGetActionDetailsByUuidsInTopologyContextStableIdEnabled()
            throws OperationFailedException, IllegalArgumentException {
        // ARRANGE
        final long topologyContextId = 20;

        setupGetActionDetailsByUuids(false);

        final ScopeUuidsApiInputDTO inputDTO = new ScopeUuidsApiInputDTO();
        inputDTO.setUuids(Arrays.asList(Long.toString(FIRST_ACTION_ID), Long.toString(SECOND_ACTION_ID)));
        inputDTO.setMarketId(Long.toString(topologyContextId));

        ApiTestUtils.mockEntityId("20", uuidMapper);

        // ACT
        Map<String, ActionDetailsApiDTO> resultDetailMap = actionsServiceUsingStableId.getActionDetailsByUuids(inputDTO);

        // ASSERT
        verify(actionsServiceBackend, never()).getInstanceIdsForRecommendationIds(any());
        verifyCallsAndResults(resultDetailMap, false, topologyContextId);
    }

    /**
     * Test get placement policies for action.
     *
     * @throws Exception shouldn't happen
     */
    @Test
    public void testGetActionPlacementPolicies() throws Exception {
        // ARRANGE
        final long sourcePMid = 707664328793449L;
        final long destinationPMid = 707664328793448L;
        final long groupOfVMsId = 45435L;
        final long groupOfPMsId = 3245246L;

        Mockito.doReturn((Optional.of(FIRST_ACTION_INSTANCE_ID)))
                .when(actionSearchUtilSpy)
                .getActionInstanceId(eq(String.valueOf(FIRST_ACTION_ID)), eq(null));
        final ActionSpec actionSpec = moveActionSpec(FIRST_ACTION_INSTANCE_ID, FIRST_ACTION_RECOMMENDATION_ID, VM_ID,
                sourcePMid, destinationPMid);
        final ActionOrchestratorAction mockedAction =
                ActionOrchestratorAction.newBuilder().setActionId(FIRST_ACTION_INSTANCE_ID).setActionSpec(
                        actionSpec).build();
        Mockito.when(actionsServiceBackend.getAction(SingleActionRequest.newBuilder().setActionId(
                FIRST_ACTION_INSTANCE_ID).setTopologyContextId(REALTIME_TOPOLOGY_ID).build())).thenReturn(
                mockedAction);

        final PartialEntityBatch partialEntityBatch = PartialEntityBatch.newBuilder().addEntities(
                buildVMEntity(VM_ID, EntityType.VIRTUAL_MACHINE_VALUE, POLICY_ID)).build();
        Mockito.when(repositoryServiceBackend.retrieveTopologyEntities(
                RetrieveTopologyEntitiesRequest.newBuilder()
                        .addAllEntityOids(Arrays.asList(VM_ID, sourcePMid, destinationPMid))
                        .setReturnType(Type.FULL)
                        .build())).thenReturn(Collections.singletonList(partialEntityBatch));

        final PolicyResponse policyResponse = PolicyResponse.newBuilder().setPolicy(
                Policy.newBuilder()
                        .setId(POLICY_ID)
                        .setPolicyInfo(PolicyInfo.newBuilder()
                                .setBindToGroupAndLicense(
                                        BindToGroupAndLicencePolicy.newBuilder()
                                                .setConsumerGroupId(groupOfVMsId)
                                                .setProviderGroupId(groupOfPMsId)
                                                .build())
                                .build())
                        .build()).build();
        Mockito.when(policyServiceBackend.getPolicies(
                PolicyRequest.newBuilder().addPolicyIds(POLICY_ID).build())).thenReturn(
                Collections.singletonList(policyResponse));

        final List<Grouping> groupings = Arrays.asList(
                buildGroup(groupOfVMsId, EntityType.VIRTUAL_MACHINE_VALUE, Collections.emptyList()),
                buildGroup(groupOfPMsId, EntityType.PHYSICAL_MACHINE_VALUE,
                        Collections.emptyList()));
        Mockito.when(groupServiceBackend.getGroups(GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .addAllId(Arrays.asList(groupOfVMsId, groupOfPMsId))
                        .setIncludeHidden(true))
                .build())).thenReturn(groupings);

        final PolicyApiDTO mockedPolicyApiDTO = new PolicyApiDTO();
        mockedPolicyApiDTO.setType(PolicyType.BIND_TO_GROUP_AND_LICENSE);
        mockedPolicyApiDTO.setUuid(String.valueOf(POLICY_ID));

        Mockito.when(
                policyMapper.policyToApiDto(Collections.singletonList(policyResponse.getPolicy()),
                        Sets.newHashSet(groupings))).thenReturn(
                Collections.singletonList(mockedPolicyApiDTO));

        // ACT
        final List<PolicyApiDTO> placementPolicies =
                actionsServiceUnderTest.getActionPlacementPolicies(String.valueOf(FIRST_ACTION_ID));

        // ASSERT
        Assert.assertFalse(placementPolicies.isEmpty());
        Assert.assertEquals(1, placementPolicies.size());
        Assert.assertEquals(String.valueOf(POLICY_ID), placementPolicies.get(0).getUuid());
    }

    @Nonnull
    private ActionSpec moveActionSpec(final long actionId, final long actionRecommendationId,
            final long vmId, final long sourcePMid, final long destinationPMid) {
        return ActionSpec.newBuilder().setRecommendationId(actionRecommendationId).setRecommendation(Action.newBuilder()
                .setId(actionId)
                .setInfo(ActionInfo.newBuilder()
                        .setMove(Move.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(vmId)
                                        .setEnvironmentType(EnvironmentType.ON_PREM)
                                        .setType(EntityType.VIRTUAL_MACHINE_VALUE))
                                .addChanges(ChangeProvider.newBuilder()
                                        .setSource(ActionEntity.newBuilder()
                                                .setId(sourcePMid)
                                                .setType(EntityType.PHYSICAL_MACHINE_VALUE)
                                                .setEnvironmentType(EnvironmentType.ON_PREM))
                                        .setDestination(ActionEntity.newBuilder()
                                                .setId(destinationPMid)
                                                .setType(EntityType.PHYSICAL_MACHINE_VALUE)
                                                .setEnvironmentType(EnvironmentType.ON_PREM))
                                        .build())))
                .setExecutable(true)
                .setSupportingLevel(SupportLevel.SUPPORTED)
                .setDeprecatedImportance(0)
                .setExplanation(Explanation.getDefaultInstance())).build();
    }

    @Nonnull
    private ActionSpec resizeActionSpec(final long actionId, final long actionRecommendationId,
            final long vmId) {
        return ActionSpec.newBuilder()
                .setRecommendationId(actionRecommendationId)
                .setRecommendation(Action.newBuilder()
                        .setId(actionId)
                        .setInfo(ActionInfo.newBuilder().setResize(Resize.newBuilder().setTarget(
                                ActionEntity.newBuilder()
                                        .setId(vmId)
                                        .setEnvironmentType(EnvironmentType.ON_PREM)
                                        .setType(EntityType.VIRTUAL_MACHINE_VALUE)).setOldCapacity(
                                1).setNewCapacity(2).build()))
                        .setExecutable(true)
                        .setSupportingLevel(SupportLevel.SUPPORTED)
                        .setDeprecatedImportance(0)
                        .setExplanation(Explanation.getDefaultInstance()))
                .build();
    }

    private Grouping buildGroup(final long groupId, final int membersEntityType,
            List<Long> members) {
        return Grouping.newBuilder().setId(groupId).addExpectedTypes(
                MemberType.newBuilder().setEntity(membersEntityType).build()).setEnvironmentType(
                EnvironmentType.ON_PREM).setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder()
                                        .setEntity(membersEntityType)
                                        .build())
                                .addAllMembers(members)
                                .build())
                        .build())).build();
    }

    @Nonnull
    private PartialEntity buildVMEntity(long entityOid, int entityType, long policyId) {
        return PartialEntity.newBuilder().setFullEntity(TopologyEntityDTO.newBuilder()
                .setEntityType(entityType)
                .setOid(entityOid)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.SOFTWARE_LICENSE_COMMODITY_VALUE)
                                        .setKey(String.valueOf(policyId))
                                        .build())
                                .build())
                        .buildPartial())
                .build()).build();
    }

    private void setupGetActionDetailsByUuids(boolean conversionReturnRecommendationId) {
        List<ActionOrchestratorAction> orchestratorActions = Arrays.asList(
                ActionOrchestratorAction.newBuilder()
                        .setActionId(SECOND_ACTION_ID)
                        .build(),
                ActionOrchestratorAction.newBuilder()
                        .setActionId(FIRST_ACTION_ID)
                        .setActionSpec(ActionSpec.newBuilder()
                                .build())
                        .build()
        );

        when(actionsServiceBackend.getActions(multiActionRequestCaptor.capture()))
                .thenReturn(orchestratorActions);

        Map<String, ActionDetailsApiDTO> dtoMap = new HashMap<>();
        dtoMap.put(Long.toString(conversionReturnRecommendationId ? FIRST_ACTION_RECOMMENDATION_ID : FIRST_ACTION_ID),
                new CloudResizeActionDetailsApiDTO());
        dtoMap.put(Long.toString(conversionReturnRecommendationId ? SECOND_ACTION_RECOMMENDATION_ID : SECOND_ACTION_ID),
                new NoDetailsApiDTO());

        when(actionSpecMapper.createActionDetailsApiDTO(orchestratorActionCaptor.capture(), anyLong()))
                .thenReturn(dtoMap);

        when(actionsServiceBackend.getInstanceIdsForRecommendationIds(instanceIdRequest.capture()))
                .thenReturn(GetInstanceIdsForRecommendationIdsResponse
                        .newBuilder()
                        .putRecommendationIdToInstanceId(FIRST_ACTION_RECOMMENDATION_ID, FIRST_ACTION_ID)
                        .putRecommendationIdToInstanceId(SECOND_ACTION_RECOMMENDATION_ID, SECOND_ACTION_ID)
                        .build()
                );
    }

    private void verifyCallsAndResults(Map<String, ActionDetailsApiDTO> actionDetails,
                                       boolean conversionReturnRecommendationId,
                                       long contextId) {
        assertEquals(2, actionDetails.size());
        long firstIdToExpect = conversionReturnRecommendationId ? FIRST_ACTION_RECOMMENDATION_ID : FIRST_ACTION_ID;
        long secondIdToExpect = conversionReturnRecommendationId ? SECOND_ACTION_RECOMMENDATION_ID : SECOND_ACTION_ID;
        assertEquals(CloudResizeActionDetailsApiDTO.class, actionDetails.get(Long.toString(firstIdToExpect)).getClass());
        assertEquals(NoDetailsApiDTO.class, actionDetails.get(Long.toString(secondIdToExpect)).getClass());

        List<MultiActionRequest> actualRequests = multiActionRequestCaptor.getAllValues();
        assertEquals(1, actualRequests.size());
        MultiActionRequest multiActionRequest = actualRequests.get(0);
        assertEquals(FIRST_ACTION_ID, multiActionRequest.getActionIds(0));
        assertEquals(SECOND_ACTION_ID, multiActionRequest.getActionIds(1));
        assertEquals(contextId, multiActionRequest.getTopologyContextId());

        Collection<ActionOrchestratorAction> actualOrchestratorActions = orchestratorActionCaptor.getValue();
        assertEquals(2, actualOrchestratorActions.size());
        assertEquals(SECOND_ACTION_ID, actualOrchestratorActions.iterator().next().getActionId());
    }

    /**
     * Test get settings policies for action.
     *
     * @throws Exception shouldn't happen
     */
    @Test
    public void testGetActionSettingsPolicies() throws Exception {
        // ARRANGE
        final long groupOfVMsId = 45435L;

        Mockito.doReturn((Optional.of(FIRST_ACTION_INSTANCE_ID)))
                .when(actionSearchUtilSpy)
                .getActionInstanceId(eq(String.valueOf(FIRST_ACTION_ID)), eq(null));
        final ActionSpec actionSpec = resizeActionSpec(FIRST_ACTION_INSTANCE_ID,
                FIRST_ACTION_RECOMMENDATION_ID, VM_ID);
        final ActionOrchestratorAction mockedAction =
                ActionOrchestratorAction.newBuilder().setActionId(FIRST_ACTION_INSTANCE_ID).setActionSpec(
                        actionSpec).build();
        Mockito.when(actionsServiceBackend.getAction(SingleActionRequest.newBuilder().setActionId(
                FIRST_ACTION_INSTANCE_ID).setTopologyContextId(REALTIME_TOPOLOGY_ID).build())).thenReturn(
                mockedAction);

        final GetEntitySettingPoliciesResponse settingPoliciesResponse =
                GetEntitySettingPoliciesResponse.newBuilder().addSettingPolicies(
                        SettingPolicy.newBuilder()
                                .setId(POLICY_ID)
                                .setSettingPolicyType(SettingPolicy.Type.DISCOVERED)
                                .setInfo(SettingPolicyInfo.newBuilder()
                                        .addSettings(Setting.newBuilder()
                                                .setNumericSettingValue(
                                                        NumericSettingValue.newBuilder()
                                                                .setValue(4)
                                                                .build())
                                                .build())
                                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                                        .setEnabled(true)
                                        .setScope(
                                                Scope.newBuilder().addGroups(groupOfVMsId).build())
                                        .setDisplayName("Flexera: restrict vCPUs")
                                        .build())
                                .build()).build();

        Mockito.when(settingPolicyServiceBackend.getEntitySettingPolicies(
                GetEntitySettingPoliciesRequest.newBuilder()
                        .addAllEntityOidList(Collections.singletonList(VM_ID))
                        .build())).thenReturn(settingPoliciesResponse);

        final SettingsPolicyApiDTO settingsPolicyApiDTO = new SettingsPolicyApiDTO();
        settingsPolicyApiDTO.setUuid(String.valueOf(POLICY_ID));

        Mockito.when(settingsMapper.convertSettingPolicies(
                settingPoliciesResponse.getSettingPoliciesList())).thenReturn(
                Collections.singletonList(settingsPolicyApiDTO));

        // ACT
        final List<SettingsPolicyApiDTO> settingsPolicies =
                actionsServiceUnderTest.getActionSettingsPolicies(String.valueOf(FIRST_ACTION_ID));

        // ASSERT
        Assert.assertFalse(settingsPolicies.isEmpty());
        Assert.assertEquals(1, settingsPolicies.size());
        Assert.assertEquals(String.valueOf(POLICY_ID), settingsPolicies.get(0).getUuid());
    }

    /**
     * Test when there is no related placement policies for action.
     *
     * @throws Exception shouldn't happen
     */
    @Test
    public void testNoRelatedPlacementPoliciesForAction() throws Exception {
        // ARRANGE
        final long sourcePMid = 707664328793449L;
        final long destinationPMid = 707664328793448L;

        Mockito.doReturn((Optional.of(FIRST_ACTION_INSTANCE_ID)))
                .when(actionSearchUtilSpy)
                .getActionInstanceId(eq(String.valueOf(FIRST_ACTION_ID)), eq(null));
        final ActionSpec actionSpec = moveActionSpec(FIRST_ACTION_INSTANCE_ID,
                FIRST_ACTION_RECOMMENDATION_ID, VM_ID, sourcePMid, destinationPMid);
        final ActionOrchestratorAction mockedAction = ActionOrchestratorAction.newBuilder()
                .setActionId(FIRST_ACTION_INSTANCE_ID)
                .setActionSpec(actionSpec)
                .build();
        Mockito.when(actionsServiceBackend.getAction(SingleActionRequest.newBuilder().setActionId(
                        FIRST_ACTION_INSTANCE_ID).setTopologyContextId(REALTIME_TOPOLOGY_ID).build()))
                .thenReturn(mockedAction);

        Mockito.when(repositoryServiceBackend.retrieveTopologyEntities(
                RetrieveTopologyEntitiesRequest.newBuilder()
                        .addAllEntityOids(Arrays.asList(VM_ID, sourcePMid, destinationPMid))
                        .setReturnType(Type.FULL)
                        .build())).thenReturn(
                Collections.singletonList(PartialEntityBatch.getDefaultInstance()));

        // ACT
        final List<PolicyApiDTO> placementPolicies =
                actionsServiceUnderTest.getActionPlacementPolicies(String.valueOf(FIRST_ACTION_ID));

        // ASSERT
        Assert.assertTrue(placementPolicies.isEmpty());
    }

    /**
     * Test when there is no related settings policies for action.
     *
     * @throws Exception shouldn't happen
     */
    @Test
    public void testNoRelatedSettingsPoliciesForAction() throws Exception {
        // ARRANGE
        Mockito.doReturn((Optional.of(FIRST_ACTION_INSTANCE_ID)))
                .when(actionSearchUtilSpy)
                .getActionInstanceId(eq(String.valueOf(FIRST_ACTION_ID)), eq(null));
        final ActionSpec actionSpec = resizeActionSpec(FIRST_ACTION_INSTANCE_ID,
                FIRST_ACTION_RECOMMENDATION_ID, VM_ID);
        final ActionOrchestratorAction mockedAction = ActionOrchestratorAction.newBuilder()
                .setActionId(FIRST_ACTION_INSTANCE_ID)
                .setActionSpec(actionSpec)
                .build();
        Mockito.when(actionsServiceBackend.getAction(SingleActionRequest.newBuilder().setActionId(
                        FIRST_ACTION_INSTANCE_ID).setTopologyContextId(REALTIME_TOPOLOGY_ID).build()))
                .thenReturn(mockedAction);

        Mockito.when(settingPolicyServiceBackend.getEntitySettingPolicies(
                GetEntitySettingPoliciesRequest.newBuilder()
                        .addAllEntityOidList(Collections.singletonList(VM_ID))
                        .build())).thenReturn(
                GetEntitySettingPoliciesResponse.getDefaultInstance());

        // ACT
        final List<SettingsPolicyApiDTO> settingsPolicies =
                actionsServiceUnderTest.getActionSettingsPolicies(String.valueOf(FIRST_ACTION_ID));

        // ASSERT
        Assert.assertTrue(settingsPolicies.isEmpty());
    }

    /**
     * Tests getting action details for actions in a specific topology context.
     */
    @Test
    public void testGetActionDetailsByUuidsWithSetMarketId()
            throws OperationFailedException, IllegalArgumentException {
        final long actionId = 10;
        final String marketId = "Market";
        final ScopeUuidsApiInputDTO inputDTO = new ScopeUuidsApiInputDTO();
        inputDTO.setUuids(Collections.singletonList(Long.toString(actionId)));
        inputDTO.setMarketId(marketId);

        final List<ActionOrchestratorAction> mockActions =
                Collections.singletonList(ActionOrchestratorAction.getDefaultInstance());
        final ArgumentCaptor<MultiActionRequest> requestCaptor =
                ArgumentCaptor.forClass(MultiActionRequest.class);
        Map<String, ActionDetailsApiDTO> dtoMap = new HashMap<>();
        dtoMap.put("1", mock(ActionDetailsApiDTO.class));

        ApiTestUtils.mockRealtimeId("Market", 777777, uuidMapper);
        when(actionsServiceBackend.getActions(requestCaptor.capture())).thenReturn(mockActions);

        when(actionSpecMapper.createActionDetailsApiDTO(anyList(), anyLong()))
                .thenReturn(dtoMap);

        Map<String, ActionDetailsApiDTO> resultDetailMap =
                actionsServiceUnderTest.getActionDetailsByUuids(inputDTO);

        assertEquals(1, resultDetailMap.size());
        final List<MultiActionRequest> requests = requestCaptor.getAllValues();
        assertEquals(1, requests.size());
        final MultiActionRequest multiActionRequest = requests.get(0);
        assertNotEquals(Long.toString(multiActionRequest.getTopologyContextId()),
                marketId);
        assertEquals(1, multiActionRequest.getActionIdsCount());
        assertEquals(actionId, multiActionRequest.getActionIds(0));
    }


    /**
     * Test the process of querying for actions based on scope UUIDs.
     * @throws Exception never.
     */
    @Test
    public void testGetActionsByUuidsQuery() throws Exception {

        // when no uuids are provided, return an empty response.
        final ActionScopesApiInputDTO inputDto = new ActionScopesApiInputDTO();
        inputDto.setActionInput(new ActionApiInputDTO());
        final EntityActionPaginationResponse responseShouldBeEmpty =
            actionsServiceUnderTest.getActionsByUuidsQuery(inputDto,
                new EntityActionPaginationRequest(null, null, true, null));
        assertEquals(0, responseShouldBeEmpty.getRawResults().size());

        final String cursor = "1";

        // discard first scope because of cursor
        final String uuid1 = "123";
        final ApiId scope1 = ApiTestUtils.mockEntityId(uuid1, uuidMapper);
        when(scope1.getDisplayName()).thenReturn("abc");

        // query markets service for plan scope
        final String uuid2 = "234";
        final ApiId scope2 = ApiTestUtils.mockPlanId(uuid2, uuidMapper);
        when(scope2.getDisplayName()).thenReturn("bcd");

        // retrieve mock action response when querying action service
        final String uuid3 = "345";
        final ApiId scope3 = ApiTestUtils.mockEntityId(uuid3, uuidMapper);
        when(scope3.getTopologyContextId()).thenReturn(987L);
        when(scope3.getDisplayName()).thenReturn("cde");
        doReturn(ImmutableMap.of(345L, ImmutableList.of(mock(ActionApiDTO.class), mock(ActionApiDTO.class))))
                .when(actionSearchUtilSpy).getActionsByScopes(eq(Collections.singleton(scope3)), any(), eq(987L));

        // discard scope for which there are no actions
        final String uuid4 = "456";
        final ApiId scope4 = ApiTestUtils.mockEntityId(uuid4, uuidMapper);
        when(scope4.getTopologyContextId()).thenReturn(876L);
        when(scope4.getDisplayName()).thenReturn("def");

        final ActionPaginationResponse mockResponse = mock(ActionPaginationResponse.class);
        when(marketsService.getActionsByMarketUuid(any(), any(), any())).thenReturn(mockResponse);

        final EntityActionPaginationRequest paginationRequest = new EntityActionPaginationRequest(cursor, null, true, null);

        // start out with 4 uuids in the input DTO
        inputDto.setScopes(ImmutableList.of(uuid1, uuid2, uuid3, uuid4));
        final EntityActionPaginationResponse result =
            actionsServiceUnderTest.getActionsByUuidsQuery(inputDto, paginationRequest);

        // plan scope is used to query markets service
        verify(marketsService).getActionsByMarketUuid(eq(uuid2), any(), any());

        // usable scopes are used to query actions service
        verify(actionSearchUtilSpy).getActionsByScopes(eq(Collections.singleton(scope3)), any(), eq(987L));
        verify(actionSearchUtilSpy).getActionsByScopes(eq(Collections.singleton(scope4)), any(), eq(876L));

        assertEquals(3, result.getRawResults().size());
        assertEquals(2, result.getRawResults().stream()
            .filter(a -> uuid3.equals(a.getUuid()))
            .mapToLong(a -> a.getActions().size())
            .sum());
    }



    /**
     * Test validating action input api dto for invalid description query.
     *
     * @throws IllegalArgumentException when action input api dto is invalid
     */
    @Test(expected = IllegalArgumentException.class)
    public void testValidateInputForDescriptionQuery() throws IllegalArgumentException {
        final ActionApiInputDTO actionApiInputDTO = new ActionApiInputDTO();
        final QueryInputApiDTO descriptionQuery = new QueryInputApiDTO();
        actionApiInputDTO.setDescriptionQuery(descriptionQuery);
        actionsServiceUnderTest.validateInput(actionApiInputDTO, null);
    }

    /**
     * Test validating action input api dto for invalid risk query.
     *
     * @throws IllegalArgumentException when action input api dto is invalid
     */
    @Test(expected = IllegalArgumentException.class)
    public void testValidateInputForRiskQuery() throws IllegalArgumentException {
        final ActionApiInputDTO actionApiInputDTO = new ActionApiInputDTO();
        final QueryInputApiDTO riskQuery = new QueryInputApiDTO();
        actionApiInputDTO.setRiskQuery(riskQuery);
        actionsServiceUnderTest.validateInput(actionApiInputDTO, null);
    }

    /**
     * Test validating action input api dto for invalid savings amount range.
     *
     * @throws IllegalArgumentException when action input api dto is invalid
     */
    @Test(expected = IllegalArgumentException.class)
    public void testValidateInputForSavingsAmountRange() throws IllegalArgumentException {
        final ActionApiInputDTO actionApiInputDTO = new ActionApiInputDTO();
        final RangeInputApiDTO savingsAmountRange = new RangeInputApiDTO();
        actionApiInputDTO.setSavingsAmountRange(savingsAmountRange);
        actionsServiceUnderTest.validateInput(actionApiInputDTO, null);
    }

    /**
     * Tests the case that we are getting action by id when stable id is enabled.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGetActionByUuidUsingStableId() throws Exception {
        // ARRANGE
        when(actionsServiceBackend.getInstanceIdsForRecommendationIds(instanceIdRequest.capture()))
                .thenReturn(GetInstanceIdsForRecommendationIdsResponse
                        .newBuilder()
                        .putRecommendationIdToInstanceId(FIRST_ACTION_RECOMMENDATION_ID, FIRST_ACTION_ID)
                        .build()
                );

        ActionSpec actionSpec = ActionSpec.newBuilder()
                .setRecommendationId(FIRST_ACTION_RECOMMENDATION_ID)
                .build();

        ActionOrchestratorAction action = ActionOrchestratorAction.newBuilder()
                .setActionId(FIRST_ACTION_ID)
                .setActionSpec(actionSpec)
                .build();

        ArgumentCaptor<SingleActionRequest> requestCaptor = ArgumentCaptor.forClass(SingleActionRequest.class);
        when(actionsServiceBackend.getAction(requestCaptor.capture()))
                .thenReturn(action);

        ActionApiDTO actionApiDTO = mock(ActionApiDTO.class);

        when(actionSpecMapper.mapActionSpecToActionApiDTO(actionSpec, REALTIME_TOPOLOGY_ID, ActionDetailLevel.STANDARD))
                .thenReturn(actionApiDTO);

        // ACT
        ActionApiDTO returnedAction = actionsServiceUsingStableId
                .getActionByUuid(Long.toString(FIRST_ACTION_RECOMMENDATION_ID), ActionDetailLevel.STANDARD);

        // ASSERT
        assertThat(returnedAction, sameInstance(actionApiDTO));
        assertThat(requestCaptor.getValue().getActionId(), equalTo(FIRST_ACTION_ID));
        assertThat(requestCaptor.getValue().getTopologyContextId(), equalTo(REALTIME_TOPOLOGY_ID));
    }


    /**
     * Tests the case that we are getting action by id when stable id is enabled and the action does not exist.
     *
     * @throws Exception if something goes wrong that is expected.
     */
    @Test(expected = UnknownObjectException.class)
    public void testGetActionByUuidActionNotFoundUsingStableId() throws Exception {
        // ARRANGE
        when(actionsServiceBackend.getInstanceIdsForRecommendationIds(instanceIdRequest.capture()))
                .thenReturn(GetInstanceIdsForRecommendationIdsResponse.getDefaultInstance());

        // ACT
        actionsServiceUsingStableId
                .getActionByUuid(Long.toString(FIRST_ACTION_RECOMMENDATION_ID), ActionDetailLevel.STANDARD);
    }

    /**
     * Accept an action using stable id.
     */
    @Test
    public void testExecuteActionUsingStableId() throws Exception {
        // ARRANGE
        when(actionsServiceBackend.getInstanceIdsForRecommendationIds(instanceIdRequest.capture()))
                .thenReturn(GetInstanceIdsForRecommendationIdsResponse
                        .newBuilder()
                        .putRecommendationIdToInstanceId(FIRST_ACTION_RECOMMENDATION_ID, FIRST_ACTION_ID)
                        .build()
                );

        // ACT
        boolean successful = actionsServiceUsingStableId
                .executeAction(Long.toString(FIRST_ACTION_RECOMMENDATION_ID), true, false);

        // ASSERT
        assertTrue(successful);
        ArgumentCaptor<MultiActionRequest> requestCaptor = ArgumentCaptor.forClass(MultiActionRequest.class);
        verify(actionsServiceBackend).acceptActions(requestCaptor.capture());
        assertThat(requestCaptor.getValue().getActionIds(0), equalTo(FIRST_ACTION_ID));
        assertThat(requestCaptor.getValue().getTopologyContextId(), equalTo(REALTIME_TOPOLOGY_ID));
    }

    /**
     * Tests the case that we are executing action by id when stable id is enabled and the action does not exist.
     *
     * @throws Exception if something goes wrong that is expected.
     */
    @Test(expected = UnknownObjectException.class)
    public void testExecuteNonExistentActionUsingStableId() throws Exception {
        // ARRANGE
        when(actionsServiceBackend.getInstanceIdsForRecommendationIds(instanceIdRequest.capture()))
                .thenReturn(GetInstanceIdsForRecommendationIdsResponse.getDefaultInstance());

        // ACT
        actionsServiceUsingStableId.executeAction(Long.toString(FIRST_ACTION_RECOMMENDATION_ID), true, false);
    }
}

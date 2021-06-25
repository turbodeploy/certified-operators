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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import io.grpc.Status;

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
import com.vmturbo.api.dto.action.ActionScopesApiInputDTO;
import com.vmturbo.api.dto.action.CloudResizeActionDetailsApiDTO;
import com.vmturbo.api.dto.action.NoDetailsApiDTO;
import com.vmturbo.api.dto.action.ScopeUuidsApiInputDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.ActionDetailLevel;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.api.pagination.EntityActionPaginationRequest;
import com.vmturbo.api.pagination.EntityActionPaginationRequest.EntityActionPaginationResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.GetInstanceIdsForRecommendationIdsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetInstanceIdsForRecommendationIdsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.MultiActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;


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

    /**
     * The backend the API forwards calls to (i.e. the part that's in the plan orchestrator).
     */
    private final ActionsServiceMole actionsServiceBackend =
            spy(new ActionsServiceMole());

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

    private final long REALTIME_TOPOLOGY_ID = 777777L;

    private final String UUID = "12345";

    private final int maxInnerPagination = 500;

    private ArgumentCaptor<MultiActionRequest> multiActionRequestCaptor;
    private ArgumentCaptor<Collection> orchestratorActionCaptor;
    private ArgumentCaptor<GetInstanceIdsForRecommendationIdsRequest> instanceIdRequest;

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(actionsServiceBackend);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws IOException {
        // set up a mock Actions RPC server
        actionSpecMapper = Mockito.mock(ActionSpecMapper.class);
        marketsService = Mockito.mock(MarketsService.class);
        supplyChainFetcherFactory = Mockito.mock(SupplyChainFetcherFactory.class);
        actionsRpcService = ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel());

        actionSearchUtil = new ActionSearchUtil(actionsRpcService, actionSpecMapper, mock(PaginationMapper.class),
                supplyChainFetcherFactory, mock(GroupExpander.class), mock(ServiceProviderExpander.class),
                REALTIME_TOPOLOGY_ID, false);

        actionSearchUtilWithStableIdEnabled = new ActionSearchUtil(actionsRpcService, actionSpecMapper,
                mock(PaginationMapper.class), supplyChainFetcherFactory, mock(GroupExpander.class),
                mock(ServiceProviderExpander.class), REALTIME_TOPOLOGY_ID, true);

        actionSearchUtilSpy = spy(actionSearchUtil);

        // set up the ActionsService to test
        actionsServiceUnderTest = new ActionsService(actionsRpcService, actionSpecMapper,
            repositoryApi, REALTIME_TOPOLOGY_ID,
            actionStatsQueryExecutor, uuidMapper, serviceProviderExpander,
                actionSearchUtilSpy, marketsService, supplyChainFetcherFactory, maxInnerPagination);

        actionsServiceUsingStableId = new ActionsService(actionsRpcService, actionSpecMapper,
                repositoryApi, REALTIME_TOPOLOGY_ID,
                actionStatsQueryExecutor, uuidMapper, serviceProviderExpander,
                actionSearchUtilWithStableIdEnabled, marketsService, supplyChainFetcherFactory, maxInnerPagination);

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
        when(actionsServiceBackend.acceptActionError(any())).thenReturn(Optional.of(Status.NOT_FOUND
            .withDescription("test").asException()));
        actionsServiceUnderTest.executeAction(UUID, true, false);
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
     * Tests getting action details for actions with/without spec, order of details the same as in request.
     * Verifies that when input DTO contains no topologyContextId, the default realtime one is used.
     */
    @Test
    public void testGetActionDetailsByUuids()
            throws OperationFailedException, IllegalArgumentException {
        // ARRANGE
        setupGetActionDetailsByUuids(false);

        ScopeUuidsApiInputDTO inputDTO = new ScopeUuidsApiInputDTO();
        inputDTO.setUuids(Arrays.asList(Long.toString(FIRST_ACTION_ID), Long.toString(SECOND_ACTION_ID)));

        // ACT
        Map<String, ActionDetailsApiDTO> actionDetails = actionsServiceUnderTest.getActionDetailsByUuids(inputDTO);

        // ASSERT
        verify(actionsServiceBackend, never()).getInstanceIdsForRecommendationIds(any());
        verifyCallsAndResults(actionDetails, false, REALTIME_TOPOLOGY_ID);
    }


    /**
     * Tests getting action details for actions when stable action id is set.
     */
    @Test
    public void testGetActionDetailsByStableUuids()
            throws OperationFailedException, IllegalArgumentException {
        // ARRANGE
        setupGetActionDetailsByUuids(true);

        ScopeUuidsApiInputDTO inputDTO = new ScopeUuidsApiInputDTO();
        inputDTO.setUuids(Arrays.asList(Long.toString(FIRST_ACTION_RECOMMENDATION_ID),
                Long.toString(SECOND_ACTION_RECOMMENDATION_ID)));

        // ACT
        Map<String, ActionDetailsApiDTO> actionDetails = actionsServiceUsingStableId.getActionDetailsByUuids(inputDTO);

        // ASSERT
        verifyCallsAndResults(actionDetails, true, REALTIME_TOPOLOGY_ID);
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
        ArgumentCaptor<SingleActionRequest> requestCaptor = ArgumentCaptor.forClass(SingleActionRequest.class);
        verify(actionsServiceBackend).acceptAction(requestCaptor.capture());
        assertThat(requestCaptor.getValue().getActionId(), equalTo(FIRST_ACTION_ID));
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

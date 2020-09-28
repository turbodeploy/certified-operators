package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
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
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.ServiceProviderExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor.ActionStatsQuery;
import com.vmturbo.api.component.external.api.util.action.ImmutableActionStatsQuery;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.action.ActionDetailsApiDTO;
import com.vmturbo.api.dto.action.ActionScopesApiInputDTO;
import com.vmturbo.api.dto.action.CloudResizeActionDetailsApiDTO;
import com.vmturbo.api.dto.action.NoDetailsApiDTO;
import com.vmturbo.api.dto.action.ScopeUuidsApiInputDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.api.pagination.EntityActionPaginationRequest;
import com.vmturbo.api.pagination.EntityActionPaginationRequest.EntityActionPaginationResponse;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.MultiActionRequest;
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
    /**
     * The backend the API forwards calls to (i.e. the part that's in the plan orchestrator).
     */
    private final ActionsServiceMole actionsServiceBackend =
            Mockito.spy(new ActionsServiceMole());

    private ActionsService actionsServiceUnderTest;

    private RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);

    private ActionStatsQueryExecutor actionStatsQueryExecutor = mock(ActionStatsQueryExecutor.class);

    private UuidMapper uuidMapper = mock(UuidMapper.class);

    private final ServiceProviderExpander serviceProviderExpander = mock(ServiceProviderExpander.class);

    ActionsServiceGrpc.ActionsServiceBlockingStub actionsRpcService;

    private ActionSpecMapper actionSpecMapper;

    private ActionSearchUtil actionSearchUtil;

    private MarketsService marketsService;

    private SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final long REALTIME_TOPOLOGY_ID = 777777L;

    private final String UUID = "12345";

    private final int maxInnerPagination = 500;

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(actionsServiceBackend);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws IOException {
        // set up a mock Actions RPC server
        actionSpecMapper = Mockito.mock(ActionSpecMapper.class);
        actionSearchUtil = Mockito.mock(ActionSearchUtil.class);
        marketsService = Mockito.mock(MarketsService.class);
        supplyChainFetcherFactory = Mockito.mock(SupplyChainFetcherFactory.class);
        actionsRpcService = ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel());

        // set up the ActionsService to test
        actionsServiceUnderTest = new ActionsService(actionsRpcService, actionSpecMapper,
            repositoryApi, REALTIME_TOPOLOGY_ID,
            actionStatsQueryExecutor, uuidMapper, serviceProviderExpander,
            actionSearchUtil, marketsService, supplyChainFetcherFactory, maxInnerPagination);
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

        final ActionStatsQuery expectedQuery = ImmutableActionStatsQuery.builder()
                .entityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .actionInput(actionApiInputDTO)
                .addScopes(scope1)
                .addScopes(scope2)
                .currentTimeStamp(DateTimeUtil.toString(Clock.systemUTC().millis()))
                .build();

        final List<StatSnapshotApiDTO> scope1Snapshots = Arrays.asList(new StatSnapshotApiDTO());
        final List<StatSnapshotApiDTO> scope2Snapshots = Arrays.asList(new StatSnapshotApiDTO(), new StatSnapshotApiDTO());

        when(actionStatsQueryExecutor.retrieveActionStats(any()))
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

        verify(actionStatsQueryExecutor).retrieveActionStats(expectedQuery);

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
    public void testGetActionDetailsByUuidsEmptyUuids() {
        ScopeUuidsApiInputDTO inputDTO = new ScopeUuidsApiInputDTO();
        inputDTO.setUuids(Collections.emptyList());

        Map<String, ActionDetailsApiDTO> actionDetails = actionsServiceUnderTest.getActionDetailsByUuids(inputDTO);
        assertEquals(0, actionDetails.size());
    }

    /**
     * Tests getting action details for actions with/without spec, order of details the same as in request.
     * Verifies that when input DTO contains no topologyContextId, the default realtime one is used.
     */
    @Test
    public void testGetActionDetailsByUuids() {
        long actionIdWithSpec = 10;
        long actionIdNoSpec = 20;
        ScopeUuidsApiInputDTO inputDTO = new ScopeUuidsApiInputDTO();
        inputDTO.setUuids(Arrays.asList(Long.toString(actionIdWithSpec), Long.toString(actionIdNoSpec)));
        Map<String, ActionDetailsApiDTO> dtoMap = new HashMap<>();
        dtoMap.put("10", new CloudResizeActionDetailsApiDTO());
        dtoMap.put("20", new NoDetailsApiDTO());

        List<ActionOrchestratorAction> orchestratorActions = Arrays.asList(
                ActionOrchestratorAction.newBuilder()
                        .setActionId(actionIdNoSpec)
                        .build(),
                ActionOrchestratorAction.newBuilder()
                        .setActionId(actionIdWithSpec)
                        .setActionSpec(ActionSpec.newBuilder()
                                .build())
                        .build()
        );
        ArgumentCaptor<MultiActionRequest> multiActionRequestCaptor =
                ArgumentCaptor.forClass(MultiActionRequest.class);
        when(actionsServiceBackend.getActions(multiActionRequestCaptor.capture()))
                .thenReturn(orchestratorActions);

        ArgumentCaptor<Collection> orchestratorActionCaptor =
                ArgumentCaptor.forClass(Collection.class);
        when(actionSpecMapper.createActionDetailsApiDTO(orchestratorActionCaptor.capture(), anyLong()))
                .thenReturn(dtoMap);

        Map<String, ActionDetailsApiDTO> actionDetails = actionsServiceUnderTest.getActionDetailsByUuids(inputDTO);

        assertEquals(2, actionDetails.size());
        assertEquals(CloudResizeActionDetailsApiDTO.class, actionDetails.get(Long.toString(actionIdWithSpec)).getClass());
        assertEquals(NoDetailsApiDTO.class, actionDetails.get(Long.toString(actionIdNoSpec)).getClass());

        List<MultiActionRequest> actualRequests = multiActionRequestCaptor.getAllValues();
        assertEquals(1, actualRequests.size());
        MultiActionRequest multiActionRequest = actualRequests.get(0);
        assertEquals(actionIdWithSpec, multiActionRequest.getActionIds(0));
        assertEquals(actionIdNoSpec, multiActionRequest.getActionIds(1));
        assertEquals(REALTIME_TOPOLOGY_ID, multiActionRequest.getTopologyContextId());

        Collection<ActionOrchestratorAction> actualOrchestratorActions = orchestratorActionCaptor.getValue();
        assertEquals(2, actualOrchestratorActions.size());
        assertEquals(actionIdNoSpec, actualOrchestratorActions.iterator().next().getActionId());
    }

    /**
     * Tests getting action details for actions in a specific topology context.
     */
    @Test
    public void testGetActionDetailsByUuidsInTopologyContext() {
        final long actionId = 10;
        final long topologyContextId = 20;
        final ScopeUuidsApiInputDTO inputDTO = new ScopeUuidsApiInputDTO();
        inputDTO.setUuids(Collections.singletonList(Long.toString(actionId)));
        inputDTO.setTopologyContextId(Long.toString(topologyContextId));

        final List<ActionOrchestratorAction> mockActions =
            Collections.singletonList(ActionOrchestratorAction.getDefaultInstance());
        final ArgumentCaptor<MultiActionRequest> requestCaptor =
            ArgumentCaptor.forClass(MultiActionRequest.class);
        Map<String, ActionDetailsApiDTO> dtoMap = new HashMap<>();
        dtoMap.put("1", mock(ActionDetailsApiDTO.class));

        when(actionsServiceBackend.getActions(requestCaptor.capture())).thenReturn(mockActions);

        when(actionSpecMapper.createActionDetailsApiDTO(anyList(), anyLong()))
            .thenReturn(dtoMap);

        final Map<String, ActionDetailsApiDTO> resultDetailMap =
            actionsServiceUnderTest.getActionDetailsByUuids(inputDTO);

        assertEquals(1, resultDetailMap.size());
        final List<MultiActionRequest> requests = requestCaptor.getAllValues();
        assertEquals(1, requests.size());
        final MultiActionRequest multiActionRequest = requests.get(0);
        assertEquals(topologyContextId, multiActionRequest.getTopologyContextId());
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
        final EntityActionPaginationResponse responseShouldBeEmpty =
            actionsServiceUnderTest.getActionsByUuidsQuery(new ActionScopesApiInputDTO(),
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
        when(actionSearchUtil.getActionsByScopes(eq(Collections.singleton(scope3)), any(), eq(987L)))
            .thenReturn(ImmutableMap.of(345L,
                ImmutableList.of(mock(ActionApiDTO.class), mock(ActionApiDTO.class))));

        // discard scope for which there are no actions
        final String uuid4 = "456";
        final ApiId scope4 = ApiTestUtils.mockEntityId(uuid4, uuidMapper);
        when(scope4.getTopologyContextId()).thenReturn(876L);
        when(scope4.getDisplayName()).thenReturn("def");

        final ActionPaginationResponse mockResponse = mock(ActionPaginationResponse.class);
        when(marketsService.getActionsByMarketUuid(any(), any(), any())).thenReturn(mockResponse);

        final EntityActionPaginationRequest paginationRequest = new EntityActionPaginationRequest(cursor, null, true, null);
        final ActionScopesApiInputDTO inputDto = new ActionScopesApiInputDTO();

        // start out with 4 uuids in the input DTO
        inputDto.setScopes(ImmutableList.of(uuid1, uuid2, uuid3, uuid4));
        final EntityActionPaginationResponse result =
            actionsServiceUnderTest.getActionsByUuidsQuery(inputDto, paginationRequest);

        // plan scope is used to query markets service
        verify(marketsService).getActionsByMarketUuid(eq(uuid2), any(), any());

        // usable scopes are used to query actions service
        verify(actionSearchUtil).getActionsByScopes(eq(Collections.singleton(scope3)), any(), eq(987L));
        verify(actionSearchUtil).getActionsByScopes(eq(Collections.singleton(scope4)), any(), eq(876L));

        // only one scope is represented in the result, because that was the only scope for which there were actions.
        assertEquals(1, result.getRawResults().size());
        assertEquals(uuid3, result.getRawResults().get(0).getUuid());
        assertEquals(2, result.getRawResults().get(0).getActions().size());
    }
}

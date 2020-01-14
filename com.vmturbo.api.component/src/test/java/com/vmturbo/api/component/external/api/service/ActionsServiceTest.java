package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor.ActionStatsQuery;
import com.vmturbo.api.component.external.api.util.action.ImmutableActionStatsQuery;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.action.ActionDetailsApiDTO;
import com.vmturbo.api.dto.action.ActionScopesApiInputDTO;
import com.vmturbo.api.dto.action.CloudResizeActionDetailsApiDTO;
import com.vmturbo.api.dto.action.NoDetailsApiDTO;
import com.vmturbo.api.dto.action.ScopeUuidsApiInputDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.MultiActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;


/**
 * Test the service methods for the /actions endpoint.
 */
public class ActionsServiceTest {

    public static final String[] ALL_ACTION_MODES = {
            "RECOMMEND", "DISABLED", "MANUAL", "AUTOMATIC"
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

    ActionsServiceGrpc.ActionsServiceBlockingStub actionsRpcService;

    private ActionSpecMapper actionSpecMapper;

    private final long REALTIME_TOPOLOGY_ID = 777777L;

    private final String UUID = "12345";

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(actionsServiceBackend);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws IOException {

        // set up a mock Actions RPC server
        actionSpecMapper = Mockito.mock(ActionSpecMapper.class);
        actionsRpcService = ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel());

        // set up the ActionsService to test
        actionsServiceUnderTest = new ActionsService(actionsRpcService, actionSpecMapper,
            repositoryApi, REALTIME_TOPOLOGY_ID,
            actionStatsQueryExecutor, uuidMapper);
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
        when(actionsServiceBackend.acceptAction(any())).thenReturn(acceptanceError("Action error"));
        actionsServiceUnderTest.executeAction(UUID, true, false);
    }

    @Test
    public void testGetActionTypeCountStatsByUuidsQuery() throws Exception {

        final ActionScopesApiInputDTO actionScopesApiInputDTO = new ActionScopesApiInputDTO();
        final ActionApiInputDTO actionApiInputDTO = new ActionApiInputDTO();
        actionScopesApiInputDTO.setScopes(Lists.newArrayList("1", "3"));

        final ApiId scope1 = ApiTestUtils.mockEntityId("1", uuidMapper);
        final ApiId scope2 = ApiTestUtils.mockGroupId("3", uuidMapper);
        actionScopesApiInputDTO.setActionInput(actionApiInputDTO);
        actionScopesApiInputDTO.setRelatedType(UIEntityType.PHYSICAL_MACHINE.apiStr());

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
            .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
            .setDisplayName("First Entity")
            .build()));
        when(repositoryApi.entitiesRequest(Collections.singleton(1L))).thenReturn(req);

        final Map<String, EntityStatsApiDTO> statsByUuid =
            actionsServiceUnderTest.getActionStatsByUuidsQuery(actionScopesApiInputDTO).stream()
                .collect(Collectors.toMap(EntityStatsApiDTO::getUuid, Function.identity()));

        verify(actionStatsQueryExecutor).retrieveActionStats(expectedQuery);
        verify(repositoryApi).entitiesRequest(Collections.singleton(1L));

        assertThat(statsByUuid.keySet(), containsInAnyOrder("1", "3"));
        assertThat(statsByUuid.get("1").getStats(), is(scope1Snapshots));
        assertThat(statsByUuid.get("1").getDisplayName(), is("First Entity"));
        assertThat(statsByUuid.get("1").getClassName(), is(UIEntityType.VIRTUAL_MACHINE.apiStr()));

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
     */
    @Test
    public void testGetActionDetailsByUuids() {
        long actionIdWithSpec = 10;
        long actionIdNoSpec = 20;
        ScopeUuidsApiInputDTO inputDTO = new ScopeUuidsApiInputDTO();
        inputDTO.setUuids(Arrays.asList(Long.toString(actionIdWithSpec), Long.toString(actionIdNoSpec)));

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

        ArgumentCaptor<ActionOrchestratorAction> orchestratorActionCaptor =
                ArgumentCaptor.forClass(ActionOrchestratorAction.class);
        when(actionSpecMapper.createActionDetailsApiDTO(orchestratorActionCaptor.capture()))
                .thenReturn(new CloudResizeActionDetailsApiDTO());

        Map<String, ActionDetailsApiDTO> actionDetails = actionsServiceUnderTest.getActionDetailsByUuids(inputDTO);

        assertEquals(2, actionDetails.size());
        assertEquals(CloudResizeActionDetailsApiDTO.class, actionDetails.get(Long.toString(actionIdWithSpec)).getClass());
        assertEquals(NoDetailsApiDTO.class, actionDetails.get(Long.toString(actionIdNoSpec)).getClass());

        List<MultiActionRequest> actualRequests = multiActionRequestCaptor.getAllValues();
        assertEquals(1, actualRequests.size());
        MultiActionRequest multiActionRequest = actualRequests.get(0);
        assertEquals(actionIdWithSpec, multiActionRequest.getActionIds(0));
        assertEquals(actionIdNoSpec, multiActionRequest.getActionIds(1));

        List<ActionOrchestratorAction> actualOrchestratorActions = orchestratorActionCaptor.getAllValues();
        assertEquals(1, actualOrchestratorActions.size());
        assertEquals(actionIdWithSpec, actualOrchestratorActions.get(0).getActionId());
    }

    private static AcceptActionResponse acceptanceError(@Nonnull final String error) {
        return AcceptActionResponse.newBuilder()
                .setError(error)
                .build();
    }
}

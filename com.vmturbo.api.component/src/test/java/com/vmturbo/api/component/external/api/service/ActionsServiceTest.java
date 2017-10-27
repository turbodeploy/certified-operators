package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.grpc.stub.StreamObserver;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.action.ActionScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter.InvolvedEntities;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityResponse.ActionCountsByEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.TypeCount;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;


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
    private final TestActionsRpcService actionsServiceBackend =
            Mockito.spy(new TestActionsRpcService());

    private ActionsService actionsServiceUnderTest;

    private RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);

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
            repositoryApi, REALTIME_TOPOLOGY_ID);
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
        actionsServiceUnderTest.executeAction(UUID, true);
    }

    @Test
    public void testGetActionTypeCountStatsByUuidsQuery() throws Exception {
        final Set<Long> entityIds = Sets.newHashSet(1L, 2L);
        final ServiceEntityApiDTO serviceEntityOne = new ServiceEntityApiDTO();
        serviceEntityOne.setDisplayName("First Entity");
        serviceEntityOne.setClassName("VM #1");
        serviceEntityOne.setUuid("1");
        final ServiceEntityApiDTO serviceEntityTwo = new ServiceEntityApiDTO();
        serviceEntityTwo.setDisplayName("Second Entity");
        serviceEntityTwo.setClassName("VM #2");
        serviceEntityTwo.setUuid("2");
        final Map<Long, Optional<ServiceEntityApiDTO>> serviceEntityMap =
            ImmutableMap.of(1L, Optional.of(serviceEntityOne),
                2L, Optional.of(serviceEntityTwo));
        final ActionScopesApiInputDTO actionScopesApiInputDTO = new ActionScopesApiInputDTO();
        final ActionApiInputDTO actionApiInputDTO = new ActionApiInputDTO();
        actionApiInputDTO.setGroupBy(Lists.newArrayList("actionTypes"));
        actionScopesApiInputDTO.setScopes(Lists.newArrayList("1", "2"));
        actionScopesApiInputDTO.setActionInput(actionApiInputDTO);

        final ActionQueryFilter filter = ActionQueryFilter.newBuilder()
            .setVisible(true)
            .setInvolvedEntities(InvolvedEntities.newBuilder()
                .addOids(1L)
                .addOids(2L))
            .build();

        Mockito.when(repositoryApi.getServiceEntitiesById(entityIds))
            .thenReturn(serviceEntityMap);

        Mockito.when(actionSpecMapper.createActionFilter(actionApiInputDTO, Optional.of(entityIds)))
            .thenReturn(filter);

        final List<EntityStatsApiDTO> entityStatsApiDTOS =
            actionsServiceUnderTest.getActionStatsByUuidsQuery(actionScopesApiInputDTO);

        assertEquals(2, entityStatsApiDTOS.size());
        assertEquals(1, entityStatsApiDTOS.get(0).getStats().size());
        assertTrue(entityStatsApiDTOS.get(1).getStats().isEmpty());
        final List<StatSnapshotApiDTO> statSnapshotApiDTOS = entityStatsApiDTOS.get(0).getStats();
        assertEquals(2, statSnapshotApiDTOS.get(0).getStatistics().size());
        final List<StatApiDTO> statApiDTOS = statSnapshotApiDTOS.get(0).getStatistics();
        assertEquals("MOVE", statApiDTOS.get(0).getFilters().get(0).getValue());
        assertEquals(2.0f, statApiDTOS.get(0).getValue(), 0.000000001);
        assertEquals("DEACTIVATE", statApiDTOS.get(1).getFilters().get(0).getValue());
        assertEquals(1.0f, statApiDTOS.get(1).getValue(), 0.000000001);

    }

    /**
     * This class mocks backend ActionsService RPC calls. An instance is injected into the
     * {@link ActionsService} under test.
     * <p>
     * No method implementations are currently needed, but will be required as we test
     * methods in ActionsService that make RPC calls.
     */
    private class TestActionsRpcService extends ActionsServiceGrpc.ActionsServiceImplBase {

        /**
         * Mock RPC accept action call return action not exist error
         */
        @Override
        public void acceptAction(SingleActionRequest request,
                                 StreamObserver<AcceptActionResponse> responseObserver) {
            responseObserver.onNext(acceptanceError("Action not exist"));
            responseObserver.onCompleted();
        }

        @Override
        public void getActionCountsByEntity(GetActionCountsByEntityRequest request,
                                            StreamObserver<GetActionCountsByEntityResponse> responseObserver) {
            GetActionCountsByEntityResponse.Builder response = GetActionCountsByEntityResponse.newBuilder();
            ActionCountsByEntity actionCountsByEntity = ActionCountsByEntity.newBuilder()
                .setEntityId(1L)
                .addCountsByType(TypeCount.newBuilder()
                    .setCount(2)
                    .setType(ActionType.MOVE))
                .addCountsByType(TypeCount.newBuilder()
                    .setCount(1)
                    .setType(ActionType.DEACTIVATE))
                .build();
            response.addActionCountsByEntity(actionCountsByEntity);
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }
    }

    private static AcceptActionResponse acceptanceError(@Nonnull final String error) {
        return AcceptActionResponse.newBuilder()
                .setError(error)
                .build();
    }
}

package com.vmturbo.action.orchestrator.rpc;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Optional;

import io.grpc.Status.Code;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.ActionPaginator.ActionPaginatorFactory;
import com.vmturbo.action.orchestrator.approval.ActionApprovalManager;
import com.vmturbo.action.orchestrator.stats.HistoricalActionStatReader;
import com.vmturbo.action.orchestrator.stats.query.live.CurrentActionStatReader;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.ActionStorehouse.StoreDeletionException;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.action.ActionDTO.DeleteActionsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.DeleteActionsResponse;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;

public class ActionDeletionRpcTest {
    private ActionsServiceBlockingStub actionOrchestratorServiceClient;

    private final ActionStorehouse actionStorehouse = mock(ActionStorehouse.class);
    private final ActionStore actionStore = mock(ActionStore.class);
    private final ActionPaginatorFactory paginatorFactory = mock(ActionPaginatorFactory.class);
    private final HistoricalActionStatReader statReader = mock(HistoricalActionStatReader.class);
    private final CurrentActionStatReader liveStatReader = mock(CurrentActionStatReader.class);
    private final AcceptedActionsDAO acceptedActionsStore = Mockito.mock(AcceptedActionsDAO.class);

    private final long topologyContextId = 3;

    private final Clock clock = new MutableFixedClock(1_000_000);

    private final UserSessionContext userSessionContext = mock(UserSessionContext.class);
    private ActionApprovalManager approvalManager;
    private ActionsRpcService actionsRpcService;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private GrpcTestServer grpcServer;

    /**
     * Initializes the tests.
     *
     * @throws Exception on exceptions occurred
     */
    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);
        approvalManager = Mockito.mock(ActionApprovalManager.class);
        actionsRpcService = new ActionsRpcService(clock, actionStorehouse,
                        approvalManager,
                        mock(ActionTranslator.class),
                        paginatorFactory,
                        statReader,
                        liveStatReader,
                        userSessionContext,
                        acceptedActionsStore,
                        500);
        grpcServer = GrpcTestServer.newServer(actionsRpcService);
        grpcServer.start();
        actionOrchestratorServiceClient = ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel());
        when(actionStorehouse.getStore(topologyContextId)).thenReturn(Optional.of(actionStore));
    }

    /**
     * Cleans up resources after tests.
     */
    @After
    public void cleanup() {
        grpcServer.close();
    }

    @Test
    public void testDeletionNotPermitted() throws Exception {
        when(actionStorehouse.deleteStore(topologyContextId))
            .thenThrow(new IllegalStateException("Not permitted"));

        DeleteActionsRequest actionRequest = DeleteActionsRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .build();

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
            .descriptionContains("Operation not permitted for context " + topologyContextId));
        actionOrchestratorServiceClient.deleteActions(actionRequest);
    }

    @Test
    public void testDeletionFails() throws Exception {
        when(actionStorehouse.deleteStore(topologyContextId))
            .thenThrow(new StoreDeletionException("Failed"));

        DeleteActionsRequest actionRequest = DeleteActionsRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .build();

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INTERNAL)
            .descriptionContains("Attempt to delete actions for context " + topologyContextId + " failed."));
        actionOrchestratorServiceClient.deleteActions(actionRequest);
    }

    @Test
    public void testDeletionContextMissingParameter() {
        DeleteActionsRequest actionRequest = DeleteActionsRequest.newBuilder()
            .build();

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
            .descriptionContains("Missing required parameter topologyContextId."));

        actionOrchestratorServiceClient.deleteActions(actionRequest);
    }

    @Test
    public void testDeletionContextNotFound() throws Exception {
        when(actionStorehouse.deleteStore(topologyContextId)).thenReturn(Optional.empty());
        DeleteActionsRequest actionRequest = DeleteActionsRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .build();

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.NOT_FOUND)
            .descriptionContains(topologyContextId + " not found."));

        actionOrchestratorServiceClient.deleteActions(actionRequest);
    }

    @Test
    public void testDeletionSucceeds() throws Exception {
        when(actionStorehouse.deleteStore(topologyContextId)).thenReturn(Optional.of(actionStore));
        when(actionStore.size()).thenReturn(12);
        DeleteActionsRequest actionRequest = DeleteActionsRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .build();

        DeleteActionsResponse response = actionOrchestratorServiceClient.deleteActions(actionRequest);
        assertEquals(topologyContextId, response.getTopologyContextId());
        assertEquals(12, response.getActionCount());
    }
}

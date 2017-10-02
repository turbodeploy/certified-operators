package com.vmturbo.action.orchestrator.rpc;

import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import io.grpc.Status.Code;

import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.ActionTranslator;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.ActionStorehouse.StoreDeletionException;
import com.vmturbo.common.protobuf.action.ActionDTO.DeleteActionsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.DeleteActionsResponse;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class ActionDeletionRpcTest {
    private GrpcTestServer grpcServer;
    private ActionsServiceBlockingStub actionOrchestratorServiceClient;

    private final ActionStorehouse actionStorehouse = Mockito.mock(ActionStorehouse.class);
    private final ActionStore actionStore = Mockito.mock(ActionStore.class);

    private final long topologyContextId = 3;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);

        ActionsRpcService actionsRpcService =
            new ActionsRpcService(actionStorehouse, Mockito.mock(ActionExecutor.class), Mockito.mock(ActionTranslator.class));

        grpcServer = GrpcTestServer.withServices(actionsRpcService);
        actionOrchestratorServiceClient = ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel());
        when(actionStorehouse.getStore(topologyContextId)).thenReturn(Optional.of(actionStore));
    }

    @After
    public void teardown() {
        grpcServer.close();
    }

    @Test
    public void testDeletionNotPermitted() throws Exception {
        when(actionStorehouse.deleteStore(topologyContextId))
            .thenThrow(new IllegalStateException("Not permitted"));

        DeleteActionsRequest actionRequest = DeleteActionsRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .build();

        expectedException.expect(GrpcExceptionMatcher.code(Code.INVALID_ARGUMENT)
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

        expectedException.expect(GrpcExceptionMatcher.code(Code.INTERNAL)
            .descriptionContains("Attempt to delete actions for context " + topologyContextId + " failed."));
        actionOrchestratorServiceClient.deleteActions(actionRequest);
    }

    @Test
    public void testDeletionContextMissingParameter() {
        DeleteActionsRequest actionRequest = DeleteActionsRequest.newBuilder()
            .build();

        expectedException.expect(GrpcExceptionMatcher.code(Code.INVALID_ARGUMENT)
            .descriptionContains("Missing required parameter topologyContextId."));

        actionOrchestratorServiceClient.deleteActions(actionRequest);
    }

    @Test
    public void testDeletionContextNotFound() throws Exception {
        when(actionStorehouse.deleteStore(topologyContextId)).thenReturn(Optional.empty());
        DeleteActionsRequest actionRequest = DeleteActionsRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .build();

        expectedException.expect(GrpcExceptionMatcher.code(Code.NOT_FOUND)
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

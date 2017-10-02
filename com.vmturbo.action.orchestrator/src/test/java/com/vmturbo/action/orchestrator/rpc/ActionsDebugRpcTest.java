package com.vmturbo.action.orchestrator.rpc;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import io.grpc.Status.Code;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsResponse;
import com.vmturbo.common.protobuf.action.ActionsDebugServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsDebugServiceGrpc.ActionsDebugServiceBlockingStub;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;

public class ActionsDebugRpcTest {
    private GrpcTestServer grpcServer;
    private ActionsDebugServiceBlockingStub actionOrchestratorServiceClient;

    private final ActionStorehouse actionStorehouse = Mockito.mock(ActionStorehouse.class);
    private final ActionStore actionStore = Mockito.mock(ActionStore.class);

    private static final long TOPOLOGY_CONTEXT_ID = 3;

    private final ActionPlan actionPlan = ActionPlan.newBuilder()
        .setId(999)
        .addAction(ActionOrchestratorTestUtils.createMoveRecommendation(1))
        .build();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);
        final ActionsDebugRpcService actionsDebugRpcService = new ActionsDebugRpcService(actionStorehouse);

        grpcServer = GrpcTestServer.withServices(actionsDebugRpcService);
        actionOrchestratorServiceClient = ActionsDebugServiceGrpc.newBlockingStub(grpcServer.getChannel());
        when(actionStorehouse.getStore(TOPOLOGY_CONTEXT_ID)).thenReturn(Optional.of(actionStore));
    }

    @After
    public void teardown() {
        if (grpcServer != null) {
            grpcServer.close();
        }
    }

    @Test
    public void testOverwriteActions() throws Exception {
        when(actionStorehouse.storeActions(eq(actionPlan))).thenReturn(actionStore);
        final ActionView actionView = new Action(actionPlan.getActionList().get(0), actionPlan.getId());
        when(actionStore.getActionViews()).thenReturn(ImmutableMap.of(actionView.getId(), actionView));

        final GetActionCountsResponse response = actionOrchestratorServiceClient.overrideActionPlan(actionPlan);
        assertEquals(1, response.getCountsByTypeCount());
        assertEquals(ActionType.MOVE, response.getCountsByType(0).getType());
        assertEquals(1, response.getCountsByType(0).getCount());
    }

    @Test
    public void testOverwriteActionsWithException() throws Exception {
        when(actionStorehouse.storeActions(eq(actionPlan)))
            .thenThrow(new IllegalArgumentException("Failed!"));

        expectedException.expect(GrpcExceptionMatcher.code(Code.INTERNAL)
            .descriptionContains("Failed!"));
        actionOrchestratorServiceClient.overrideActionPlan(actionPlan);
    }
}
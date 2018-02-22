package com.vmturbo.action.orchestrator.rpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Optional;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status.Code;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

import com.vmturbo.action.orchestrator.ActionOrchestratorComponent;
import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.ActionTranslator;
import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor;
import com.vmturbo.action.orchestrator.store.ActionFactory;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.ActionSupportResolver;
import com.vmturbo.action.orchestrator.store.EntitySettingsCache;
import com.vmturbo.action.orchestrator.store.EntityTypeMap;
import com.vmturbo.action.orchestrator.store.IActionFactory;
import com.vmturbo.action.orchestrator.store.IActionStoreFactory;
import com.vmturbo.action.orchestrator.store.IActionStoreLoader;
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.auth.api.authorization.jwt.JwtCallCredential;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.auth.api.authorization.jwt.JwtServerInterceptor;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.authorization.kvstore.ApiKVAuthStore;
import com.vmturbo.auth.api.authorization.kvstore.IApiAuthStore;
import com.vmturbo.auth.test.JwtContextUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;

/**
 * Integration tests for secure action execution RPC.
 *
 * Test flow:
 *  gRPC client side:
 *  sends requests with JwtCallCredential or JwtClientInterceptor
 *
 *  gPRC server side:
 *  server interceptor intercepts the request, validate the JWT token and store subject (user name)
 *  to {@link Context}.
 *
 * Covered gRPC JWT classes:
 * {@link JwtClientInterceptor}
 * {@link JwtCallCredential}
 * {@link JwtServerInterceptor}
 *
 * Covered gRPC server classes:
 * {@link ActionOrchestratorComponent}
 * {@link ActionsRpcService#acceptAction}
 * {@link SecurityConstant}
 */
public class ActionExecutionSecureRpcTest {
    private final static long ACTION_PLAN_ID = 2;
    private final static long TOPOLOGY_CONTEXT_ID = 3;
    private final static long ACTION_ID = 9999;
    private final IActionFactory actionFactory = new ActionFactory();
    private final IActionStoreFactory actionStoreFactory = mock(IActionStoreFactory.class);
    private final IActionStoreLoader actionStoreLoader = mock(IActionStoreLoader.class);
    private final AutomatedActionExecutor executor = mock(AutomatedActionExecutor.class);
    private final ActionStorehouse actionStorehouse = new ActionStorehouse(actionStoreFactory,
            executor, actionStoreLoader);
    private final ActionExecutor actionExecutor = mock(ActionExecutor.class);
    // Have the translator pass-through translate all actions.
    private final ActionTranslator actionTranslator = Mockito.spy(new ActionTranslator(actionStream ->
            actionStream.peek(action -> {
                action.getActionTranslation().setPassthroughTranslationSuccess();
            })));
    private final ActionSupportResolver filter = mock
            (ActionSupportResolver.class);

    private final EntitySettingsCache entitySettingsCache = mock(EntitySettingsCache.class);
    private final EntityTypeMap entityTypeMap = mock(EntityTypeMap.class);
    private final ActionsRpcService actionsRpcService =
            new ActionsRpcService(actionStorehouse, actionExecutor, actionTranslator);
    private ActionsServiceBlockingStub actionOrchestratorServiceClient;
    private ActionsServiceBlockingStub actionOrchestratorServiceClientWithInterceptor;
    private ActionStore actionStoreSpy;
    private Server secureGrpcServer;
    private ManagedChannel channel;
    private final ActionHistoryDao actionHistoryDao = mock(ActionHistoryDao.class);

    // utility for creating / interacting with a debugging JWT context
    JwtContextUtil jwtContextUtil;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final static ByteArrayOutputStream outputStream;
    private static final PrintStream old_out = System.out;

    static {
        outputStream = new ByteArrayOutputStream();
        // redirect output stream to PrintStream, so we can verify the audit output.
        System.setOut(new PrintStream(outputStream));
    }
    private static ActionPlan actionPlan(ActionDTO.Action recommendation) {
        return ActionPlan.newBuilder()
                .setId(ACTION_PLAN_ID)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .addAction(recommendation)
                .build();
    }

    @AfterClass
    public static void afterClass() {
        System.setOut(old_out);
        System.out.println("finished testing");
    }

    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws Exception {

        jwtContextUtil = new JwtContextUtil();
        jwtContextUtil.setupSecurityContext(actionsRpcService, 1234567890L, "userid");

        ManagedChannel channel = jwtContextUtil.getChannel();
        actionOrchestratorServiceClientWithInterceptor = ActionsServiceGrpc.newBlockingStub(channel)
                .withInterceptors(new JwtClientInterceptor());

        // setup gPRC client
        actionOrchestratorServiceClient = ActionsServiceGrpc.newBlockingStub(channel);

        // setup gPRC client with client interceptor
        actionOrchestratorServiceClientWithInterceptor = ActionsServiceGrpc.newBlockingStub(channel)
                .withInterceptors(new JwtClientInterceptor());

        // mock action store
        actionStoreSpy = Mockito.spy(new LiveActionStore(actionFactory, TOPOLOGY_CONTEXT_ID,
                filter, entitySettingsCache, entityTypeMap, actionHistoryDao));
        when(entityTypeMap.getTypeForEntity(anyLong())).thenReturn(Optional.empty());
        when(actionStoreFactory.newStore(anyLong())).thenReturn(actionStoreSpy);
        when(actionStoreLoader.loadActionStores()).thenReturn(Collections.emptyList());
        when(actionStoreFactory.getContextTypeName(anyLong())).thenReturn("foo");
    }

    @After
    public void tearDown() {
        System.out.println("shutting down gRPC channel and server");
        jwtContextUtil.shutdown();
    }

    /**
     * Test accepting an existing action with client providing JwtCallCredential
     */
    @Test
    public void testAcceptAction() {
        final ActionPlan plan = actionPlan(ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID));
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
                .setActionId(ACTION_ID)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();

        actionStorehouse.storeActions(plan);
        AcceptActionResponse response = actionOrchestratorServiceClient
                .withCallCredentials(new JwtCallCredential(jwtContextUtil.getToken()
                        .getCompactRepresentation()))
                .acceptAction(acceptActionRequest);

        assertFalse(response.hasError());
        assertTrue(response.hasActionSpec());
        assertEquals(ACTION_ID, response.getActionSpec().getRecommendation().getId());
        assertEquals(ActionState.IN_PROGRESS, response.getActionSpec().getActionState());
        // verify log message includes admin user.
        // redirect System.out is not working when running in a test suite. Comment out for now.
        // verifyMessage(outputStream.toString(),
        //        ADMIN + " from IP address: " + IP_ADDRESS +" is trying to execute Action: 9999");
    }


    /**
     * Test accepting an existing action with client interceptor
     */
    @Test
    public void testAcceptActionWithClientInterceptor() {
        final ActionPlan plan = actionPlan(ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID));
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
                .setActionId(ACTION_ID)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();

        actionStorehouse.storeActions(plan);
        AcceptActionResponse response = actionOrchestratorServiceClientWithInterceptor
                .acceptAction(acceptActionRequest);

        assertFalse(response.hasError());
        assertTrue(response.hasActionSpec());
        assertEquals(ACTION_ID, response.getActionSpec().getRecommendation().getId());
        assertEquals(ActionState.IN_PROGRESS, response.getActionSpec().getActionState());
        // verify log message include admin user
        // redirect System.out is not working when running in a test suite. Comment out for now.
        // verifyMessage(outputStream.toString(),
        //        ADMIN + " from IP address: " + IP_ADDRESS +" is trying to execute Action: 9999");
    }

    /**
     * Test accepting an existing action with invalid JWT token.
     */
    @Test
    public void testAcceptActionWithInvalidJwtToken() {
        final ActionPlan plan = actionPlan(ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID));
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
                .setActionId(ACTION_ID)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();

        actionStorehouse.storeActions(plan);
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.UNAUTHENTICATED)
                .descriptionContains("JWT strings must contain exactly 2 period characters"));
        actionOrchestratorServiceClient
                .withCallCredentials(new JwtCallCredential("wrong token"))
                .acceptAction(acceptActionRequest);
    }

    /**
     * Test accepting an existing action without JWT token.
     */
    @Test
    public void testAcceptActionWithoutJwtToken() {
        final ActionPlan plan = actionPlan(ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID));
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
                .setActionId(ACTION_ID)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();

        actionStorehouse.storeActions(plan);
        AcceptActionResponse response = actionOrchestratorServiceClient
                .acceptAction(acceptActionRequest); // don't pass JWT token

        assertFalse(response.hasError());
        assertTrue(response.hasActionSpec());
        assertEquals(ACTION_ID, response.getActionSpec().getRecommendation().getId());
        assertEquals(ActionState.IN_PROGRESS, response.getActionSpec().getActionState());
    }

    // we want to ensure gRPC service can still be functional even the public key is NOT available
    @Test
    public void testAcceptActionWithoutPublicKey() throws Exception {
        // setup gRPC service with null public key server interceptor
        final IApiAuthStore emptyApiAuthStore = mock(ApiKVAuthStore.class);
        when(emptyApiAuthStore.retrievePublicKey()).thenReturn(null); // Setting the public key to null

        // setup JWT ServerInterceptor with empty public key.
        JwtServerInterceptor jwtInterceptorWithoutPublicKey = new JwtServerInterceptor(emptyApiAuthStore);

        // start secure gRPC
        final String serviceName = "grpc-security-JWT-testWithoutPublicKey";
        final InProcessServerBuilder inProcessServerBuilder = InProcessServerBuilder.forName(serviceName);
        inProcessServerBuilder.addService(ServerInterceptors.intercept(actionsRpcService, jwtInterceptorWithoutPublicKey));
        Server secureGrpcServerWithNullPublicKeyInInterceptor = inProcessServerBuilder.build();
        secureGrpcServerWithNullPublicKeyInInterceptor.start();
        ManagedChannel managedChannel = InProcessChannelBuilder.forName(serviceName).build();

        // setup gPRC client
        ActionsServiceBlockingStub actionOrchestratorServiceTestClient = ActionsServiceGrpc.newBlockingStub(managedChannel);

        // perform actual action execution test
        final ActionPlan plan = actionPlan(ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID));
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
                .setActionId(ACTION_ID)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();

        actionStorehouse.storeActions(plan);
        AcceptActionResponse response = actionOrchestratorServiceTestClient
                .withCallCredentials(new JwtCallCredential(jwtContextUtil.getToken()
                        .getCompactRepresentation()))
                .acceptAction(acceptActionRequest);

        assertFalse(response.hasError());
        assertTrue(response.hasActionSpec());
        assertEquals(ACTION_ID, response.getActionSpec().getRecommendation().getId());
        assertEquals(ActionState.IN_PROGRESS, response.getActionSpec().getActionState());

        // clean up
        managedChannel.shutdown();
        secureGrpcServerWithNullPublicKeyInInterceptor.shutdown();

    }
}

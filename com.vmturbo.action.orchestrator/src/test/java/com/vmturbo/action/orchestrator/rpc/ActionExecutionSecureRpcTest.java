package com.vmturbo.action.orchestrator.rpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionPaginator.ActionPaginatorFactory;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor;
import com.vmturbo.action.orchestrator.execution.ImmutableActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache;
import com.vmturbo.action.orchestrator.stats.HistoricalActionStatReader;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician;
import com.vmturbo.action.orchestrator.stats.query.live.CurrentActionStatReader;
import com.vmturbo.action.orchestrator.store.ActionFactory;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.store.IActionFactory;
import com.vmturbo.action.orchestrator.store.IActionStoreFactory;
import com.vmturbo.action.orchestrator.store.IActionStoreLoader;
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.jwt.JwtCallCredential;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.auth.api.authorization.jwt.JwtServerInterceptor;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.authorization.kvstore.AuthStore;
import com.vmturbo.auth.api.authorization.kvstore.IAuthStore;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.auth.test.JwtContextUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.identity.ArrayOidSet;

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

    // Have the translator pass-through translate all actions.
    private final ActionTranslator actionTranslator = ActionOrchestratorTestUtils.passthroughTranslator();
    private final ActionModeCalculator actionModeCalculator = new ActionModeCalculator();
    private final IActionFactory actionFactory = new ActionFactory(actionModeCalculator);
    private final IActionStoreFactory actionStoreFactory = mock(IActionStoreFactory.class);
    private final IActionStoreLoader actionStoreLoader = mock(IActionStoreLoader.class);
    private final AutomatedActionExecutor executor = mock(AutomatedActionExecutor.class);
    private final WorkflowStore workflowStore = mock(WorkflowStore.class);
    private final HistoricalActionStatReader statReader = mock(HistoricalActionStatReader.class);
    private final CurrentActionStatReader currentActionStatReader = mock(CurrentActionStatReader.class);
    private final ActionExecutor actionExecutor = mock(ActionExecutor.class);
    private final ActionTargetSelector actionTargetSelector = mock(ActionTargetSelector.class);
    private final ProbeCapabilityCache probeCapabilityCache = mock(ProbeCapabilityCache.class);
    private final ActionStorehouse actionStorehouse = new ActionStorehouse(actionStoreFactory,
            executor, actionStoreLoader, actionModeCalculator);
    private final ActionPaginatorFactory paginatorFactory = mock(ActionPaginatorFactory.class);

    private final LiveActionsStatistician actionsStatistician = mock(LiveActionsStatistician.class);

    private final EntitiesAndSettingsSnapshotFactory entitySettingsCache = mock(EntitiesAndSettingsSnapshotFactory.class);

    private final EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);

    private final Clock clock = new MutableFixedClock(1_000_000);

    private final UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private final EntitiesAndSettingsSnapshotFactory snapshotFactory = mock(EntitiesAndSettingsSnapshotFactory.class);

    private final LicenseCheckClient licenseCheckClient = mock(LicenseCheckClient.class);

    private final ActionsRpcService actionsRpcService =
        new ActionsRpcService(clock,
            actionStorehouse,
            actionExecutor,
            actionTargetSelector,
            entitySettingsCache,
            actionTranslator,
            paginatorFactory,
            workflowStore,
            statReader,
            currentActionStatReader,
            userSessionContext);
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
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyId(123)
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID))))
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

        ActionTargetInfo targetInfo = ImmutableActionTargetInfo.builder()
            .supportingLevel(SupportLevel.SUPPORTED)
            .targetId(123)
            .build();
        when(actionTargetSelector.getTargetsForActions(any(), any())).thenAnswer(invocation -> {
            Stream<Action> actions = invocation.getArgumentAt(0, Stream.class);
            return actions.collect(Collectors.toMap(ActionDTO.Action::getId, action -> targetInfo));
        });
        when(actionTargetSelector.getTargetForAction(any(), any())).thenReturn(targetInfo);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());

        // mock action store
        actionStoreSpy = Mockito.spy(new LiveActionStore(actionFactory, TOPOLOGY_CONTEXT_ID,
            actionTargetSelector, probeCapabilityCache, entitySettingsCache,
            actionHistoryDao, actionsStatistician, actionTranslator, clock, userSessionContext,
                licenseCheckClient));
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
        ActionDTO.Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
                .setActionId(ACTION_ID)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), any(), anyLong(), anyLong())).thenReturn(snapshot);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());

        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot,recommendation);

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
        Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
                .setActionId(ACTION_ID)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), any(), anyLong(), anyLong())).thenReturn(snapshot);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());

        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot,recommendation);

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
        Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
                .setActionId(ACTION_ID)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), any(), anyLong(), anyLong())).thenReturn(snapshot);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());

        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot,recommendation);

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
        Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
                .setActionId(ACTION_ID)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), any(), anyLong(), anyLong())).thenReturn(snapshot);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());

        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot,recommendation);

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
        final IAuthStore emptyApiAuthStore = mock(AuthStore.class);
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
        Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
                .setActionId(ACTION_ID)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), any(), anyLong(), anyLong())).thenReturn(snapshot);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());

        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot,recommendation);

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

    @Test
    public void testAcceptActionWithScopedUser() {
        final Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID, 10L, 0, 1, 1, 1 );
        final ActionPlan plan = actionPlan(recommendation);
        when(entitySettingsCache.newSnapshot(any(), any(), anyLong(), anyLong())).thenReturn(snapshot);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot, recommendation);
        actionStorehouse.storeActions(plan);

        when(userSessionContext.isUserScoped()).thenReturn(true);

        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), any(), anyLong(), anyLong())).thenReturn(snapshot);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());
        // a user WITH access CAN execute the action
        EntityAccessScope accessScopeOK = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(10L, 0L, 1L)), null);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScopeOK);

        AcceptActionResponse response = actionOrchestratorServiceClient.acceptAction(acceptActionRequest);
        assertFalse(response.hasError());

        // A user WITHOUT access to entity 10L CANNOT execute the action.
        EntityAccessScope accessScopeFail = new EntityAccessScope(null, null,
                new ArrayOidSet(Arrays.asList(0L, 1L, 2L)), null);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScopeFail);

        // verify that a grpc runtime exception is thrown. In the actual runtime env we expect the
        // JwtServerInterceptor to translate the status code for us, but not applying that for
        // this test.
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.UNKNOWN).anyDescription());
        response = actionOrchestratorServiceClient.acceptAction(acceptActionRequest);
        assertTrue(response.hasError());
    }
}

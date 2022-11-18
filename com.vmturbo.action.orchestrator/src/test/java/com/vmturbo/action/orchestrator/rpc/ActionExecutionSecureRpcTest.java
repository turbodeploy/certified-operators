package com.vmturbo.action.orchestrator.rpc;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status.Code;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.ActionOrchestratorComponent;
import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionPaginator.ActionPaginatorFactory;
import com.vmturbo.action.orchestrator.action.AuditedActionsManager;
import com.vmturbo.action.orchestrator.action.ExecutedActionsChangeWindowDao;
import com.vmturbo.action.orchestrator.action.RejectedActionsDAO;
import com.vmturbo.action.orchestrator.approval.ActionApprovalManager;
import com.vmturbo.action.orchestrator.audit.ActionAuditSender;
import com.vmturbo.action.orchestrator.execution.ActionAutomationManager;
import com.vmturbo.action.orchestrator.execution.ActionCombiner;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.ActionExecutionStore;
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
import com.vmturbo.action.orchestrator.store.EntitySeverityCache;
import com.vmturbo.action.orchestrator.store.IActionFactory;
import com.vmturbo.action.orchestrator.store.IActionStoreFactory;
import com.vmturbo.action.orchestrator.store.IActionStoreLoader;
import com.vmturbo.action.orchestrator.store.InvolvedEntitiesExpander;
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.action.orchestrator.store.atomic.AtomicActionFactory;
import com.vmturbo.action.orchestrator.store.atomic.AtomicActionSpecsCache;
import com.vmturbo.action.orchestrator.store.identity.IdentityServiceImpl;
import com.vmturbo.action.orchestrator.store.pipeline.LiveActionPipelineFactory;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
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
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionExecution;
import com.vmturbo.common.protobuf.action.ActionDTO.MultiActionRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.oid.identity.ArrayOidSet;
import com.vmturbo.topology.processor.api.ActionExecutionListener;

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
 * {@link ActionsRpcService#acceptActions}
 * {@link SecurityConstant}
 */
public class ActionExecutionSecureRpcTest {
    private static final long ACTION_PLAN_ID = 2;
    private static final long TOPOLOGY_CONTEXT_ID = 3;
    private static final long ACTION_ID_1 = 9999;
    private static final long ACTION_ID_2 = 8888;

    // Have the translator pass-through translate all actions.
    private final ActionTranslator actionTranslator = ActionOrchestratorTestUtils.passthroughTranslator();
    final AtomicActionSpecsCache atomicActionSpecsCache = Mockito.spy(new AtomicActionSpecsCache());
    final AtomicActionFactory atomicActionFactory = Mockito.spy(new AtomicActionFactory(atomicActionSpecsCache));
    private final ActionModeCalculator actionModeCalculator = new ActionModeCalculator();
    private final IActionFactory actionFactory = new ActionFactory(actionModeCalculator,
            Collections.emptyList());
    private final AcceptedActionsDAO acceptedActionsStore = Mockito.mock(AcceptedActionsDAO.class);
    private final RejectedActionsDAO rejectedActionsStore = Mockito.mock(RejectedActionsDAO.class);
    private final IActionStoreFactory actionStoreFactory = mock(IActionStoreFactory.class);
    private final IActionStoreLoader actionStoreLoader = mock(IActionStoreLoader.class);
    private final WorkflowStore workflowStore = mock(WorkflowStore.class);
    private final HistoricalActionStatReader statReader = mock(HistoricalActionStatReader.class);
    private final CurrentActionStatReader currentActionStatReader = mock(CurrentActionStatReader.class);
    private final ActionExecutor actionExecutor = mock(ActionExecutor.class);
    private final Executor executorPool = mock(Executor.class);
    private final ActionExecutionStore actionExecutionStore = new ActionExecutionStore();
    private final ActionTopologyStore actionTopologyStore = mock(ActionTopologyStore.class);
    private final ActionCombiner actionCombiner = new ActionCombiner(actionTopologyStore);
    private final ActionTargetSelector actionTargetSelector = mock(ActionTargetSelector.class);
    private final ProbeCapabilityCache probeCapabilityCache = mock(ProbeCapabilityCache.class);
    private final ActionStorehouse actionStorehouse = new ActionStorehouse(actionStoreFactory,
            actionStoreLoader);
    private final ActionExecutionListener actionExecutionListener =
        Mockito.mock(ActionExecutionListener.class);
    private final ActionPaginatorFactory paginatorFactory = mock(ActionPaginatorFactory.class);

    private final LiveActionsStatistician actionsStatistician = mock(LiveActionsStatistician.class);

    private final EntitiesAndSettingsSnapshotFactory entitySettingsCache = mock(EntitiesAndSettingsSnapshotFactory.class);

    private final EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);

    private final ActionAuditSender actionAuditSender = mock(ActionAuditSender.class);
    private final AuditedActionsManager auditedActionsManager = mock(AuditedActionsManager.class);
    private final ActionAutomationManager actionAutomationManager = mock(ActionAutomationManager.class);

    private final Clock clock = new MutableFixedClock(1_000_000);

    private final UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private final LicenseCheckClient licenseCheckClient = mock(LicenseCheckClient.class);
    private final ActionApprovalManager actionApprovalManager = new ActionApprovalManager(
            actionExecutor, actionTargetSelector, entitySettingsCache, actionTranslator,
            workflowStore, acceptedActionsStore, actionExecutionListener, executorPool);

    private final InvolvedEntitiesExpander involvedEntitiesExpander =
        mock(InvolvedEntitiesExpander.class);

    private LiveActionPipelineFactory pipelineFactory;

    private final ActionHistoryDao actionHistoryDao = mock(ActionHistoryDao.class);

    private final ExecutedActionsChangeWindowDao executedActionsChangeWindowDao = mock(ExecutedActionsChangeWindowDao.class);

    private final ActionsRpcService actionsRpcService = new ActionsRpcService(
        clock,
        actionStorehouse,
        actionApprovalManager,
        actionTranslator,
        paginatorFactory,
        statReader,
        currentActionStatReader,
        userSessionContext,
        acceptedActionsStore,
        rejectedActionsStore,
        auditedActionsManager,
        actionAuditSender,
        actionExecutionStore,
        actionCombiner,
        actionAutomationManager,
        actionHistoryDao,
        executedActionsChangeWindowDao,
            500,
        777777L);
    private ActionsServiceBlockingStub actionOrchestratorServiceClient;
    private ActionsServiceBlockingStub actionOrchestratorServiceClientWithInterceptor;
    private final SupplyChainServiceMole supplyChainServiceMole = spy(new SupplyChainServiceMole());
    private final RepositoryServiceMole repositoryServiceMole = spy(new RepositoryServiceMole());
    private IdentityServiceImpl actionIdentityService;
    private final EntitySeverityCache entitySeverityCache = mock(EntitySeverityCache.class);

    private long lastGeneratedActionOid;

    // utility for creating / interacting with a debugging JWT context
    JwtContextUtil jwtContextUtil;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Grpc server for mocking services. The rule handles starting it and cleaning it up.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(
        supplyChainServiceMole,
        repositoryServiceMole);

    private final static ByteArrayOutputStream outputStream;
    private static final PrintStream old_out = System.out;

    static {
        outputStream = new ByteArrayOutputStream();
        // redirect output stream to PrintStream, so we can verify the audit output.
        System.setOut(new PrintStream(outputStream));
    }

    /**
     * Static test initializer.
     */
    @BeforeClass
    public static void staticInit() {
        IdentityGenerator.initPrefix(0);
    }

    private static ActionPlan actionPlan(@Nonnull Collection<ActionDTO.Action> recommendations) {
        return ActionPlan.newBuilder()
            .setId(ACTION_PLAN_ID)
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyId(123)
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID))))
            .addAllAction(recommendations)
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

        // setup gPRC client
        actionOrchestratorServiceClient = ActionsServiceGrpc.newBlockingStub(channel);

        // setup gPRC client with client interceptor
        actionOrchestratorServiceClientWithInterceptor = ActionsServiceGrpc.newBlockingStub(channel)
            .withInterceptors(new JwtClientInterceptor());

        ActionTargetInfo targetInfo = ImmutableActionTargetInfo.builder()
            .supportingLevel(SupportLevel.SUPPORTED)
            .targetId(123)
            .build();
        when(actionTargetSelector.getTargetsForActions(any(), any(), any())).thenAnswer(invocation -> {
            Stream<Action> actions = invocation.getArgumentAt(0, Stream.class);
            return actions.collect(Collectors.toMap(ActionDTO.Action::getId, action -> targetInfo));
        });
        when(actionTargetSelector.getTargetForAction(any(), any(), any())).thenReturn(targetInfo);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());

        actionIdentityService = Mockito.mock(IdentityServiceImpl.class);
        Mockito.when(actionIdentityService.getOidsForObjects(Mockito.any())).thenAnswer(invocation -> {
            final int size = invocation.getArgumentAt(0, List.class).size();
            final List<Long> result = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                lastGeneratedActionOid = IdentityGenerator.next();
                result.add(lastGeneratedActionOid);
            }
            return result;
        });

        // mock action store
        ActionStore actionStoreSpy = Mockito.spy(new LiveActionStore(actionFactory, TOPOLOGY_CONTEXT_ID,
                actionTargetSelector, entitySettingsCache, actionHistoryDao,
                actionTranslator, clock,
                userSessionContext, licenseCheckClient, acceptedActionsStore, rejectedActionsStore,
                actionIdentityService, involvedEntitiesExpander,
                entitySeverityCache, workflowStore));
        when(actionStoreFactory.newStore(anyLong())).thenReturn(actionStoreSpy);
        when(actionStoreLoader.loadActionStores()).thenReturn(Collections.emptyList());
        when(actionStoreFactory.getContextTypeName(anyLong())).thenReturn("foo");
        when(actionTopologyStore.getSourceTopology()).thenReturn(Optional.empty());

        pipelineFactory = new LiveActionPipelineFactory(actionStorehouse, mock(ActionAutomationManager.class),
            atomicActionFactory, entitySettingsCache, 10, probeCapabilityCache,
            actionHistoryDao, actionFactory, clock, 10,
            actionIdentityService, actionTargetSelector, actionTranslator, actionsStatistician,
            actionAuditSender, auditedActionsManager, actionTopologyStore, 777777L, 100);
    }

    @After
    public void tearDown() {
        System.out.println("shutting down gRPC channel and server");
        jwtContextUtil.shutdown();
    }

    /**
     * Test accepting an existing action with client providing JwtCallCredential.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testAcceptAction() throws Exception {
        ActionDTO.Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(
                ACTION_ID_1);
        final ActionPlan plan = actionPlan(Collections.singleton(recommendation));
        final MultiActionRequest acceptActionRequest = MultiActionRequest.newBuilder()
            .addActionIds(ACTION_ID_1)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), anyLong())).thenReturn(snapshot);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());

        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot,recommendation);

        pipelineFactory.actionPipeline(plan).run(plan);
        ActionExecution response = actionOrchestratorServiceClient
            .withCallCredentials(new JwtCallCredential(jwtContextUtil.getToken()
                .getCompactRepresentation()))
            .acceptActions(acceptActionRequest);

        assertEquals(1, response.getActionIdCount());
        assertEquals(lastGeneratedActionOid, response.getActionId(0));
        // verify log message includes admin user.
        // redirect System.out is not working when running in a test suite. Comment out for now.
        // verifyMessage(outputStream.toString(),
        //        ADMIN + " from IP address: " + IP_ADDRESS +" is trying to execute Action: 9999");
    }


    /**
     * Test accepting an existing action with client interceptor.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testAcceptActionWithClientInterceptor() throws Exception {
        Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID_1);
        final ActionPlan plan = actionPlan(Collections.singleton(recommendation));
        final MultiActionRequest acceptActionRequest = MultiActionRequest.newBuilder()
                .addActionIds(ACTION_ID_1)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), anyLong())).thenReturn(snapshot);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());

        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot,recommendation);

        pipelineFactory.actionPipeline(plan).run(plan);
        ActionExecution response = actionOrchestratorServiceClientWithInterceptor
            .acceptActions(acceptActionRequest);

        assertEquals(1, response.getActionIdCount());
        assertEquals(lastGeneratedActionOid, response.getActionId(0));
        // verify log message include admin user
        // redirect System.out is not working when running in a test suite. Comment out for now.
        // verifyMessage(outputStream.toString(),
        //        ADMIN + " from IP address: " + IP_ADDRESS +" is trying to execute Action: 9999");
    }

    /**
     * Test accepting an existing action with invalid JWT token.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testAcceptActionWithInvalidJwtToken() throws Exception {
        Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID_1);
        final ActionPlan plan = actionPlan(Collections.singleton(recommendation));
        final MultiActionRequest acceptActionRequest = MultiActionRequest.newBuilder()
            .addActionIds(ACTION_ID_1)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), anyLong())).thenReturn(snapshot);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());

        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot,recommendation);

        pipelineFactory.actionPipeline(plan).run(plan);
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.UNAUTHENTICATED)
            .descriptionContains("JWT strings must contain exactly 2 period characters"));
        actionOrchestratorServiceClient
            .withCallCredentials(new JwtCallCredential("wrong token"))
            .acceptActions(acceptActionRequest);
    }

    /**
     * Test accepting an existing action without JWT token.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testAcceptActionWithoutJwtToken() throws Exception {
        Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID_1);
        final ActionPlan plan = actionPlan(Collections.singleton(recommendation));
        final MultiActionRequest acceptActionRequest = MultiActionRequest.newBuilder()
                .addActionIds(ACTION_ID_1)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), anyLong())).thenReturn(snapshot);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());

        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot,recommendation);

        pipelineFactory.actionPipeline(plan).run(plan);
        ActionExecution response = actionOrchestratorServiceClient
            .acceptActions(acceptActionRequest); // don't pass JWT token

        assertEquals(1, response.getActionIdCount());
        assertEquals(lastGeneratedActionOid, response.getActionId(0));
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
        Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID_1);
        final ActionPlan plan = actionPlan(Collections.singleton(recommendation));
        final MultiActionRequest acceptActionRequest = MultiActionRequest.newBuilder()
                .addActionIds(ACTION_ID_1)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), anyLong())).thenReturn(snapshot);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());

        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot,recommendation);

        pipelineFactory.actionPipeline(plan).run(plan);
        ActionExecution response = actionOrchestratorServiceTestClient
            .withCallCredentials(new JwtCallCredential(jwtContextUtil.getToken()
                .getCompactRepresentation()))
            .acceptActions(acceptActionRequest);

        assertEquals(1, response.getActionIdCount());
        assertEquals(lastGeneratedActionOid, response.getActionId(0));

        // clean up
        managedChannel.shutdown();
        secureGrpcServerWithNullPublicKeyInInterceptor.shutdown();
    }

    /**
     * Tests accept action with a scoped user.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testAcceptActionWithScopedUser() throws Exception {
        final Action recommendation1 =
                ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID_1, 10L, 0, 1, 1, 1);
        final Action recommendation2 =
                ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID_2, 10L, 0, 1, 1, 1);
        final ActionPlan plan = actionPlan(Arrays.asList(recommendation1, recommendation2));
        when(entitySettingsCache.newSnapshot(any(), anyLong())).thenReturn(
                snapshot);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot, recommendation1);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot, recommendation2);
        pipelineFactory.actionPipeline(plan).run(plan);

        when(userSessionContext.isUserScoped()).thenReturn(true);

        final MultiActionRequest acceptActionRequest1 = MultiActionRequest.newBuilder()
                .addActionIds(ACTION_ID_1)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();
        final EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), anyLong())).thenReturn(
                snapshot);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());
        // a user WITH access CAN execute the action
        EntityAccessScope accessScopeOK =
                new EntityAccessScope(null, null, new ArrayOidSet(Arrays.asList(10L, 0L, 1L)),
                        null);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScopeOK);

        actionOrchestratorServiceClient.acceptActions(acceptActionRequest1);

        final MultiActionRequest acceptActionRequest2 = MultiActionRequest.newBuilder()
                .addActionIds(ACTION_ID_2)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();
        // A user WITHOUT access to at least one entity related to this action CANNOT execute it.
        final EntityAccessScope accessScopeFail =
                new EntityAccessScope(null, null, new ArrayOidSet(Arrays.asList(11L, 12L)), null);
        when(userSessionContext.getUserAccessScope()).thenReturn(accessScopeFail);

        // verify that a grpc runtime exception is thrown. In the actual runtime env we expect the
        // JwtServerInterceptor to translate the status code for us, but not applying that for
        // this test.
        expectedException.expect(
                GrpcRuntimeExceptionMatcher.hasCode(Code.UNKNOWN).anyDescription());
        actionOrchestratorServiceClient.acceptActions(acceptActionRequest2);
    }
}

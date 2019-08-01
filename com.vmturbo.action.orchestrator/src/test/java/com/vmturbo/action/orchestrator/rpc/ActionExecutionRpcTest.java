package com.vmturbo.action.orchestrator.rpc;

import static com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils.passthroughTranslator;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.AcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionPaginator.ActionPaginatorFactory;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor;
import com.vmturbo.action.orchestrator.execution.ExecutionStartException;
import com.vmturbo.action.orchestrator.execution.ImmutableActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache;
import com.vmturbo.action.orchestrator.state.machine.Transition.TransitionResult;
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
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.translation.ActionTranslator.TranslationExecutor;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.ActionPlanType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;

/**
 * Tests for action execution RPCs.
 */
public class ActionExecutionRpcTest {

    private static final Optional<WorkflowDTO.Workflow> EMPTY_WORKFLOW_OPTIONAL = Optional.empty();

    private ActionsServiceBlockingStub actionOrchestratorServiceClient;

    // Have the translator pass-through translate all actions.
    private final ActionTranslator actionTranslator = passthroughTranslator();

    private ActionModeCalculator actionModeCalculator = new ActionModeCalculator(actionTranslator);
    private final IActionFactory actionFactory = new ActionFactory(actionModeCalculator);
    private final IActionStoreFactory actionStoreFactory = mock(IActionStoreFactory.class);
    private final IActionStoreLoader actionStoreLoader = mock(IActionStoreLoader.class);
    private final AutomatedActionExecutor executor = mock(AutomatedActionExecutor.class);
    private final ActionHistoryDao actionHistoryDao = mock(ActionHistoryDao.class);

    private final ActionExecutor actionExecutor = mock(ActionExecutor.class);
    private final ProbeCapabilityCache probeCapabilityCache = mock(ProbeCapabilityCache.class);
    private final ActionTargetSelector actionTargetSelector = mock(ActionTargetSelector.class);
    private final ActionStorehouse actionStorehouse = new ActionStorehouse(actionStoreFactory,
            executor, actionStoreLoader, actionModeCalculator);
    private final ActionPaginatorFactory paginatorFactory = mock(ActionPaginatorFactory.class);

    private final WorkflowStore workflowStore = mock(WorkflowStore.class);

    private final HistoricalActionStatReader statReader = mock(HistoricalActionStatReader.class);

    private final CurrentActionStatReader liveStatReader = mock(CurrentActionStatReader.class);

    private final EntitiesAndSettingsSnapshotFactory entitySettingsCache = mock(EntitiesAndSettingsSnapshotFactory.class);

    private final EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);

    private final LiveActionsStatistician statistician = mock(LiveActionsStatistician.class);

    private final UserSessionContext userSessionContext = mock(UserSessionContext.class);

    private static final long ACTION_PLAN_ID = 2;
    private static final long TOPOLOGY_CONTEXT_ID = 3;
    private static final long ACTION_ID = 9999;

    private final Clock clock = new MutableFixedClock(1_000_000);

    private final ActionsRpcService actionsRpcService =
        new ActionsRpcService(clock,
            actionStorehouse,
            actionExecutor,
            actionTargetSelector,
            actionTranslator,
            paginatorFactory,
            workflowStore,
            statReader,
            liveStatReader,
            userSessionContext);

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(actionsRpcService);

    private ActionStore actionStoreSpy;

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);

        ActionTargetInfo targetInfo = ImmutableActionTargetInfo.builder()
            .supportingLevel(SupportLevel.SUPPORTED)
            .targetId(123)
            .build();
        when(actionTargetSelector.getTargetsForActions(any(), any())).thenAnswer(invocation -> {
            Stream<ActionDTO.Action> actions = invocation.getArgumentAt(0, Stream.class);
            return actions.collect(Collectors.toMap(ActionDTO.Action::getId, action -> targetInfo));
        });
        when(actionTargetSelector.getTargetForAction(any())).thenReturn(targetInfo);

        actionStoreSpy =
            Mockito.spy(new LiveActionStore(actionFactory, TOPOLOGY_CONTEXT_ID,
                actionTargetSelector, probeCapabilityCache,
                entitySettingsCache, actionHistoryDao, statistician, actionTranslator,
                clock, userSessionContext));

        actionOrchestratorServiceClient = ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel());
        when(actionStoreFactory.newStore(anyLong())).thenReturn(actionStoreSpy);
        when(actionStoreLoader.loadActionStores()).thenReturn(Collections.emptyList());
        when(actionStoreFactory.getContextTypeName(anyLong())).thenReturn("foo");
    }

    /**
     * Test accepting an existing action.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testAcceptAction() throws Exception {
        ActionDTO.Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), anyLong(), anyLong())).thenReturn(snapshot);

        when(entitySettingsCache.newSnapshot(any(), anyLong(), anyLong())).thenReturn(snapshot);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot,recommendation);

        actionStorehouse.storeActions(plan);
        AcceptActionResponse response =  actionOrchestratorServiceClient.acceptAction(acceptActionRequest);

        assertFalse(response.hasError());
        assertTrue(response.hasActionSpec());
        assertEquals(ACTION_ID, response.getActionSpec().getRecommendation().getId());
        assertEquals(ActionState.IN_PROGRESS, response.getActionSpec().getActionState());
    }

    /**
     * Test accepting an action that's not found.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testAcceptActionNotFound() throws Exception {
        ActionDTO.Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), anyLong(), anyLong())).thenReturn(snapshot);

        when(entitySettingsCache.newSnapshot(any(), anyLong(), anyLong())).thenReturn(snapshot);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot,recommendation);

        actionStorehouse.storeActions(plan);
        actionStorehouse.getStore(TOPOLOGY_CONTEXT_ID).get().overwriteActions(ImmutableMap.of(
                ActionPlanType.MARKET, Collections.emptyList())); // Clear the action from the store
        AcceptActionResponse response = actionOrchestratorServiceClient.acceptAction(acceptActionRequest);

        assertTrue(response.hasError());
        assertEquals("Action " + ACTION_ID + " doesn't exist.", response.getError());
        assertFalse(response.hasActionSpec());
    }

    @Test
    public void testAcceptTopologyContextNotFound() throws Exception {
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();

        AcceptActionResponse response = actionOrchestratorServiceClient.acceptAction(acceptActionRequest);

        assertTrue(response.hasError());
        assertEquals("Unknown topology context: " + TOPOLOGY_CONTEXT_ID, response.getError());
        assertFalse(response.hasActionSpec());
    }

    @Test
    public void testAcceptActionUpdatesSeverityCache() throws Exception {
        ActionDTO.Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), anyLong(), anyLong())).thenReturn(snapshot);

        when(entitySettingsCache.newSnapshot(any(), anyLong(), anyLong())).thenReturn(snapshot);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot,recommendation);

        EntitySeverityCache severityCacheMock = mock(EntitySeverityCache.class);
        doReturn(severityCacheMock).when(actionStoreSpy).getEntitySeverityCache();

        actionStorehouse.storeActions(plan);
        actionOrchestratorServiceClient.acceptAction(acceptActionRequest);

        Mockito.verify(severityCacheMock).refresh(
            eq(plan.getAction(0)),
            eq(actionStorehouse.getStore(TOPOLOGY_CONTEXT_ID).get())
        );
    }

    @Test
    public void testAcceptNotAuthorized() throws Exception {
        ActionDTO.Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), anyLong(), anyLong())).thenReturn(snapshot);

        when(entitySettingsCache.newSnapshot(any(), anyLong(), anyLong())).thenReturn(snapshot);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot,recommendation);

        actionStorehouse.storeActions(plan);

        // Replace the action in the store with a spy that can be mocked
        ActionStore actionStore = actionStorehouse.getStore(TOPOLOGY_CONTEXT_ID).get();
        Action actionSpy = Mockito.spy(actionStore.getAction(ACTION_ID).get());
        Mockito.doReturn(new TransitionResult<>(ActionState.READY, ActionState.READY, false))
            .when(actionSpy).receive(Mockito.any(AcceptanceEvent.class));
        actionStore.overwriteActions(ImmutableMap.of(ActionPlanType.MARKET, Collections.singletonList(actionSpy)));

        AcceptActionResponse response =  actionOrchestratorServiceClient.acceptAction(acceptActionRequest);

        assertTrue(response.hasError());
        assertFalse(response.hasActionSpec());
        assertEquals("Unauthorized to accept action in mode " + actionSpy.getMode(), response.getError());
    }

    @Test
    public void testTargetResolutionError() throws Exception {
        final ActionDTO.Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionContext = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), anyLong(), anyLong())).thenReturn(snapshot);

        when(entitySettingsCache.newSnapshot(any(), anyLong(), anyLong())).thenReturn(snapshot);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot,recommendation);

        actionStorehouse.storeActions(plan);
        when(actionTargetSelector.getTargetForAction(eq(recommendation)))
            .thenReturn(ImmutableActionTargetInfo.builder()
                .supportingLevel(SupportLevel.UNSUPPORTED)
                .build());

        AcceptActionResponse response =  actionOrchestratorServiceClient.acceptAction(acceptActionContext);

        assertTrue(response.hasError());
        Assert.assertThat(response.getError(), CoreMatchers.containsString("Action cannot be executed by any target"));
    }

    @Test
    public void testExecutionError() throws Exception {
        final long targetId = 7777;
        final ActionDTO.Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionContext = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), anyLong(), anyLong())).thenReturn(snapshot);

        when(entitySettingsCache.newSnapshot(any(), anyLong(), anyLong())).thenReturn(snapshot);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot,recommendation);

        actionStorehouse.storeActions(plan);

        when(actionTargetSelector.getTargetForAction(Mockito.eq(recommendation))).thenReturn(
            ImmutableActionTargetInfo.builder()
                .supportingLevel(SupportLevel.SUPPORTED)
                .targetId(targetId)
                .build());
        doThrow(new ExecutionStartException("ERROR!"))
            .when(actionExecutor).execute(eq(targetId), eq(recommendation), eq(EMPTY_WORKFLOW_OPTIONAL));

        AcceptActionResponse response =  actionOrchestratorServiceClient.acceptAction(acceptActionContext);

        assertThat(response.getError(), CoreMatchers.containsString("ERROR!"));
    }

    @Test
    public void testExecutionUsesTranslation() throws Exception {
        final long targetId = 7777;
        final ActionDTO.Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionDTO.Action translationResult = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID + 1);
        assertNotEquals(recommendation, translationResult);

        when(entitySettingsCache.newSnapshot(any(), anyLong(), anyLong())).thenReturn(snapshot);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot,recommendation);

        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionContext = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();

        actionStorehouse.storeActions(plan);
        when(actionTargetSelector.getTargetForAction(Mockito.eq(recommendation))).thenReturn(
            ImmutableActionTargetInfo.builder()
                .supportingLevel(SupportLevel.SUPPORTED)
                .targetId(targetId)
                .build());
        // Have the action translator act as a passthrough.
        doAnswer(invocation -> {
            final Action action = (Action)invocation.getArguments()[0];
            action.getActionTranslation().setTranslationSuccess(translationResult);
            return true;
        }).when(actionTranslator).translate(any(Action.class));

        actionOrchestratorServiceClient.acceptAction(acceptActionContext);
        Mockito.verify(actionExecutor).execute(eq(targetId), eq(translationResult),
                eq(EMPTY_WORKFLOW_OPTIONAL));
    }

    @Test
    public void testTranslationError() throws Exception {
        final long targetId = 7777;
        final ActionDTO.Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionContext = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();

        when(entitySettingsCache.newSnapshot(any(), anyLong(), anyLong())).thenReturn(snapshot);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot,recommendation);

        // We use a new actionTranslator for this test. This spy translator does not do the
        // translation successfully.
        // Since wthis test uses a new actionTranslator, all the dependent objects are
        // created again - actionModeCalculator, actionStoreHouse, actionsRpcService, grpcServer,
        // actionOrchestratorServiceClient, LiveActionStore
        final ActionTranslator actionTranslator = Mockito.spy(new ActionTranslator(new TranslationExecutor() {
            @Override
            public <T extends ActionView> Stream<T> translate(@Nonnull final Stream<T> actionStream) {
                return actionStream;
            }
        }));
        ActionModeCalculator actionModeCalculator = new ActionModeCalculator(actionTranslator);
        final ActionStorehouse actionStorehouse = new ActionStorehouse(actionStoreFactory,
                executor, actionStoreLoader, actionModeCalculator);
        final ActionsRpcService actionsRpcService =
                new ActionsRpcService(clock,
                    actionStorehouse,
                    actionExecutor,
                    actionTargetSelector,
                    actionTranslator,
                    paginatorFactory,
                    workflowStore,
                    statReader,
                    liveStatReader,
                    userSessionContext);
        GrpcTestServer grpcServer = GrpcTestServer.newServer(actionsRpcService);
        grpcServer.start();
        ActionsServiceBlockingStub actionOrchestratorServiceClient = ActionsServiceGrpc.newBlockingStub(
                grpcServer.getChannel());
        IActionFactory actionFactory = new ActionFactory(actionModeCalculator);
        actionStoreSpy =
            Mockito.spy(new LiveActionStore(actionFactory, TOPOLOGY_CONTEXT_ID,
                actionTargetSelector, probeCapabilityCache, entitySettingsCache,
                actionHistoryDao, statistician, actionTranslator, clock, userSessionContext));
        when(actionStoreFactory.newStore(anyLong())).thenReturn(actionStoreSpy);

        actionStorehouse.storeActions(plan);
        when(actionTargetSelector.getTargetForAction(Mockito.eq(recommendation))).thenReturn(
            ImmutableActionTargetInfo.builder()
                .supportingLevel(SupportLevel.SUPPORTED)
                .targetId(targetId)
                .build());
        doReturn(false).when(actionTranslator).translate(any(Action.class));
        AcceptActionResponse response =  actionOrchestratorServiceClient.acceptAction(acceptActionContext);
        assertThat(response.getError(), CoreMatchers.containsString("Unauthorized to accept action in mode RECOMMEND"));
        grpcServer.close();
    }

    private static ActionPlan actionPlan(ActionDTO.Action recommendation) {
        return ActionPlan.newBuilder()
            .setId(ACTION_PLAN_ID)
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID))))
            .addAction(recommendation)
            .build();
    }
}

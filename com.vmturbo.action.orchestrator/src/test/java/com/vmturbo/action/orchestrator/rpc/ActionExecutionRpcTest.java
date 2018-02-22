package com.vmturbo.action.orchestrator.rpc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.AcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.ActionTranslator;
import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor;
import com.vmturbo.action.orchestrator.execution.ExecutionStartException;
import com.vmturbo.action.orchestrator.execution.TargetResolutionException;
import com.vmturbo.action.orchestrator.state.machine.Transition.TransitionResult;
import com.vmturbo.action.orchestrator.store.ActionFactory;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.ActionSupportResolver;
import com.vmturbo.action.orchestrator.store.EntitySettingsCache;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache;
import com.vmturbo.action.orchestrator.store.EntityTypeMap;
import com.vmturbo.action.orchestrator.store.IActionFactory;
import com.vmturbo.action.orchestrator.store.IActionStoreFactory;
import com.vmturbo.action.orchestrator.store.IActionStoreLoader;
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests for action execution RPCs.
 */
public class ActionExecutionRpcTest {
    private ActionsServiceBlockingStub actionOrchestratorServiceClient;

    private final IActionFactory actionFactory = new ActionFactory();
    private final IActionStoreFactory actionStoreFactory = mock(IActionStoreFactory.class);
    private final IActionStoreLoader actionStoreLoader = mock(IActionStoreLoader.class);
    private final AutomatedActionExecutor executor = mock(AutomatedActionExecutor.class);
    private final ActionHistoryDao actionHistoryDao = mock(ActionHistoryDao.class);

    private final ActionStorehouse actionStorehouse = new ActionStorehouse(actionStoreFactory,
            executor, actionStoreLoader);

    private final ActionExecutor actionExecutor = mock(ActionExecutor.class);
    // Have the translator pass-through translate all actions.
    private final ActionTranslator actionTranslator = Mockito.spy(new ActionTranslator(actionStream ->
        actionStream.map(action -> {
            action.getActionTranslation().setPassthroughTranslationSuccess();
            return action;
        })));

    private final ActionSupportResolver filter = mock(ActionSupportResolver.class);

    private final EntitySettingsCache entitySettingsCache = mock(EntitySettingsCache.class);
    private final EntityTypeMap entityTypeMap = mock(EntityTypeMap.class);

    private static final long ACTION_PLAN_ID = 2;
    private static final long TOPOLOGY_CONTEXT_ID = 3;
    private static final long ACTION_ID = 9999;

    private final ActionsRpcService actionsRpcService =
            new ActionsRpcService(actionStorehouse, actionExecutor, actionTranslator);

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(actionsRpcService);

    private ActionStore actionStoreSpy;

    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);

        when(entityTypeMap.getTypeForEntity(anyLong())).thenReturn(Optional.of(EntityType.PHYSICAL_MACHINE));
        actionStoreSpy =
                Mockito.spy(new LiveActionStore(actionFactory, TOPOLOGY_CONTEXT_ID,
                        filter, entitySettingsCache, entityTypeMap, actionHistoryDao));

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
        final ActionPlan plan = actionPlan(ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID));
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();

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
        final ActionPlan plan = actionPlan(ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID));
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();

        actionStorehouse.storeActions(plan);
        actionStorehouse.getStore(TOPOLOGY_CONTEXT_ID).get().overwriteActions(Collections.emptyList()); // Clear the action from the store
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
        final ActionPlan plan = actionPlan(ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID));
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();

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
        final ActionPlan plan = actionPlan(ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID));
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();

        actionStorehouse.storeActions(plan);

        // Replace the action in the store with a spy that can be mocked
        ActionStore actionStore = actionStorehouse.getStore(TOPOLOGY_CONTEXT_ID).get();
        Action actionSpy = Mockito.spy(actionStore.getAction(ACTION_ID).get());
        Mockito.doReturn(new TransitionResult<>(ActionState.READY, ActionState.READY, false))
            .when(actionSpy).receive(Mockito.any(AcceptanceEvent.class));
        actionStore.overwriteActions(Collections.singletonList(actionSpy));

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

        actionStorehouse.storeActions(plan);
        when(actionExecutor.getTargetId(eq(recommendation)))
            .thenThrow(new TargetResolutionException("Could not resolve target"));

        AcceptActionResponse response =  actionOrchestratorServiceClient.acceptAction(acceptActionContext);

        assertTrue(response.hasError());
        Assert.assertThat(response.getError(), CoreMatchers.containsString("Could not resolve target"));
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

        actionStorehouse.storeActions(plan);

        when(actionExecutor.getTargetId(Mockito.eq(recommendation))).thenReturn(targetId);
        doThrow(new ExecutionStartException("ERROR!"))
            .when(actionExecutor).execute(eq(targetId), eq(recommendation));

        AcceptActionResponse response =  actionOrchestratorServiceClient.acceptAction(acceptActionContext);

        assertThat(response.getError(), CoreMatchers.containsString("ERROR!"));
    }

    @Test
    public void testExecutionUsesTranslation() throws Exception {
        final long targetId = 7777;
        final ActionDTO.Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionDTO.Action translationResult = ActionOrchestratorTestUtils.createResizeRecommendation(ACTION_ID + 1, CommodityType.VMEM);
        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionContext = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();

        actionStorehouse.storeActions(plan);
        when(actionExecutor.getTargetId(Mockito.eq(recommendation))).thenReturn(targetId);
        // Have the action translator act as a passthrough.
        doAnswer(invocation -> {
            final Action action = (Action)invocation.getArguments()[0];
            action.getActionTranslation().setTranslationSuccess(translationResult);
            return true;
        }).when(actionTranslator).translate(any(Action.class));

        actionOrchestratorServiceClient.acceptAction(acceptActionContext);
        Mockito.verify(actionExecutor).execute(eq(targetId), eq(translationResult));
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

        actionStorehouse.storeActions(plan);
        when(actionExecutor.getTargetId(Mockito.eq(recommendation))).thenReturn(targetId);
        doReturn(false).when(actionTranslator).translate(any(Action.class));

        AcceptActionResponse response =  actionOrchestratorServiceClient.acceptAction(acceptActionContext);

        assertThat(response.getError(), CoreMatchers.containsString("Failed to translate action"));
    }

    private static ActionPlan actionPlan(ActionDTO.Action recommendation) {
        return ActionPlan.newBuilder()
            .setId(ACTION_PLAN_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .addAction(recommendation)
            .build();
    }
}
package com.vmturbo.action.orchestrator.rpc;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.junit.After;
import org.junit.Assert;
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
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.ActionTranslator;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.action.orchestrator.store.PlanActionStore;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsResponse.TypeCount;
import com.vmturbo.common.protobuf.action.ActionDTO.MultiActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.TopologyContextInfoRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.TopologyContextResponse;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

public class ActionQueryRpcTest {

    private GrpcTestServer grpcServer;
    private ActionsServiceBlockingStub actionOrchestratorServiceClient;

    private final ActionStorehouse actionStorehouse = Mockito.mock(ActionStorehouse.class);
    private final ActionStore actionStore = Mockito.mock(ActionStore.class);
    private final ActionTranslator actionTranslator = new ActionTranslator(actionStream ->
        actionStream.map(action -> {
            action.getActionTranslation().setPassthroughTranslationSuccess();
            return action;
        }));

    private final long actionPlanId = 2;
    private final long topologyContextId = 3;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);

        ActionsRpcService actionsRpcService =
            new ActionsRpcService(actionStorehouse, Mockito.mock(ActionExecutor.class), actionTranslator);

        grpcServer = GrpcTestServer.withServices(actionsRpcService);
        actionOrchestratorServiceClient = ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel());

        when(actionStorehouse.getStore(topologyContextId)).thenReturn(Optional.of(actionStore));
    }

    @After
    public void teardown() {
        grpcServer.close();
    }

    /**
     * Test a simple GET for an individual action.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetAction() throws Exception {
        final ActionView actionView = ActionOrchestratorTestUtils.createMoveAction(1, actionPlanId);
        when(actionStore.getActionView(1)).thenReturn(Optional.of(actionView));

        SingleActionRequest actionRequest = SingleActionRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .setActionId(1)
            .build();

        ActionOrchestratorAction action = actionOrchestratorServiceClient.getAction(actionRequest);
        assertEquals(1, action.getActionId());
        assertTrue(action.hasActionSpec());
        assertEquals(spec(actionView), action.getActionSpec());
    }

    @Test
    public void testGetActionMissingArgument() throws Exception {
        SingleActionRequest actionRequest = SingleActionRequest.newBuilder()
            .build();

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
            .descriptionContains("Missing required parameter actionId or topologyContextId."));

        actionOrchestratorServiceClient.getAction(actionRequest);
    }

    @Test
    public void testGetActionContextNotFound() throws Exception {
        when(actionStorehouse.getStore(1234)).thenReturn(Optional.empty());
        SingleActionRequest actionRequest = SingleActionRequest.newBuilder()
            .setTopologyContextId(1234)
            .setActionId(1)
            .build();

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.NOT_FOUND)
            .descriptionContains("1234 not found"));
        actionOrchestratorServiceClient.getAction(actionRequest);
    }

    /**
     * Test a simple GET for a missing action.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetActionNotFound() throws Exception {
        when(actionStore.getActionView(1)).thenReturn(Optional.empty());

        SingleActionRequest actionRequest = SingleActionRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .setActionId(1)
            .build();

        ActionOrchestratorAction action = actionOrchestratorServiceClient.getAction(actionRequest);
        assertEquals(1, action.getActionId());
        assertFalse(action.hasActionSpec());
    }

    @Test
    public void testGetAllActions() throws Exception {
        final ActionView visibleAction = ActionOrchestratorTestUtils.createMoveAction(1, actionPlanId);
        final ActionView disabledAction = spy(ActionOrchestratorTestUtils.createMoveAction(2, actionPlanId));
        when(disabledAction.getMode()).thenReturn(ActionMode.DISABLED);
        final Map<Long, ActionView> actionViews = ImmutableMap.of(
            visibleAction.getId(), visibleAction,
            disabledAction.getId(), disabledAction);
        when(actionStore.getActionViews()).thenReturn(actionViews);
        when(actionStore.getVisibilityPredicate()).thenReturn(PlanActionStore.VISIBILITY_PREDICATE);

        FilteredActionRequest actionRequest = FilteredActionRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .build();

        List<ActionSpec> actionSpecs = fetchSpecList(() -> actionOrchestratorServiceClient.getAllActions(actionRequest));
        assertThat(actionSpecs, containsInAnyOrder(spec(visibleAction), spec(disabledAction)));
    }

    @Test
    public void testGetFilteredActionsForRealTime() throws Exception {
        final ActionView visibleAction = ActionOrchestratorTestUtils.createMoveAction(1, actionPlanId);
        final ActionView disabledAction = spy(ActionOrchestratorTestUtils.createMoveAction(2, actionPlanId));
        when(disabledAction.getMode()).thenReturn(ActionMode.DISABLED);
        final Map<Long, ActionView> actionViews = ImmutableMap.of(
            visibleAction.getId(), visibleAction,
            disabledAction.getId(), disabledAction);
        when(actionStore.getActionViews()).thenReturn(actionViews);
        when(actionStore.getVisibilityPredicate()).thenReturn(LiveActionStore.VISIBILITY_PREDICATE);

        final FilteredActionRequest actionRequest = FilteredActionRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .setFilter(ActionQueryFilter.newBuilder().setVisible(true))
            .build();

        List<ActionSpec> actionSpecs = fetchSpecList(() -> actionOrchestratorServiceClient.getAllActions(actionRequest));

        assertThat(actionSpecs, contains(spec(visibleAction)));
        assertThat(actionSpecs, not(contains(spec(disabledAction))));
    }

    // All plan actions should be visible.
    @Test
    public void testGetFilteredActionsForPlan() throws Exception {
        final ActionView visibleAction = ActionOrchestratorTestUtils.createMoveAction(1, actionPlanId);
        final ActionView disabledAction = spy(ActionOrchestratorTestUtils.createMoveAction(2, actionPlanId));
        when(disabledAction.getMode()).thenReturn(ActionMode.DISABLED);
        final Map<Long, ActionView> actionViews = ImmutableMap.of(
            visibleAction.getId(), visibleAction,
            disabledAction.getId(), disabledAction);
        when(actionStore.getActionViews()).thenReturn(actionViews);
        when(actionStore.getVisibilityPredicate()).thenReturn(PlanActionStore.VISIBILITY_PREDICATE);

        FilteredActionRequest actionRequest = FilteredActionRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .setFilter(ActionQueryFilter.newBuilder().setVisible(true))
            .build();

        List<ActionSpec> actionSpecs = fetchSpecList(() -> actionOrchestratorServiceClient.getAllActions(actionRequest));

        assertThat(actionSpecs, containsInAnyOrder(spec(visibleAction), spec(disabledAction)));
    }

    @Test
    public void testGetAllActionsMissingArgument() throws Exception {
        FilteredActionRequest actionRequest = FilteredActionRequest.newBuilder()
            .build();

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
            .descriptionContains("topologyContextId"));

        // Because getAllActions returns a lazily-evaluated stream, we have to force the evaluation somehow
        // to get the desired exception.
        fetchSpecList(() -> actionOrchestratorServiceClient.getAllActions(actionRequest));
    }

    @Test
    public void testGetAllActionsContextNotFound() throws Exception {
        when(actionStorehouse.getStore(1234)).thenReturn(Optional.empty());
        FilteredActionRequest actionRequest = FilteredActionRequest.newBuilder()
            .setTopologyContextId(1234)
            .build();

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.NOT_FOUND)
            .descriptionContains("1234 not found"));

        // Because getAllActions returns a lazily-evaluated stream, we have to force the evaluation somehow
        // to get the desired exception.
        fetchSpecList(() -> actionOrchestratorServiceClient.getAllActions(actionRequest));
    }

    @Test
    public void testGetMultiAction() throws Exception {
        final ActionView visibleAction = ActionOrchestratorTestUtils.createMoveAction(1, actionPlanId);
        final ActionView disabledAction = spy(ActionOrchestratorTestUtils.createMoveAction(2, actionPlanId));
        final ActionView notRetrievedAction = ActionOrchestratorTestUtils.createMoveAction(3, actionPlanId);
        when(disabledAction.getMode()).thenReturn(ActionMode.DISABLED);
        final Map<Long, ActionView> actionViews = ImmutableMap.of(
            visibleAction.getId(), visibleAction,
            disabledAction.getId(), disabledAction,
            notRetrievedAction.getId(), notRetrievedAction);
        when(actionStore.getActionViews()).thenReturn(actionViews);

        MultiActionRequest actionRequest = MultiActionRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .addActionIds(1)
            .addActionIds(2)
            .build();

        List<ActionSpec> actionSpecs = fetchSpecList(() -> actionOrchestratorServiceClient.getActions(actionRequest));

        assertThat(actionSpecs, containsInAnyOrder(spec(visibleAction), spec(disabledAction)));
        assertThat(actionSpecs, not(contains(spec(notRetrievedAction))));
    }

    /**
     * Test a GET for multiple actions, some of which are missing.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetMultiActionSomeMissing() throws Exception {
        final ActionView visibleAction = ActionOrchestratorTestUtils.createMoveAction(1, actionPlanId);
        final ActionView disabledAction = spy(ActionOrchestratorTestUtils.createMoveAction(2, actionPlanId));
        when(disabledAction.getMode()).thenReturn(ActionMode.DISABLED);
        final Map<Long, ActionView> actionViews = ImmutableMap.of(
            visibleAction.getId(), visibleAction,
            disabledAction.getId(), disabledAction);
        when(actionStore.getActionViews()).thenReturn(actionViews);

        MultiActionRequest actionRequest = MultiActionRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .addActionIds(1)
            .addActionIds(2)
            .addActionIds(3)
            .build();

        Iterable<ActionOrchestratorAction> actionsIter = () -> actionOrchestratorServiceClient.getActions(actionRequest);
        Map<Long, Optional<ActionSpec>> actionSpecs = StreamSupport.stream(actionsIter.spliterator(), false)
            .collect(Collectors.toMap(
                ActionOrchestratorAction::getActionId,
                action -> action.hasActionSpec() ? Optional.of(action.getActionSpec()) : Optional.empty()
            ));

        assertEquals(Optional.of(spec(visibleAction)), actionSpecs.get(1L));
        assertEquals(Optional.of(spec(disabledAction)), actionSpecs.get(2L));
        assertEquals(Optional.empty(), actionSpecs.get(3L));
    }

    @Test
    public void testGetMultiActionMissingParameter() throws Exception {
        MultiActionRequest actionRequest = MultiActionRequest.newBuilder()
            .build();

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
            .descriptionContains("topologyContextId"));

        fetchSpecList(() -> actionOrchestratorServiceClient.getActions(actionRequest));
    }

    @Test
    public void testGetMultiActionUnknownTopologyContext() throws Exception {
        when(actionStorehouse.getStore(topologyContextId)).thenReturn(Optional.empty());

        MultiActionRequest actionRequest = MultiActionRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .addActionIds(1)
            .build();


        List<ActionSpec> actionSpecs = fetchSpecList(() -> actionOrchestratorServiceClient.getActions(actionRequest));
        assertTrue(actionSpecs.isEmpty());
    }

    @Test
    public void testGetAllActionContextsEmpty() throws Exception {
        when(actionStorehouse.getAllStores()).thenReturn(Collections.emptyMap());

        Iterable<TopologyContextResponse> responses = () -> actionOrchestratorServiceClient
            .getTopologyContextInfo(TopologyContextInfoRequest.getDefaultInstance());
        assertEquals(0, StreamSupport.stream(responses.spliterator(), false).count());
    }

    @Test
    public void testGetAllActionContextsSome() throws Exception {
        ActionStore store1 = Mockito.mock(ActionStore.class);
        when(store1.size()).thenReturn(123);
        ActionStore store2 = Mockito.mock(ActionStore.class);
        when(store2.size()).thenReturn(456);
        when(actionStorehouse.getAllStores()).thenReturn(ImmutableMap.of(
            123L, store1,
            456L, store2
        ));

        Iterable<TopologyContextResponse> responses = () -> actionOrchestratorServiceClient
            .getTopologyContextInfo(TopologyContextInfoRequest.getDefaultInstance());
        List<TopologyContextResponse> contexts = StreamSupport.stream(responses.spliterator(), false)
            .collect(Collectors.toList());

        assertEquals(2, contexts.size());
        assertThat(
            contexts.stream()
                .map(TopologyContextResponse::getTopologyContextId)
                .collect(Collectors.toList()),
            containsInAnyOrder(123L, 456L)
        );
        assertThat(
            contexts.stream()
                .map(TopologyContextResponse::getActionCount)
                .collect(Collectors.toList()),
            containsInAnyOrder(123, 456)
        );
    }

    @Test
    public void testGetAllActionCounts() throws Exception {
        final int moveActions = 3;
        final int resizeActions = 4;
        final Map<Long, ActionView> actionViews = new HashMap<>();

        LongStream.range(0, moveActions).forEach(i -> {
            final ActionView actionView = ActionOrchestratorTestUtils.createMoveAction(i, actionPlanId);
            actionViews.put(actionView.getId(), actionView);
        });

        // Need to add "moveActions" to the ID to avoid having ovelapping IDs.
        LongStream.range(0, resizeActions).map(actionNum -> moveActions + actionNum).forEach(i -> {
            final ActionView actionView = new Action(
                ActionOrchestratorTestUtils.createResizeRecommendation(i, CommodityType.VMEM), actionPlanId);
            actionViews.put(actionView.getId(), actionView);
        });


        final ActionStore store = Mockito.mock(ActionStore.class);
        when(store.getActionViews()).thenReturn(actionViews);

        when(actionStorehouse.getStore(Mockito.eq(topologyContextId)))
            .thenReturn(Optional.of(store));

        final GetActionCountsResponse response = actionOrchestratorServiceClient.getActionCounts(
            GetActionCountsRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .build());

        Assert.assertEquals(moveActions + resizeActions, response.getTotal());
        // Should only have counts-by-type for the two action types.
        Assert.assertEquals(2, response.getCountsByTypeCount());
        assertThat(response.getCountsByTypeList(), containsInAnyOrder(
                TypeCount.newBuilder()
                    .setCount(moveActions)
                    .setType(ActionType.MOVE)
                    .build(),
                TypeCount.newBuilder()
                    .setCount(resizeActions)
                    .setType(ActionType.RESIZE)
                    .build()));
    }

    @Test
    public void testGetActionCountsWithQueryFilter() throws Exception {
        final ActionView firstAction = spy(ActionOrchestratorTestUtils.createMoveAction(1, actionPlanId));
        when(firstAction.getState()).thenReturn(ActionState.QUEUED);
        final ActionView secondAction = ActionOrchestratorTestUtils.createMoveAction(2, actionPlanId);
        final Map<Long, ActionView> actionViews = ImmutableMap.of(
            firstAction.getId(), firstAction,
            secondAction.getId(), secondAction);
        when(actionStore.getActionViews()).thenReturn(actionViews);

        final ActionStore store = Mockito.mock(ActionStore.class);
        when(store.getActionViews()).thenReturn(actionViews);
        when(store.getVisibilityPredicate()).thenReturn(spec -> true);

        when(actionStorehouse.getStore(Mockito.eq(topologyContextId)))
                .thenReturn(Optional.of(store));

        final GetActionCountsResponse response = actionOrchestratorServiceClient.getActionCounts(
                GetActionCountsRequest.newBuilder()
                        .setTopologyContextId(topologyContextId)
                        .setFilter(ActionQueryFilter.newBuilder()
                            .addStates(ActionState.QUEUED))
                        .build());

        // Only one QUEUED action.
        Assert.assertEquals(1, response.getTotal());
        // Should only have counts-by-type for the move action types.
        Assert.assertEquals(1, response.getCountsByTypeCount());
        assertThat(response.getCountsByTypeList(), contains(
                TypeCount.newBuilder()
                        .setCount(1)
                        .setType(ActionType.MOVE)
                        .build()));
    }

    @Test
    public void testGetActionCountsContextNotFound() throws Exception {
        when(actionStorehouse.getStore(Mockito.eq(topologyContextId)))
                .thenReturn(Optional.empty());

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.NOT_FOUND)
            .descriptionContains(Long.toString(topologyContextId)));

        actionOrchestratorServiceClient.getActionCounts(
                GetActionCountsRequest.newBuilder()
                        .setTopologyContextId(topologyContextId)
                        .build());
    }

    private List<ActionSpec> fetchSpecList(Iterable<ActionOrchestratorAction> rpcIter) {
        return StreamSupport.stream(rpcIter.spliterator(), false)
            .map(ActionOrchestratorAction::getActionSpec)
            .collect(Collectors.toList());
    }

    @Nonnull
    private ActionSpec spec(@Nonnull final ActionView actionView) {
        return actionTranslator.translateToSpec(actionView);
    }
}
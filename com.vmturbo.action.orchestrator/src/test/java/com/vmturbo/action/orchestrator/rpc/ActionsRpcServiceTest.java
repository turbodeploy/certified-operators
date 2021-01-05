package com.vmturbo.action.orchestrator.rpc;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.Action.SerializationState;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionPaginator;
import com.vmturbo.action.orchestrator.action.ActionSchedule;
import com.vmturbo.action.orchestrator.action.ActionTranslation;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.RejectedActionsDAO;
import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.action.orchestrator.approval.ActionApprovalManager;
import com.vmturbo.action.orchestrator.exception.ExecutionInitiationException;
import com.vmturbo.action.orchestrator.stats.HistoricalActionStatReader;
import com.vmturbo.action.orchestrator.stats.query.live.CurrentActionStatReader;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByDateResponse.Builder;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;

/**
 * test helper methods {@link ActionsRpcService#getActionCountsByDateResponseBuilder}.
 */
public class ActionsRpcServiceTest {
    private static final long ACTION_PLAN_ID = 9876;
    private static final long ASSOCIATED_ID_ACCT = 123123;
    private static final long ASSOCIATED_RESOURCE_GROUP_ID = 111;
    private static final long CONTEXT_ID = 777777L;
    private static final long ACTION_LEGACY_INSTANCE_ID = 8822L;
    private static final long ACTION_STABLE_IMPACT_ID = 2244L;
    private final ActionModeCalculator actionModeCalculator = new ActionModeCalculator();
    private final ActionDTO.SingleActionRequest request = ActionDTO.SingleActionRequest.newBuilder()
        .setActionId(ACTION_LEGACY_INSTANCE_ID)
        .setTopologyContextId(CONTEXT_ID)
        .build();

    private ActionStorehouse actionStorehouse;
    private ActionApprovalManager actionApprovalManager;
    private ActionTranslator actionTranslator;
    private ActionPaginator.ActionPaginatorFactory paginatorFactory;
    private HistoricalActionStatReader historicalActionStatReader;
    private CurrentActionStatReader currentActionStatReader;
    private UserSessionContext userSessionContext;
    private AcceptedActionsDAO acceptedActionsStore;
    private RejectedActionsDAO rejectedActionsStore;
    private ActionStore actionStore;
    private ActionsRpcService actionsRpcService;
    private ActionsRpcService actionsByImpactOidRpcService;

    /**
     * Setups the environment for test.
     */
    @Before
    public void setup() {
        actionStorehouse = mock(ActionStorehouse.class);
        actionApprovalManager = mock(ActionApprovalManager.class);
        actionTranslator = mock(ActionTranslator.class);
        paginatorFactory = mock(ActionPaginator.ActionPaginatorFactory.class);
        historicalActionStatReader = mock(HistoricalActionStatReader.class);
        currentActionStatReader = mock(CurrentActionStatReader.class);
        userSessionContext = mock(UserSessionContext.class);
        acceptedActionsStore = mock(AcceptedActionsDAO.class);
        rejectedActionsStore = mock(RejectedActionsDAO.class);
        actionStore = mock(ActionStore.class);
        actionsRpcService = new ActionsRpcService(
            null,
            actionStorehouse,
            actionApprovalManager,
            actionTranslator,
            paginatorFactory,
            historicalActionStatReader,
            currentActionStatReader,
            userSessionContext,
            acceptedActionsStore,
            rejectedActionsStore,
            10,
            false);
        actionsByImpactOidRpcService = new ActionsRpcService(
            null,
            actionStorehouse,
            actionApprovalManager,
            actionTranslator,
            paginatorFactory,
            historicalActionStatReader,
            currentActionStatReader,
            userSessionContext,
            acceptedActionsStore,
            rejectedActionsStore,
            10,
            true);
        when(actionStorehouse.getStore(CONTEXT_ID)).thenReturn(Optional.of(actionStore));

    }

    @Test
    public void testGetActionCountsByDateResponseBuilder() throws Exception {
        final ActionView actionView1 =
            executableMoveAction(123L, 1L, 1/*srcType*/, 2L, 1/*desType*/, 10L/*tgtId*/, ActionState.SUCCEEDED);
        final ActionView actionView2 =
            executableMoveAction(124L, 1L, 2/*srcType*/, 3L, 2/*destType*/, 11L, ActionState.SUCCEEDED);
        final ActionView actionView3 =
            executableMoveAction(125L, 4L, 3/*srcType*/, 2L, 3/*destType*/, 12L, ActionState.FAILED);

        List<ActionView> actionViewList = ImmutableList.of(actionView1, actionView2, actionView3);
        final long k1 = 1111L;
        final long k2 = 2222L;
        final Map<Long, List<ActionView>> actionViewsMap = ImmutableMap.of(
                k1, actionViewList, k2, actionViewList);

        Builder builder = ActionsRpcService.getActionCountsByDateResponseBuilder(actionViewsMap);
        assertEquals(k1, builder.getActionCountsByDateBuilderList().get(0).getDate());
        assertEquals(k2, builder.getActionCountsByDateBuilderList().get(1).getDate());
        // one is mode = manual, state = succeeded, the second is mode = manual, state = failed
        assertEquals(2, builder.getActionCountsByDateBuilderList().get(0).getCountsByStateAndModeCount());
        assertEquals(actionView1.getDescription(),"Move VM10 from PM1 to PM2");
        assertEquals(actionView2.getDescription(),"Move VM11 from PM1 to PM3");
        assertEquals(actionView3.getDescription(),"Move VM12 from PM4 to PM2");
    }


    @Test
    public void testGetActionCountsByDateResponseBuilderWithTwoTypes() throws Exception {
        final ActionView actionView1 =
            executableMoveAction(123L, 1L, 1/*srcType*/, 2L, 1/*desType*/, 10L/*tgtId*/, ActionState.SUCCEEDED);
        final ActionView actionView2 =
            executableMoveAction(124L, 1L, 2/*srcType*/, 3L, 2/*destType*/, 11L, ActionState.SUCCEEDED);
        final ActionView actionView3 =
            executableMoveAction(125L, 4L, 3/*srcType*/, 2L, 3/*destType*/, 12L, ActionState.FAILED);

        final ActionView actionView4 = executableActivateAction(126L, ACTION_STABLE_IMPACT_ID, 13L);
        final ActionView actionView5 = executableActivateAction(127L, ACTION_STABLE_IMPACT_ID, 14L);

        List<ActionView> actionViewList1 = ImmutableList.of(
                actionView1, actionView2, actionView3);
        List<ActionView> actionViewList2 = ImmutableList.of(
                actionView4, actionView5);
        final long k1 = 1111L;
        final long k2 = 2222L;
        final Map<Long, List<ActionView>> actionViewsMap = ImmutableMap.of(
                k1, actionViewList1, k2, actionViewList2);

        Builder builder = ActionsRpcService.getActionCountsByDateResponseBuilder(actionViewsMap);
        assertEquals(2, builder.getActionCountsByDateBuilderList().size());
        assertEquals(k1, builder.getActionCountsByDateBuilderList().get(0).getDate());
        assertEquals(k2, builder.getActionCountsByDateBuilderList().get(1).getDate());
        assertEquals(actionView1.getDescription(),"Move VM10 from PM1 to PM2");
        assertEquals(actionView2.getDescription(),"Move VM11 from PM1 to PM3");
        assertEquals(actionView3.getDescription(),"Move VM12 from PM4 to PM2");
    }

    /**
     * Test acceptance of action.
     *
     * @throws ExecutionInitiationException never.
     */
    @Test
    public void testAcceptAction() throws ExecutionInitiationException {
        // ARANGE
        Action action = executableActivateAction(ACTION_LEGACY_INSTANCE_ID, ACTION_STABLE_IMPACT_ID, 13L);
        when(actionStore.getAction(ACTION_LEGACY_INSTANCE_ID)).thenReturn(Optional.of(action));

        when(actionApprovalManager.attemptAndExecute(eq(actionStore), any(), eq(action))).thenThrow(new ExecutionInitiationException("test", Status.Code.INTERNAL));
        StreamObserver<ActionDTO.AcceptActionResponse> observer = mock(StreamObserver.class);

        // ACT
        actionsRpcService.acceptAction(request, observer);

        // ASSERT
        ArgumentCaptor<Throwable> argumentCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(observer).onError(argumentCaptor.capture());
        assertThat(argumentCaptor.getValue().getMessage(), containsString("test"));
        verify(observer, never()).onCompleted();
    }

    /**
     * Tests acceptance of action when the action is expired.
     *
     * @throws ExecutionInitiationException never.
     */
    @Test
    public void testAcceptedActionExpiredSchedule() throws ExecutionInitiationException {
        Action action = executableActivateAction(ACTION_LEGACY_INSTANCE_ID, ACTION_STABLE_IMPACT_ID, 13L);
        ActionSchedule actionSchedule = new ActionSchedule(null, null, "America/Toronto", 11L,
            "Schedule 1", ActionDTO.ActionMode.MANUAL, null);
        action.setSchedule(actionSchedule);
        when(actionStore.getAction(ACTION_LEGACY_INSTANCE_ID)).thenReturn(Optional.of(action));

        StreamObserver<ActionDTO.AcceptActionResponse> observer = mock(StreamObserver.class);

        // ACT
        actionsRpcService.acceptAction(request, observer);

        // ASSERT
        verify(actionApprovalManager, never()).attemptAndExecute(any(), any(), any());
        verify(observer, never()).onNext(any());
        verify(observer, never()).onCompleted();
        ArgumentCaptor<StatusException> argumentCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(observer).onError(argumentCaptor.capture());
        assertThat(argumentCaptor.getValue().getMessage(), containsString("does not have a next "
            + "occurrence."));
    }

    /**
     * Accepting action by recommendation ID should only be done when it is enabled.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testAcceptActionByRecommendationIdOnlyWhenEnabled() throws Exception {
        final long matchingId = ACTION_STABLE_IMPACT_ID;
        final long notMatchingId = ACTION_LEGACY_INSTANCE_ID;

        final Action actionByStableId = executableActivateAction(
            notMatchingId, matchingId, 13L);
        Mockito.when(actionStore.getAction(notMatchingId))
            .thenReturn(Optional.of(actionByStableId));
        Mockito.when(actionStore.getActionByRecommendationId(matchingId))
            .thenReturn(Optional.of(actionByStableId));

        final Action actionByLegacyId = executableActivateAction(
            matchingId, notMatchingId, 13L);
        Mockito.when(actionStore.getAction(matchingId))
            .thenReturn(Optional.of(actionByLegacyId));
        Mockito.when(actionStore.getActionByRecommendationId(notMatchingId))
            .thenReturn(Optional.of(actionByLegacyId));

        ArgumentCaptor<Action> actionArgumentCaptor = ArgumentCaptor.forClass(Action.class);
        Mockito.when(actionApprovalManager.attemptAndExecute(eq(actionStore), any(), actionArgumentCaptor.capture()))
            .thenReturn(AcceptActionResponse.newBuilder()
                .buildPartial());

        final StreamObserver<ActionDTO.AcceptActionResponse> observer = mock(StreamObserver.class);
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
            .setActionId(matchingId)
            .setTopologyContextId(CONTEXT_ID)
            .build();

        actionsByImpactOidRpcService.acceptAction(acceptActionRequest, observer);
        // actionByStableId should be sent to approval manager for execution
        assertEquals(actionByStableId, actionArgumentCaptor.getValue());

        actionsRpcService.acceptAction(acceptActionRequest, observer);
        // actionByStableId should be sent to approval manager for execution
        assertEquals(actionByLegacyId, actionArgumentCaptor.getValue());
    }

    /**
     * Getting an action by recommendation ID should only be done when it is enabled and on a live
     * topology.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testGetActionByRecommendationIdOnlyWhenEnabledAndLiveTopology() throws Exception {
        final long matchingId = ACTION_STABLE_IMPACT_ID;
        final long notMatchingId = ACTION_LEGACY_INSTANCE_ID;

        final Action actionByStableId = executableActivateAction(
            notMatchingId, matchingId, 13L);
        Mockito.when(actionStore.getActionView(notMatchingId))
            .thenReturn(Optional.of(actionByStableId));
        Mockito.when(actionStore.getActionViewByRecommendationId(matchingId))
            .thenReturn(Optional.of(actionByStableId));

        final Action actionByLegacyId = executableActivateAction(
            matchingId, notMatchingId, 13L);
        Mockito.when(actionStore.getActionView(matchingId))
            .thenReturn(Optional.of(actionByLegacyId));
        Mockito.when(actionStore.getActionViewByRecommendationId(notMatchingId))
            .thenReturn(Optional.of(actionByLegacyId));

        ArgumentCaptor<Action> actionArgumentCaptor = ArgumentCaptor.forClass(Action.class);
        Mockito.when(actionTranslator.translateToSpec(actionArgumentCaptor.capture()))
            .thenReturn(ActionSpec.newBuilder().buildPartial());

        final StreamObserver<ActionDTO.ActionOrchestratorAction> observer = mock(StreamObserver.class);
        final SingleActionRequest getActionRequest = SingleActionRequest.newBuilder()
            .setActionId(matchingId)
            .setTopologyContextId(CONTEXT_ID)
            .build();

        actionsByImpactOidRpcService.getAction(getActionRequest, observer);
        // actionByLegacyId should be returned because it's a plan topology
        assertEquals(actionByLegacyId, actionArgumentCaptor.getValue());

        Mockito.when(actionStore.getStoreTypeName())
            .thenReturn(LiveActionStore.STORE_TYPE_NAME);
        actionsByImpactOidRpcService.getAction(getActionRequest, observer);
        // actionByLegacyId should be returned because it's the live topology and stable id is enabled
        assertEquals(actionByStableId, actionArgumentCaptor.getValue());

        actionsRpcService.getAction(getActionRequest, observer);
        // actionByStableId should be sent to approval manager for execution
        assertEquals(actionByLegacyId, actionArgumentCaptor.getValue());
    }

    private Action executableMoveAction(
                long id,
                long sourceId,
                int sourceType,
                long destId,
                int destType,
                long targetId,
                ActionState state) {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(id)
            .setDeprecatedImportance(0)
            .setExecutable(true)
            .setExplanation(Explanation.newBuilder().build())
            .setInfo(TestActionBuilder
                .makeMoveInfo(targetId, sourceId, sourceType, destId, destType))
            .build();

        String actionDescription = "Move VM"+targetId+" from PM"+sourceId+" to PM"+destId;
        SerializationState orchestratorAction = new SerializationState(ACTION_PLAN_ID,
            action,
            LocalDateTime.now(),
            ActionDecision.getDefaultInstance(),
            ExecutionStep.getDefaultInstance(),
            state,
            new ActionTranslation(action),
            ASSOCIATED_ID_ACCT,
            ASSOCIATED_RESOURCE_GROUP_ID,
            actionDescription.getBytes(),
                2244L);
        return spy(new Action(orchestratorAction, actionModeCalculator));
    }

    private Action executableActivateAction(long legacyInstanceId, long stableImpactId, long targetId) {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(legacyInstanceId)
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.newBuilder()
                        .setActivate(Activate.newBuilder()
                                .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId))
                                .build())
                        .build())
                .build();

        return spy(new Action(action, ACTION_PLAN_ID, actionModeCalculator, stableImpactId));
    }
}

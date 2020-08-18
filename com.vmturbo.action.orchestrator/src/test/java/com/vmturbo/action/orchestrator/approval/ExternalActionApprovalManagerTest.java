package com.vmturbo.action.orchestrator.approval;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Optional;

import io.grpc.Status;
import io.opentracing.SpanContext;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.RejectedActionsDAO;
import com.vmturbo.action.orchestrator.exception.ExecutionInitiationException;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.TriConsumer;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.ActionExecution.ExternalActionInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionApprovalResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.GetActionStateResponse;

/**
 * Unit test for {@link ExternalActionApprovalManager}.
 */
public class ExternalActionApprovalManagerTest {

    private static final long CTX_ID = 1234567L;
    private static final long ACTION1 = 10001L;
    private static final long ACTION2 = 10002L;
    private static final long ACTION3 = 10003L;
    private static final long RECOMMENDATION_ID = 10202L;
    private static final long ACTION_PLAN_ID = 10203L;
    private static final String ACTION1_EXTERNAL_NAME = "CHG0030039";
    private static final String ACTION1_EXTERNAL_URL =
        "https://dev77145.service-now.com/nav_to.do?uri=%2Fchange_request.do%3Fsys_id%3Dee362595db02101093ac84da0b9619d9";
    private static final String ACTION2_EXTERNAL_NAME = "CHG0030040";
    private static final String ACTION2_EXTERNAL_URL =
        "https://dev77145.service-now.com/nav_to.do?uri=%2Fchange_request.do%3Fsys_id%3Dee362595db02101093ac84da0b9619e0";

    @Captor
    private ArgumentCaptor<TriConsumer<GetActionStateResponse, Runnable, SpanContext>> actionStateResponseMsgCaptor;
    @Captor
    private ArgumentCaptor<TriConsumer<ActionApprovalResponse, Runnable, SpanContext>> actionApprovalResponseMsgCaptor;
    @Mock
    private IMessageReceiver<GetActionStateResponse> getActionStateResponseReceiver;
    @Mock
    private IMessageReceiver<ActionApprovalResponse> actionApprovalResponseReceiver;
    private ActionStorehouse storehouse;
    private ActionApprovalManager mgr;
    private RejectedActionsDAO rejectedActionsStore;
    private TriConsumer<GetActionStateResponse, Runnable, SpanContext> getActionStateResponseConsumer;
    private TriConsumer<ActionApprovalResponse, Runnable, SpanContext> actionApprovalResponseConsumer;
    private ActionStore actionStore;

    /**
     * Initializes the tests.
     *
     * @throws ExecutionInitiationException never.
     */
    @Before
    public void init() throws ExecutionInitiationException {
        MockitoAnnotations.initMocks(this);
        mgr = Mockito.mock(ActionApprovalManager.class);
        rejectedActionsStore = Mockito.mock(RejectedActionsDAO.class);
        Mockito.when(mgr.attemptAndExecute(Mockito.any(), Mockito.anyString(),
                Mockito.any(Action.class)))
                .thenThrow(new ExecutionInitiationException("Some error", Status.Code.INTERNAL));
        actionStore = Mockito.mock(ActionStore.class);
        storehouse = Mockito.mock(ActionStorehouse.class);
        Mockito.when(storehouse.getStore(CTX_ID))
                .thenReturn(Optional.of(actionStore));
        // link the receivers to the ExternalActionApprovalManager. This ExternalActionApprovalManager
        // will not be garbage collected because getActionStateResponseReceiver and actionApprovalResponseReceiver
        // will hold reference to them.
        new ExternalActionApprovalManager(mgr, storehouse,
                        getActionStateResponseReceiver, actionApprovalResponseReceiver,
                        CTX_ID,
                        rejectedActionsStore);
        Mockito.verify(getActionStateResponseReceiver)
                .addListener(actionStateResponseMsgCaptor.capture());
        getActionStateResponseConsumer = actionStateResponseMsgCaptor.getValue();

        Mockito.verify(actionApprovalResponseReceiver)
            .addListener(actionApprovalResponseMsgCaptor.capture());
        actionApprovalResponseConsumer = actionApprovalResponseMsgCaptor.getValue();
    }

    /**
     * Tests successful flow of receiving action state updates from message receiver.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testSuccessfulFlow() throws Exception {
        final Runnable commit = Mockito.mock(Runnable.class);
        final Action rejectedAction = Mockito.spy(
                new Action(ActionDTO.Action.getDefaultInstance(), ACTION_PLAN_ID,
                        Mockito.mock(ActionModeCalculator.class), RECOMMENDATION_ID));
        final Action acceptedAction = Mockito.mock(Action.class);
        final Action readyAction = Mockito.mock(Action.class);
        Mockito.when(rejectedAction.getMode()).thenReturn(ActionDTO.ActionMode.EXTERNAL_APPROVAL);
        Mockito.when(acceptedAction.getMode()).thenReturn(ActionDTO.ActionMode.EXTERNAL_APPROVAL);
        Mockito.when(readyAction.getMode()).thenReturn(ActionDTO.ActionMode.EXTERNAL_APPROVAL);

        Mockito.when(rejectedAction.getState()).thenReturn(ActionState.READY);
        Mockito.when(rejectedAction.getId()).thenReturn(ACTION3);
        Mockito.when(rejectedAction.getAssociatedSettingsPolicies())
                .thenReturn(Collections.singleton(122L));
        Mockito.when(actionStore.getActionByRecommendationId(ACTION3))
                .thenReturn(Optional.of(rejectedAction));
        Mockito.when(actionStore.getActionByRecommendationId(ACTION1))
                .thenReturn(Optional.of(readyAction));
        Mockito.when(actionStore.getActionByRecommendationId(ACTION2))
                .thenReturn(Optional.of(acceptedAction));

        getActionStateResponseConsumer.accept(GetActionStateResponse.newBuilder()
                .putActionState(ACTION1, ActionResponseState.PENDING_ACCEPT)
                .putActionState(ACTION2, ActionResponseState.ACCEPTED)
                .putActionState(ACTION3, ActionResponseState.REJECTED)
                .build(), commit, Mockito.mock(SpanContext.class));

        Mockito.verify(mgr)
                .attemptAndExecute(Mockito.eq(actionStore),
                        Mockito.eq(ExternalActionApprovalManager.USER_ID),
                        Mockito.eq(acceptedAction));
        Mockito.verify(mgr, Mockito.never())
                .attemptAndExecute(Mockito.any(), Mockito.anyString(), Mockito.eq(readyAction));
        Mockito.verify(mgr, Mockito.never())
                .attemptAndExecute(Mockito.any(), Mockito.anyString(), Mockito.eq(rejectedAction));
        Mockito.verify(rejectedActionsStore)
                .persistRejectedAction(Mockito.eq(RECOMMENDATION_ID), Mockito.any(String.class),
                        Mockito.any(LocalDateTime.class), Mockito.any(String.class),
                        Mockito.anyCollectionOf(Long.class));
        Mockito.verify(commit).run();
    }

    /**
     * Tests the case that the action update is accepted but the action is not in external
     * approval state.
     *
     * @throws ExecutionInitiationException never.
     */
    @Test
    public void testAcceptedActionInUnexpectedModeFlow() throws ExecutionInitiationException {
        final Runnable commit = Mockito.mock(Runnable.class);
        final Action readyAction = Mockito.mock(Action.class);
        Mockito.when(readyAction.getMode()).thenReturn(ActionDTO.ActionMode.MANUAL);

        Mockito.when(actionStore.getActionByRecommendationId(ACTION1))
            .thenReturn(Optional.of(readyAction));

        getActionStateResponseConsumer.accept(GetActionStateResponse.newBuilder()
            .putActionState(ACTION1, ActionResponseState.ACCEPTED)
            .build(), commit, Mockito.mock(SpanContext.class));

        Mockito.verify(mgr, Mockito.never())
            .attemptAndExecute(Mockito.any(), Mockito.anyString(), Mockito.eq(readyAction));
        Mockito.verify(commit).run();
    }

    /**
     * Tests no live actions store is present.
     */
    @Test
    public void testNoLiveActionStore() {
        final Runnable commit = Mockito.mock(Runnable.class);
        Mockito.when(storehouse.getStore(Mockito.anyLong())).thenReturn(Optional.empty());
        getActionStateResponseConsumer.accept(GetActionStateResponse.newBuilder()
                .putActionState(ACTION1, ActionResponseState.PENDING_ACCEPT)
                .putActionState(ACTION2, ActionResponseState.ACCEPTED)
                .build(), commit, Mockito.mock(SpanContext.class));
        Mockito.verifyZeroInteractions(mgr);
        Mockito.verify(commit, Mockito.never()).run();
    }

    /**
     * Tests no actions reported in the response.
     */
    @Test
    public void testNoActionsInResponse() {
        final Runnable commit = Mockito.mock(Runnable.class);
        Mockito.when(storehouse.getStore(Mockito.anyLong())).thenReturn(Optional.empty());
        getActionStateResponseConsumer.accept(GetActionStateResponse.newBuilder().build(), commit, Mockito.mock(SpanContext.class));
        Mockito.verifyZeroInteractions(mgr);
        Mockito.verify(commit).run();
    }

    /**
     * The action view's external name and url should be set.
     */
    @Test
    public void testExternalApprovalResponse() {
        final Runnable commit = Mockito.mock(Runnable.class);
        final Action action1 = Mockito.mock(Action.class);
        final Action action2 = Mockito.mock(Action.class);

        Mockito.when(actionStore.getActionByRecommendationId(ACTION1))
            .thenReturn(Optional.of(action1));
        Mockito.when(actionStore.getActionByRecommendationId(ACTION2))
            .thenReturn(Optional.of(action2));

        actionApprovalResponseConsumer.accept(ActionApprovalResponse.newBuilder()
            .putActionState(ACTION1, ExternalActionInfo.newBuilder()
                .setShortName(ACTION1_EXTERNAL_NAME)
                .setUrl(ACTION1_EXTERNAL_URL)
                .buildPartial())
            .putActionState(ACTION2, ExternalActionInfo.newBuilder()
                .setShortName(ACTION2_EXTERNAL_NAME)
                .setUrl(ACTION2_EXTERNAL_URL)
                .buildPartial())
            .buildPartial(), commit, Mockito.mock(SpanContext.class));
        Mockito.verifyZeroInteractions(mgr);
        Mockito.verify(commit).run();

        Mockito.verify(action1).setExternalActionName(ACTION1_EXTERNAL_NAME);
        Mockito.verify(action1).setExternalActionUrl(ACTION1_EXTERNAL_URL);
        Mockito.verify(action2).setExternalActionName(ACTION2_EXTERNAL_NAME);
        Mockito.verify(action2).setExternalActionUrl(ACTION2_EXTERNAL_URL);
    }

    /**
     * Tests no live actions store is present so nothing should happen. The commit should not be
     * run.
     */
    @Test
    public void testNoLiveActionStoreExternalApprovalResponse() {
        final Runnable commit = Mockito.mock(Runnable.class);
        Mockito.when(storehouse.getStore(Mockito.anyLong())).thenReturn(Optional.empty());
        actionApprovalResponseConsumer.accept(ActionApprovalResponse.newBuilder()
            .putActionState(1L, ExternalActionInfo.newBuilder()
                .buildPartial())
            .buildPartial(), commit, Mockito.mock(SpanContext.class));
        Mockito.verifyZeroInteractions(mgr);
        Mockito.verify(commit, Mockito.never()).run();
    }

    /**
     * Tests no live actions store is present so nothing should happen. The commit should be
     * run.
     */
    @Test
    public void testNoActionsInResponseExternalApprovalResponse() {
        final Runnable commit = Mockito.mock(Runnable.class);
        actionApprovalResponseConsumer.accept(ActionApprovalResponse.newBuilder()
            .buildPartial(), commit, Mockito.mock(SpanContext.class));
        Mockito.verifyZeroInteractions(mgr);
        Mockito.verify(commit).run();
    }
}

package com.vmturbo.action.orchestrator.approval;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Optional;
import java.util.function.BiConsumer;

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
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
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


    @Captor
    private ArgumentCaptor<BiConsumer<GetActionStateResponse, Runnable>> msgCaptor;
    @Mock
    private IMessageReceiver<GetActionStateResponse> msgReceiver;
    private ActionStorehouse storehouse;
    private ActionApprovalManager mgr;
    private RejectedActionsDAO rejectedActionsStore;
    private BiConsumer<GetActionStateResponse, Runnable> consumer;
    private ActionStore actionStore;

    /**
     * Initializes the tests.
     */
    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        mgr = Mockito.mock(ActionApprovalManager.class);
        rejectedActionsStore = Mockito.mock(RejectedActionsDAO.class);
        Mockito.when(mgr.attemptAndExecute(Mockito.any(), Mockito.anyString(),
                Mockito.any(Action.class)))
                .thenReturn(AcceptActionResponse.newBuilder().setError("Some error").build());
        actionStore = Mockito.mock(ActionStore.class);
        storehouse = Mockito.mock(ActionStorehouse.class);
        Mockito.when(storehouse.getStore(CTX_ID))
                .thenReturn(Optional.of(actionStore));
        final ExternalActionApprovalManager externalManager =
                new ExternalActionApprovalManager(mgr, storehouse, msgReceiver, CTX_ID,
                        rejectedActionsStore);
        Mockito.verify(msgReceiver)
                .addListener(msgCaptor.capture());
        consumer = msgCaptor.getValue();
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

        consumer.accept(GetActionStateResponse.newBuilder()
                .putActionState(ACTION1, ActionResponseState.PENDING_ACCEPT)
                .putActionState(ACTION2, ActionResponseState.ACCEPTED)
                .putActionState(ACTION3, ActionResponseState.REJECTED)
                .build(), commit);

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
     * Tests no live actions store is present.
     */
    @Test
    public void testNoLiveActionStore() {
        final Runnable commit = Mockito.mock(Runnable.class);
        Mockito.when(storehouse.getStore(Mockito.anyLong())).thenReturn(Optional.empty());
        consumer.accept(GetActionStateResponse.newBuilder()
                .putActionState(ACTION1, ActionResponseState.PENDING_ACCEPT)
                .putActionState(ACTION2, ActionResponseState.ACCEPTED)
                .build(), commit);
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
        consumer.accept(GetActionStateResponse.newBuilder().build(), commit);
        Mockito.verifyZeroInteractions(mgr);
        Mockito.verify(commit).run();
    }
}

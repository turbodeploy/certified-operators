package com.vmturbo.action.orchestrator.approval;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionApprovalRequests;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.action.orchestrator.store.PlanActionStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeactivateExplanation;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * Unit test for {@link ActionApprovalSender}.
 */
public class ActionApprovalSenderTest {
    private static final long ACTION_PLAN_ID = 10000L;
    private static final long ACTION1 = 10001L;
    private static final long ACTION2 = 10002L;
    private static final long ACTION3 = 10003L;
    private static final long ACTION4 = 10004L;
    private static final long TARGET_ID = 1234L;
    private static final String EXPLANATION = "Resize VM from 3.0GB to 4.0GB";
    @Mock
    private IMessageSender<ActionApprovalRequests> requestSender;
    @Mock
    private WorkflowStore workflowStore;
    private ActionApprovalSender aas;
    private Map<Long, Action> storeActions;
    private ActionStore actionStore;

    @Mock
    private ActionTargetSelector actionTargetSelector;
    @Mock
    private EntitiesAndSettingsSnapshotFactory entitySettingsCache;

    /**
     * Initializes the tests.
     */
    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        storeActions = new HashMap<>();
        actionStore = Mockito.mock(ActionStore.class);
        Mockito.when(actionStore.getActions()).thenReturn(storeActions);
        Mockito.when(actionStore.getAction(Mockito.anyLong())).thenAnswer(invocation -> {
            final long actionOid = invocation.getArgumentAt(0, Long.class);
            return storeActions.get(actionOid);
        });
        Mockito.when(actionStore.getStoreTypeName()).thenReturn(LiveActionStore.STORE_TYPE_NAME);
        final ActionTargetInfo actionTargetInfo = Mockito.mock(ActionTargetInfo.class);
        Mockito.when(actionTargetInfo.targetId()).thenReturn(Optional.of(TARGET_ID));
        Mockito.when(actionTargetSelector.getTargetForAction(Mockito.any(ActionDTO.Action.class),
                Mockito.any(EntitiesAndSettingsSnapshotFactory.class)))
                .thenReturn(actionTargetInfo);
        this.aas = new ActionApprovalSender(workflowStore, requestSender, actionTargetSelector, entitySettingsCache);
    }

    /**
     * Tests successful flow.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testSuccessfulFlow() throws Exception {
        createAction(ACTION1, ActionMode.EXTERNAL_APPROVAL, ActionState.READY);
        createAction(ACTION2, ActionMode.MANUAL, ActionState.READY);
        aas.sendApprovalRequests(actionStore);
        final ArgumentCaptor<ActionApprovalRequests> captor = ArgumentCaptor.forClass(
                ActionApprovalRequests.class);
        Mockito.verify(requestSender)
                .sendMessage(captor.capture());
        Assert.assertEquals(Collections.singleton(ACTION1), captor.getAllValues()
                .get(0)
                .getActionsList()
                .stream()
                .map(ExecuteActionRequest::getActionId)
                .collect(Collectors.toSet()));

        // all actions should have an explanation
        Assert.assertTrue(
            captor.getAllValues().stream()
                .map(ActionApprovalRequests::getActionsList)
                .flatMap(List::stream)
                .allMatch(
                    executeActionRequest -> executeActionRequest.hasExplanation()
                        && EXPLANATION.equals(executeActionRequest.getExplanation())
                )
        );
    }

    /**
     * Tests that there is no message sent when action store is not executable.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testActionNotExecutable() throws Exception {
        Mockito.when(actionStore.getStoreTypeName()).thenReturn(PlanActionStore.STORE_TYPE_NAME);
        createAction(ACTION1, ActionMode.EXTERNAL_APPROVAL, ActionState.READY);
        aas.sendApprovalRequests(actionStore);
        Mockito.verifyZeroInteractions(requestSender);
    }

    /**
     * Tests that there is no message sent when action has REJECTED state.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testRejectedAction() throws Exception {
        Mockito.when(actionStore.getStoreTypeName()).thenReturn(LiveActionStore.STORE_TYPE_NAME);
        createAction(ACTION1, ActionMode.EXTERNAL_APPROVAL, ActionState.REJECTED);
        aas.sendApprovalRequests(actionStore);
        Mockito.verifyZeroInteractions(requestSender);
    }

    /**
     * Tests that if one request send fails with exception, the flow is not interrupted.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testOneFailureOneSuccess() throws Exception {
        final Action action1 = createAction(ACTION1, ActionMode.EXTERNAL_APPROVAL, ActionState.READY);
        final Action action2 = createAction(ACTION2, ActionMode.EXTERNAL_APPROVAL, ActionState.READY);
        Mockito.doThrow(new CommunicationException("something went wrong"))
                .doNothing()
                .when(requestSender)
                .sendMessage(Mockito.any());
        storeActions.clear();
        storeActions.put(ACTION1, action1);
        aas.sendApprovalRequests(actionStore);
        final ArgumentCaptor<ActionApprovalRequests> captor = ArgumentCaptor.forClass(
                ActionApprovalRequests.class);

        storeActions.clear();
        storeActions.put(ACTION2, action2);
        aas.sendApprovalRequests(actionStore);

        Mockito.verify(requestSender, Mockito.times(2))
                .sendMessage(captor.capture());
        Assert.assertEquals(Collections.singleton(action1.getRecommendationOid()),
                captor.getAllValues()
                        .get(0)
                        .getActionsList()
                        .stream()
                        .map(ExecuteActionRequest::getActionId)
                        .collect(Collectors.toSet()));
        Assert.assertEquals(Collections.singleton(action2.getRecommendationOid()),
                captor.getAllValues()
                        .get(1)
                        .getActionsList()
                        .stream()
                        .map(ExecuteActionRequest::getActionId)
                        .collect(Collectors.toSet()));

        // all actions should have an explanation
        Assert.assertTrue(
            captor.getAllValues().stream()
                .map(ActionApprovalRequests::getActionsList)
                .flatMap(List::stream)
                .allMatch(
                    executeActionRequest -> executeActionRequest.hasExplanation()
                        && EXPLANATION.equals(executeActionRequest.getExplanation())
                )
        );
    }

    /**
     * Tests that only READY actions are sent to external approval backend.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testActionsInTeminalState() throws Exception {
        createAction(ACTION1, ActionMode.EXTERNAL_APPROVAL, ActionState.READY);
        createAction(ACTION2, ActionMode.EXTERNAL_APPROVAL, ActionState.IN_PROGRESS);
        createAction(ACTION3, ActionMode.EXTERNAL_APPROVAL, ActionState.SUCCEEDED);
        createAction(ACTION4, ActionMode.EXTERNAL_APPROVAL, ActionState.QUEUED);
        aas.sendApprovalRequests(actionStore);
        final ArgumentCaptor<ActionApprovalRequests> captor = ArgumentCaptor.forClass(
                ActionApprovalRequests.class);
        Mockito.verify(requestSender)
                .sendMessage(captor.capture());
        Assert.assertEquals(Sets.newHashSet(ACTION1), captor.getValue()
                .getActionsList()
                .stream()
                .map(ExecuteActionRequest::getActionId)
                .collect(Collectors.toSet()));
    }

    /**
     * Test that actions without associated target don't send to external approval backend.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testActionWithoutTarget() throws Exception {
        final Action actionWithoutTarget =
                createAction(ACTION1, ActionMode.EXTERNAL_APPROVAL, ActionState.READY);
        final ActionTargetInfo actionTargetInfo = Mockito.mock(ActionTargetInfo.class);
        Mockito.when(actionTargetInfo.targetId()).thenReturn(Optional.empty());
        Mockito.when(actionTargetSelector.getTargetForAction(
                Mockito.eq(actionWithoutTarget.getRecommendation()),
                Mockito.any(EntitiesAndSettingsSnapshotFactory.class)))
                .thenReturn(actionTargetInfo);
        aas.sendApprovalRequests(actionStore);
        Mockito.verifyZeroInteractions(requestSender);
    }

    private Action createAction(long oid, @Nonnull ActionMode actionMode,
            @Nonnull ActionState actionState) {
        final ActionDTO.Action actionDTO = ActionDTO.Action.newBuilder()
                .setId(oid)
                .setInfo(ActionInfo.newBuilder()
                        .setDelete(Delete.newBuilder()
                                .setTarget(
                                        ActionEntity.newBuilder().setId(10).setType(12).build())))
                .setExplanation(Explanation.newBuilder()
                        .setDeactivate(DeactivateExplanation.newBuilder().build())
                        .build())
                .setDeprecatedImportance(23)
                .build();
        final Action action = Mockito.spy(
                new Action(actionDTO, ACTION_PLAN_ID, Mockito.mock(ActionModeCalculator.class),
                        oid));
        action.getActionTranslation().setTranslationSuccess(actionDTO);
        Mockito.when(action.getMode()).thenReturn(actionMode);
        Mockito.when(action.getState()).thenReturn(actionState);
        Mockito.when(action.getDescription()).thenReturn(EXPLANATION);
        storeActions.put(oid, action);
        return action;
    }
}

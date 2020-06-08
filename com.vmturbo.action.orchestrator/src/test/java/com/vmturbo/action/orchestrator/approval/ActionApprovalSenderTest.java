package com.vmturbo.action.orchestrator.approval;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

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
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
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
    @Mock
    private IMessageSender<ActionApprovalRequests> requestSender;
    @Mock
    private WorkflowStore workflowStore;
    private ActionApprovalSender aas;
    private Map<Long, Action> storeActions;
    private ActionStore actionStore;

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
        Mockito.when(actionStore.allowsExecution()).thenReturn(true);
        this.aas = new ActionApprovalSender(workflowStore, requestSender);
    }

    /**
     * Tests successful flow.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testSuccessfulFlow() throws Exception {
        createAction(ACTION1, ActionMode.EXTERNAL_APPROVAL);
        createAction(ACTION2, ActionMode.MANUAL);
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
    }

    /**
     * Tests that there is no message sent when action store is not executable.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testActionNotExecutable() throws Exception {
        Mockito.when(actionStore.allowsExecution())
                .thenReturn(false);
        createAction(ACTION1, ActionMode.EXTERNAL_APPROVAL);
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
        final Action action1 = createAction(ACTION1, ActionMode.EXTERNAL_APPROVAL);
        final Action action2 = createAction(ACTION2, ActionMode.EXTERNAL_APPROVAL);
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
        Assert.assertEquals(Collections.singleton(ACTION1), captor.getAllValues()
                .get(0)
                .getActionsList()
                .stream()
                .map(ExecuteActionRequest::getActionId)
                .collect(Collectors.toSet()));
        Assert.assertEquals(Collections.singleton(ACTION2), captor.getAllValues()
                .get(1)
                .getActionsList()
                .stream()
                .map(ExecuteActionRequest::getActionId)
                .collect(Collectors.toSet()));
    }

    private Action createAction(long oid, @Nonnull ActionMode actionMode) {
        final ActionDTO.Action actionDTO = ActionDTO.Action.newBuilder()
                .setId(oid)
                .setInfo(ActionInfo.newBuilder()
                        .setDelete(Delete.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(10)
                                        .setType(12)
                                        .build())))
                .setExplanation(Explanation.newBuilder()
                        .setDeactivate(DeactivateExplanation.newBuilder()
                                .build())
                        .build())
                .setDeprecatedImportance(23)
                .build();
        final Action action = Mockito.spy(
                new Action(actionDTO, ACTION_PLAN_ID, Mockito.mock(ActionModeCalculator.class),
                        oid));
        action.getActionTranslation()
                .setTranslationSuccess(actionDTO);
        Mockito.when(action.getMode())
                .thenReturn(actionMode);
        storeActions.put(oid, action);
        return action;
    }
}

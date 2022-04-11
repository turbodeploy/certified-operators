package com.vmturbo.action.orchestrator.execution;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.RollBackToAcceptedEvent;
import com.vmturbo.action.orchestrator.action.ActionSchedule;
import com.vmturbo.action.orchestrator.approval.ActionApprovalSender;
import com.vmturbo.action.orchestrator.execution.ConditionalSubmitter.ConditionalFuture;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;

/**
 * Tests for {@link ActionAutomationManager}.
 */
public class ActionAutomationManagerTest {

    private final AutomatedActionExecutor executor = mock(AutomatedActionExecutor.class);
    private final ActionApprovalSender actionApprovalSender = mock(ActionApprovalSender.class);
    private final ActionAutomationManager automationManager = new ActionAutomationManager(
        executor, actionApprovalSender);
    private final ActionStore actionStore = mock(ActionStore.class);

    /**
     * Setup.
     */
    @Before
    public void setup() {
        when(actionStore.allowsExecution()).thenReturn(true);
        when(actionStore.getStoreTypeName()).thenReturn("test");
    }

    /**
     * testCancelQueuedActions.
     *
     * @throws InterruptedException on interruption.
     */
    @Test
    public void testCancelQueuedActions() throws InterruptedException {
        List<Action> actionList = ImmutableList.of(
                mockAction(1),
                mockAction(2),
                mockAction(3));
        AutomatedActionTask actionTask = mock(AutomatedActionTask.class);
        when(actionTask.getActionList()).thenReturn(actionList);
        List<ConditionalFuture> actionExecutionTaskList = new ArrayList<>();
        actionExecutionTaskList.add(new FutureMock(actionTask));
        actionList.forEach(action -> when(action.getState()).thenReturn(ActionState.QUEUED));
        when(executor.executeAutomatedFromStore(any()))
            .thenReturn(actionExecutionTaskList);
        automationManager.updateAutomation(actionStore);
        assertThat(automationManager.cancelQueuedActions(), is(1));
    }

    /**
     * testStoreActionsExecutesAutomaticActions.
     *
     * @throws InterruptedException on interruption.
     */
    @Test
    public void testStoreActionsExecutesAutomaticActions() throws InterruptedException {
        automationManager.updateAutomation(actionStore);
        verify(executor).executeAutomatedFromStore(actionStore);
    }

    /**
     * Tests case when action is removing from queue (state is rolled back from QUEUED) because of
     * non active status of execution window.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testCancelActionsWithNonActiveExecutionWindows() throws Exception {
        final ActionSchedule nonActiveSchedule = mock(ActionSchedule.class);
        final List<Action> actionList = ImmutableList.of(
                mockAction(1),
                mockAction(2),
                mockAction(3));
        final AutomatedActionTask actionTask = mock(AutomatedActionTask.class);
        when(actionTask.getActionList()).thenReturn(actionList);
        final List<ConditionalFuture> actionExecutionTaskList = new ArrayList<>();
        actionExecutionTaskList.add(new FutureMock(actionTask));
        actionList.forEach(action -> when(action.getState()).thenReturn(ActionState.QUEUED));
        when(executor.executeAutomatedFromStore(any())).thenReturn(actionExecutionTaskList);
        automationManager.updateAutomation(actionStore);

        actionList.forEach(action ->
                when(action.getSchedule()).thenReturn(Optional.of(nonActiveSchedule)));
        when(nonActiveSchedule.isActiveScheduleNow()).thenReturn(false);
        // emulate next market cycle
        automationManager.updateAutomation(actionStore);
        actionList.forEach(action ->
                verify(action).receive(any(RollBackToAcceptedEvent.class)));
    }

    private Action mockAction(long id) {
        final Action action = mock(Action.class);
        when(action.getId()).thenReturn(id);
        when(actionStore.getAction(action.getId())).thenReturn(Optional.of(action));
        return action;
    }

    /**
     * FutureMock.
     */
    private static class FutureMock extends ConditionalFuture {

        FutureMock(Callable<ConditionalSubmitter.ConditionalTask> task) {
            super(task);
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return true;
        }
    }
}

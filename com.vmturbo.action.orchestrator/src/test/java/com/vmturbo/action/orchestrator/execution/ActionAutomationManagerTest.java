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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.RollBackToAcceptedEvent;
import com.vmturbo.action.orchestrator.action.ActionSchedule;
import com.vmturbo.action.orchestrator.approval.ActionApprovalSender;
import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor.ActionExecutionTask;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;

/**
 * Tests for ActionAutomationManager.
 */
public class ActionAutomationManagerTest {

    private final AutomatedActionExecutor executor = Mockito.mock(AutomatedActionExecutor.class);
    private ActionApprovalSender actionApprovalSender = Mockito.mock(ActionApprovalSender.class);
    private final ActionAutomationManager automationManager = new ActionAutomationManager(
        executor, actionApprovalSender);
    private final ActionStore actionStore = Mockito.mock(ActionStore.class);

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
        Action action = mock(Action.class);
        List<ActionExecutionTask> actionExecutionTaskList = new ArrayList<>();
        actionExecutionTaskList.add(new AutomatedActionExecutor.ActionExecutionTask(action,
            new FutureMock<>(action)));
        when(action.getState()).thenReturn(ActionState.QUEUED);
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
        final ActionSchedule nonActiveSchedule = Mockito.mock(ActionSchedule.class);
        final com.vmturbo.action.orchestrator.action.Action action =
            mock(com.vmturbo.action.orchestrator.action.Action.class);
        final List<ActionExecutionTask> actionExecutionTaskList = new ArrayList<>();
        actionExecutionTaskList.add(
            new AutomatedActionExecutor.ActionExecutionTask(action, new FutureMock<>(action)));
        Mockito.when(action.getState()).thenReturn(ActionState.QUEUED);
        Mockito.when(executor.executeAutomatedFromStore(any())).thenReturn(actionExecutionTaskList);
        automationManager.updateAutomation(actionStore);

        Mockito.when(actionStore.getAction(action.getId())).thenReturn(Optional.of(action));
        Mockito.when(action.getSchedule()).thenReturn(Optional.of(nonActiveSchedule));
        Mockito.when(nonActiveSchedule.isActiveScheduleNow()).thenReturn(false);
        // emulate next market cycle
        automationManager.updateAutomation(actionStore);
        Mockito.verify(action).receive(Mockito.any(RollBackToAcceptedEvent.class));
    }

    /**
     * FutureMock.
     *
     * @param <V> result type.
     */
    private class FutureMock<V> implements Future<V> {

        V result;

        /**
         * Create new FutureMock.
         *
         * @param result the result.
         */
        private FutureMock(V result) {
            this.result = result;
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

        @Override
        public V get() {
            return result;
        }

        @Override
        public V get(long timeout, TimeUnit unit) {
            return result;
        }
    }
}
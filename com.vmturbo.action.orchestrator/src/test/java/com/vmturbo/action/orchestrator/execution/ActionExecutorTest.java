package com.vmturbo.action.orchestrator.execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.action.orchestrator.execution.ActionExecutor.SynchronousExecutionException;
import com.vmturbo.action.orchestrator.execution.ActionExecutor.SynchronousExecutionState;
import com.vmturbo.action.orchestrator.execution.ActionExecutor.SynchronousExecutionStateFactory;
import com.vmturbo.action.orchestrator.execution.ActionExecutor.SynchronousExecutionStateFactory.DefaultSynchronousExecutionStateFactory;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionResponse;
import com.vmturbo.common.protobuf.topology.ActionExecutionMoles.ActionExecutionServiceMole;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.ActionsLost;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.ActionsLost.ActionIds;

/**
 * Unit tests for the {@link ActionExecutor} class.
 */
public class ActionExecutorTest {

    /**
     * The class under test
     */
    private ActionExecutor actionExecutor;

    private final long probeId = 10;
    private final long targetId = 7;

    private final ActionExecutionServiceMole actionExecutionBackend =
            Mockito.spy(new ActionExecutionServiceMole());

    private final SynchronousExecutionStateFactory executionStateFactory =
            mock(SynchronousExecutionStateFactory.class);

    // A test helper class for building move actions
    TestActionBuilder testActionBuilder = new TestActionBuilder();

    @Captor
    private ArgumentCaptor<ExecuteActionRequest> actionSpecCaptor;

    @Rule
    public final GrpcTestServer server =
            GrpcTestServer.newServer(actionExecutionBackend);
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private Optional<WorkflowDTO.Workflow> workflowOpt = Optional.empty();

    private Clock clock = new MutableFixedClock(1_000_000);

    private final long targetEntityId = 1L;

    private final ActionDTO.Action testAction =
        testActionBuilder.buildMoveAction(targetEntityId, 2L, 1, 3L, 1);

    private final LicenseCheckClient licenseCheckClient = mock(LicenseCheckClient.class);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        // license check client by default will act as if a valid license is installed.
        when(licenseCheckClient.hasValidNonExpiredLicense()).thenReturn(true);
        // The class under test
        actionExecutor = new ActionExecutor(server.getChannel(), executionStateFactory, 1,
                TimeUnit.HOURS, licenseCheckClient);
    }

    /**
     * Test completing with error.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testExecutionStateError() throws Exception {
        final ActionFailure actionFailure = ActionFailure.newBuilder()
            .setErrorDescription("foo")
            .setActionId(12)
            .build();
        final SynchronousExecutionState state = new DefaultSynchronousExecutionStateFactory(clock).newState();
        state.complete(new SynchronousExecutionException(actionFailure));

        try {
            state.waitForActionCompletion(1, TimeUnit.MILLISECONDS);
            Assert.fail("Expected exception.");
        } catch (SynchronousExecutionException e) {
            assertThat(e.getFailure(), is(actionFailure));
        }
    }

    /**
     * Test completing with timeout.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testExecutionStateTimeout() throws Exception {
        final SynchronousExecutionState state = new DefaultSynchronousExecutionStateFactory(clock).newState();

        expectedException.expect(TimeoutException.class);
        state.waitForActionCompletion(1, TimeUnit.MILLISECONDS);
    }

    /**
     * Test completing successfully.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testExecutionStateComplete() throws Exception {
        final SynchronousExecutionState state = new DefaultSynchronousExecutionStateFactory(clock).newState();
        state.complete(null);

        state.waitForActionCompletion(1, TimeUnit.MILLISECONDS);
    }

    /**
     * Test "started before" method to make sure it compares things properly.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testExecutionStateStartedBefore() throws Exception {
        final SynchronousExecutionState state = new DefaultSynchronousExecutionStateFactory(clock).newState();

        assertTrue(state.startedBefore(clock.millis() + 1));
        assertFalse(state.startedBefore(clock.millis()));
        assertFalse(state.startedBefore(clock.millis() - 1));
    }

    /**
     * Test starting an asynchronous move.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testMove() {

        try {
            actionExecutor.execute(targetId, testAction, workflowOpt);
        } catch (ExecutionStartException e) {
            // We expect this to happen, since the backend implementation
            // is not implemented.
        }

        // However, the backend should have been called, and we can capture
        // and examine the arguments.
        verify(actionExecutionBackend).executeAction(actionSpecCaptor.capture(), any());
        final ExecuteActionRequest sentSpec = actionSpecCaptor.getValue();
        assertTrue(sentSpec.hasActionInfo());
        Assert.assertEquals(ActionTypeCase.MOVE, sentSpec.getActionInfo().getActionTypeCase());
        final Move move = sentSpec.getActionInfo().getMove();
        Assert.assertEquals(targetId, sentSpec.getTargetId());
        Assert.assertEquals(targetEntityId, move.getTarget().getId());
        Assert.assertEquals(1, move.getChangesCount());
        Assert.assertEquals(2, move.getChanges(0).getSource().getId());
        Assert.assertEquals(3, move.getChanges(0).getDestination().getId());
    }

    /**
     * Test timing out of a synchronous move.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testSynchronousMoveTimeout() throws Exception {
        doReturn(ExecuteActionResponse.getDefaultInstance())
            .when(actionExecutionBackend).executeAction(any());

        SynchronousExecutionState state = mock(SynchronousExecutionState.class);
        doThrow(new TimeoutException("BOO")).when(state).waitForActionCompletion(anyLong(), any());
        when(executionStateFactory.newState()).thenReturn(state);

        try {
            actionExecutor.executeSynchronously(targetId, testAction, workflowOpt);
            Assert.fail("Expected synchronous execution exception.");
        } catch (SynchronousExecutionException e) {
            Assert.assertEquals(testAction.getId(), e.getFailure().getActionId());
            assertTrue(e.getFailure().getErrorDescription().contains("Action timed out"));
        }
    }

    /**
     * Test success of a synchronous move.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testSynchronousMoveSucceed() throws Exception {
        SynchronousExecutionState state = mock(SynchronousExecutionState.class);
        when(executionStateFactory.newState()).thenReturn(state);

        // This should return, because the mock SynchronousExecutionState is not blocking.
        actionExecutor.executeSynchronously(targetId, testAction, workflowOpt);

        actionExecutor.onActionSuccess(ActionSuccess.newBuilder()
            .setActionId(testAction.getId())
            .build());

        // We should find the state, and complete it.
        verify(state).complete(null);
    }

    /**
     * Test failure of a synchronous move.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testSynchronousMoveFailed() throws Exception {
        SynchronousExecutionState state = mock(SynchronousExecutionState.class);
        when(executionStateFactory.newState()).thenReturn(state);

        // This should return, because the mock SynchronousExecutionState is not blocking.
        actionExecutor.executeSynchronously(targetId, testAction, workflowOpt);

        // Notify about the failure.
        ActionFailure failure = ActionFailure.newBuilder()
            .setActionId(testAction.getId())
            .setErrorDescription("boo")
            .build();
        actionExecutor.onActionFailure(failure);

        // We should find the state, and complete it.
        ArgumentCaptor<SynchronousExecutionException> exceptionCaptor = ArgumentCaptor.forClass(SynchronousExecutionException.class);
        verify(state).complete(exceptionCaptor.capture());

        assertThat(exceptionCaptor.getValue().getFailure(), is(failure));
    }

    /**
     * Test losing state of specific actions.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testSynchronousMoveLostSpecific() throws Exception {
        SynchronousExecutionState state1 = mock(SynchronousExecutionState.class);
        SynchronousExecutionState state2 = mock(SynchronousExecutionState.class);
        when(executionStateFactory.newState()).thenReturn(state1, state2);

        // This should return, because the mock SynchronousExecutionState is not blocking.
        actionExecutor.executeSynchronously(targetId, testAction, workflowOpt);
        // Fake-execute another action. We want to make sure this one DOESN'T get lost.
        actionExecutor.executeSynchronously(targetId, testAction.toBuilder()
            .setId(testAction.getId() + 1)
            .build(), workflowOpt);

        final ActionsLost lost = ActionsLost.newBuilder()
            .setLostActionId(ActionIds.newBuilder()
                .addActionIds(testAction.getId()))
            .build();
        actionExecutor.onActionsLost(lost);

        // We should find the state, and complete it.
        ArgumentCaptor<SynchronousExecutionException> exceptionCaptor = ArgumentCaptor.forClass(SynchronousExecutionException.class);
        verify(state1).complete(exceptionCaptor.capture());

        assertThat(exceptionCaptor.getValue().getFailure(), is(ActionFailure.newBuilder()
            .setActionId(testAction.getId())
            .setErrorDescription("Topology Processor lost action state.")
            .build()));

        // The other action shouldn't have completed.
        verify(state2, never()).complete(any());
    }

    /**
     * Test losing state of all actions before a timestamp.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testSynchronousMoveLostAllBeforeTime() throws Exception {
        final ActionsLost lost = ActionsLost.newBuilder()
            .setBeforeTime(1_000)
            .build();
        // The first action started before the "before time."
        SynchronousExecutionState state1 = mock(SynchronousExecutionState.class);
        when(state1.startedBefore(lost.getBeforeTime())).thenReturn(true);
        // The second action started after the "before time."
        SynchronousExecutionState state2 = mock(SynchronousExecutionState.class);
        when(state2.startedBefore(lost.getBeforeTime())).thenReturn(false);
        when(executionStateFactory.newState()).thenReturn(state1, state2);

        // This should return, because the mock SynchronousExecutionState is not blocking.
        actionExecutor.executeSynchronously(targetId, testAction, workflowOpt);
        // Fake-execute another action. We want to make sure this one DOESN'T get lost.
        actionExecutor.executeSynchronously(targetId, testAction.toBuilder()
            .setId(testAction.getId() + 1)
            .build(), workflowOpt);
        actionExecutor.onActionsLost(lost);

        // We should find the state for the action that started before the time, and complete it.
        final ArgumentCaptor<SynchronousExecutionException> exceptionCaptor =
            ArgumentCaptor.forClass(SynchronousExecutionException.class);
        verify(state1).complete(exceptionCaptor.capture());

        assertThat(exceptionCaptor.getValue().getFailure(), is(ActionFailure.newBuilder()
            .setActionId(testAction.getId())
            .setErrorDescription("Topology Processor lost action state.")
            .build()));

        // The other action shouldn't have completed.
        verify(state2, never()).complete(any());
    }

    /**
     * Verify that an action can't be completed when the license is invalid.
     */
    @Test(expected = ExecutionStartException.class)
    public void testActionWithInvalidLicense() throws ExecutionStartException {
        when(licenseCheckClient.hasValidNonExpiredLicense()).thenReturn(false);
        actionExecutor.execute(targetId, testAction, workflowOpt);
    }
}

package com.vmturbo.action.orchestrator.action;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.ActionEvent.AutomaticAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.CannotExecuteEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ManualAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.NotRecommendedEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ProgressEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.SuccessEvent;
import com.vmturbo.action.orchestrator.store.EntitySettingsCache;
import com.vmturbo.action.orchestrator.store.EntityTypeMap;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ClearingDecision.Reason;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ExecutionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep.Status;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;

/**
 * Integration tests for the {@link ActionStateMachine} interaction with {@link Action}s.
 */
public class ActionStateMachineTest {

    private final EntitySettingsCache entitySettingsCache = mock(EntitySettingsCache.class);
    private final EntityTypeMap entityTypeMap = mock(EntityTypeMap.class);

    private final ActionDTO.Action move = ActionDTO.Action.newBuilder()
                    .setId(0)
                    .setImportance(0)
                    .setInfo(ActionTest.makeMoveInfo(1, 2, 2))
                    .setExplanation(Explanation.newBuilder().build())
                .build();

    private final long actionPlanId = 4;
    private final String userUuid = "5";
    private final long clearingPlanId = 6;
    private final long probeId = 7;
    private final long targetId = 8;

    @Before
    public void setup() {
        when(entityTypeMap.getTypeForEntity(anyLong())).thenReturn(Optional.empty());
    }

    @Test
    public void testManuallyAccept() {
        when(entitySettingsCache.getSettingsForEntity(eq(1L)))
            .thenReturn(ActionOrchestratorTestUtils.makeActionModeSetting(ActionMode.MANUAL));

        Action action = new Action(move, entitySettingsCache, entityTypeMap, actionPlanId);
        assertEquals(ActionState.QUEUED, action.receive(new ManualAcceptanceEvent(userUuid, targetId)).getAfterState());

        Assert.assertEquals(ActionState.QUEUED, action.getState());
        Assert.assertEquals(
            ExecutionDecision.Reason.MANUALLY_ACCEPTED,
            action.getDecision().get().getExecutionDecision().getReason()
        );
        Assert.assertEquals(
                userUuid,
            action.getDecision().get().getExecutionDecision().getUserUuid()
        );
        verifyQueuedExecutionStep(action.getExecutableStep().get());
    }

    @Test
    public void testAutomaticallyAccept() {
        when(entitySettingsCache.getSettingsForEntity(eq(1L)))
                .thenReturn(ActionOrchestratorTestUtils.makeActionModeSetting(ActionMode.AUTOMATIC));
        Action action = new Action(move, entitySettingsCache, entityTypeMap, actionPlanId);
        assertEquals(ActionState.QUEUED, action.receive(new AutomaticAcceptanceEvent(userUuid, targetId)).getAfterState());

        Assert.assertEquals(ActionState.QUEUED, action.getState());
        Assert.assertEquals(
            ExecutionDecision.Reason.AUTOMATICALLY_ACCEPTED,
            action.getDecision().get().getExecutionDecision().getReason()
        );
        Assert.assertEquals(
                userUuid,
            action.getDecision().get().getExecutionDecision().getUserUuid()
        );
        verifyQueuedExecutionStep(action.getExecutableStep().get());
    }

    @Test
    public void testBeginExecution() {
        when(entitySettingsCache.getSettingsForEntity(eq(1L)))
                .thenReturn(ActionOrchestratorTestUtils.makeActionModeSetting(ActionMode.AUTOMATIC));
        Action action = new Action(move, entitySettingsCache, entityTypeMap, actionPlanId);
        assertEquals(ActionState.QUEUED, action.receive(new AutomaticAcceptanceEvent(userUuid, targetId)).getAfterState());
        assertEquals(ActionState.IN_PROGRESS, action.receive(new BeginExecutionEvent()).getAfterState());

        Assert.assertEquals(ActionState.IN_PROGRESS, action.getState());
    }

    @Test
    public void testNotRecommended() {
        Action action = new Action(move, actionPlanId);
        assertEquals(ActionState.CLEARED, action.receive(new NotRecommendedEvent(clearingPlanId)).getAfterState());

        Assert.assertEquals(ActionState.CLEARED, action.getState());
        Assert.assertEquals(
            Reason.NO_LONGER_RECOMMENDED,
            action.getDecision().get().getClearingDecision().getReason()
        );
        Assert.assertEquals(
            clearingPlanId,
            action.getDecision().get().getClearingDecision().getActionPlanId()
        );
    }

    @Test
    public void testCannotExecute() {
        Action action = new Action(move, actionPlanId);
        assertEquals(ActionState.CLEARED, action.receive(new CannotExecuteEvent(probeId)).getAfterState());

        Assert.assertEquals(ActionState.CLEARED, action.getState());
        Assert.assertEquals(
            Reason.PROBE_UNABLE_TO_EXECUTE,
            action.getDecision().get().getClearingDecision().getReason()
        );
        Assert.assertEquals(
            probeId,
            action.getDecision().get().getClearingDecision().getProbeId()
        );
    }

    @Test
    public void testExecutionCreatesExecutionStep() {
        when(entitySettingsCache.getSettingsForEntity(eq(1L)))
                .thenReturn(ActionOrchestratorTestUtils.makeActionModeSetting(ActionMode.MANUAL));
        Action action = new Action(move, entitySettingsCache, entityTypeMap, actionPlanId);

        action.receive(new ManualAcceptanceEvent(userUuid, targetId));
        Assert.assertEquals(Status.QUEUED, action.getExecutableStep().get().getStatus());

        action.receive(new BeginExecutionEvent());
        Assert.assertTrue(action.getExecutableStep().isPresent());
        Assert.assertEquals(Status.IN_PROGRESS, action.getExecutableStep().get().getStatus());
        Assert.assertEquals(0, (int)action.getExecutableStep().flatMap(ExecutableStep::getProgressPercentage).get());
        Assert.assertNotNull(action.getExecutableStep().get().getStartTime());

        Assert.assertEquals(
            ExecutableStep.INITIAL_EXECUTION_DESCRIPTION,
            action.getExecutableStep().flatMap(ExecutableStep::getProgressDescription).get()
        );
    }

    @Test
    public void testProgressUpdates() {
        when(entitySettingsCache.getSettingsForEntity(eq(1L)))
                .thenReturn(ActionOrchestratorTestUtils.makeActionModeSetting(ActionMode.MANUAL));
        Action action = new Action(move, entitySettingsCache, entityTypeMap, actionPlanId);
        action.receive(new ManualAcceptanceEvent(userUuid, targetId));
        action.receive(new BeginExecutionEvent());

        Assert.assertEquals(ActionState.IN_PROGRESS, action.receive(new ProgressEvent(10, "Setting clip-jawed monodish to 6")).getAfterState());
        Assert.assertEquals(10, (int)action.getExecutableStep().flatMap(ExecutableStep::getProgressPercentage).get());
        Assert.assertEquals(
            "Setting clip-jawed monodish to 6",
            action.getExecutableStep().flatMap(ExecutableStep::getProgressDescription).get()
        );

        Assert.assertEquals(ActionState.IN_PROGRESS, action.receive(new ProgressEvent(93, "Decompressing amplitude device")).getAfterState());
        Assert.assertEquals(93, (int)action.getExecutableStep().flatMap(ExecutableStep::getProgressPercentage).get());
        Assert.assertEquals(
            "Decompressing amplitude device",
            action.getExecutableStep().flatMap(ExecutableStep::getProgressDescription).get()
        );
    }

    @Test
    public void testActionSuccess() {
        when(entitySettingsCache.getSettingsForEntity(eq(1L)))
                .thenReturn(ActionOrchestratorTestUtils.makeActionModeSetting(ActionMode.MANUAL));
        Action action = new Action(move, entitySettingsCache, entityTypeMap, actionPlanId);
        action.receive(new ManualAcceptanceEvent(userUuid, targetId));
        action.receive(new BeginExecutionEvent());

        ExecutableStep executableStep = action.getExecutableStep().get();
        Assert.assertEquals(Status.IN_PROGRESS, executableStep.getStatus());

        Assert.assertEquals(ActionState.SUCCEEDED, action.receive(new SuccessEvent()).getAfterState());
        Assert.assertEquals(Status.SUCCESS, action.getExecutableStep().get().getStatus());
        Assert.assertEquals(Status.SUCCESS, executableStep.getStatus());
        Assert.assertEquals(0, executableStep.getErrors().size());
        Assert.assertNotNull(executableStep.getCompletionTime());
    }

    @Test
    public void testActionFailure() {
        when(entitySettingsCache.getSettingsForEntity(eq(1L)))
                .thenReturn(ActionOrchestratorTestUtils.makeActionModeSetting(ActionMode.MANUAL));
        Action action = new Action(move, entitySettingsCache, entityTypeMap, actionPlanId);
        action.receive(new ManualAcceptanceEvent(userUuid, targetId));
        action.receive(new BeginExecutionEvent());

        ExecutableStep executableStep = action.getExecutableStep().get();
        Assert.assertEquals(Status.IN_PROGRESS, executableStep.getStatus());
        Assert.assertEquals(0, executableStep.getErrors().size());
        Assert.assertFalse(executableStep.getCompletionTime().isPresent());

        Assert.assertEquals(ActionState.FAILED, action.receive(new FailureEvent("Insufficient haptic fozzlers")).getAfterState());
        Assert.assertEquals("Insufficient haptic fozzlers", executableStep.getErrors().get(0));
        Assert.assertEquals(Status.FAILED, executableStep.getStatus());
        Assert.assertTrue(executableStep.getCompletionTime().isPresent());
    }

    private void verifyQueuedExecutionStep(@Nonnull final ExecutableStep executableStep) {
        Objects.requireNonNull(executableStep);

        Assert.assertFalse(executableStep.getStartTime().isPresent());
        Assert.assertEquals(Status.QUEUED, executableStep.getStatus());
        Assert.assertNotNull(executableStep.getEnqueueTime());
    }
}
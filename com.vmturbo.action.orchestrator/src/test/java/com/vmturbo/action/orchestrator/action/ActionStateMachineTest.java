package com.vmturbo.action.orchestrator.action;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.ActionEvent.AutomaticAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.CannotExecuteEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ManualAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.NotRecommendedEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.AfterFailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.AfterSuccessEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.PrepareExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ProgressEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.SuccessEvent;
import com.vmturbo.action.orchestrator.store.EntitiesCache;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ClearingDecision.Reason;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ExecutionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep.Status;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.components.common.setting.EntitySettingSpecs;

/**
 * Integration tests for the {@link ActionStateMachine} interaction with {@link Action}s.
 */
public class ActionStateMachineTest {

    private final EntitiesCache entitySettingsCache = mock(EntitiesCache.class);
    private final ActionTranslator actionTranslator = Mockito.spy(new ActionTranslator(actionStream ->
            actionStream.map(action -> {
                action.getActionTranslation().setPassthroughTranslationSuccess();
                return action;
            })));
    private final ActionModeCalculator actionModeCalculator = new ActionModeCalculator(actionTranslator);

    private final ActionDTO.Action move = ActionDTO.Action.newBuilder()
        .setId(0)
        .setImportance(0)
        .setSupportingLevel(SupportLevel.SUPPORTED)
        .setInfo(TestActionBuilder.makeMoveInfo(1, 2, 1, 2, 1))
        .setExplanation(Explanation.newBuilder().build())
        .build();

    private final long actionPlanId = 4;
    private final String userUuid = "5";
    private final long clearingPlanId = 6;
    private final long probeId = 7;
    private final long targetId = 8;

    @Test
    public void testManuallyAccept() {
        when(entitySettingsCache.getSettingsForEntity(eq(1L)))
            .thenReturn(ActionOrchestratorTestUtils.makeActionModeSetting(ActionMode.MANUAL));

        Action action = new Action(move, entitySettingsCache, actionPlanId, actionModeCalculator);
        action.getActionTranslation().setPassthroughTranslationSuccess();
        assertEquals(ActionState.QUEUED, action.receive(new ManualAcceptanceEvent(userUuid, targetId)).getAfterState());

        Assert.assertEquals(ActionState.QUEUED, action.getState());
        verifyQueuedExecutionStep(action.getCurrentExecutableStep().get());
    }

    @Test
    public void testAutomaticallyAccept() {
        when(entitySettingsCache.getSettingsForEntity(eq(1L)))
                .thenReturn(ActionOrchestratorTestUtils.makeActionModeSetting(ActionMode.AUTOMATIC));
        Action action = new Action(move, entitySettingsCache, actionPlanId, actionModeCalculator);
        assertEquals(ActionState.QUEUED, action.receive(new AutomaticAcceptanceEvent(userUuid, targetId)).getAfterState());

        assertEquals(ActionState.QUEUED, action.getState());
        verifyQueuedExecutionStep(action.getCurrentExecutableStep().get());
    }

    @Test
    public void testBeginExecutionAutomaticAction() {
        when(entitySettingsCache.getSettingsForEntity(eq(1L)))
                .thenReturn(ActionOrchestratorTestUtils.makeActionModeSetting(ActionMode.AUTOMATIC));
        Action action = new Action(move, entitySettingsCache, actionPlanId, actionModeCalculator);
        assertEquals(ActionState.QUEUED, action.receive(new AutomaticAcceptanceEvent(userUuid, targetId)).getAfterState());
        assertEquals(ActionState.PRE_IN_PROGRESS, action.receive(new PrepareExecutionEvent()).getAfterState());
        assertEquals(ActionState.IN_PROGRESS, action.receive(new BeginExecutionEvent()).getAfterState());
        assertEquals(
                ExecutionDecision.Reason.AUTOMATICALLY_ACCEPTED,
                action.getDecision().get().getExecutionDecision().getReason()
        );
        assertEquals(userUuid,
                action.getDecision().get().getExecutionDecision().getUserUuid()
        );

        Assert.assertEquals(ActionState.IN_PROGRESS, action.getState());
    }

    @Test
    public void testBeginExecutionManualAction() {
        when(entitySettingsCache.getSettingsForEntity(eq(1L)))
                .thenReturn(ActionOrchestratorTestUtils.makeActionModeSetting(ActionMode.MANUAL));
        Action action = new Action(move, entitySettingsCache, actionPlanId, actionModeCalculator);
        assertEquals(ActionState.QUEUED, action.receive(new ManualAcceptanceEvent(userUuid, targetId)).getAfterState());
        assertEquals(ActionState.PRE_IN_PROGRESS, action.receive(new PrepareExecutionEvent()).getAfterState());
        assertEquals(ActionState.IN_PROGRESS, action.receive(new BeginExecutionEvent()).getAfterState());
        assertEquals(
                ExecutionDecision.Reason.MANUALLY_ACCEPTED,
                action.getDecision().get().getExecutionDecision().getReason()
        );
        assertEquals(userUuid,
                action.getDecision().get().getExecutionDecision().getUserUuid()
        );

        Assert.assertEquals(ActionState.IN_PROGRESS, action.getState());
    }

    @Test
    public void testNotRecommended() {
        Action action = new Action(move, actionPlanId, actionModeCalculator);
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
    public void testQueuedToClearedActionStateChange() {
        // Test the state transition from QUEUED to CLEARED state.
        Action action = new Action(move, entitySettingsCache, actionPlanId, actionModeCalculator);
        action.receive(new AutomaticAcceptanceEvent(userUuid, targetId));
        assertEquals(ActionState.QUEUED, action.getState());
        assertEquals(ActionState.CLEARED,
                action.receive(new NotRecommendedEvent(clearingPlanId)).getAfterState());
    }

    @Test
    public void testCannotExecute() {
        Action action = new Action(move, actionPlanId, actionModeCalculator);
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
        Action action = new Action(move, entitySettingsCache, actionPlanId, actionModeCalculator);

        action.receive(new ManualAcceptanceEvent(userUuid, targetId));
        Assert.assertEquals(Status.QUEUED, action.getCurrentExecutableStep().get().getStatus());

        action.receive(new PrepareExecutionEvent());
        action.receive(new BeginExecutionEvent());
        Assert.assertTrue(action.getCurrentExecutableStep().isPresent());
        Assert.assertEquals(Status.IN_PROGRESS, action.getCurrentExecutableStep().get().getStatus());
        Assert.assertEquals(0, (int)action.getCurrentExecutableStep().flatMap(ExecutableStep::getProgressPercentage).get());
        Assert.assertNotNull(action.getCurrentExecutableStep().get().getStartTime());

        Assert.assertEquals(
            ExecutableStep.INITIAL_EXECUTION_DESCRIPTION,
            action.getCurrentExecutableStep().flatMap(ExecutableStep::getProgressDescription).get()
        );
    }

    @Test
    public void testProgressUpdates() {
        when(entitySettingsCache.getSettingsForEntity(eq(1L)))
                .thenReturn(ActionOrchestratorTestUtils.makeActionModeSetting(ActionMode.MANUAL));
        Action action = new Action(move, entitySettingsCache, actionPlanId, actionModeCalculator);
        action.receive(new ManualAcceptanceEvent(userUuid, targetId));
        action.receive(new PrepareExecutionEvent());
        action.receive(new BeginExecutionEvent());

        Assert.assertEquals(ActionState.IN_PROGRESS, action.receive(new ProgressEvent(10, "Setting clip-jawed monodish to 6")).getAfterState());
        Assert.assertEquals(10, (int)action.getCurrentExecutableStep().flatMap(ExecutableStep::getProgressPercentage).get());
        Assert.assertEquals(
            "Setting clip-jawed monodish to 6",
            action.getCurrentExecutableStep().flatMap(ExecutableStep::getProgressDescription).get()
        );

        Assert.assertEquals(ActionState.IN_PROGRESS, action.receive(new ProgressEvent(93, "Decompressing amplitude device")).getAfterState());
        Assert.assertEquals(93, (int)action.getCurrentExecutableStep().flatMap(ExecutableStep::getProgressPercentage).get());
        Assert.assertEquals(
            "Decompressing amplitude device",
            action.getCurrentExecutableStep().flatMap(ExecutableStep::getProgressDescription).get()
        );
    }

    @Test
    public void testActionSuccess() {
        when(entitySettingsCache.getSettingsForEntity(eq(1L)))
                .thenReturn(ActionOrchestratorTestUtils.makeActionModeSetting(ActionMode.MANUAL));
        Action action = new Action(move, entitySettingsCache, actionPlanId, actionModeCalculator);
        action.receive(new ManualAcceptanceEvent(userUuid, targetId));
        action.receive(new PrepareExecutionEvent());
        action.receive(new BeginExecutionEvent());

        ExecutableStep executableStep = action.getCurrentExecutableStep().get();
        Assert.assertEquals(Status.IN_PROGRESS, executableStep.getStatus());

        Assert.assertEquals(ActionState.POST_IN_PROGRESS,
            action.receive(new SuccessEvent()).getAfterState());
        Assert.assertEquals(ActionState.SUCCEEDED,
            action.receive(new AfterSuccessEvent()).getAfterState());
        Assert.assertEquals(Status.SUCCESS, action.getCurrentExecutableStep().get().getStatus());
        Assert.assertEquals(Status.SUCCESS, executableStep.getStatus());
        Assert.assertEquals(0, executableStep.getErrors().size());
        Assert.assertNotNull(executableStep.getCompletionTime());
    }

    @Test
    public void testActionFailure() {
        when(entitySettingsCache.getSettingsForEntity(eq(1L)))
                .thenReturn(ActionOrchestratorTestUtils.makeActionModeSetting(ActionMode.MANUAL));
        Action action = new Action(move, entitySettingsCache, actionPlanId, actionModeCalculator);
        action.receive(new ManualAcceptanceEvent(userUuid, targetId));
        action.receive(new PrepareExecutionEvent());
        action.receive(new BeginExecutionEvent());

        ExecutableStep executableStep = action.getCurrentExecutableStep().get();
        Assert.assertEquals(Status.IN_PROGRESS, executableStep.getStatus());
        Assert.assertEquals(0, executableStep.getErrors().size());
        Assert.assertFalse(executableStep.getCompletionTime().isPresent());

        final String errorDescription = "Insufficient haptic fozzlers";
        Assert.assertEquals(ActionState.POST_IN_PROGRESS,
            action.receive(new FailureEvent(errorDescription)).getAfterState());
        Assert.assertEquals(ActionState.FAILED,
            action.receive(new AfterFailureEvent(errorDescription)).getAfterState());
        Assert.assertEquals(errorDescription, executableStep.getErrors().get(0));
        Assert.assertEquals(Status.FAILED, executableStep.getStatus());
        Assert.assertTrue(executableStep.getCompletionTime().isPresent());
    }

    @Test
    public void testPostAfterActionSuccess() {
        // Define a workflow to be executed in the POST phase
        when(entitySettingsCache.getSettingsForEntity(eq(1L)))
            .thenReturn(makePostMoveWorkflowSetting(ActionMode.MANUAL, "postMove.sh"));
        Action action = new Action(move, entitySettingsCache, actionPlanId, actionModeCalculator);
        action.receive(new ManualAcceptanceEvent(userUuid, targetId));
        action.receive(new PrepareExecutionEvent());
        action.receive(new BeginExecutionEvent());

        ExecutableStep executableStep = action.getCurrentExecutableStep().get();
        Assert.assertEquals(Status.IN_PROGRESS, executableStep.getStatus());

        // Action will progress to POST_IN_PROGRESS after successful completion of the main action execution
        Assert.assertEquals(ActionState.POST_IN_PROGRESS, action.receive(new SuccessEvent()).getAfterState());
        // Action will not progress to SUCCEEDED when there is a POST workflow still to execute
        Assert.assertEquals(ActionState.POST_IN_PROGRESS,
            action.receive(new AfterSuccessEvent()).getAfterState());
        // Action will finally enter the SUCCEEDED state, when the POST workflow completes successfully
        Assert.assertEquals(ActionState.SUCCEEDED,
            action.receive(new SuccessEvent()).getAfterState());
        Assert.assertEquals(Status.SUCCESS, action.getCurrentExecutableStep().get().getStatus());
        Assert.assertEquals(Status.SUCCESS, executableStep.getStatus());
        Assert.assertEquals(0, executableStep.getErrors().size());
        Assert.assertNotNull(executableStep.getCompletionTime());
    }

    @Test
    public void testPostAfterActionFailure() {
        // Define a workflow to be executed in the POST phase
        when(entitySettingsCache.getSettingsForEntity(eq(1L)))
            .thenReturn(makePostMoveWorkflowSetting(ActionMode.MANUAL, "postMove.sh"));
        Action action = new Action(move, entitySettingsCache, actionPlanId, actionModeCalculator);
        action.receive(new ManualAcceptanceEvent(userUuid, targetId));
        action.receive(new PrepareExecutionEvent());
        action.receive(new BeginExecutionEvent());

        ExecutableStep executableStep = action.getCurrentExecutableStep().get();
        Assert.assertEquals(Status.IN_PROGRESS, executableStep.getStatus());
        Assert.assertEquals(0, executableStep.getErrors().size());
        Assert.assertFalse(executableStep.getCompletionTime().isPresent());


        final String errorDescription = "Insufficient haptic fozzlers";
        // Action will enter POST phase to execute the POST workflow, even after action failure
        Assert.assertEquals(ActionState.POST_IN_PROGRESS,
            action.receive(new FailureEvent(errorDescription)).getAfterState());
        // Action will not progress to FAILED when there is a POST workflow still to execute
        Assert.assertEquals(ActionState.POST_IN_PROGRESS,
            action.receive(new AfterFailureEvent(errorDescription)).getAfterState());
        // Action will not transition to SUCCEEDED, since the main execution failed
        Assert.assertEquals(ActionState.POST_IN_PROGRESS,
            action.receive(new SuccessEvent()).getAfterState());
        // Action will finally enter the FAILED state, when the POST workflow completes successfully
        final String postFailureDescription = "Failing action due to previous failure.";
        Assert.assertEquals(ActionState.FAILED,
            action.receive(new FailureEvent(postFailureDescription)).getAfterState());
        // Get the results of the main action execution phase
        executableStep = action.getExecutableSteps().get(ActionState.IN_PROGRESS);
        Assert.assertEquals(errorDescription, executableStep.getErrors().get(0));
        Assert.assertEquals(Status.FAILED, executableStep.getStatus());
        Assert.assertTrue(executableStep.getCompletionTime().isPresent());
        // Get the results of the POST action execution phase
        ExecutableStep postExecutableStep = action.getExecutableSteps().get(ActionState.POST_IN_PROGRESS);
        Assert.assertEquals(postFailureDescription, postExecutableStep.getErrors().get(0));
        Assert.assertEquals(Status.FAILED, postExecutableStep.getStatus());
        Assert.assertTrue(postExecutableStep.getCompletionTime().isPresent());

    }

    private static Map<String, Setting> makePostMoveWorkflowSetting(ActionMode mode, String workflowName) {
        final String settingName = EntitySettingSpecs.PostMoveActionWorkflow.getSettingName();
        return ImmutableMap.of(settingName, Setting.newBuilder()
            .setSettingSpecName(settingName)
            .setStringSettingValue(StringSettingValue.newBuilder()
                .setValue(workflowName)).build(),
            "move", Setting.newBuilder()
                .setSettingSpecName("move")
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                    .setValue(mode.toString()).build())
            .build());
    }

    private void verifyQueuedExecutionStep(@Nonnull final ExecutableStep executableStep) {
        Objects.requireNonNull(executableStep);

        Assert.assertFalse(executableStep.getStartTime().isPresent());
        Assert.assertEquals(Status.QUEUED, executableStep.getStatus());
        Assert.assertNotNull(executableStep.getEnqueueTime());
    }
}

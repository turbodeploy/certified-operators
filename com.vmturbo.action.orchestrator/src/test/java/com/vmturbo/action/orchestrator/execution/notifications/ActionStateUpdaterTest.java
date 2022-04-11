package com.vmturbo.action.orchestrator.execution.notifications;

import static com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils.makeActionModeAndWorkflowSettings;
import static com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils.makeActionModeSetting;
import static com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils.makeGeneralActionModeSetting;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.Action.SerializationState;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ManualAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.QueuedEvent;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ExecutableStep;
import com.vmturbo.action.orchestrator.action.ExecutedActionsChangeWindowDao;
import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.action.orchestrator.api.ActionOrchestratorNotificationSender;
import com.vmturbo.action.orchestrator.audit.ActionAuditSender;
import com.vmturbo.action.orchestrator.execution.ActionExecutionStore;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.FailedCloudVMGroupProcessor;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.store.query.QueryableActionViews;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep.Status;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.schedule.ScheduleProto;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.components.common.setting.ActionSettingType;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResponse;
import com.vmturbo.test.utils.FeatureFlagTestRule;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.ActionsLost;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.ActionsLost.ActionIds;

/**
 * Tests for the {@link ActionStateUpdater}.
 */
@RunWith(Parameterized.class)
public class ActionStateUpdaterTest {

    /**
     * Parameterized test data.
     *
     * @return whether to enable TEM.
     */
    @Parameters(name = "{index}: Test with enableExecActChgWindow = {0}")
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][] {{true}, {false}};
        return Arrays.asList(data);
    }

    /**
     * Test parameter.
     */
    @Parameter(0)
    public boolean
            enableExecActChgWindow;

    private final ActionStorehouse actionStorehouse = mock(ActionStorehouse.class);
    private final ActionStore actionStore = mock(ActionStore.class);
    private final ActionOrchestratorNotificationSender notificationSender = mock(ActionOrchestratorNotificationSender.class);
    private final ActionHistoryDao actionHistoryDao = mock(ActionHistoryDao.class);
    private final ActionExecutor actionExecutorMock = mock(ActionExecutor.class);
    private final ActionExecutionStore actionExecutionStore = mock(ActionExecutionStore.class);
    private final WorkflowStore workflowStoreMock = mock(WorkflowStore.class);
    private final ActionTranslator actionTranslator = mock(ActionTranslator.class);
    private final ExecutedActionsChangeWindowDao executedActionsChangeWindowDao = mock(ExecutedActionsChangeWindowDao.class);
    private final FailedCloudVMGroupProcessor failedCloudVMGroupProcessor = mock(FailedCloudVMGroupProcessor.class);
    private final long realtimeTopologyContextId = 0;
    private final ActionModeCalculator actionModeCalculator = new ActionModeCalculator();
    private final AcceptedActionsDAO acceptedActionsStore = Mockito.mock(AcceptedActionsDAO.class);
    private final long externalApprovalId = 123456;
    private final long manualId = 12345667;
    private final long manualWithWorkflowsId = 18234566;
    private final long cloudScaleId = 7777777;
    private final long notFoundId = 99999;
    private final long actionTargetId1 = 11;
    private final long actionTargetId2 = 22;
    private final long actionTargetId3 = 33;
    private final long actionTargetId4 = 44;
    private final ActionDTO.ActionSpec externalApprovalSpec =
            createActionSpec(externalApprovalId, actionTargetId1);
    private final ActionDTO.ActionSpec manualSpec =
            createActionSpec(manualId, actionTargetId2);
    private final ActionDTO.ActionSpec manualWithWorkflowsSpec =
            createActionSpec(manualWithWorkflowsId, actionTargetId3);
    private final ActionDTO.ActionSpec cloudScaleSpec =
            createScaleActionSpec(cloudScaleId, actionTargetId4);

    private final EntitiesAndSettingsSnapshot entitySettingsCache = mock(EntitiesAndSettingsSnapshot.class);
    private ActionAuditSender actionAuditSender;
    private ActionStateUpdater actionStateUpdater;

    private IMessageSender<ActionResponse> actionStateUpdatesSender;

    private Action externalApprovalAction;
    private Action manualAction;
    private Action manualWithWorkflowsAction;
    private Action cloudScaleAction;

    /**
     * Rule to manage feature flag enablement to make sure FeatureFlagManager store is set up.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule(FeatureFlags.EXECUTED_ACTIONS_CHANGE_WINDOW);

    /**
     * Sets up the tests.
     *
     * @throws Exception on exceptions occurred
     */
    @Before
    public void setup() throws Exception {
        if (enableExecActChgWindow) {
            featureFlagTestRule.enable(FeatureFlags.EXECUTED_ACTIONS_CHANGE_WINDOW);
        } else {
            featureFlagTestRule.disable(FeatureFlags.EXECUTED_ACTIONS_CHANGE_WINDOW);
        }
        actionAuditSender = Mockito.mock(ActionAuditSender.class);
        actionStateUpdatesSender = Mockito.mock(IMessageSender.class);
        actionStateUpdater =
                new ActionStateUpdater(actionStorehouse, notificationSender, actionHistoryDao,
                        acceptedActionsStore, actionExecutorMock, actionExecutionStore,
                        workflowStoreMock, realtimeTopologyContextId, failedCloudVMGroupProcessor,
                        actionAuditSender, actionStateUpdatesSender, actionTranslator,
                        executedActionsChangeWindowDao);
        when(entitySettingsCache.getSettingsForEntity(eq(actionTargetId1))).thenReturn(
                makeActionModeSetting(ActionMode.EXTERNAL_APPROVAL));
        when(entitySettingsCache.getSettingsForEntity(eq(actionTargetId2))).thenReturn(
                makeActionModeSetting(ActionMode.MANUAL));
        when(entitySettingsCache.getSettingsForEntity(eq(actionTargetId3))).thenReturn(
                makeActionModeAndWorkflowSettings(ConfigurableActionSettings.Move,
                        ActionMode.MANUAL, ActionSettingType.POST, 1L));
        when(entitySettingsCache.getSettingsForEntity(eq(actionTargetId4))).thenReturn(
                makeGeneralActionModeSetting(ActionMode.MANUAL, "scale"));
        when(workflowStoreMock.fetchWorkflow(1L)).thenReturn(Optional.of(Workflow.getDefaultInstance()));
        when(entitySettingsCache.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());
        when(entitySettingsCache.getResourceGroupForEntity(anyLong())).thenReturn(Optional.empty());
        when(entitySettingsCache.getNodePoolForEntity(anyLong())).thenReturn(Optional.empty());
        when(actionStorehouse.getStore(eq(realtimeTopologyContextId))).thenReturn(Optional.of(actionStore));
        when(actionStore.getAction(eq(notFoundId))).thenReturn(Optional.empty());
        when(actionStore.getActionByRecommendationId(eq(notFoundId))).thenReturn(Optional.empty());
        externalApprovalAction = makeTestAction(externalApprovalId, externalApprovalSpec);
        manualAction = makeTestAction(manualId, manualSpec);
        manualWithWorkflowsAction = makeTestAction(manualWithWorkflowsId, manualWithWorkflowsSpec);
        cloudScaleAction = makeTestAction(cloudScaleId, cloudScaleSpec);
    }

    private Action makeTestAction(final long actionId, ActionDTO.ActionSpec actionSpec,
            final long recommendationId) throws UnsupportedActionException {
        final Action testAction =
                new Action(actionSpec.getRecommendation().toBuilder().setId(actionId).setExecutable(true).build(), 4,
                        actionModeCalculator, recommendationId);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(entitySettingsCache, testAction);
        testAction.getActionTranslation().setPassthroughTranslationSuccess();
        testAction.refreshAction(entitySettingsCache);
        when(actionStore.getAction(eq(actionId))).thenReturn(Optional.of(testAction));
        when(actionStore.getActionByRecommendationId(eq(actionId))).thenReturn(Optional.of(testAction));
        testAction.receive(new ManualAcceptanceEvent("99", 102));
        testAction.receive(new QueuedEvent());
        testAction.receive(new BeginExecutionEvent());
        when(actionTranslator.translateToSpec(testAction)).thenReturn(actionSpec);
        return testAction;
    }

    private Action makeTestAction(final long actionId, ActionDTO.ActionSpec actionSpec)
            throws UnsupportedActionException {
        return makeTestAction(actionId, actionSpec, 2244L);
    }

    /**
     * ActionStateUpdater should send updates to actionStateUpdatesSender (kafka) and notification
     * sender.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testOnActionProgress() throws Exception {
        ActionProgress progress = ActionProgress.newBuilder()
            .setActionId(externalApprovalId)
            .setProgressPercentage(33)
            .setDescription("Moving vm from foo to bar")
            .build();

        actionStateUpdater.onActionProgress(progress);
        assertEquals(ActionState.IN_PROGRESS, externalApprovalAction.getState());
        assertEquals(33, (int)externalApprovalAction.getCurrentExecutableStep().flatMap(ExecutableStep::getProgressPercentage).get());
        assertEquals("Moving vm from foo to bar",
            externalApprovalAction.getCurrentExecutableStep().flatMap(ExecutableStep::getProgressDescription).get());
        verify(actionStateUpdatesSender).sendMessage(ActionResponse.newBuilder()
                .setProgress(progress.getProgressPercentage())
                .setResponseDescription(progress.getDescription())
                .setActionOid(externalApprovalAction.getRecommendationOid())
                .setActionResponseState(ActionResponseState.IN_PROGRESS)
                .build());
        verify(notificationSender).notifyActionProgress(progress);
    }

    /**
     * Test if action state update (i.g. {@link ActionProgress} hasn't description (or has
     * blank description) then actionStateUpdatesSender sends messages with default description.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testStateUpdateDescription() throws Exception {
        final ActionProgress progress = ActionProgress.newBuilder()
                .setActionId(externalApprovalAction.getId())
                .setProgressPercentage(33)
                .build();
        actionStateUpdater.onActionProgress(progress);
        final String defaultDescription = String.format("Action with %s OID %s",
                externalApprovalAction.getRecommendationOid(), "in progress");
        verify(actionStateUpdatesSender).sendMessage(ActionResponse.newBuilder()
                .setProgress(progress.getProgressPercentage())
                .setResponseDescription(defaultDescription)
                .setActionOid(externalApprovalAction.getRecommendationOid())
                .setActionResponseState(ActionResponseState.IN_PROGRESS)
                .build());
    }

    /**
     * ActionStateUpdater should never call notifyActionProgress when the action id is not found.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testActionProgressNotFound() throws Exception {
        ActionProgress progress = ActionProgress.newBuilder()
            .setActionId(notFoundId)
            .setProgressPercentage(33)
            .setDescription("Moving vm from foo to bar")
            .build();

        actionStateUpdater.onActionProgress(progress);
        verify(notificationSender, never()).notifyActionProgress(progress);
    }

    /**
     * Tests action success reported.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testOnActionSuccess() throws Exception {
        ActionSuccess success = ActionSuccess.newBuilder()
                .setActionId(externalApprovalId)
                .setSuccessDescription("Success!")
                .setActionSpec(externalApprovalSpec)
                .build();

        actionStateUpdater.onActionSuccess(success);
        assertEquals(ActionState.SUCCEEDED, externalApprovalAction.getState());
        assertEquals(Status.SUCCESS, externalApprovalAction.getCurrentExecutableStep().get().getStatus());
        verify(notificationSender).notifyActionSuccess(success);
        SerializationState serializedAction = new SerializationState(externalApprovalAction);

        verify(actionHistoryDao).persistActionHistory(externalApprovalSpec.getRecommendation().getId(),
                externalApprovalSpec.getRecommendation(),
                realtimeTopologyContextId,
                serializedAction.getRecommendationTime(),
                serializedAction.getActionDecision(),
                serializedAction.getExecutionStep(),
                serializedAction.getCurrentState().getNumber(),
                serializedAction.getActionDetailData(),
                serializedAction.getAssociatedAccountId(),
                serializedAction.getAssociatedResourceGroupId(),
                2244L);
        verify(acceptedActionsStore, Mockito.never()).deleteAcceptedAction(
                externalApprovalAction.getRecommendationOid());
        Mockito.verify(actionAuditSender).sendAfterExecutionEvents(externalApprovalAction);
        verify(actionStateUpdatesSender).sendMessage(ActionResponse.newBuilder()
                .setProgress(100)
                .setResponseDescription(success.getSuccessDescription())
                .setActionOid(externalApprovalAction.getRecommendationOid())
                .setActionResponseState(ActionResponseState.SUCCEEDED)
                .build());
    }

    /**
     * Tests action success reported for cloud scale action.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testCloudScaleOnActionSuccess() throws Exception {
        ActionSuccess success = ActionSuccess.newBuilder()
                .setActionId(cloudScaleId)
                .setSuccessDescription("Success!")
                .setActionSpec(cloudScaleSpec)
                .build();

        actionStateUpdater.onActionSuccess(success);
        assertEquals(ActionState.SUCCEEDED, cloudScaleAction.getState());
        assertEquals(Status.SUCCESS, cloudScaleAction.getCurrentExecutableStep().get().getStatus());
        verify(notificationSender).notifyActionSuccess(success);
        SerializationState serializedAction = new SerializationState(cloudScaleAction);

        verify(actionHistoryDao).persistActionHistory(cloudScaleSpec.getRecommendation().getId(),
                cloudScaleSpec.getRecommendation(),
                realtimeTopologyContextId,
                serializedAction.getRecommendationTime(),
                serializedAction.getActionDecision(),
                serializedAction.getExecutionStep(),
                serializedAction.getCurrentState().getNumber(),
                serializedAction.getActionDetailData(),
                serializedAction.getAssociatedAccountId(),
                serializedAction.getAssociatedResourceGroupId(),
                2244L);
        verify(acceptedActionsStore, Mockito.never()).deleteAcceptedAction(
                cloudScaleAction.getRecommendationOid());
        Mockito.verify(actionAuditSender).sendAfterExecutionEvents(cloudScaleAction);

        if (enableExecActChgWindow) {
            verify(executedActionsChangeWindowDao).persistExecutedActionsChangeWindow(cloudScaleSpec.getRecommendation().getId(),
                    actionTargetId4,
                    serializedAction.getExecutionStep().getCompletionTime());
        } else {
            verifyZeroInteractions(executedActionsChangeWindowDao);
        }
    }

    /**
     * Tests that action with several execution steps (i.g. with PRE or POST in addition to main
     * execution) persist execution results in database only one time after finishing final
     * execution step.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testActionWithMultipleExecutionSteps() throws Exception {
        final ActionSuccess success = ActionSuccess.newBuilder()
                .setActionId(manualWithWorkflowsId)
                .setSuccessDescription("Success!")
                .setActionSpec(manualWithWorkflowsSpec)
                .build();

        Assert.assertEquals(ActionState.IN_PROGRESS, manualWithWorkflowsAction.getState());
        // successfully finished IN_PROGRESS execution step
        actionStateUpdater.onActionSuccess(success);

        Assert.assertEquals(ActionState.POST_IN_PROGRESS, manualWithWorkflowsAction.getState());
        Mockito.verifyZeroInteractions(actionHistoryDao);

        // successfully finished POST execution step
        actionStateUpdater.onActionSuccess(success);

        Assert.assertEquals(ActionState.SUCCEEDED, manualWithWorkflowsAction.getState());
        Assert.assertEquals(Status.SUCCESS,
                manualWithWorkflowsAction.getCurrentExecutableStep().get().getStatus());
        Mockito.verify(notificationSender).notifyActionSuccess(success);
        SerializationState serializedAction = new SerializationState(manualWithWorkflowsAction);

        Mockito.verify(actionHistoryDao, times(1))
                .persistActionHistory(manualWithWorkflowsSpec.getRecommendation().getId(),
                        manualWithWorkflowsSpec.getRecommendation(),
                        realtimeTopologyContextId, serializedAction.getRecommendationTime(),
                        serializedAction.getActionDecision(), serializedAction.getExecutionStep(),
                        serializedAction.getCurrentState().getNumber(),
                        serializedAction.getActionDetailData(),
                        serializedAction.getAssociatedAccountId(),
                        serializedAction.getAssociatedResourceGroupId(), 2244L);
    }

    /**
     * Tests persisting failed execution step for completed failed actions.
     *
     * <p>Note: short-term fix for OM-73943</p>
     */
    @Test
    public void testPersistingFailedStepForFailedMultipleStepActions() {
        final ActionFailure failure = ActionFailure.newBuilder()
                .setActionId(manualWithWorkflowsId)
                .setErrorDescription("Failed to execute workflow!")
                .setActionSpec(manualWithWorkflowsSpec)
                .build();

        final ActionSuccess success = ActionSuccess.newBuilder()
                .setActionId(manualWithWorkflowsId)
                .setSuccessDescription("Success!")
                .setActionSpec(manualWithWorkflowsSpec)
                .build();

        Assert.assertEquals(ActionState.IN_PROGRESS, manualWithWorkflowsAction.getState());
        // failed IN_PROGRESS execution step
        actionStateUpdater.onActionFailure(failure);

        Assert.assertEquals(ActionState.FAILING, manualWithWorkflowsAction.getState());

        // successfully finished POST execution step
        actionStateUpdater.onActionSuccess(success);

        // latest "After execution" step was successful, but final action state is FAILED,
        // because main "Action execution" step was failed
        Assert.assertEquals(Status.SUCCESS,
                manualWithWorkflowsAction.getCurrentExecutableStep().get().getStatus());
        Assert.assertEquals(ActionState.FAILED, manualWithWorkflowsAction.getState());

        final SerializationState serializedAction = new SerializationState(
                manualWithWorkflowsAction);

        // ASSERT
        // Checks that failed step was persisted for failed action
        Mockito.verify(actionHistoryDao, times(1)).persistActionHistory(
                manualWithWorkflowsSpec.getRecommendation().getId(),
                manualWithWorkflowsSpec.getRecommendation(), realtimeTopologyContextId,
                serializedAction.getRecommendationTime(), serializedAction.getActionDecision(),
                manualWithWorkflowsAction.getExecutableSteps()
                        .get(ActionState.IN_PROGRESS)
                        .getExecutionStep(), serializedAction.getCurrentState().getNumber(),
                serializedAction.getActionDetailData(), serializedAction.getAssociatedAccountId(),
                serializedAction.getAssociatedResourceGroupId(), 2244L);
    }

    /**
     * For completed FAILED actions we persist failed execution step.
     * For the rest actions (completed successfully, in-progress or ready actions) persist the
     * latest execution step if any.
     *
     * <p>Note: short-term fix for OM-73943</p>
     */
    @Test
    public void testPersistingLatestExecutionStepForSucceededActions() {
        final ActionSuccess success = ActionSuccess.newBuilder()
                .setActionId(manualWithWorkflowsId)
                .setSuccessDescription("Success!")
                .setActionSpec(manualWithWorkflowsSpec)
                .build();

        Assert.assertEquals(ActionState.IN_PROGRESS, manualWithWorkflowsAction.getState());
        // successfully finished IN_PROGRESS execution step
        actionStateUpdater.onActionSuccess(success);

        Assert.assertEquals(ActionState.POST_IN_PROGRESS, manualWithWorkflowsAction.getState());
        Mockito.verifyZeroInteractions(actionHistoryDao);

        // successfully finished POST execution step
        actionStateUpdater.onActionSuccess(success);

        Assert.assertEquals(ActionState.SUCCEEDED, manualWithWorkflowsAction.getState());
        Assert.assertEquals(Status.SUCCESS,
                manualWithWorkflowsAction.getCurrentExecutableStep().get().getStatus());
        SerializationState serializedAction = new SerializationState(manualWithWorkflowsAction);

        // Checks that latest execution step was persisted for successfully completed action
        Mockito.verify(actionHistoryDao, times(1))
                .persistActionHistory(manualWithWorkflowsSpec.getRecommendation().getId(),
                        manualWithWorkflowsSpec.getRecommendation(),
                        realtimeTopologyContextId, serializedAction.getRecommendationTime(),
                        serializedAction.getActionDecision(),
                        manualWithWorkflowsAction.getCurrentExecutableStep().get().getExecutionStep(),
                        serializedAction.getCurrentState().getNumber(),
                        serializedAction.getActionDetailData(),
                        serializedAction.getAssociatedAccountId(),
                        serializedAction.getAssociatedResourceGroupId(), 2244L);
    }

    /**
     * Completed action could have more than one failed step only in case
     * when "Action execution" and "After execution" steps were failed.
     * For that case we persist failed "Action execution" step as the most important one.
     * Later it will be improved to persist all execution steps for completed actions.
     *
     * <p>Note: short-term fix for OM-73943</p>
     */
    @Test
    public void testPersistingActionExecutionFailedStepForFailedActionsWithMultipleFailedSteps() {
        final ActionFailure failure = ActionFailure.newBuilder()
                .setActionId(manualWithWorkflowsId)
                .setErrorDescription("Failed to execute workflow!")
                .setActionSpec(manualWithWorkflowsSpec)
                .build();

        Assert.assertEquals(ActionState.IN_PROGRESS, manualWithWorkflowsAction.getState());
        // failed "Action execution" step
        actionStateUpdater.onActionFailure(failure);

        Assert.assertEquals(ActionState.FAILING, manualWithWorkflowsAction.getState());

        // failed "After execution" execution step
        actionStateUpdater.onActionFailure(failure);

        // latest "After execution" step was failed and final action state is also FAILED
        Assert.assertEquals(Status.FAILED,
                manualWithWorkflowsAction.getCurrentExecutableStep().get().getStatus());
        Assert.assertEquals(ActionState.FAILED, manualWithWorkflowsAction.getState());

        final SerializationState serializedAction = new SerializationState(
                manualWithWorkflowsAction);

        // ASSERT
        // Checks that main "Action execution" failed step was persisted for failed action
        Mockito.verify(actionHistoryDao, times(1)).persistActionHistory(
                manualWithWorkflowsSpec.getRecommendation().getId(),
                manualWithWorkflowsSpec.getRecommendation(), realtimeTopologyContextId,
                serializedAction.getRecommendationTime(), serializedAction.getActionDecision(),
                manualWithWorkflowsAction.getExecutableSteps()
                        .get(ActionState.IN_PROGRESS)
                        .getExecutionStep(), serializedAction.getCurrentState().getNumber(),
                serializedAction.getActionDetailData(), serializedAction.getAssociatedAccountId(),
                serializedAction.getAssociatedResourceGroupId(), 2244L);
    }

    /**
     * Test sending failure action execution notification when action failed main (IN-PROGRESS)
     * step, but successfully passed POST execution step.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testActionFailedPostScriptSucceeded()
            throws Exception {
        final ActionSuccess success = ActionSuccess.newBuilder()
                .setActionId(manualWithWorkflowsId)
                .setActionSpec(manualWithWorkflowsSpec)
                .setSuccessDescription("Success!")
                .build();

        final ActionFailure failure = ActionFailure.newBuilder()
                .setActionId(manualWithWorkflowsId)
                .setActionSpec(manualWithWorkflowsSpec)
                .setErrorDescription("Failure!")
                .build();

        Assert.assertEquals(ActionState.IN_PROGRESS, manualWithWorkflowsAction.getState());
        // failed IN_PROGRESS execution step
        actionStateUpdater.onActionFailure(failure);

        // When REPLACE/Native fails and there's a post workflow we go to the FAILING state.
        // As a result do not update the database yet. We still need to run the POST workflow.
        Assert.assertEquals(ActionState.FAILING, manualWithWorkflowsAction.getState());
        Mockito.verifyZeroInteractions(actionHistoryDao);

        // Not yet! The notification should be sent when the POST step finishes.
        Mockito.verify(notificationSender, never()).notifyActionFailure(failure);

        // successfully finished POST execution step for action
        actionStateUpdater.onActionSuccess(success);

        Assert.assertEquals(Status.SUCCESS,
                manualWithWorkflowsAction.getCurrentExecutableStep().get().getStatus());
        Assert.assertEquals(ActionState.FAILED, manualWithWorkflowsAction.getState());

        // checked that to the system was sent failure notification regardless of successful
        // execution of POST step
        Mockito.verify(notificationSender, Mockito.times(1)).notifyActionFailure(failure);
        Mockito.verify(notificationSender, Mockito.never()).notifyActionSuccess(any());
    }

    /**
     * Test that a failed action execution followed by a failed POST execution step sends
     * exactly one action failure notification.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testActionFailedPostScriptFailed()
            throws Exception {
        final ActionFailure failure = ActionFailure.newBuilder()
                .setActionId(manualWithWorkflowsId)
                .setActionSpec(manualWithWorkflowsSpec)
                .setErrorDescription("Failure!")
                .build();

        Assert.assertEquals(ActionState.IN_PROGRESS, manualWithWorkflowsAction.getState());
        // failed IN_PROGRESS execution step
        actionStateUpdater.onActionFailure(failure);

        // When REPLACE/Native fails and there's a post workflow we go to the FAILING state.
        // As a result do not update the database yet. We still need to run the POST workflow.
        Assert.assertEquals(ActionState.FAILING, manualWithWorkflowsAction.getState());
        Mockito.verifyZeroInteractions(actionHistoryDao);

        // Not yet! The notification should be sent when the POST step finishes.
        Mockito.verify(notificationSender, never()).notifyActionFailure(failure);

        // Failed POST execution step for action
        actionStateUpdater.onActionFailure(failure);

        Assert.assertEquals(Status.FAILED,
                manualWithWorkflowsAction.getCurrentExecutableStep().get().getStatus());
        Assert.assertEquals(ActionState.FAILED, manualWithWorkflowsAction.getState());

        // checked that to the system was sent failure notification exactly once.
        Mockito.verify(notificationSender, Mockito.times(1)).notifyActionFailure(failure);
        Mockito.verify(notificationSender, Mockito.never()).notifyActionSuccess(any());
    }

    /**
     * Test that we don't send state updates when action mode isn't
     * {@link ActionMode#EXTERNAL_APPROVAL}.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testOnActionSuccessWithoutSendingStateUpdates() throws Exception {
        final ActionSuccess success = ActionSuccess.newBuilder()
                .setActionId(manualId)
                .setSuccessDescription("Success!")
                .build();

        actionStateUpdater.onActionSuccess(success);
        assertEquals(ActionState.SUCCEEDED, manualAction.getState());
        assertEquals(Status.SUCCESS, manualAction.getCurrentExecutableStep().get().getStatus());

        verify(actionStateUpdatesSender, Mockito.never()).sendMessage(ActionResponse.newBuilder()
                .setProgress(100)
                .setResponseDescription(success.getSuccessDescription())
                .setActionOid(manualAction.getId())
                .setActionResponseState(ActionResponseState.SUCCEEDED)
                .build());
    }

    /**
     * Tests removing acceptance for successfully executed action with schedule.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testOnActionSuccessActionWithSchedule() throws Exception {
        // initialize
        final long actionWithScheduleId = 12L;
        final long recommendationId = 224L;
        final long actionTargetId = 4L;
        final long executionScheduleId = 111L;
        final ActionDTO.ActionSpec actionSpec =
                createActionSpec(actionWithScheduleId, actionTargetId);
        final ScheduleProto.Schedule executionSchedule =
                ActionOrchestratorTestUtils.createActiveSchedule(executionScheduleId);
        final Map<Long, Schedule> scheduleMap =
                ImmutableMap.of(executionScheduleId, executionSchedule);

        when(entitySettingsCache.getSettingsForEntity(eq(actionTargetId))).thenReturn(
                ActionOrchestratorTestUtils.makeActionModeAndExecutionScheduleSetting(
                        ActionMode.MANUAL, Collections.singleton(executionScheduleId)));
        when(entitySettingsCache.getAcceptingUserForAction(recommendationId)).thenReturn(
                Optional.of("admin"));
        when(entitySettingsCache.getScheduleMap()).thenReturn(scheduleMap);

        final Action actionWithExecutionSchedule =
                makeTestAction(actionWithScheduleId, actionSpec, recommendationId);

        final ActionSuccess success = ActionSuccess.newBuilder()
                .setActionId(actionWithScheduleId)
                .setSuccessDescription("Success!")
                .setActionSpec(actionSpec)
                .build();

        actionStateUpdater.onActionSuccess(success);
        assertEquals(ActionState.SUCCEEDED, actionWithExecutionSchedule.getState());
        assertEquals(Status.SUCCESS,
                actionWithExecutionSchedule.getCurrentExecutableStep().get().getStatus());
        verify(notificationSender).notifyActionSuccess(success);
        verify(acceptedActionsStore).deleteAcceptedAction(actionWithExecutionSchedule.getRecommendationOid());
    }

    /**
     * ActionStateUpdater should never call notifyActionSuccess when the action id is not found.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testActionSuccessNotFound() throws Exception {
        ActionSuccess success = ActionSuccess.newBuilder()
            .setActionId(notFoundId)
            .setSuccessDescription("Success!")
            .build();

        actionStateUpdater.onActionSuccess(success);
        verify(notificationSender, never()).notifyActionSuccess(success);
    }

    /**
     * Tests action failure reported.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testOnActionFailed() throws Exception {
        ActionFailure failure = ActionFailure.newBuilder()
            .setActionId(externalApprovalId)
            .setErrorDescription("Failure!")
            .setActionSpec(externalApprovalSpec)
            .build();

        actionStateUpdater.onActionFailure(failure);
        assertEquals(ActionState.FAILED, externalApprovalAction.getState());
        assertEquals(Status.FAILED, externalApprovalAction.getCurrentExecutableStep().get().getStatus());
        verify(notificationSender).notifyActionFailure(failure);
        SerializationState serializedAction = new SerializationState(externalApprovalAction);
        verify(actionHistoryDao).persistActionHistory(externalApprovalSpec.getRecommendation().getId(),
                externalApprovalSpec.getRecommendation(),
            realtimeTopologyContextId,
            serializedAction.getRecommendationTime(),
            serializedAction.getActionDecision(),
            serializedAction.getExecutionStep(),
            serializedAction.getCurrentState().getNumber(),
            serializedAction.getActionDetailData(),
            serializedAction.getAssociatedAccountId(),
            serializedAction.getAssociatedResourceGroupId(),
                2244L);
        Mockito.verify(actionAuditSender).sendAfterExecutionEvents(externalApprovalAction);
        verify(actionStateUpdatesSender).sendMessage(ActionResponse.newBuilder()
                .setProgress(100)
                .setResponseDescription(failure.getErrorDescription())
                .setActionOid(externalApprovalAction.getRecommendationOid())
                .setActionResponseState(ActionResponseState.FAILED)
                .build());
    }

    /**
     * Test that specific actions that got lost get marked as failed.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testSomeActionsLost() throws Exception {
        ActionsLost actionsLost = ActionsLost.newBuilder()
            .setLostActionId(ActionIds.newBuilder()
                .addActionIds(externalApprovalId))
            .build();
        final QueryableActionViews views = mock(QueryableActionViews.class);
        when(views.get(Collections.singletonList(externalApprovalId))).thenReturn(Stream.of(
                externalApprovalAction));
        when(actionStore.getActionViews()).thenReturn(views);

        actionStateUpdater.onActionsLost(actionsLost);

        verify(views).get(Collections.singletonList(externalApprovalId));
        assertEquals(ActionState.FAILED, externalApprovalAction.getState());
        assertEquals(Status.FAILED, externalApprovalAction.getCurrentExecutableStep().get().getStatus());
        verify(notificationSender).notifyActionFailure(ActionFailure.newBuilder()
            .setActionId(externalApprovalId)
            .setActionSpec(externalApprovalSpec)
            .setErrorDescription("Topology Processor lost action state.")
            .build());
        SerializationState serializedAction = new SerializationState(externalApprovalAction);
        verify(actionHistoryDao).persistActionHistory(externalApprovalSpec.getRecommendation().getId(),
                externalApprovalSpec.getRecommendation(),
            realtimeTopologyContextId,
            serializedAction.getRecommendationTime(),
            serializedAction.getActionDecision(),
            serializedAction.getExecutionStep(),
            serializedAction.getCurrentState().getNumber(),
            serializedAction.getActionDetailData(), null, null,
                2244L);
    }

    /**
     * Test that actions that got executed before the actions lost message's timestamp get
     * marked as failed.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testAllActionsLost() throws Exception {
        ActionsLost actionsLost = ActionsLost.newBuilder()
            .setBeforeTime(System.currentTimeMillis() + 10)
            .build();
        final QueryableActionViews views = mock(QueryableActionViews.class);
        when(views.get(any(ActionQueryFilter.class))).thenReturn(Stream.of(externalApprovalAction,
                manualAction));

        when(actionStore.getActionViews()).thenReturn(views);

        actionStateUpdater.onActionsLost(actionsLost);

        verify(views).get(ActionQueryFilter.newBuilder()
            .addStates(ActionState.IN_PROGRESS)
            .addStates(ActionState.PRE_IN_PROGRESS)
            .addStates(ActionState.POST_IN_PROGRESS)
            .addStates(ActionState.FAILING)
            .build());
        assertEquals(ActionState.FAILED, externalApprovalAction.getState());
        assertEquals(ActionState.FAILED, manualAction.getState());
        assertEquals(Status.FAILED, externalApprovalAction.getCurrentExecutableStep().get().getStatus());
        assertEquals(Status.FAILED, manualAction.getCurrentExecutableStep().get().getStatus());
        verify(notificationSender).notifyActionFailure(ActionFailure.newBuilder()
            .setActionId(externalApprovalId)
            .setActionSpec(externalApprovalSpec)
            .setErrorDescription("Topology Processor lost action state.")
            .build());
        verify(notificationSender).notifyActionFailure(ActionFailure.newBuilder()
            .setActionId(manualId)
            .setActionSpec(manualSpec)
            .setErrorDescription("Topology Processor lost action state.")
            .build());
        SerializationState serializedAction = new SerializationState(externalApprovalAction);
        verify(actionHistoryDao).persistActionHistory(externalApprovalSpec.getRecommendation().getId(),
                externalApprovalSpec.getRecommendation(),
            realtimeTopologyContextId,
            serializedAction.getRecommendationTime(),
            serializedAction.getActionDecision(),
            serializedAction.getExecutionStep(),
            serializedAction.getCurrentState().getNumber(),
                serializedAction.getActionDetailData(), null, null,
                serializedAction.getRecommendationOid());
        SerializationState serializedAction2 = new SerializationState(externalApprovalAction);
        verify(actionHistoryDao).persistActionHistory(externalApprovalSpec.getRecommendation().getId(),
                externalApprovalSpec.getRecommendation(),
            realtimeTopologyContextId,
            serializedAction2.getRecommendationTime(),
            serializedAction2.getActionDecision(),
            serializedAction2.getExecutionStep(),
            serializedAction2.getCurrentState().getNumber(),
                serializedAction2.getActionDetailData(), null, null,
                serializedAction2.getRecommendationOid());
    }

    /**
     * Test that actions that got executed after the actions lost message's timestamp don't get
     * marked as failed.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testAllActionsLostRespectBeforeTime() throws Exception {
        ActionsLost actionsLost = ActionsLost.newBuilder()
            // Very early on. The actions shouldn't get dropped.
            .setBeforeTime(100)
            .build();
        final QueryableActionViews views = mock(QueryableActionViews.class);
        when(views.get(any(ActionQueryFilter.class))).thenReturn(Stream.of(externalApprovalAction,
                manualAction));

        when(actionStore.getActionViews()).thenReturn(views);

        actionStateUpdater.onActionsLost(actionsLost);

        verify(views).get(ActionQueryFilter.newBuilder()
            .addStates(ActionState.IN_PROGRESS)
            .addStates(ActionState.PRE_IN_PROGRESS)
            .addStates(ActionState.POST_IN_PROGRESS)
            .addStates(ActionState.FAILING)
            .build());

        // No failure.
        assertNotEquals(ActionState.FAILED, externalApprovalAction.getState());
        assertNotEquals(ActionState.FAILED, manualAction.getState());
        assertNotEquals(Status.FAILED, externalApprovalAction.getCurrentExecutableStep().get().getStatus());
        assertNotEquals(Status.FAILED, manualAction.getCurrentExecutableStep().get().getStatus());

        verify(notificationSender, never()).notifyActionFailure(any());
        verifyZeroInteractions(actionHistoryDao);
    }

    /**
     * ActionStateUpdater should never call notifyActionFailure when the action id is not found.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testActionFailureNotFound() throws Exception {
        ActionFailure failure = ActionFailure.newBuilder()
            .setActionId(notFoundId)
            .setErrorDescription("Failure!")
            .build();

        actionStateUpdater.onActionFailure(failure);
        verify(notificationSender, never()).notifyActionFailure(failure);
    }

    @Nonnull
    private ActionDTO.ActionSpec createActionSpec(long actionId1, long actionTargetId) {
        return ActionSpec.newBuilder()
            .setRecommendation(ActionDTO.Action.newBuilder()
                .setExecutable(true)
                .setId(actionId1)
                .setDeprecatedImportance(0)
                .setSupportingLevel(SupportLevel.SUPPORTED)
                .setInfo(TestActionBuilder.makeMoveInfo(actionTargetId, 2, 1, 1, 1))
                .setExplanation(Explanation.newBuilder().build()))
            .build();
    }

    @Nonnull
    private ActionDTO.ActionSpec createScaleActionSpec(long actionId1, long actionTargetId) {
        return ActionSpec.newBuilder()
                .setRecommendation(ActionDTO.Action.newBuilder()
                        .setExecutable(true)
                        .setId(actionId1)
                        .setDeprecatedImportance(0)
                        .setSupportingLevel(SupportLevel.SUPPORTED)
                        .setInfo(TestActionBuilder.makeScaleInfo(actionTargetId, 2, 1, 1, 1, null))
                        .setExplanation(Explanation.newBuilder().build()))
                .build();
    }
}

package com.vmturbo.action.orchestrator.execution.notifications;

import static com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils.makeActionModeSetting;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.Action.SerializationState;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ManualAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.PrepareExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ExecutableStep;
import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.action.orchestrator.api.ActionOrchestratorNotificationSender;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.FailedCloudVMGroupProcessor;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep.Status;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;

/**
 * Tests for the {@link ActionStateUpdater}.
 */
public class ActionStateUpdaterTest {

    private final ActionStorehouse actionStorehouse = mock(ActionStorehouse.class);
    private final ActionStore actionStore = mock(ActionStore.class);
    private final ActionOrchestratorNotificationSender notificationSender = mock(ActionOrchestratorNotificationSender.class);
    private final ActionHistoryDao actionHistoryDao = mock(ActionHistoryDao.class);
    private final ActionExecutor actionExecutorMock = mock(ActionExecutor.class);
    private final WorkflowStore workflowStoreMock = mock(WorkflowStore.class);
    private final FailedCloudVMGroupProcessor failedCloudVMGroupProcessor = mock(FailedCloudVMGroupProcessor.class);
    private final long realtimeTopologyContextId = 0;
    private final ActionTranslator actionTranslator = ActionOrchestratorTestUtils.passthroughTranslator();
    private ActionModeCalculator actionModeCalculator = new ActionModeCalculator(actionTranslator);
    private final ActionStateUpdater actionStateUpdater =
            new ActionStateUpdater(actionStorehouse, notificationSender, actionHistoryDao, actionExecutorMock, workflowStoreMock, realtimeTopologyContextId, failedCloudVMGroupProcessor);

    private final long actionId = 123456;
    private final long notFoundId = 99999;
    private final ActionDTO.Action recommendation = ActionDTO.Action.newBuilder()
        .setId(actionId)
        .setImportance(0)
        .setSupportingLevel(SupportLevel.SUPPORTED)
        .setInfo(TestActionBuilder.makeMoveInfo(3, 2, 1, 1, 1))
        .setExplanation(Explanation.newBuilder().build())
        .build();

    private final EntitiesAndSettingsSnapshot entitySettingsCache = mock(EntitiesAndSettingsSnapshot.class);

    private Action testAction;

    @Before
    public void setup() {
        when(entitySettingsCache.getSettingsForEntity(eq(3L)))
            .thenReturn(makeActionModeSetting(ActionMode.MANUAL));
        testAction = new Action(recommendation, 4, actionModeCalculator);
        testAction.refreshActionMode(entitySettingsCache);
        when(actionStorehouse.getStore(eq(realtimeTopologyContextId))).thenReturn(Optional.of(actionStore));
        when(actionStore.getAction(eq(actionId))).thenReturn(Optional.of(testAction));
        when(actionStore.getAction(eq(notFoundId))).thenReturn(Optional.empty());
        testAction.receive(new ManualAcceptanceEvent("99", 102));
        testAction.receive(new PrepareExecutionEvent());
        testAction.receive(new BeginExecutionEvent());
    }

    @Test
    public void testOnActionProgress() throws Exception {
        ActionProgress progress = ActionProgress.newBuilder()
            .setActionId(actionId)
            .setProgressPercentage(33)
            .setDescription("Moving vm from foo to bar")
            .build();

        actionStateUpdater.onActionProgress(progress);
        assertEquals(ActionState.IN_PROGRESS, testAction.getState());
        assertEquals(33, (int)testAction.getCurrentExecutableStep().flatMap(ExecutableStep::getProgressPercentage).get());
        assertEquals("Moving vm from foo to bar",
            testAction.getCurrentExecutableStep().flatMap(ExecutableStep::getProgressDescription).get());
        verify(notificationSender).notifyActionProgress(progress);
    }

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

    @Test
    public void testOnActionSuccess() throws Exception {
        ActionSuccess success = ActionSuccess.newBuilder()
            .setActionId(actionId)
            .setSuccessDescription("Success!")
            .build();

        actionStateUpdater.onActionSuccess(success);
        assertEquals(ActionState.SUCCEEDED, testAction.getState());
        assertEquals(Status.SUCCESS, testAction.getCurrentExecutableStep().get().getStatus());
        verify(notificationSender).notifyActionSuccess(success);
        SerializationState serializedAction = new SerializationState(testAction);
        verify(actionHistoryDao).persistActionHistory(recommendation.getId(),
                recommendation,
                realtimeTopologyContextId,
                serializedAction.getRecommendationTime(),
                serializedAction.getActionDecision(),
                serializedAction.getExecutionStep(),
                serializedAction.getCurrentState().getNumber());
    }


    @Test
    public void testActionSuccessNotFound() throws Exception {
        ActionSuccess success = ActionSuccess.newBuilder()
            .setActionId(notFoundId)
            .setSuccessDescription("Success!")
            .build();

        actionStateUpdater.onActionSuccess(success);
        verify(notificationSender, never()).notifyActionSuccess(success);
    }

    @Test
    public void testOnActionFailed() throws Exception {
        ActionFailure failure = ActionFailure.newBuilder()
            .setActionId(actionId)
            .setErrorDescription("Failure!")
            .build();

        actionStateUpdater.onActionFailure(failure);
        assertEquals(ActionState.FAILED, testAction.getState());
        assertEquals(Status.FAILED, testAction.getCurrentExecutableStep().get().getStatus());
        verify(notificationSender).notifyActionFailure(failure);
        SerializationState serializedAction = new SerializationState(testAction);
        verify(actionHistoryDao).persistActionHistory(recommendation.getId(),
                recommendation,
                realtimeTopologyContextId,
                serializedAction.getRecommendationTime(),
                serializedAction.getActionDecision(),
                serializedAction.getExecutionStep(),
                serializedAction.getCurrentState().getNumber());
    }

    @Test
    public void testActionFailureNotFound() throws Exception {
        ActionFailure failure = ActionFailure.newBuilder()
            .setActionId(notFoundId)
            .setErrorDescription("Failure!")
            .build();

        actionStateUpdater.onActionFailure(failure);
        verify(notificationSender, never()).notifyActionFailure(failure);
    }
}

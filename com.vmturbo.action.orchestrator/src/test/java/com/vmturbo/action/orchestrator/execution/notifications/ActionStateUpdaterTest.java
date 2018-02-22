package com.vmturbo.action.orchestrator.execution.notifications;

import static com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils.makeActionModeSetting;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.Action.SerializationState;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ManualAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionTest;
import com.vmturbo.action.orchestrator.action.ExecutableStep;
import com.vmturbo.action.orchestrator.api.ActionOrchestratorNotificationSender;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitySettingsCache;
import com.vmturbo.action.orchestrator.store.EntityTypeMap;
import com.vmturbo.common.protobuf.action.ActionDTO;
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
    private final long realtimeTopologyContextId = 0;
    private final ActionStateUpdater actionStateUpdater =
        new ActionStateUpdater(actionStorehouse, notificationSender, actionHistoryDao,  realtimeTopologyContextId);

    private final long actionId = 123456;
    private final long notFoundId = 99999;
    private final ActionDTO.Action recommendation = ActionDTO.Action.newBuilder()
        .setId(actionId)
        .setImportance(0)
        .setInfo(ActionTest.makeMoveInfo(3, 2, 1))
        .setExplanation(Explanation.newBuilder().build())
        .build();

    private final EntitySettingsCache entitySettingsCache = mock(EntitySettingsCache.class);
    private final EntityTypeMap entityTypeMap = mock(EntityTypeMap.class);

    private Action testAction;

    @Before
    public void setup() {
        when(entityTypeMap.getTypeForEntity(anyLong())).thenReturn(Optional.empty());
        testAction = new Action(recommendation, entitySettingsCache, entityTypeMap, 4);
        when(actionStorehouse.getStore(eq(realtimeTopologyContextId))).thenReturn(Optional.of(actionStore));
        when(actionStore.getAction(eq(actionId))).thenReturn(Optional.of(testAction));
        when(actionStore.getAction(eq(notFoundId))).thenReturn(Optional.empty());
        when(entitySettingsCache.getSettingsForEntity(eq(3L)))
            .thenReturn(makeActionModeSetting(ActionMode.MANUAL));
        testAction.receive(new ManualAcceptanceEvent("99", 102));
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
        assertEquals(33, (int)testAction.getExecutableStep().flatMap(ExecutableStep::getProgressPercentage).get());
        assertEquals("Moving vm from foo to bar",
            testAction.getExecutableStep().flatMap(ExecutableStep::getProgressDescription).get());
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
        assertEquals(Status.SUCCESS, testAction.getExecutableStep().get().getStatus());
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
        assertEquals(Status.FAILED, testAction.getExecutableStep().get().getStatus());
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
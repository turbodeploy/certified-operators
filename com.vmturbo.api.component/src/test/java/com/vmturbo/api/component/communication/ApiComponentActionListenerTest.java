package com.vmturbo.api.component.communication;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.ActionNotificationDTO.ActionNotification;
import com.vmturbo.api.ActionNotificationDTO.ActionStatusNotification;
import com.vmturbo.api.ActionNotificationDTO.ActionStatusNotification.Status;
import com.vmturbo.api.ActionNotificationDTO.ActionsChangedNotification;
import com.vmturbo.api.component.external.api.websocket.UINotificationChannel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionsUpdated;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;

public class ApiComponentActionListenerTest {
    private static final long ACTION_STABLE_ID = 1363L;

    private final UINotificationChannel uiNotificationChannel = mock(UINotificationChannel.class);
    private ApiComponentActionListener actionListener;
    private ApiComponentActionListener actionListenerUsingStableId;

    private final long actionId = 1234;

    @Captor
    private ArgumentCaptor<ActionNotification> notificationCaptor;

    @Before
    public final void init() {
        MockitoAnnotations.initMocks(this);
        actionListener = new ApiComponentActionListener(uiNotificationChannel,
                false, 77777);
        actionListenerUsingStableId = new ApiComponentActionListener(uiNotificationChannel,
                true, 77777);
    }

    @Test
    public void testOnActionProgress() throws Exception {
        actionListener.onActionProgress(ActionProgress.newBuilder()
            .setActionId(actionId)
            .setActionStableId(ACTION_STABLE_ID)
            .setDescription("foo")
            .setProgressPercentage(42)
            .build());

        verify(uiNotificationChannel).broadcastActionNotification(notificationCaptor.capture());
        final ActionStatusNotification notification = notificationCaptor.getValue().getActionProgressNotification();
        assertEquals(Long.toString(actionId), notification.getActionId());
        assertEquals("foo", notification.getDescription());
        assertEquals(42, notification.getProgressPercentage());
        assertEquals(Status.IN_PROGRESS, notification.getStatus());
    }

    @Test
    public void testOnActionSuccess() throws Exception {
        actionListener.onActionSuccess(ActionSuccess.newBuilder()
            .setActionId(actionId)
            .setActionStableId(ACTION_STABLE_ID)
            .setSuccessDescription("success")
            .build());

        verify(uiNotificationChannel).broadcastActionNotification(notificationCaptor.capture());
        final ActionStatusNotification notification = notificationCaptor.getValue().getActionStatusNotification();
        assertEquals(Long.toString(actionId), notification.getActionId());
        assertEquals("success", notification.getDescription());
        assertEquals(Status.SUCCEEDED, notification.getStatus());
    }

    @Test
    public void testOnActionFailure() throws Exception {
        actionListener.onActionFailure(ActionFailure.newBuilder()
            .setActionId(actionId)
            .setActionStableId(ACTION_STABLE_ID)
            .setErrorDescription("error")
            .build());

        verify(uiNotificationChannel).broadcastActionNotification(notificationCaptor.capture());
        final ActionStatusNotification notification = notificationCaptor.getValue().getActionStatusNotification();
        assertEquals(Long.toString(actionId), notification.getActionId());
        assertEquals("error", notification.getDescription());
        assertEquals(Status.FAILED, notification.getStatus());
    }

    /**
     * Tests if the action progress is sent to ui notification channel when stable id is used as action ID.
     */
    @Test
    public void testOnActionProgressUsingStableId() {
        // ACT
        actionListenerUsingStableId.onActionProgress(ActionProgress.newBuilder()
                .setActionId(actionId)
                .setActionStableId(ACTION_STABLE_ID)
                .setDescription("foo")
                .setProgressPercentage(42)
                .build());

        // ASSERT
        verify(uiNotificationChannel).broadcastActionNotification(notificationCaptor.capture());
        final ActionStatusNotification notification = notificationCaptor.getValue().getActionProgressNotification();
        assertEquals(Long.toString(ACTION_STABLE_ID), notification.getActionId());
        assertEquals("foo", notification.getDescription());
        assertEquals(42, notification.getProgressPercentage());
        assertEquals(Status.IN_PROGRESS, notification.getStatus());
    }

    /**
     * Tests if the action success is sent to ui notification channel when stable id is used as action ID.
     */
    @Test
    public void testOnActionSuccessUsingStableId() {
        // ACT
        actionListenerUsingStableId.onActionSuccess(ActionSuccess.newBuilder()
                .setActionId(actionId)
                .setActionStableId(ACTION_STABLE_ID)
                .setSuccessDescription("success")
                .build());

        // ASSERT
        verify(uiNotificationChannel).broadcastActionNotification(notificationCaptor.capture());
        final ActionStatusNotification notification = notificationCaptor.getValue().getActionStatusNotification();
        assertEquals(Long.toString(ACTION_STABLE_ID), notification.getActionId());
        assertEquals("success", notification.getDescription());
        assertEquals(Status.SUCCEEDED, notification.getStatus());
    }

    /**
     * Tests if the action failure is sent to ui notification channel when stable id is used as action ID.
     */
    @Test
    public void testOnActionFailureUsingStableId() {
        //ACT
        actionListenerUsingStableId.onActionFailure(ActionFailure.newBuilder()
                .setActionId(actionId)
                .setActionStableId(ACTION_STABLE_ID)
                .setErrorDescription("error")
                .build());

        // ASSERT
        verify(uiNotificationChannel).broadcastActionNotification(notificationCaptor.capture());
        final ActionStatusNotification notification = notificationCaptor.getValue().getActionStatusNotification();
        assertEquals(Long.toString(ACTION_STABLE_ID), notification.getActionId());
        assertEquals("error", notification.getDescription());
        assertEquals(Status.FAILED, notification.getStatus());
    }

    @Test
    public void testOnActionUpdated() {
        actionListener.onActionsUpdated(ActionsUpdated.newBuilder()
                .setActionPlanId(3456L)
                .setActionPlanInfo(ActionPlanInfo.newBuilder()
                        .setMarket(MarketActionPlanInfo.newBuilder()
                                .setSourceTopologyInfo(TopologyInfo.newBuilder()
                                        .setTopologyContextId(77777))))
                .setActionCount(10).build());

        verify(uiNotificationChannel).broadcastActionNotification(notificationCaptor.capture());
        final ActionsChangedNotification notification = notificationCaptor.getValue().getActionChangedNotification();
        assertEquals(10, notification.getActionCount());
    }
}
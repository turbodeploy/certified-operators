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
import com.vmturbo.api.ActionNotificationDTO.ActionStatusNotification.Status;
import com.vmturbo.api.component.external.api.websocket.UINotificationChannel;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;

public class ApiComponentActionListenerTest {

    private final UINotificationChannel uiNotificationChannel = mock(UINotificationChannel.class);
    private final ApiComponentActionListener actionListener = new ApiComponentActionListener(uiNotificationChannel);

    private final long actionId = 1234;

    @Captor
    private ArgumentCaptor<ActionNotification> notificationCaptor;

    @Before
    public final void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testOnActionProgress() throws Exception {
        actionListener.onActionProgress(ActionProgress.newBuilder()
            .setActionId(actionId)
            .setDescription("foo")
            .setProgressPercentage(42)
            .build());

        verify(uiNotificationChannel).broadcastActionNotification(notificationCaptor.capture());
        final ActionNotification notification = notificationCaptor.getValue();
        assertEquals(Long.toString(actionId), notification.getActionId());
        assertEquals("foo", notification.getActionProgressNotification().getDescription());
        assertEquals(42, notification.getActionProgressNotification().getProgressPercentage());
        assertEquals(Status.IN_PROGRESS, notification.getActionProgressNotification().getStatus());
    }

    @Test
    public void testOnActionSuccess() throws Exception {
        actionListener.onActionSuccess(ActionSuccess.newBuilder()
            .setActionId(actionId)
            .setSuccessDescription("success")
            .build());

        verify(uiNotificationChannel).broadcastActionNotification(notificationCaptor.capture());
        final ActionNotification notification = notificationCaptor.getValue();
        assertEquals(Long.toString(actionId), notification.getActionId());
        assertEquals("success", notification.getActionStatusNotification().getDescription());
        assertEquals(Status.SUCCEEDED, notification.getActionStatusNotification().getStatus());
    }

    @Test
    public void testOnActionFailure() throws Exception {
        actionListener.onActionFailure(ActionFailure.newBuilder()
            .setActionId(actionId)
            .setErrorDescription("error")
            .build());

        verify(uiNotificationChannel).broadcastActionNotification(notificationCaptor.capture());
        final ActionNotification notification = notificationCaptor.getValue();
        assertEquals(Long.toString(actionId), notification.getActionId());
        assertEquals("error", notification.getActionStatusNotification().getDescription());
        assertEquals(Status.FAILED, notification.getActionStatusNotification().getStatus());
    }
}
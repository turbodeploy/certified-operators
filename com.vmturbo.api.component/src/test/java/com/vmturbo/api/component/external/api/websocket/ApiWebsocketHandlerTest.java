package com.vmturbo.api.component.external.api.websocket;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.WebSocketSession;

import com.vmturbo.api.ActionNotificationDTO.ActionNotification;
import com.vmturbo.api.ActionNotificationDTO.ActionStatusNotification;
import com.vmturbo.api.MarketNotificationDTO.MarketNotification;
import com.vmturbo.api.MarketNotificationDTO.StatusNotification;
import com.vmturbo.api.MarketNotificationDTO.StatusNotification.Status;
import com.vmturbo.api.NotificationDTO.Notification;
import com.vmturbo.commons.idgen.IdentityGenerator;

public class ApiWebsocketHandlerTest {

    private final WebSocketSession session = mock(WebSocketSession.class);
    private final ApiWebsocketHandler websocketHandler = new ApiWebsocketHandler();

    @Captor
    private ArgumentCaptor<BinaryMessage> notificationCaptor;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        IdentityGenerator.initPrefix(0);
        websocketHandler.afterConnectionEstablished(session);
    }

    @Test
    public void testBroadcastMarketNotification() throws Exception {
        websocketHandler.broadcastMarketNotification(MarketNotification.newBuilder()
            .setMarketId("1")
            .setStatusNotification(StatusNotification.newBuilder()
                .setStatus(Status.DELETED))
            .build());

        verify(session).sendMessage(notificationCaptor.capture());
        final Notification notification = Notification.parseFrom(
            notificationCaptor.getValue().getPayload().array());
        assertEquals("1", notification.getMarketNotification().getMarketId());
        assertEquals(Status.DELETED, notification.getMarketNotification().getStatusNotification().getStatus());
    }

    @Test
    public void testBroadcastActionNotification() throws Exception {
        websocketHandler.broadcastActionNotification(ActionNotification.newBuilder()
            .setActionId("1")
            .setActionProgressNotification(ActionStatusNotification.newBuilder()
                    .setStatus(ActionStatusNotification.Status.IN_PROGRESS)
                    .setDescription("foo")
                    .setProgressPercentage(42)
                    .build()
            ).build());

        verify(session).sendMessage(notificationCaptor.capture());
        final Notification notification = Notification.parseFrom(
            notificationCaptor.getValue().getPayload().array());
        assertEquals("1", notification.getActionNotification().getActionId());
        assertEquals("foo", notification.getActionNotification().getActionProgressNotification().getDescription());
        assertEquals(42, notification.getActionNotification().getActionProgressNotification().getProgressPercentage());
    }
}
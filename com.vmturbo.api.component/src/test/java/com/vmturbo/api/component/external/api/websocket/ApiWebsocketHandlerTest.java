package com.vmturbo.api.component.external.api.websocket;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;

import com.vmturbo.api.ActionNotificationDTO.ActionNotification;
import com.vmturbo.api.ActionNotificationDTO.ActionStatusNotification;
import com.vmturbo.api.ExportNotificationDTO.ExportNotification;
import com.vmturbo.api.ExportNotificationDTO.ExportStatusNotification;
import com.vmturbo.api.ExportNotificationDTO.ExportStatusNotification.ExportStatus;
import com.vmturbo.api.MarketNotificationDTO.MarketNotification;
import com.vmturbo.api.MarketNotificationDTO.StatusNotification;
import com.vmturbo.api.MarketNotificationDTO.StatusNotification.Status;
import com.vmturbo.api.NotificationDTO.Notification;
import com.vmturbo.api.ReportNotificationDTO.ReportNotification;
import com.vmturbo.api.ReportNotificationDTO.ReportStatusNotification;
import com.vmturbo.api.ReportNotificationDTO.ReportStatusNotification.ReportStatus;
import com.vmturbo.api.ReservationNotificationDTO.ReservationNotification;
import com.vmturbo.api.ReservationNotificationDTO.ReservationStatus;
import com.vmturbo.api.ReservationNotificationDTO.ReservationStatusNotification;
import com.vmturbo.api.TargetNotificationDTO.TargetNotification;
import com.vmturbo.api.TargetNotificationDTO.TargetStatusNotification;
import com.vmturbo.api.TargetNotificationDTO.TargetStatusNotification.TargetStatus;
import com.vmturbo.api.TargetNotificationDTO.TargetsNotification;
import com.vmturbo.commons.idgen.IdentityGenerator;

public class ApiWebsocketHandlerTest {

    private final WebSocketSession session = generateWebsocketSessionWithHTTPSession("0");
    private final ApiWebsocketHandler websocketHandler = new ApiWebsocketHandler();

    @Captor
    private ArgumentCaptor<BinaryMessage> notificationCaptor;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        IdentityGenerator.initPrefix(0);
        websocketHandler.afterConnectionEstablished(session);
    }

    /**
     * Test that testBroadcastMarketNotification() publishes the notification.
     *
     * @throws Exception if the test fails.
     */
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
        assertEquals(Status.DELETED,
            notification.getMarketNotification().getStatusNotification().getStatus());
    }

    /**
     * Test that testBroadcastActionNotification() publishes the notification.
     *
     * @throws Exception if the test fails.
     */
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
        assertEquals("foo",
            notification.getActionNotification().getActionProgressNotification().getDescription());
        assertEquals(42,
            notification.getActionNotification().getActionProgressNotification().getProgressPercentage());
    }

    /**
     * Test that testBroadcastReportNotification() publishes the notification.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testBroadcastReportNotification() throws Exception {
        websocketHandler.broadcastReportNotification(ReportNotification.newBuilder()
            .setReportId("1")
            .setReportStatusNotification(ReportStatusNotification.newBuilder()
                .setDescription("report")
                .setStatus(ReportStatus.GENERATED)
                .build()
            ).build());

        verify(session).sendMessage(notificationCaptor.capture());
        final Notification notification = Notification.parseFrom(
            notificationCaptor.getValue().getPayload().array());
        assertEquals("1", notification.getReportNotification().getReportId());
        assertEquals("report",
            notification.getReportNotification().getReportStatusNotification().getDescription());
        assertEquals(ReportStatus.GENERATED,
            notification.getReportNotification().getReportStatusNotification().getStatus());
    }

    /**
     * Test that testBroadcastTargetsNotification() publishes the notification.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testBroadcastTargetsNotification() throws Exception {
        websocketHandler.broadcastTargetsNotification(TargetsNotification.newBuilder()
            .setStatusNotification(TargetStatusNotification.newBuilder()
                .setStatus(TargetStatus.DISCOVERED)
                .setDescription("target")
                .build()
            ).build());

        verify(session).sendMessage(notificationCaptor.capture());
        final Notification notification = Notification.parseFrom(
            notificationCaptor.getValue().getPayload().array());
        assertEquals(TargetStatus.DISCOVERED,
            notification.getTargetsNotification().getStatusNotification().getStatus());
        assertEquals("target",
            notification.getTargetsNotification().getStatusNotification().getDescription());
    }

    /**
     * Test that testBroadcastTargetValidationNotification() publishes the notification.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testBroadcastTargetValidationNotification() throws Exception {
        websocketHandler.broadcastTargetValidationNotification(TargetNotification.newBuilder()
            .setTargetId("123")
            .setStatusNotification(TargetStatusNotification.newBuilder()
                .setStatus(TargetStatus.NOT_VALIDATED)
                .setDescription("target-validation")
                .build()
            ).build());

        verify(session).sendMessage(notificationCaptor.capture());
        final Notification notification = Notification.parseFrom(
            notificationCaptor.getValue().getPayload().array());
        assertEquals("123", notification.getTargetNotification().getTargetId());
        assertEquals(TargetStatus.NOT_VALIDATED,
            notification.getTargetNotification().getStatusNotification().getStatus());
        assertEquals("target-validation",
            notification.getTargetNotification().getStatusNotification().getDescription());
    }

    /**
     * Test that broadcastDiagsExportNotification() publishes the notification.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testBroadcastDiagsExportNotification() throws Exception {
        websocketHandler.broadcastDiagsExportNotification(ExportNotification.newBuilder()
            .setStatusNotification(ExportStatusNotification.newBuilder()
                .setStatus(ExportStatus.SUCCEEDED)
                .setDescription("export")
                .build()
            ).build());

        verify(session).sendMessage(notificationCaptor.capture());
        final Notification notification = Notification.parseFrom(
            notificationCaptor.getValue().getPayload().array());
        assertEquals(ExportStatus.SUCCEEDED,
            notification.getExportNotification().getStatusNotification().getStatus());
        assertEquals("export",
            notification.getExportNotification().getStatusNotification().getDescription());
    }

    /**
     * Test that broadcastReservationNotification() publishes the notification.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testBroadcastReservationNotification() throws Exception {
        websocketHandler.broadcastReservationNotification(ReservationNotification.newBuilder()
            .setStatusNotification(ReservationStatusNotification.newBuilder()
                .addReservationStatus(ReservationStatus.newBuilder()
                    .setId("12345")
                    .setStatus("RESERVED"))
                .build()
            ).build());

        verify(session).sendMessage(notificationCaptor.capture());
        final Notification notification = Notification.parseFrom(
            notificationCaptor.getValue().getPayload().array());
        assertEquals(1,
            notification.getReservationNotification().getStatusNotification().getReservationStatusCount());
        assertEquals("12345",
            notification.getReservationNotification().getStatusNotification().getReservationStatus(0).getId());
        assertEquals("RESERVED",
            notification.getReservationNotification().getStatusNotification().getReservationStatus(0).getStatus());
    }

    /**
     * Test that all websockets tied to the given HTTP Session IDs are closed.
     * Expect that all sessions with same HTTP ID are closed.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testCloseWSSessionsByHttpSessionId() throws Exception {
        //GIVEN
        final String sId1 = "1";
        final String sId2 = "2";
        final WebSocketSession mockSession1 = generateWebsocketSessionWithHTTPSession(sId1);
        final WebSocketSession mockSession2 = generateWebsocketSessionWithHTTPSession(sId1);
        final WebSocketSession mockSession3 = generateWebsocketSessionWithHTTPSession(sId2);
        final WebSocketSession mockSession4 = generateWebsocketSessionWithHTTPSession(sId2);
        final WebSocketSession mockSession5 = generateWebsocketSessionWithHTTPSession(sId2);
        final CloseStatus closeStatus = CloseStatus.NORMAL.withReason("test");

        websocketHandler.afterConnectionEstablished(mockSession1);
        websocketHandler.afterConnectionEstablished(mockSession2);
        websocketHandler.afterConnectionEstablished(mockSession3);
        websocketHandler.afterConnectionEstablished(mockSession4);
        websocketHandler.afterConnectionEstablished(mockSession5);

        //WHEN
        websocketHandler.closeWSSessionsByHttpSessionId(sId2, closeStatus);

        //THEN
        verify(mockSession1, times(0)).close(any());
        verify(mockSession2, times(0)).close(any());
        verify(mockSession3, times(1)).close(closeStatus);
        verify(mockSession4, times(1)).close(closeStatus);
        verify(mockSession5, times(1)).close(closeStatus);
    }

    /**
     * Test the scenario when a websocket connection is established w/o an HTTP Session.
     * Expect that the websocket is closed.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testOpenWebsocketWithNoHttpSessionId() throws Exception {
        //GIVEN
        final WebSocketSession mockSession1 = mock(WebSocketSession.class);

        //WHEN
        websocketHandler.afterConnectionEstablished(mockSession1);

        //THEN
        verify(mockSession1, times(1)).close(any(CloseStatus.class));
    }

    /**
     * Helper method to generate websocket session mocks with HTTP ID attributes set.
     *
     * @param id the HTTP Session ID attribute
     * @return a mocked WebSocketSession
     */
    private WebSocketSession generateWebsocketSessionWithHTTPSession(final String id) {
        final Map<String, Object> att = new HashMap<>();
        att.put("HTTP.SESSION.ID", id);
        final WebSocketSession mockSession = mock(WebSocketSession.class);
        when(mockSession.getAttributes()).thenReturn(att);
        return mockSession;
    }
}
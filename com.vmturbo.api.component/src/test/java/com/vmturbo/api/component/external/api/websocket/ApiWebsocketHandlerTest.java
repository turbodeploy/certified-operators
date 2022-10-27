package com.vmturbo.api.component.external.api.websocket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketMessage;
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
import com.vmturbo.api.PlanDestinationNotificationDTO.PlanDestinationNotification;
import com.vmturbo.api.PlanDestinationNotificationDTO.PlanDestinationStatusNotification;
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
    private ArgumentCaptor<TextMessage> notificationCaptor;

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
        // GIVEN
        MarketNotification notificationOut = MarketNotification.newBuilder()
                .setMarketId("1")
                .setStatusNotification(StatusNotification.newBuilder()
                        .setStatus(Status.DELETED))
                .build();

        // WHEN
        websocketHandler.broadcastMarketNotification(notificationOut);
        verify(session).sendMessage(notificationCaptor.capture());

        // THEN
        assertPayloadEquals(notificationCaptor, notificationOut, "marketNotification");

        final Notification.Builder received = jsonStringToNotification(notificationCaptor);

        assertTrue(received.hasMarketNotification());
        assertEquals(notificationOut, received.getMarketNotification());
        assertEquals("1", received.getMarketNotification().getMarketId());
        assertEquals(Status.DELETED,
                received.getMarketNotification().getStatusNotification().getStatus());
    }

    /**
     * Test that broadcastPlanDestinationNotification() publishes the notification.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testBroadcastPlanDestinationNotification() throws Exception {
        // GIVEN
        PlanDestinationNotification notificationOut = PlanDestinationNotification.newBuilder()
            .setPlanDestinationId("42")
            .setPlanDestinationName("Destination name")
            .setPlanDestinationAccountId("accountId")
            .setPlanDestinationAccountName("accountName")
            .setPlanDestinationHasExportedData(true)
            .setPlanDestinationMarketId("12345")
            .setPlanDestinationProgressNotification(PlanDestinationStatusNotification.newBuilder()
                .setStatus(PlanDestinationStatusNotification.Status.IN_PROGRESS)
                .setProgressPercentage(42)
                .setDescription("A status description message")
                .build())
            .build();

        // WHEN
        websocketHandler.broadcastPlanDestinationNotification(notificationOut);
        verify(session).sendMessage(notificationCaptor.capture());

        // THEN
        assertPayloadEquals(notificationCaptor, notificationOut, "planDestinationNotification");

        final Notification.Builder received = jsonStringToNotification(notificationCaptor);

        assertTrue(received.hasPlanDestinationNotification());
        assertEquals(notificationOut, received.getPlanDestinationNotification());
    }

    /**
     * Test that testBroadcastActionNotification() publishes the notification.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testBroadcastActionNotification() throws Exception {
        // GIVEN
        ActionNotification notificationOut = ActionNotification.newBuilder()
                .setActionProgressNotification(ActionStatusNotification.newBuilder()
                        .setActionId("1")
                        .setStatus(ActionStatusNotification.Status.IN_PROGRESS)
                        .setDescription("foo")
                        .setProgressPercentage(42)
                        .build())
                .build();

        // WHEN
        websocketHandler.broadcastActionNotification(notificationOut);
        verify(session).sendMessage(notificationCaptor.capture());

        // THEN
        assertPayloadEquals(notificationCaptor, notificationOut, "actionNotification");
        final Notification.Builder received = jsonStringToNotification(notificationCaptor);

        assertTrue(received.hasActionNotification());
        assertEquals(notificationOut, received.getActionNotification());

        assertTrue(received.getActionNotification().hasActionProgressNotification());
        final ActionStatusNotification actionStatusNotification = received.getActionNotification().getActionProgressNotification();

        assertEquals("1", actionStatusNotification.getActionId());
        assertEquals("foo",
                actionStatusNotification.getDescription());
        assertEquals(42,
                actionStatusNotification.getProgressPercentage());
    }

    /**
     * Test that testBroadcastReportNotification() publishes the notification.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testBroadcastReportNotification() throws Exception {
        // GIVEN
        ReportNotification notificationOut = ReportNotification.newBuilder()
                .setReportId("1")
                .setReportStatusNotification(ReportStatusNotification.newBuilder()
                        .setDescription("report")
                        .setStatus(ReportStatus.GENERATED)
                        .build()
                ).build();

        // WHEN
        websocketHandler.broadcastReportNotification(notificationOut);
        verify(session).sendMessage(notificationCaptor.capture());

        // THEN
        assertPayloadEquals(notificationCaptor, notificationOut, "reportNotification");
        final Notification.Builder received = jsonStringToNotification(notificationCaptor);

        assertTrue(received.hasReportNotification());
        assertEquals(notificationOut, received.getReportNotification());

        assertEquals("1", received.getReportNotification().getReportId());

        assertTrue(received.getReportNotification().hasReportStatusNotification());
        assertEquals("report",
                received.getReportNotification().getReportStatusNotification().getDescription());
        assertEquals(ReportStatus.GENERATED,
                received.getReportNotification().getReportStatusNotification().getStatus());
    }

    /**
     * Test that testBroadcastTargetsNotification() publishes the notification.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testBroadcastTargetsNotification() throws Exception {
        // GIVEN
        TargetsNotification notificationOut = TargetsNotification.newBuilder()
                .setStatusNotification(TargetStatusNotification.newBuilder()
                        .setStatus(TargetStatus.DISCOVERED)
                        .setDescription("target")
                        .build()
                ).build();

        // WHEN
        websocketHandler.broadcastTargetsNotification(notificationOut);
        verify(session).sendMessage(notificationCaptor.capture());

        // THEN
        assertPayloadEquals(notificationCaptor, notificationOut, "targetsNotification");
        final Notification.Builder received = jsonStringToNotification(notificationCaptor);

        assertTrue(received.hasTargetsNotification());
        assertEquals(notificationOut, received.getTargetsNotification());

        assertTrue(received.getTargetsNotification().hasStatusNotification());
        assertEquals(TargetStatus.DISCOVERED,
                received.getTargetsNotification().getStatusNotification().getStatus());
        assertEquals("target",
                received.getTargetsNotification().getStatusNotification().getDescription());
    }

    /**
     * Test that testBroadcastTargetValidationNotification() publishes the notification.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testBroadcastTargetValidationNotification() throws Exception {
        // GIVEN
        TargetNotification notificationOut = TargetNotification.newBuilder()
                .setTargetId("123")
                .setStatusNotification(TargetStatusNotification.newBuilder()
                        .setStatus(TargetStatus.NOT_VALIDATED)
                        .setDescription("target-validation")
                        .build()
                ).build();

        // WHEN
        websocketHandler.broadcastTargetValidationNotification(notificationOut);
        verify(session).sendMessage(notificationCaptor.capture());

        // THEN
        assertPayloadEquals(notificationCaptor, notificationOut, "targetNotification");
        final Notification.Builder received = jsonStringToNotification(notificationCaptor);

        assertTrue(received.hasTargetNotification());
        assertEquals(notificationOut, received.getTargetNotification());

        assertEquals("123", received.getTargetNotification().getTargetId());

        assertTrue(received.getTargetNotification().hasStatusNotification());
        assertEquals(TargetStatus.NOT_VALIDATED,
                received.getTargetNotification().getStatusNotification().getStatus());
        assertEquals("target-validation",
                received.getTargetNotification().getStatusNotification().getDescription());
    }

    /**
     * Test that broadcastDiagsExportNotification() publishes the notification.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testBroadcastDiagsExportNotification() throws Exception {
        // GIVEN
        ExportNotification notificationOut = ExportNotification.newBuilder()
                .setStatusNotification(ExportStatusNotification.newBuilder()
                        .setStatus(ExportStatus.SUCCEEDED)
                        .setDescription("export")
                        .build()
                ).build();

        // WHEN
        websocketHandler.broadcastDiagsExportNotification(notificationOut);
        verify(session).sendMessage(notificationCaptor.capture());

        // THEN
        assertPayloadEquals(notificationCaptor, notificationOut, "exportNotification");
        final Notification.Builder received = jsonStringToNotification(notificationCaptor);

        assertTrue(received.hasExportNotification());
        assertEquals(notificationOut, received.getExportNotification());

        assertTrue(received.getExportNotification().hasStatusNotification());
        assertEquals(ExportStatus.SUCCEEDED,
                received.getExportNotification().getStatusNotification().getStatus());
        assertEquals("export",
                received.getExportNotification().getStatusNotification().getDescription());
    }

    /**
     * Test that broadcastReservationNotification() publishes the notification.
     *
     * @throws Exception if the test fails.
     */
    @Test
    public void testBroadcastReservationNotification() throws Exception {
        // GIVEN
        ReservationNotification notificationOut = ReservationNotification.newBuilder()
                .setStatusNotification(ReservationStatusNotification.newBuilder()
                        .addReservationStatus(ReservationStatus.newBuilder()
                                .setId("12345")
                                .setStatus("RESERVED"))
                        .build()
                ).build();

        // WHEN
        websocketHandler.broadcastReservationNotification(notificationOut);
        verify(session).sendMessage(notificationCaptor.capture());

        // THEN
        assertPayloadEquals(notificationCaptor, notificationOut, "reservationNotification");
        final Notification.Builder received = jsonStringToNotification(notificationCaptor);

        assertTrue(received.hasReservationNotification());
        assertEquals(notificationOut, received.getReservationNotification());

        assertTrue(received.getReservationNotification().hasStatusNotification());
        assertEquals(1,
                received.getReservationNotification().getStatusNotification().getReservationStatusCount());
        assertEquals("12345",
                received.getReservationNotification().getStatusNotification().getReservationStatus(0).getId());
        assertEquals("RESERVED",
                received.getReservationNotification().getStatusNotification().getReservationStatus(0).getStatus());
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

    /**
     * Test helper method to return an instance of a Notification given a JSON String.
     * This is helpful to convert a captured notification from websocket back to a proper Notification instance.
     * @param notificationCaptor - the ArgumentCapture for the {@link WebSocketSession#sendMessage(WebSocketMessage)}
     * @return Notification.Builder - built instance of the Notification class
     * @throws InvalidProtocolBufferException - Thrown if the json data is malformed when building the Proto Notification
     */
    private Notification.Builder jsonStringToNotification(
            final ArgumentCaptor<TextMessage> notificationCaptor
    ) throws InvalidProtocolBufferException {

        final String jsonPayload = notificationCaptor.getValue().getPayload();
        final Notification.Builder notification = Notification.newBuilder();
        JsonFormat.parser().merge(jsonPayload, notification);

        return notification;
    }

    /**
     * Test helper method to return a JSON element from a JSON String.
     * If a path is provided the child object will be extracted from that property.
     * @param json - JSON string
     * @param path - (optional/nullable) string of the property to access the child object
     * @return - JSON object from the parsed JSON String
     */
    private JsonElement extractJsonElement(String json, @Nullable String path) {
        JsonElement element = JsonParser.parseString(json);
        if (path != null) {
            element = element.getAsJsonObject().get(path);
        }
        return element;
    }

    /**
     * Test helper method to assert that an intended Proto Notification Message is broadcast and captured as a
     * well-formed JSON String.
     *
     * @implNote Your sentNotification MarketNotification,
     *   - the notificationCaptor JSON would be: { marketNotification: {...}, ... }
     *   - the path to access your sent message from the captured JSON would be "marketNotification"
     * @param notificationCaptor - the ArgumentCapture for the {@link WebSocketSession#sendMessage(WebSocketMessage)}
     * @param sentNotification - build instance of the Proto Message being sent for the test
     * @param path - the property name on {@link Notification} JSON object where the {@param sentNotification} will be found.
     */
    private <T extends GeneratedMessageV3> void assertPayloadEquals(
            final ArgumentCaptor<TextMessage> notificationCaptor,
            final T sentNotification,
            final String path
    ) {
        if (notificationCaptor == null) {
            Assert.fail();
        }
        if (sentNotification == null) {
            Assert.fail();
        }
        try {
            JsonElement capturedJSON = extractJsonElement(notificationCaptor.getValue().getPayload(), path);
            JsonElement expectedJSON = extractJsonElement(ApiWebsocketHandler.notificationToJsonString(sentNotification), null);
            assertEquals(capturedJSON, expectedJSON);
        } catch(InvalidProtocolBufferException e) {
            Assert.fail(e.getMessage());
        }
    }
}
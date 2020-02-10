package com.vmturbo.api.component.external.api.websocket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.PingMessage;
import org.springframework.web.socket.PongMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.adapter.jetty.JettyWebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import com.vmturbo.api.ActionNotificationDTO.ActionNotification;
import com.vmturbo.api.ExportNotificationDTO.ExportNotification;
import com.vmturbo.api.MarketNotificationDTO.MarketNotification;
import com.vmturbo.api.NotificationDTO.Notification;
import com.vmturbo.api.ReportNotificationDTO.ReportNotification;
import com.vmturbo.api.ReservationNotificationDTO.ReservationNotification;
import com.vmturbo.api.TargetNotificationDTO.TargetNotification;
import com.vmturbo.api.TargetNotificationDTO.TargetsNotification;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.proactivesupport.DataMetricGauge;

/**
 * Implements websocket handler for the UX.
 */
public class ApiWebsocketHandler extends TextWebSocketHandler implements UINotificationChannel, AutoCloseable {

    static final DataMetricGauge NUM_WEBSOCKET_SESSIONS = DataMetricGauge.builder()
            .withName("api_websocket_sessions_active")
            .withHelp("Number of active websocket sessions.")
            .build()
            .register();

    private static final String HTTP_SESSION_ID_PATH = "HTTP.SESSION.ID";
    private static final String SECURITY_CONTEXT_PATH = "SPRING_SECURITY_CONTEXT";

    /**
     * The logger.
     */
    private final Logger logger_ = LogManager.getLogger();

    /**
     * The sessions.
     */
    private final Set<WebSocketSession> sessions_ = Collections.synchronizedSet(new HashSet<>());
    private final long sessionTimeoutMilliSeconds;
    private final int pingIntervalSeconds;

    /**
     * Scheduler for ping message intervals.
     */
    private final ScheduledExecutorService scheduler;

    public ApiWebsocketHandler(final int sessionTimeoutSeconds, final int pingIntervalSeconds) {
        // convert from second to milliseconds
        this.sessionTimeoutMilliSeconds = TimeUnit.SECONDS.toMillis(sessionTimeoutSeconds);
        this.pingIntervalSeconds = pingIntervalSeconds;

        if (pingIntervalSeconds > 0) {
            // we'll only schedule pings if a valid interval was set.
            logger_.info("Scheduling websocket pings every {} secs.", pingIntervalSeconds);
            scheduler = Executors.newScheduledThreadPool(1,
                    new ThreadFactoryBuilder().setNameFormat("websocket-session-monitor-%d").build());
            scheduler.scheduleAtFixedRate(this::broadcastPing, pingIntervalSeconds, pingIntervalSeconds,
                    TimeUnit.SECONDS);
        } else {
            logger_.info("Not scheduling websocket pings.");
            scheduler = null;
        }
    }

    /**
     * no-args constructor to facilitate testing.
     */
    @VisibleForTesting
    public ApiWebsocketHandler() {
        // default 30 min user time out and no websocket pings
        this(1800, 0);
    }

    /**
     * Handles the message.
     *
     * @param session The session.
     * @param message The message
     */
    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        // We expect websocket communication with the UI to be one-way.
        try {
            session.close(CloseStatus.NOT_ACCEPTABLE.withReason("Text messages not supported"));
        }
        catch (IOException ex) {
            // ignore
        }
    }

    @Override
    protected void handlePongMessage(final WebSocketSession session, final PongMessage message) throws Exception {
        // We are not doing anything special in terms of liveness tracking at the moment, so we'll
        // just quietly log the response. The ping-pong traffic is more for avoiding idle timeouts
        // right now, but in the future we can use it to detect dead clients / bad connections.
        logger_.debug("Received pong message from session {}.", session.getId());
    }

    /**
     * Handle the established sessions.
     *
     * @param session The session.
     * @throws Exception In case of an error processing the opened connection.
     */
    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        // It's tactical to cast WebSocketSession to JettyWebSocketSession, but it seems
        // there is not other ways to configure Jetty WebSocket timeout using Spring WebSocket.
        // Quote from spring.io: Spring Session’s WebSocket support only works with Spring’s
        // WebSocket support.
        // Link: https://docs.spring.io/spring-session/docs/current/reference/html5/#websocket
        // TODO (Gary, Feb 12, 2019) To avoid this tactical and ugly casting, consider convert this
        // TODO to Jetty WebSocket implementations.
        if (session instanceof JettyWebSocketSession) {
            JettyWebSocketSession jettySession = (JettyWebSocketSession) session;
            logger_.debug("Setting websocket idle timeout to {} ms", sessionTimeoutMilliSeconds);
            jettySession.getNativeSession().setIdleTimeout(sessionTimeoutMilliSeconds);
            jettySession.initializeNativeSession(jettySession.getNativeSession());
        } else {
            logger_.error("Failed to enable Jetty WebSockSession timeout," +
                " it seems the session is not generated by Jetty.");
        }
        logger_.info("Connection established: {}, {}", session, this);
        sessions_.add(session);
        NUM_WEBSOCKET_SESSIONS.setData((double) sessions_.size());

        // If Session credentials are not found, user does not have a valid session
        // so close the websocket to prevent information leak to unauthorized user.
        if (!session.getAttributes().containsKey(HTTP_SESSION_ID_PATH)) {
            try {
                logger_.warn("Http Session credentials were not found for user attempting websocket connection." +
                    "  Websocket connection terminated.");
                session.close(CloseStatus.POLICY_VIOLATION
                    .withReason("Session credentials not found.  Websocket connection terminated."));
            } catch (IOException e) {
                logger_.info("Failed to close Websocket session - {}", e.getMessage());
            }
        }
    }

    /**
     * Handle errors.
     *
     * @param session   The session.
     * @param exception The exception that happened.
     * @throws Exception In case of an error handling the situation.
     */
    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception)
            throws Exception {
        // idle websocket timeout exceptions are expected, since we aren't doing keepalives
        if (exception instanceof TimeoutException) {
            logger_.info(exception.getMessage());
        } else {
            // TODO: Figure out the multiple sessions handling.
            logger_.error("Transport error", exception);
        }
    }

    /**
     * Handles the closed connection.
     *
     * @param session The session.
     * @param status  The status.
     * @throws Exception In case of an error processing the connection closing.
     */
    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status)
            throws Exception {
        logger_.info("Connection closed: {}", session);
        sessions_.remove(session);
        NUM_WEBSOCKET_SESSIONS.setData((double) sessions_.size());
    }

    @Override
    public void broadcastMarketNotification(@Nonnull final MarketNotification notification) {
        logger_.debug("Broadcasting market notification: {}", notification);
        broadcastNotification(Notification.newBuilder()
                .setId(IdentityGenerator.next())
                .setTime(Instant.now().toEpochMilli())
                .setMarketNotification(Objects.requireNonNull(notification))
                .build());
    }

    @Override
    public void broadcastActionNotification(@Nonnull final ActionNotification notification) {
        logger_.debug("Broadcasting action notification: {}", notification);
        broadcastNotification(Notification.newBuilder()
                .setId(IdentityGenerator.next())
                .setTime(Instant.now().toEpochMilli())
                .setActionNotification(Objects.requireNonNull(notification))
                .build());
    }

    @Override
    public void broadcastReportNotification(@Nonnull final ReportNotification notification) {
        logger_.debug("Broadcasting report notification: {}", notification);
        broadcastNotification(Notification.newBuilder()
                .setId(IdentityGenerator.next())
                .setTime(Instant.now().toEpochMilli())
                .setReportNotification(Objects.requireNonNull(notification))
                .build());
    }

    @Override
    public void broadcastTargetsNotification(@Nonnull final TargetsNotification notification) {
        logger_.debug("Broadcasting target notification: {}", notification);
        broadcastNotification(Notification.newBuilder()
                .setId(IdentityGenerator.next())
                .setTime(Instant.now().toEpochMilli())
                .setTargetsNotification(Objects.requireNonNull(notification))
                .build());
    }

    @Override
    public void broadcastTargetValidationNotification(@Nonnull final TargetNotification notification) {
        logger_.debug("Broadcasting target notification: {}", notification);
        broadcastNotification(Notification.newBuilder()
            .setId(IdentityGenerator.next())
            .setTime(Instant.now().toEpochMilli())
            .setTargetNotification(Objects.requireNonNull(notification))
            .build());
    }

    @Override
    public void broadcastDiagsExportNotification(@Nonnull final ExportNotification notification) {
        logger_.debug("Broadcasting export diagnostics notification: {}", notification);
        broadcastNotification(Notification.newBuilder()
            .setId(IdentityGenerator.next())
            .setTime(Instant.now().toEpochMilli())
            .setExportNotification(Objects.requireNonNull(notification))
            .build());
    }

    @Override
    public void broadcastReservationNotification(@Nonnull final ReservationNotification notification) {
        logger_.debug("Broadcasting reservation notification: {}", notification);
        broadcastNotification(Notification.newBuilder()
            .setId(IdentityGenerator.next())
            .setTime(Instant.now().toEpochMilli())
            .setReservationNotification(Objects.requireNonNull(notification))
            .build());
    }

    public void broadcastPing() {
        if (sessions_.size() == 0) {
            return; // no need to ping
        }
        logger_.debug("Pinging {} sessions", sessions_.size());
        sessions_.forEach(session -> {
            try {
                session.sendMessage(new PingMessage());
            } catch (IOException e) {
                logger_.error("Error sending notification.", e);
            }
        });
    }

    private void broadcastNotification(@Nonnull final Notification notification) {
        sessions_.forEach(session -> {
            try {
                session.sendMessage(new BinaryMessage(ByteBuffer.wrap(notification.toByteArray())));
            } catch (IOException e) {
                logger_.error("Error sending notification.", e);
            }
        });
    }

    @Override
    public void close() throws Exception {
        if (scheduler != null) {
            logger_.info("Shutting down websocket session monitor scheduler.");
            scheduler.shutdownNow();
        }
    }

    /**
     * Given an HTTP Session ID, close all open websockets that are tied to that ID.
     *
     * @param httpSessionId the HTTP Session ID to close websockets for
     * @param closeStatus the status/reason for closing the websockets
     */
    public void closeWSSessionsByHttpSessionId(@Nonnull final String httpSessionId,
                                               @Nonnull final CloseStatus closeStatus) {
        sessions_.forEach(wsSession -> {
            final Object wsHttpSessionId = wsSession.getAttributes().get(HTTP_SESSION_ID_PATH);
            if (wsHttpSessionId instanceof String
                && StringUtils.equals((String)wsHttpSessionId, httpSessionId)) {
                try {
                    wsSession.close(closeStatus);
                } catch (IOException e) {
                    logger_.info("Failed to close Websocket session.", e);
                }
            }
        });
    }
}

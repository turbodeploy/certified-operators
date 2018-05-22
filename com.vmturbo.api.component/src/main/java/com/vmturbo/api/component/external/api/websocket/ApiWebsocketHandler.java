package com.vmturbo.api.component.external.api.websocket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import com.vmturbo.api.ActionNotificationDTO.ActionNotification;
import com.vmturbo.api.MarketNotificationDTO.MarketNotification;
import com.vmturbo.api.NotificationDTO.Notification;
import com.vmturbo.api.ReportNotificationDTO.ReportNotification;
import com.vmturbo.commons.idgen.IdentityGenerator;

/**
 * Implements websocket handler for the UX.
 */
public class ApiWebsocketHandler extends TextWebSocketHandler implements UINotificationChannel {

    /**
     * The logger.
     */
    private final Logger logger_ = LogManager.getLogger();

    /**
     * The sessions.
     */
    private final Set<WebSocketSession> sessions_ = Collections.synchronizedSet(new HashSet<>());

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

    /**
     * Handle the established sessions.
     *
     * @param session The session.
     * @throws Exception In case of an error processing the opened connection.
     */
    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        logger_.info("Connection established: {}, {}", session, this);
        sessions_.add(session);
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
            logger_.debug(exception.getMessage());
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

    private void broadcastNotification(@Nonnull final Notification notification) {
        sessions_.forEach(session -> {
            try {
                session.sendMessage(new BinaryMessage(ByteBuffer.wrap(notification.toByteArray())));
            } catch (IOException e) {
                logger_.error("Error sending notification.", e);
            }
        });
    }

}

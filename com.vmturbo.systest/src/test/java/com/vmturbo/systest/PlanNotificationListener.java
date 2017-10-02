package com.vmturbo.systest;

import javax.annotation.Nonnull;
import javax.websocket.ClientEndpoint;
import javax.websocket.OnMessage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.api.MarketNotificationDTO.MarketNotification;
import com.vmturbo.api.NotificationDTO;

/**
 * A websocket listener for market status progress notifications. Makes the progress percentage
 * available.
 **/
@ClientEndpoint
public class PlanNotificationListener {

    private static final Logger logger = LogManager.getLogger();

    private int progress = -1;

    /**
     * Process a progress message. Save the "progress" field, if any.
     *
     * Synchronized to help serialize message processing, in case websocket delivers another message
     * before previous message has completed processing.
     *
     * @param protobufMessage a byte array containing a NotificationDTO message
     */
    @OnMessage
    public synchronized void processMessageFromServer(@Nonnull byte[] protobufMessage) {

        NotificationDTO.Notification notification = null;
        try {
            notification = NotificationDTO.Notification.parseFrom(protobufMessage);
            logger.debug("Notification Message came from the server: {}", notification);
            if (notification.hasMarketNotification()) {
                final MarketNotification marketNotification = notification.getMarketNotification();
                if (marketNotification.hasStatusNotification()) {
                    progress = marketNotification.getStatusNotification().getProgressPercentage();
                }
            }
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Error reading notification protobuf.", e);
        }
    }

    /**
     * Fetch the most recent progress value captured from a Plan Notification message.
     * Synchronized since 'progress' may be set from different thread than getProgress() call.
     *
     * @return the most recent prgress value recevied in a Notification
     */
    public synchronized int getProgress() {
        return progress;
    }
}

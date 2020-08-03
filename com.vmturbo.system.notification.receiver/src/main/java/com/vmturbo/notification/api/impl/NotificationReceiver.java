package com.vmturbo.notification.api.impl;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.MulticastNotificationReceiver;
import com.vmturbo.notification.api.NotificationsListener;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;


/**
 * Notification receiver to distribute system notification to listeners.
 */
public class NotificationReceiver extends MulticastNotificationReceiver<SystemNotification, NotificationsListener> {

    /**
     * Kafka system notification name
     */
    public static final String NOTIFICATION_TOPIC = "system-notifications";

    /**
     * {@inheritDoc}
     */
    public NotificationReceiver(
            @Nonnull final IMessageReceiver<SystemNotification> messageReceiver,
            @Nonnull final ExecutorService executorService, int kafkaReceiverTimeoutSeconds) {
        super(messageReceiver, executorService, kafkaReceiverTimeoutSeconds,
                message -> listener -> listener.onNotificationAvailable(message));
    }
}

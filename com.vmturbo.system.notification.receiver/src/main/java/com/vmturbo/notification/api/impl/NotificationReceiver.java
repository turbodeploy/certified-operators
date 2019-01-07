package com.vmturbo.notification.api.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.notification.api.NotificationsListener;
import com.vmturbo.notification.api.SystemNotificationListener;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;


/**
 * Notification receiver to distribute system notification to listeners.
 */
public class NotificationReceiver extends ComponentNotificationReceiver<SystemNotification>
        implements SystemNotificationListener {

    /**
     * Kafka system notification name
     */
    public static final String NOTIFICATION_TOPIC = "system-notifications";

    private final Set<NotificationsListener> listeners = Collections.synchronizedSet(new HashSet<>());

    /**
     * {@inheritDoc}
     */
    public NotificationReceiver(
            @Nonnull final IMessageReceiver<SystemNotification> messageReceiver,
            @Nonnull final ExecutorService executorService) {
        super(messageReceiver, executorService);
    }

    @Override
    protected void processMessage(@Nonnull final SystemNotification message) {
        listeners.forEach(listener -> getExecutorService().submit(() -> {
            try {
                listener.onNotificationAvailable(message);
            } catch (RuntimeException e) {
                getLogger().error("Error executing system notification: ", e);
            }
        }));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addNotificationListener(@Nonnull final NotificationsListener listener) {
        listeners.add(Objects.requireNonNull(listener));
    }
}

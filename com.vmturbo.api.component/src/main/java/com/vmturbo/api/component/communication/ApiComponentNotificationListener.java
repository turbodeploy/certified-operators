package com.vmturbo.api.component.communication;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.notification.NotificationStore;
import com.vmturbo.notification.api.NotificationsListener;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;


/**
 * A listener to listen system notification and store to {@link NotificationStore}.
 */
public class ApiComponentNotificationListener implements NotificationsListener {

    private final NotificationStore store;

    public ApiComponentNotificationListener(@Nonnull final NotificationStore store) {
        this.store = Objects.requireNonNull(store);
    }

    @Override
    public void onNotificationAvailable(@Nonnull final SystemNotification notification) {
        store.storeNotification(Objects.requireNonNull(notification));
    }
}

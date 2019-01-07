package com.vmturbo.notification.api;

import javax.annotation.Nonnull;

import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;


/**
 * A listener for updates about system notification availability.
 */
public interface NotificationsListener {
    /**
     * Indicates that a system notifiation is available.
     *
     * @param notifications A message describing the notification.
     */
    void onNotificationAvailable(@Nonnull final SystemNotification notifications);
}

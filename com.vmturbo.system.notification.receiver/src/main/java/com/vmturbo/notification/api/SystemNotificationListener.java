package com.vmturbo.notification.api;

import javax.annotation.Nonnull;

/**
 * Interface to add system notification listeners
 */
public interface SystemNotificationListener {
    void addNotificationListener(@Nonnull final NotificationsListener listener);
}
package com.vmturbo.notification;

import java.util.Collection;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;


/**
 * Interface to retrieve system notifications
 */
public interface NotificationStore {
    /**
     * Add notification to store
     *
     * @param notification the notification object
     */
    void storeNotification(@Nonnull final SystemNotification notification);

    /**
     * Get the number of notifications after the {@param startTime}
     *
     * @param startTime the time to filter notification
     * @return number of the notification after the {@param startTime}
     */
    long getNotificationCountAfterTimestamp(long startTime);

    /**
     * Retrieve all currently available system notifications
     *
     * @return all notifications currently stored in the store
     */
    @Nonnull
    Collection<SystemNotification> getAllNotifications();

    /**
     * Retrieve system notification by id
     *
     * @param id notification id
     * @return system notification if found
     */
    @Nonnull
    Optional<SystemNotification> getNotification(final long id);
}
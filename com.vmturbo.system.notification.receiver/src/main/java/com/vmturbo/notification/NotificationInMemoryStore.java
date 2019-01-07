package com.vmturbo.notification;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.springframework.util.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;

/**
 * Stores notification in memory. It's designed to:
 * </p>
 * 1. Store small number of notifications (less than 1000).
 * 2. Invalidation:
 * </p>
 * a. the entries will be invalided automatically after storing in Cache for a day (default value)
 * b. the old entries will be invalided automatically if number of notifications exceeded 1000 (default value)
 */
@ThreadSafe
public class NotificationInMemoryStore implements NotificationStore {

    // lock protecting access to dayNotificationCache cache.
    private final Object cacheLock = new Object();

    /**
     * Ensure correct concurrent access. Key is notification id; Value is notification.
     */
    @GuardedBy("cacheLock")
    private final Cache<Long, SystemNotification> dayNotificationCache;

    /**
     * Constructor {@link NotificationInMemoryStore}.
     *
     * @param maximumSize the maximum size of the cache
     * @param duration    the length of time after an entry is created that it should be automatically
     *                    removed
     * @param unit        the unit that {@code duration} is expressed in
     */
    public NotificationInMemoryStore(
            final long maximumSize,
            final long duration,
            @Nonnull final TimeUnit unit) {
        dayNotificationCache = CacheBuilder
                .newBuilder()
                .expireAfterWrite(duration, unit)
                .maximumSize(maximumSize)
                .build();
    }

    @Override
    public void storeNotification(@Nonnull final SystemNotification notification) {
        synchronized (cacheLock) {
            dayNotificationCache.put(notification.getBroadcastId(), notification);
            // To ensure cache are up-to-date, trigger clean up to force invalidation.
            dayNotificationCache.cleanUp();
        }
    }

    @Override
    public long getNotificationCountAfterTimestamp(final long startTime) {
        synchronized (cacheLock) {
            return dayNotificationCache.asMap().values().stream()
                    .filter(notification -> notification.getGenerationTime() > startTime)
                    .count();
        }
    }

    @Override
    public Collection<SystemNotification> getAllNotifications() {
        synchronized (cacheLock) {
            return Collections.unmodifiableCollection(dayNotificationCache.asMap().values());
        }
    }

    @Nonnull
    @Override
    public Optional<SystemNotification> getNotification(final long id) {
        Preconditions.checkArgument(!StringUtils.isEmpty(id));
        synchronized (cacheLock) {
            final SystemNotification systemNotification = dayNotificationCache.getIfPresent(id);
            return Optional.ofNullable(systemNotification);
        }
    }
}

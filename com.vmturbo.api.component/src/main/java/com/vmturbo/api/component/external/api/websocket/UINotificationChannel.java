package com.vmturbo.api.component.external.api.websocket;

import javax.annotation.Nonnull;

import com.vmturbo.api.ActionNotificationDTO.ActionNotification;
import com.vmturbo.api.MarketNotificationDTO.MarketNotification;

/**
 * This is the channel that API services use to push notifications to UI listeners
 * (currently over websocket).
 */
public interface UINotificationChannel {

    /**
     * Broadcast a notification related to a market (i.e. a plan).
     *
     * @param notification The notification to send.
     */
    void broadcastMarketNotification(@Nonnull final MarketNotification notification);

    /**
     * Broadcast a notification related to actions to the UI.
     *
     * @param notification The notificaiton to send.
     */
    void broadcastActionNotification(@Nonnull final ActionNotification notification);
}

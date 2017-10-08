package com.vmturbo.history.component.api.impl;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.WebsocketNotificationReceiver;
import com.vmturbo.history.component.api.HistoryComponentNotifications.HistoryComponentNotification;

/**
 * Websocket message receiver for History clients.
 */
public class HistoryMessageReceiver extends
        WebsocketNotificationReceiver<HistoryComponentNotification> {
    /**
     * Constructs message receiver.
     *
     * @param connectionConfig connection config
     * @param threadPool thread pool to use
     */
    public HistoryMessageReceiver(@Nonnull ComponentApiConnectionConfig connectionConfig,
            @Nonnull ExecutorService threadPool) {
        super(connectionConfig, HistoryComponentNotificationReceiver.WEBSOCKET_PATH, threadPool,
                HistoryComponentNotification::parseFrom);
    }
}

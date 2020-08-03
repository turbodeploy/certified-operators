package com.vmturbo.history.component.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.MulticastNotificationReceiver;
import com.vmturbo.history.component.api.HistoryComponentNotifications.HistoryComponentNotification;
import com.vmturbo.history.component.api.StatsListener;

public class HistoryComponentNotificationReceiver
        extends MulticastNotificationReceiver<HistoryComponentNotification, StatsListener> {

    public static final String NOTIFICATION_TOPIC = "historyNotifications";

    /**
     * {@inheritDoc}
     */
    public HistoryComponentNotificationReceiver(
            @Nonnull final IMessageReceiver<HistoryComponentNotification> messageReceiver,
            @Nonnull final ExecutorService executorService, int kafkaReceiverTimeoutSeconds) {
        super(messageReceiver, executorService, kafkaReceiverTimeoutSeconds,
                HistoryComponentNotificationReceiver::routeMessage);
    }

    private static Consumer<StatsListener> routeMessage(@Nonnull final HistoryComponentNotification message) {
        switch (message.getTypeCase()) {
            case STATS_AVAILABLE:
                return listener -> listener.onStatsAvailable(message.getStatsAvailable());
            default:
                return listener -> { };
        }
    }
}

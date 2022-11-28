package com.vmturbo.history.component.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.MulticastNotificationReceiver;
import com.vmturbo.history.component.api.HistoryComponentNotifications.HistoryComponentNotification;
import com.vmturbo.history.component.api.StatsListener;

public class HistoryComponentNotificationReceiver
        extends MulticastNotificationReceiver<HistoryComponentNotification, StatsListener> {

    private static final Logger logger = LogManager.getLogger();
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

    private static Consumer<StatsListener> routeMessage(@Nonnull final HistoryComponentNotification msg) {
        switch (msg.getTypeCase()) {
            case STATS_AVAILABLE:
                return listener -> {
                    logger.info("Invoking HistoryComponentNotification listener, "
                                    + "listener={} type={}, broadcastId={}",
                            listener.getClass().getSimpleName(), msg.getTypeCase(), msg.getBroadcastId());
                    listener.onStatsAvailable(msg.getStatsAvailable());
                };
            default:
                return listener -> { };
        }
    }
}

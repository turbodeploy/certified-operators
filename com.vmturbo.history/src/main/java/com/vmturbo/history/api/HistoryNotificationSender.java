package com.vmturbo.history.api;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.history.component.api.HistoryComponentNotifications.HistoryComponentNotification;
import com.vmturbo.history.component.api.HistoryComponentNotifications.StatsAvailable;

/**
 * The class responsible for sending notifications to websocket listeners.
 */
@ThreadSafe
public class HistoryNotificationSender extends ComponentNotificationSender<HistoryComponentNotification> {
    private final Logger logger = LogManager.getLogger();

    HistoryNotificationSender(@Nonnull final ExecutorService executorService) {
        super(executorService);
    }

    public void statsAvailable(final long topologyContextId) {
        final long messageChainId = newMessageChainId();
        logger.info("Stats available for context: {}", topologyContextId);

        sendMessage(messageChainId,
            HistoryComponentNotification.newBuilder()
                .setBroadcastId(messageChainId)
                .setStatsAvailable(StatsAvailable.newBuilder().setTopologyContextId(topologyContextId))
                .build());
    }

    @Override
    protected String describeMessage(
            @Nonnull HistoryComponentNotification historyComponentNotification) {
        return HistoryComponentNotification.class.getSimpleName() + "[" +
                historyComponentNotification.getBroadcastId() + "]";
    }
}

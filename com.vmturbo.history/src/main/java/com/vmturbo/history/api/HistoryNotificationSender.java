package com.vmturbo.history.api;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import oracle.net.ns.Communication;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.history.component.api.HistoryComponentNotifications.HistoryComponentNotification;
import com.vmturbo.history.component.api.HistoryComponentNotifications.StatsAvailable;

/**
 * The class responsible for sending notifications to websocket listeners.
 */
@ThreadSafe
public class HistoryNotificationSender extends ComponentNotificationSender<HistoryComponentNotification> {

    private final IMessageSender<HistoryComponentNotification> sender;

    public HistoryNotificationSender(@Nonnull IMessageSender<HistoryComponentNotification> sender) {
        this.sender = Objects.requireNonNull(sender);
    }

    public void statsAvailable(final long topologyContextId)
            throws CommunicationException, InterruptedException {
        final long messageChainId = newMessageChainId();
        getLogger().info("Stats available for context: {}", topologyContextId);

        sendMessage(sender,
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

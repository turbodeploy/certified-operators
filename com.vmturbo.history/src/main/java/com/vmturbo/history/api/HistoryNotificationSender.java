package com.vmturbo.history.api;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.history.component.api.HistoryComponentNotifications.HistoryComponentNotification;
import com.vmturbo.history.component.api.HistoryComponentNotifications.HistoryComponentNotification.Builder;
import com.vmturbo.history.component.api.HistoryComponentNotifications.StatsAvailable;
import com.vmturbo.history.component.api.HistoryComponentNotifications.StatsAvailable.UpdateFailure;

/**
 * The class responsible for sending notifications to websocket listeners.
 */
@ThreadSafe
public class HistoryNotificationSender extends ComponentNotificationSender<HistoryComponentNotification> {

    private final IMessageSender<HistoryComponentNotification> sender;

    public HistoryNotificationSender(@Nonnull IMessageSender<HistoryComponentNotification> sender) {
        this.sender = Objects.requireNonNull(sender);
    }

    /**
     * Sends the stats available notification.
     *
     * @param topologyContextId The topology context ID
     */
    public void statsAvailable(final long topologyContextId) {
        final long messageChainId = newMessageChainId();
        getLogger().info("Stats available for context: {}", topologyContextId);

        try {
            sendMessage(sender, buildHistoryNotification(topologyContextId, messageChainId,
                    false));
        } catch (InterruptedException | CommunicationException e) {
            getLogger().error("An error happened while sending stats available " +
                    "notification.", e);
        }
    }

    /**
     * Sends the stats failure notification.
     *
     * @param topologyContextId The topology context ID
     */
    public void statsFailure(final long topologyContextId) {
        final long messageChainId = newMessageChainId();
        getLogger().info("Stats is not available for context: {}", topologyContextId);

        try {
            sendMessage(sender, buildHistoryNotification(topologyContextId, messageChainId,
                    true));
        } catch (InterruptedException | CommunicationException e) {
            getLogger().error("An error happened while trying to send stats failure " +
                    "notification.", e);
        }
    }

    private HistoryComponentNotification buildHistoryNotification(final long topologyContextId,
                                                                  final long messageChainId,
                                                                  boolean isFailure) {
        final Builder historyComponentNotificationBuilder = HistoryComponentNotification.newBuilder()
                .setBroadcastId(messageChainId);
        if (isFailure) {
            historyComponentNotificationBuilder.setStatsAvailable(StatsAvailable.newBuilder()
                    .setTopologyContextId(topologyContextId)
                    .setUpdateFailure(UpdateFailure.newBuilder()
                            .setErrorMessage("An error happened while getting the stats.")));
        } else {
            historyComponentNotificationBuilder.setStatsAvailable(StatsAvailable.newBuilder()
                    .setTopologyContextId(topologyContextId));
        }
        return historyComponentNotificationBuilder.build();
    }

    @Override
    protected String describeMessage(
            @Nonnull HistoryComponentNotification historyComponentNotification) {
        return HistoryComponentNotification.class.getSimpleName() + "[" +
                historyComponentNotification.getBroadcastId() + "]";
    }
}

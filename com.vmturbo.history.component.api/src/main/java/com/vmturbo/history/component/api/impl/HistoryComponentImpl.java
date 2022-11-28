package com.vmturbo.history.component.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.history.VolAttachmentHistoryOuterClass;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.MulticastNotificationReceiver;
import com.vmturbo.history.component.api.HistoryNotificationListener;

/**
 * The notification receiver connecting to the repo component.
 */
public class HistoryComponentImpl extends
        MulticastNotificationReceiver<VolAttachmentHistoryOuterClass.VolAttachmentHistory, HistoryNotificationListener> implements HistoryComponent {

    private static final Logger logger = LogManager.getLogger();
    /**
     * Kafka topic to receive history notification messages (volume unattachedDays updates).
     */
    public static final String HISTORY_VOL_NOTIFICATIONS = "volume-notifications";

    /**
     * The constructor of the cost component message receiver.
     *
     * @param historyNotificationMessageReceiver Message receiver for history notifications
     * @param executorService Executor service to use for communication with the server.
     * @param kafkaReceiverTimeoutSeconds The max number of seconds to wait for all notification
     *                                    listeners to finish handling a notification.
     */
    public HistoryComponentImpl(
            @Nullable final IMessageReceiver<VolAttachmentHistoryOuterClass.VolAttachmentHistory> historyNotificationMessageReceiver,
            @Nonnull final ExecutorService executorService, int kafkaReceiverTimeoutSeconds) {
        super(historyNotificationMessageReceiver, executorService, kafkaReceiverTimeoutSeconds,
                msg -> routeMessage(msg));
    }

    private static Consumer<HistoryNotificationListener> routeMessage(
            @Nonnull final VolAttachmentHistoryOuterClass.VolAttachmentHistory msg) {
        return listener -> {
            logger.info("Invoking HistoryNotificationListener listener, "
                            + "listener={} lastAttachedMapSize={}, topologyId={}",
                    listener.getClass().getSimpleName(), msg.getVolIdToLastAttachedDateMap().size(),
                    msg.getTopologyId());
            listener.onHistoryNotificationReceived(msg);
        };
    }

    @Override
    public void addVolumeHistoryNotificationListener(@NotNull HistoryNotificationListener listener) {
        addListener(listener);
    }

}

package com.vmturbo.cost.api.impl;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.MulticastNotificationReceiver;
import com.vmturbo.cost.api.CostComponent;
import com.vmturbo.cost.api.CostNotificationListener;

/**
 * The notification receiver connecting to the cost component.
 */
public class CostComponentImpl extends
        MulticastNotificationReceiver<CostNotification, CostNotificationListener> implements CostComponent {

    /**
     * Kafka topic to receive cost notification messages (status updates).
     */
    public static final String COST_NOTIFICATIONS = "cost-notifications";

    /**
     * The constructor of the cost component message receiver
     *
     * @param costNotificationMessageReceiver Message receiver for cost status notifications
     * @param executorService Executor service to use for communication with the server.
     * @param kafkaReceiverTimeoutSeconds The max number of seconds to wait for all notification
     *                                    listeners to finish handling a notification.
     */
    public CostComponentImpl(
            @Nullable final IMessageReceiver<CostNotification> costNotificationMessageReceiver,
            @Nonnull final ExecutorService executorService, int kafkaReceiverTimeoutSeconds) {
        super(costNotificationMessageReceiver, executorService, kafkaReceiverTimeoutSeconds,
                msg -> listener -> listener.onCostNotificationReceived(msg));
    }

    @Override
    public void addCostNotificationListener(@Nonnull final CostNotificationListener listener) {
        addListener(listener);
    }

}

package com.vmturbo.cost.api.impl;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.cost.api.CostComponent;
import com.vmturbo.cost.api.CostNotificationListener;

/**
 * The notification receiver connecting to the cost component.
 */
public class CostComponentImpl extends
        ComponentNotificationReceiver<CostNotification> implements CostComponent {

    /**
     * Kafka topic to receive cost notifications.
     */
    public static final String COST_NOTIFICATIONS = "cost-notifications";

    private final Set<CostNotificationListener> listeners =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * The constructor of the cost notification receiver.
     *
     * @param messageReceiver Message receiver to await messages from
     * @param executorService Executor service to use for communication with the server.
     */
    public CostComponentImpl(
            @Nonnull final IMessageReceiver<CostNotification> messageReceiver,
            @Nonnull final ExecutorService executorService) {
        super(messageReceiver, executorService);
    }

    @Override
    protected void processMessage(@Nonnull final CostNotification message) {
        for (CostNotificationListener listener : listeners) {
            listener.onCostNotificationReceived(message);
        }
    }

    @Override
    public void addCostNotificationListener(@Nonnull final CostNotificationListener listener) {
        listeners.add(listener);
    }

}

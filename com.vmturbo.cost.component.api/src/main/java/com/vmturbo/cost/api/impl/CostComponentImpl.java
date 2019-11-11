package com.vmturbo.cost.api.impl;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
     * Kafka topic to receive cost notification messages (status updates).
     */
    public static final String COST_NOTIFICATIONS = "cost-notifications";

    private final Logger logger = LogManager.getLogger();

    private final Set<CostNotificationListener> costNotificationListeners =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * The constructor of the cost component message receiver
     *
     * @param costNotificationMessageReceiver Message receiver for cost status notifications
     * @param executorService Executor service to use for communication with the server.
     */
    public CostComponentImpl(
            @Nullable final IMessageReceiver<CostNotification> costNotificationMessageReceiver,
            @Nonnull final ExecutorService executorService) {
        super(costNotificationMessageReceiver, executorService);
    }

    @Override
    protected void processMessage(@Nonnull final CostNotification message) {
        for (CostNotificationListener listener : costNotificationListeners) {
            listener.onCostNotificationReceived(message);
        }
    }

    @Override
    public void addCostNotificationListener(@Nonnull final CostNotificationListener listener) {
        costNotificationListeners.add(listener);
    }

}

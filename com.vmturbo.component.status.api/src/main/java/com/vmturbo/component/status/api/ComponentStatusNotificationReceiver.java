package com.vmturbo.component.status.api;

import java.util.concurrent.ExecutorService;

import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentStatusNotification;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.MulticastNotificationReceiver;

/**
 * Receiver for {@link ComponentStatusNotification}s.
 */
public class ComponentStatusNotificationReceiver extends MulticastNotificationReceiver<ComponentStatusNotification, ComponentStatusListener> {

    ComponentStatusNotificationReceiver(
            final IMessageReceiver<ComponentStatusNotification> notificationClientMessageReceiver,
            final ExecutorService executorService,
            final int kafkaReceiverTimeoutSeconds) {
        super(notificationClientMessageReceiver, executorService, kafkaReceiverTimeoutSeconds,
            message -> listener -> listener.onComponentNotification(message));
    }
}

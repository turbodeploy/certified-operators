package com.vmturbo.components.common.notification;

import java.time.Clock;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentIdentifier;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentStarting;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentStatusNotification;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentStopping;
import com.vmturbo.common.protobuf.cluster.ComponentStatusProtoUtil;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * Used to send {@link ComponentStatusNotification}s. Provides utility methods, and abstracts
 * away the "lower-level" message details (like the broadcast id or the message time).
 */
public class ComponentStatusNotificationSender extends ComponentNotificationSender<ComponentStatusNotification> {

    private final IMessageSender<ComponentStatusNotification> sender;
    private final Clock clock;

    /**
     * Create a new notification sender.
     *
     * @param sender The actual sender for notifications.
     * @param clock System clock.
     */
    public ComponentStatusNotificationSender(@Nonnull IMessageSender<ComponentStatusNotification> sender,
                                             @Nonnull final Clock clock) {
        this.sender = Objects.requireNonNull(sender);
        this.clock = Objects.requireNonNull(clock);
    }

    /**
     * Send a "component starting" notification.
     *
     * @param componentStarting The {@link ComponentStarting} notification to send.
     * @throws CommunicationException If there is an error sending.
     * @throws InterruptedException If the thread is interrupted while sending.
     */
    public void sendStartingNotification(@Nonnull final ComponentStarting componentStarting)
            throws CommunicationException, InterruptedException {

        getLogger().info("Component starting notification available: {}", componentStarting);

        final long time = clock.millis();
        sendMessage(sender, ComponentStatusNotification.newBuilder()
            .setTimeMs(time)
            .setStartup(componentStarting)
            .build());
    }

    /**
     * Send a "component stopping" notification.
     *
     * @param componentStopping The {@link ComponentStopping} notification to send.
     * @throws CommunicationException If there is an error sending.
     * @throws InterruptedException If the thread is interrupted while sending.
     */
    public void sendStoppingNotification(@Nonnull final ComponentStopping componentStopping)
            throws CommunicationException, InterruptedException {

        getLogger().info("Component stopping notification available: {}", componentStopping);

        final long time = clock.millis();
        sendMessage(sender, ComponentStatusNotification.newBuilder()
            .setTimeMs(time)
            .setShutdown(componentStopping)
            .build());
    }

    @Override
    protected String describeMessage(@Nonnull final ComponentStatusNotification componentNotification) {
        final ComponentIdentifier id = componentNotification.hasStartup() ?
            componentNotification.getStartup().getComponentInfo().getId() :
            componentNotification.getShutdown().getComponentId();
        return ComponentStatusNotification.class.getSimpleName() + ":" + ComponentStatusProtoUtil.getComponentLogId(id);
    }
}

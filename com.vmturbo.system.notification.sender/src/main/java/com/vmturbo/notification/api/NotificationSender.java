package com.vmturbo.notification.api;

import java.time.Clock;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.State;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification.Builder;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification.Category;

/**
 * The class responsible for sending notifications to Kafka system-topic listeners.
 */
@ThreadSafe
public class NotificationSender extends ComponentNotificationSender<SystemNotification> {

    private final IMessageSender<SystemNotification> sender;
    private final Clock clock;

    public NotificationSender(@Nonnull IMessageSender<SystemNotification> sender, @Nonnull final Clock clock) {
        this.sender = Objects.requireNonNull(sender);
        this.clock = Objects.requireNonNull(clock);
    }

    /**
     * Sent notification to registered notification listeners.
     *
     * @param category         notification category, see {@link SystemNotification.Category} for details
     * @param description      notification description
     * @param shortDescription notification shortDescription
     * @param severity         notification severity, see {@link com.vmturbo.common.protobuf.action.ActionDTO.Severity} for details
     * @param state            notification state
     * @param generationTime   notification generation time
     * @throws CommunicationException during communications
     * @throws InterruptedException when the thread is interrupted
     */
    public void sendNotification(@Nonnull final Category category,
                                 @Nonnull final String description,
                                 @Nonnull final String shortDescription,
                                 @Nonnull final Severity severity,
                                 @Nonnull final State state,
                                 @Nonnull final Optional<Long> generationTime)
            throws CommunicationException, InterruptedException {
        final long messageChainId = newMessageChainId();
        getLogger().info("System notification available: {}", description);

        final long notificationTime = generationTime.orElse(clock.millis());
        final Builder builder = SystemNotification.newBuilder();
        builder.setBroadcastId(messageChainId)
                .setCategory(category)
                .setDescription(description)
                .setShortDescription(shortDescription)
                .setSeverity(severity)
                .setState(state)
                .setGenerationTime(notificationTime);

        sendMessage(sender, builder.build());
    }

    /**
     * Sent notification to registered notification listeners.
     *
     * @param category         notification category, see {@link SystemNotification.Category} for details
     * @param description      notification description
     * @param shortDescription notification shortDescription
     * @param severity         notification severity
     * @throws CommunicationException during communications
     * @throws InterruptedException when the thread is interrupted
     */
    public void sendNotification(@Nonnull final Category category,
                                 @Nonnull final String description,
                                 @Nonnull final String shortDescription,
                                 @Nonnull final Severity severity) throws CommunicationException, InterruptedException {
        sendNotification(category,
                description,
                shortDescription,
                severity,
                State.NOTIFY,
                Optional.empty());
    }

    @Override
    protected String describeMessage(
            @Nonnull SystemNotification systemNotification) {
        return SystemNotification.class.getSimpleName() + "[" + systemNotification.getBroadcastId() + "]";
    }
}

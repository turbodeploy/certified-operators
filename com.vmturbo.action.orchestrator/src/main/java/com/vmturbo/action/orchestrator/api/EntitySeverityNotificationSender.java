package com.vmturbo.action.orchestrator.api;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.EntitySeverityNotificationOuterClass.EntitySeverityNotification;
import com.vmturbo.common.protobuf.action.EntitySeverityNotificationOuterClass.EntitySeverityNotification.EntitiesWithSeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityNotificationOuterClass.EntitySeverityNotification.SeverityBreakdown;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * Responsible for sending out {@link EntitySeverityNotification}s.
 */
public class EntitySeverityNotificationSender extends ComponentNotificationSender<EntitySeverityNotification> {
    private static final Logger logger = LogManager.getLogger();

    private final IMessageSender<EntitySeverityNotification> sender;

    /**
     * Create a new instance.
     *
     * @param sender The {@link IMessageSender} used to send messages to the notification channel.
     */
    public EntitySeverityNotificationSender(@Nonnull final IMessageSender<EntitySeverityNotification> sender) {
        this.sender = sender;
    }

    /**
     * Send a full refresh - indicating there is a completely new set of severities.
     *
     * @param refreshedSeverities The new severities.
     * @param severityBreakdowns The new severity breakdowns.
     * @throws CommunicationException If there is an error sending the message.
     * @throws InterruptedException If there is an error sending the message.
     */
    public void sendSeverityRefresh(Collection<EntitiesWithSeverity> refreshedSeverities,
                                    Map<Long, SeverityBreakdown> severityBreakdowns)
            throws CommunicationException, InterruptedException {
        final EntitySeverityNotification notification = EntitySeverityNotification.newBuilder()
                .setFullRefresh(true)
                .addAllEntitiesWithSeverity(refreshedSeverities)
                .putAllSeverityBreakdowns(severityBreakdowns)
                .build();
        sendMessage(notification);
    }

    private void sendMessage(EntitySeverityNotification serverMsg)
            throws CommunicationException, InterruptedException {
        final String messageDescription = describeMessage(serverMsg);
        logger.info("Sending message " + messageDescription + " for broadcast to listeners.");
        sender.sendMessage(serverMsg);
    }

    /**
     * Send an update - indicating there is one (or more) entities that have changed their severity
     * since the last refresh.
     *
     * @param updatedSeverities The updated severities.
     * @throws CommunicationException If there is an error sending the message.
     * @throws InterruptedException If there is an error sending the message.
     */
    public void sendSeverityUpdate(Collection<EntitiesWithSeverity> updatedSeverities)
            throws CommunicationException, InterruptedException {
        sendMessage(EntitySeverityNotification.newBuilder()
            .setFullRefresh(false)
            .addAllEntitiesWithSeverity(updatedSeverities)
            .build());
    }

    @Override
    protected String describeMessage(@Nonnull EntitySeverityNotification entitySeverityNotification) {
        return EntitySeverityNotification.class.getSimpleName();
    }

}

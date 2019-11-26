package com.vmturbo.cost.component.notification;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdate;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * It enables the cost component to send notifications to.
 */
public class CostNotificationSender extends
        ComponentNotificationSender<CostNotification> {

    private final Logger logger = LogManager.getLogger();

    /**
     * The sender of the cost notifications.
     */
    private final IMessageSender<CostNotification> notificationSender;

    /**
     * The constructor of the cost notification sender.
     *
     * @param notificationSender The sender of cost notification messages
     */
    public CostNotificationSender(
            @Nonnull IMessageSender<CostNotification> notificationSender) {
        this.notificationSender = Objects.requireNonNull(notificationSender);
    }

    /**
     * Sends the cost status notification.
     *
     * @param costNotification The cost notification
     * @throws InterruptedException   if sending thread has been interrupted. This does not
     *                                guarantee, that message has ben sent nor it has not been sent
     * @throws CommunicationException if persistent communication error occurred (message could
     *                                not be sent in future).
     */
    public void sendStatusNotification(@Nonnull final CostNotification costNotification)
            throws CommunicationException, InterruptedException {
        sendMessage(notificationSender, costNotification);
    }

    @Override
    protected String describeMessage(@Nonnull final CostNotification
                                             costNotification) {
        if (costNotification.hasStatusUpdate()) {
            final StatusUpdate statusUpdate = costNotification.getStatusUpdate();
            return new ToStringBuilder(costNotification, ToStringStyle.SHORT_PREFIX_STYLE)
                    .append("Type", statusUpdate.getType())
                    .append("Topology ID", statusUpdate.getTopologyId())
                    .append("Topology Context ID", statusUpdate.getTopologyContextId())
                    .build();
        }
        return CostNotificationSender.class.getSimpleName()
                + "[ This message type is not implemented. ]";
    }

}

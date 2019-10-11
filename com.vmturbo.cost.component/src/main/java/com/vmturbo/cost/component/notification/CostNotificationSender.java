package com.vmturbo.cost.component.notification;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * It enables the cost component to send notifications to.
 */
public class CostNotificationSender extends
        ComponentNotificationSender<CostNotification> {

    /**
     * The sender of the cost notification.
     */
    private final IMessageSender<CostNotification> sender;

    /**
     * The constructor of the cost notification sender.
     *
     * @param sender The sender of the message
     */
    public CostNotificationSender(@Nonnull IMessageSender<CostNotification> sender) {
        this.sender = Objects.requireNonNull(sender);
    }

    /**
     * Sends the cost notification.
     *
     * @param costNotification The cost notification
     * @throws InterruptedException   if sending thread has been interrupted. This does not
     *                                guarantee, that message has ben sent nor it has not been sent
     * @throws CommunicationException if persistent communication error occurred (message could
     *                                not be sent in future).
     */
    public void sendNotification(@Nonnull final CostNotification costNotification)
            throws CommunicationException, InterruptedException {
        sendMessage(sender, costNotification);
    }

    @Override
    protected String describeMessage(@Nonnull final CostNotification
                                             costNotification) {
        if (costNotification.hasProjectedCostUpdate()) {
            return CostNotificationSender.class.getSimpleName()
                    + "[ Topology ID: " +
                    costNotification.getProjectedCostUpdate()
                            .getTopologyId()
                    + ", Topology context ID: " +
                    costNotification.getProjectedCostUpdate()
                            .getTopologyContextId() + " ]";
        }
        return CostNotificationSender.class.getSimpleName()
                + "[ This message type is not implemented. ]";
    }

}

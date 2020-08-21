package com.vmturbo.plan.orchestrator.project;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.protobuf.TextFormat;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectStatusNotification;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectStatusNotification.StatusUpdate;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * Used to receive and send plan project status change related notifications.
 */
public class PlanProjectNotificationSender
        extends ComponentNotificationSender<PlanProjectStatusNotification>
        implements PlanProjectStatusListener {

    /**
     * Sender that is used to notify status changes.
     */
    private final IMessageSender<PlanProjectStatusNotification> sender;

    /**
     * Creates a new sender.
     *
     * @param sender Message sender instance.
     */
    public PlanProjectNotificationSender(@Nonnull final
                                         IMessageSender<PlanProjectStatusNotification> sender) {
        this.sender = Objects.requireNonNull(sender);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onPlanStatusChanged(@Nonnull final PlanProject planProject)
            throws PlanProjectStatusListenerException {
        try {
            sendMessage(sender, PlanProjectStatusNotification.newBuilder()
                    .setUpdate(StatusUpdate.newBuilder()
                            .setPlanProjectId(planProject.getPlanProjectId())
                            .setPlanProjectStatus(planProject.getStatus())
                            .build()).build());
        } catch (CommunicationException e) {
            throw new PlanProjectStatusListenerException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Used to format notification message.
     *
     * @param planProjectMessage Project received message.
     * @return Formatted message.
     */
    @Override
    protected String describeMessage(@Nonnull PlanProjectStatusNotification planProjectMessage) {
        return TextFormat.printer().printToString(planProjectMessage);
    }
}
package com.vmturbo.plan.orchestrator.plan.export;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.protobuf.TextFormat;

import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportNotification;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportNotification.DestinationUpdate;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * API backend for plan export related notifications.
 */
public class PlanExportNotificationSender
    extends ComponentNotificationSender<PlanExportNotification> implements PlanExportListener {

    private final IMessageSender<PlanExportNotification> sender;

    PlanExportNotificationSender(@Nonnull IMessageSender<PlanExportNotification> sender) {
        this.sender = Objects.requireNonNull(sender);
    }

    @Override
    public void onPlanDestinationStateChanged(@Nonnull final PlanDestination updatedDestination)
            throws PlanExportListenerException {
        try {
            sendMessage(sender, PlanExportNotification.newBuilder().setDestinationStateUpdate(
                DestinationUpdate.newBuilder().setUpdatedDestination(updatedDestination)
            ).build());
        } catch (CommunicationException e) {
            throw new PlanExportListenerException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void onPlanDestinationProgress(@Nonnull final PlanDestination updatedDestination)
        throws PlanExportListenerException {
        try {
            sendMessage(sender, PlanExportNotification.newBuilder().setDestinationProgressUpdate(
                DestinationUpdate.newBuilder().setUpdatedDestination(updatedDestination)
            ).build());
        } catch (CommunicationException e) {
            throw new PlanExportListenerException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    protected String describeMessage(@Nonnull PlanExportNotification exportNotification) {
        return TextFormat.printer().printToString(exportNotification);
    }
}

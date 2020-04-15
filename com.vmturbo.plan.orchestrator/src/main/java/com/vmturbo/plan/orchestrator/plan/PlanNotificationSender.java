package com.vmturbo.plan.orchestrator.plan;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.protobuf.TextFormat;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanStatusNotification;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanStatusNotification.PlanDeleted;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanStatusNotification.StatusUpdate;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * API backend for plan-related notifications.
 */
public class PlanNotificationSender extends ComponentNotificationSender<PlanStatusNotification> implements PlanStatusListener {

    private final IMessageSender<PlanStatusNotification> sender;

    PlanNotificationSender(@Nonnull IMessageSender<PlanStatusNotification> sender) {
        this.sender = Objects.requireNonNull(sender);
    }

    @Override
    public void onPlanStatusChanged(@Nonnull final PlanInstance plan)
            throws PlanStatusListenerException {
        try {
            sendMessage(sender, PlanStatusNotification.newBuilder()
                .setUpdate(StatusUpdate.newBuilder()
                    .setPlanId(plan.getPlanId())
                    .setNewPlanStatus(plan.getStatus())
                    .build())
                .build());
        } catch (CommunicationException e) {
            throw new PlanStatusListenerException(e);
        } catch (InterruptedException e) {
            // Reset the interrupted status
            // TODO (roman, Nov 29 2017): We should probably propagate the exception up, all
            // the way to the caller of the updatePlanInstance() method. However, an equally
            // good approach is to get rid of the InterruptedException in IMessageSender
            // after we deprecate/get rid of the websocket implementation. The Kafka
            // implementation doesn't actually throw that exception.
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void onPlanDeleted(@Nonnull final PlanInstance plan) throws PlanStatusListenerException {
        try {

            sendMessage(sender, PlanStatusNotification.newBuilder()
                .setDelete(PlanDeleted.newBuilder()
                    .setPlanId(plan.getPlanId())
                    .setStatusBeforeDelete(plan.getStatus()))
                .build());
        } catch (CommunicationException e) {
            throw new PlanStatusListenerException(e);
        } catch (InterruptedException e) {
            // Reset the interrupted status
            // TODO (roman, Nov 29 2017): We should probably propagate the exception up, all
            // the way to the caller of the updatePlanInstance() method. However, an equally
            // good approach is to get rid of the InterruptedException in IMessageSender
            // after we deprecate/get rid of the websocket implementation. The Kafka
            // implementation doesn't actually throw that exception.
            Thread.currentThread().interrupt();
        }
    }

    @Override
    protected String describeMessage(@Nonnull PlanStatusNotification planNotification) {
        return TextFormat.printer().printToString(planNotification);
    }
}

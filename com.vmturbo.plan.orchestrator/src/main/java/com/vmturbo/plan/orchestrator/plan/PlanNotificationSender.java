package com.vmturbo.plan.orchestrator.plan;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * API backend for plan-related notifications.
 */
public class PlanNotificationSender extends ComponentNotificationSender<PlanInstance> implements PlanStatusListener {

    private final IMessageSender<PlanInstance> sender;

    public PlanNotificationSender(@Nonnull IMessageSender<PlanInstance> sender) {
        this.sender = Objects.requireNonNull(sender);
    }

    public void onPlanStatusChanged(@Nonnull final PlanInstance plan)
            throws PlanStatusListenerException {
        try {
            sendMessage(sender, plan);
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
    protected String describeMessage(@Nonnull PlanInstance planNotification) {
        return PlanInstance.class.getSimpleName() + "[" + planNotification.getPlanId() + "]";
    }
}

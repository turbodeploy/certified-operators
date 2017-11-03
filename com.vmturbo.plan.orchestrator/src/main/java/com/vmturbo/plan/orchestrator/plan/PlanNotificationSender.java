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
public class PlanNotificationSender extends ComponentNotificationSender<PlanInstance> {

    private final IMessageSender<PlanInstance> sender;

    public PlanNotificationSender(@Nonnull IMessageSender<PlanInstance> sender) {
        this.sender = Objects.requireNonNull(sender);
    }

    public void onPlanStatusChanged(@Nonnull final PlanInstance plan)
            throws CommunicationException, InterruptedException {
        sendMessage(sender, plan);
    }

    @Override
    protected String describeMessage(@Nonnull PlanInstance planNotification) {
        return PlanInstance.class.getSimpleName() + "[" + planNotification.getPlanId() + "]";
    }
}

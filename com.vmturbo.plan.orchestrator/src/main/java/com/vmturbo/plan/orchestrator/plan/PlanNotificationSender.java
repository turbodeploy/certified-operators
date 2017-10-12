package com.vmturbo.plan.orchestrator.plan;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * API backend for plan-related notifications.
 */
public class PlanNotificationSender extends ComponentNotificationSender<PlanInstance> {

    private final IMessageSender<PlanInstance> sender;

    public PlanNotificationSender(@Nonnull final ExecutorService threadPool,
            @Nonnull IMessageSender<PlanInstance> sender) {
        super(threadPool);
        this.sender = Objects.requireNonNull(sender);
    }

    public void onPlanStatusChanged(@Nonnull final PlanInstance plan) {
        sendMessage(sender, plan);
    }

    @Override
    protected String describeMessage(@Nonnull PlanInstance planNotification) {
        return PlanInstance.class.getSimpleName() + "[" + planNotification.getPlanId() + "]";
    }
}

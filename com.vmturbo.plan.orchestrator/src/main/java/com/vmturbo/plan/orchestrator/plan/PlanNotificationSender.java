package com.vmturbo.plan.orchestrator.plan;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.api.PlanOrchestratorDTO.PlanNotification;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * API backend for plan-related notifications.
 */
public class PlanNotificationSender extends ComponentNotificationSender<PlanNotification> {

    private final IMessageSender<PlanNotification> sender;

    public PlanNotificationSender(@Nonnull final ExecutorService threadPool,
            @Nonnull IMessageSender<PlanNotification> sender) {
        super(threadPool);
        this.sender = Objects.requireNonNull(sender);
    }

    public void onPlanStatusChanged(@Nonnull final PlanInstance plan) {
        final PlanNotification message = PlanNotification.newBuilder()
                .setBroadcastId(newMessageChainId())
                .setStatusChanged(plan)
                .build();
        sendMessage(sender, message);
    }

    @Override
    protected String describeMessage(@Nonnull PlanNotification planNotification) {
        return PlanNotification.class.getSimpleName() + "[" + planNotification.getBroadcastId() +
                "]";
    }
}

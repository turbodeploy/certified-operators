package com.vmturbo.api.component.communication;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.MarketNotificationDTO.MarketNotification;
import com.vmturbo.api.component.external.api.mapper.MarketMapper;
import com.vmturbo.api.component.external.api.websocket.UINotificationChannel;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.plan.orchestrator.api.PlanListener;

/**
 * Listens to plan progress notifications and forwards them to the UI.
 */
public class ApiComponentPlanListener implements PlanListener {

    private final Logger logger = LogManager.getLogger();

    private final UINotificationChannel uiNotificationChannel;

    public ApiComponentPlanListener(@Nonnull final UINotificationChannel uiNotificationChannel) {
        this.uiNotificationChannel = Objects.requireNonNull(uiNotificationChannel);
    }

    @Override
    public void onPlanStatusChanged(@Nonnull final PlanInstance planInstance) {
        final MarketNotification marketNotification =
                MarketMapper.notificationFromPlanInstance(planInstance);
        logger.debug("Received new plan instance: {} Broadcasting notification: {}",
                planInstance, marketNotification);
        uiNotificationChannel.broadcastMarketNotification(marketNotification);
    }
}

package com.vmturbo.api.component.communication;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.PlanDestinationNotificationDTO.PlanDestinationNotification;
import com.vmturbo.api.component.external.api.mapper.PlanDestinationMapper;
import com.vmturbo.api.component.external.api.websocket.UINotificationChannel;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination;
import com.vmturbo.plan.orchestrator.api.PlanExportListener;

/**
 * Listens to plan export notifications and forwards them to the UI.
 */
public class ApiComponentPlanExportListener implements PlanExportListener {

    private final Logger logger = LogManager.getLogger();

    private final UINotificationChannel uiNotificationChannel;
    private final PlanDestinationMapper mapper;

    /**
     * Create a listener for plan notifications.
     *
     * @param uiNotificationChannel the channel on which to send notifications to the UI
     * @param mapper maps notifications to UI format.
     */
    public ApiComponentPlanExportListener(@Nonnull final UINotificationChannel uiNotificationChannel,
                                          @Nonnull final PlanDestinationMapper mapper) {
        this.uiNotificationChannel = Objects.requireNonNull(uiNotificationChannel);
        this.mapper = mapper;
    }

    @Override
    public void onPlanDestinationStateChanged(@Nonnull final PlanDestination updatedDestination) {
        PlanDestinationNotification notification =
            mapper.notificationFromPlanDestination(updatedDestination, false);

        logger.debug("Received new plan destination state: {} Broadcasting notification: {}",
            updatedDestination, notification);
        uiNotificationChannel.broadcastPlanDestinationNotification(notification);
    }

    @Override
    public void onPlanDestinationProgress(@Nonnull final PlanDestination updatedDestination) {
        PlanDestinationNotification notification =
            mapper.notificationFromPlanDestination(updatedDestination, true);

        logger.debug("Received new plan destination progress: {} Broadcasting notification: {}",
            updatedDestination, notification);
        uiNotificationChannel.broadcastPlanDestinationNotification(notification);
    }
}

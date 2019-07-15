package com.vmturbo.api.component.external.api.websocket;

import javax.annotation.Nonnull;

import com.vmturbo.api.ActionNotificationDTO.ActionNotification;
import com.vmturbo.api.ExportNotificationDTO.ExportNotification;
import com.vmturbo.api.MarketNotificationDTO.MarketNotification;
import com.vmturbo.api.ReportNotificationDTO.ReportNotification;
import com.vmturbo.api.TargetNotificationDTO.TargetNotification;
import com.vmturbo.api.TargetNotificationDTO.TargetsNotification;

/**
 * This is the channel that API services use to push notifications to UI listeners
 * (currently over websocket).
 */
public interface UINotificationChannel {

    /**
     * Broadcast a notification related to a market (i.e. a plan).
     *
     * @param notification The notification to send.
     */
    void broadcastMarketNotification(@Nonnull final MarketNotification notification);

    /**
     * Broadcast a notification related to actions to the UI.
     *
     * @param notification The notification to send.
     */
    void broadcastActionNotification(@Nonnull final ActionNotification notification);

    /**
     * Broadcast a notification related to reports to the UI.
     *
     * @param notification The notification to send.
     */
    void broadcastReportNotification(@Nonnull final ReportNotification notification);


    /**
     * Broadcast a notification related to targets notification to the UI.
     *
     * @param notification The notification to send.
     */
    void broadcastTargetsNotification(@Nonnull final TargetsNotification notification);

    /**
     * Broadcast a target validation notification to the UI.
     *
     * @param notification The notification to send.
     */
    void broadcastTargetValidationNotification(@Nonnull final TargetNotification notification);

    /**
     * Broadcast a diagnostics export notification to the UI.
     *
     * @param notification The notification to send.
     */
    void broadcastDiagsExportNotification(@Nonnull final ExportNotification notification);
}

package com.vmturbo.api.component.communication;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.ReservationNotificationDTO.ReservationNotification;
import com.vmturbo.api.component.external.api.mapper.ReservationMapper;
import com.vmturbo.api.component.external.api.websocket.UINotificationChannel;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationChanges;
import com.vmturbo.plan.orchestrator.api.ReservationListener;

/**
 * Listens to reservation progress notifications and forwards them to the UI.
 */
public class ApiComponentReservationListener implements ReservationListener {

    private final Logger logger = LogManager.getLogger();

    private final UINotificationChannel uiNotificationChannel;

    /**
     * Create an instance of {@link ApiComponentReservationListener}.
     *
     * @param uiNotificationChannel the UI notification channel to broadcast captured messages to.
     */
    public ApiComponentReservationListener(@Nonnull final UINotificationChannel uiNotificationChannel) {
        this.uiNotificationChannel = Objects.requireNonNull(uiNotificationChannel);
    }

    /**
     * Handler method for when reservation changes are captured by the listener.  On the change,
     * convert the {@link ReservationChanges} to a {@link ReservationNotification} and broadcast to
     * the UI notification channel.
     *
     * @param reservationChanges the reservation changes.
     */
    @Override
    public void onReservationChanged(@Nonnull final ReservationChanges reservationChanges) {
        final ReservationNotification reservationStatusNotification =
                ReservationMapper.notificationFromReservationChanges(reservationChanges);
        logger.debug("Received new reservation changes instance: {} Broadcasting notification: {}",
                reservationChanges, reservationStatusNotification);
        uiNotificationChannel.broadcastReservationNotification(reservationStatusNotification);
    }
}

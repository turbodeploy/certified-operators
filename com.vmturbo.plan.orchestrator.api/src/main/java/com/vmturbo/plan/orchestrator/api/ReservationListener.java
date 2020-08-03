package com.vmturbo.plan.orchestrator.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationChanges;

/**
 * Listener for reservation-related events.
 */
public interface ReservationListener {
    /**
     * Handler method for when a reservation change broadcast is captured by the ReservationListener.
     *
     * @param reservationChanges the reservation changes.
     */
    void onReservationChanged(@Nonnull ReservationChanges reservationChanges);
}

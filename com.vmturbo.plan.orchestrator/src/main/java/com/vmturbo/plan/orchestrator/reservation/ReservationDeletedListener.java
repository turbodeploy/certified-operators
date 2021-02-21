package com.vmturbo.plan.orchestrator.reservation;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;

/**
 * An interface to be implemented by classes that wish to receive events related to reservations.
 */
public interface ReservationDeletedListener {

    /**
     * A callback to be called when a reservation has been successfully deleted.
     *
     * @param reservation The reservation that was removed.
     * @param deployed true if the associated VM is deployed.
     */
    default void onReservationDeleted(@Nonnull final Reservation reservation, boolean deployed) { }

}

package com.vmturbo.plan.orchestrator.reservation;

import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;

/**
 * Data access object for creating, updating, searching, deleting reservations.
 */
public interface ReservationDao {

    /**
     * Get all reservations which are stored in reservation table.
     *
     * @return Set of reservations.
     */
    @Nonnull
    Set<Reservation> getAllReservations();

    /**
     * Get one reservation which Reservation's ID is equal to parameter id.
     *
     * @param id id of reservation.
     * @return Optional reservation, if not found, it will be Optional.empty().
     */
    @Nonnull
    Optional<Reservation> getReservationById(final long id);

    /**
     * Get reservations which status are equal to input parameter status.
     *
     * @param status status of reservation.
     * @return Set of reservations.
     */
    Set<Reservation> getReservationsByStatus(@Nonnull final ReservationStatus status);

    /**
     * Create a new reservation to database, it will ignore the input reservation's id and
     * create a new Id for the new reservation.
     *
     * @param reservation describe the contents of new reservation.
     * @return a new created reservation.
     */
    @Nonnull
    Reservation createReservation(@Nonnull final Reservation reservation);

    /**
     * Update the existing reservation with a new reservation.
     *
     * @param id The id of reservation needs to update.
     * @param reservation a new reservation need to store.
     * @return a new updated Reservation object.
     * @throws NoSuchObjectException if can not find existing reservation.
     */
    @Nonnull
    Reservation updateReservation(final long id, @Nonnull final Reservation reservation)
            throws NoSuchObjectException;

    /**
     * Bathc update the existing reservations.
     *
     * @param reservations A set of new reservations.
     * @return A set of new updated reservations.
     * @throws NoSuchObjectException if can not find existing reservation.
     */
    Set<Reservation> updateReservationBatch(@Nonnull final Set<Reservation> reservations)
            throws NoSuchObjectException;

    /**
     * Delete the existing reservation which reservation' ID equal to parameter id.
     *
     * @param id The id of reservation needs to delete.
     * @return deleted Reservation object.
     * @throws NoSuchObjectException if can not find existing reservation.
     */
    @Nonnull
    Reservation deleteReservationById(final long id) throws NoSuchObjectException;
}

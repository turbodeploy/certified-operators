package com.vmturbo.plan.orchestrator.reservation;

import static com.vmturbo.plan.orchestrator.db.Tables.RESERVATION;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.plan.ReservationDTO;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.plan.orchestrator.db.tables.pojos.Reservation;
import com.vmturbo.plan.orchestrator.db.tables.records.ReservationRecord;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;

/**
 * Implementation of {@link ReservationDao}.
 */
public class ReservationDaoImpl implements ReservationDao {
    private final DSLContext dsl;

    public ReservationDaoImpl(@Nonnull final DSLContext dsl) {
        this.dsl = Objects.requireNonNull(dsl);
    }

    /**
     * Get all existing reservations.
     *
     * @return Set of all reservations.
     */
    @Nonnull
    @Override
    public Set<ReservationDTO.Reservation> getAllReservations() {
        final List<Reservation> reservations = dsl.selectFrom(RESERVATION).fetch().into(Reservation.class);
        return convertReservationListToProto(reservations);
    }

    /**
     * Get the reservation by its id.
     *
     * @param id id of reservation.
     * @return Optional Reservation, if not found, will return Optional.empty().
     */
    @Nonnull
    @Override
    public Optional<ReservationDTO.Reservation> getReservationById(final long id) {
        return Optional.ofNullable(dsl.selectFrom(RESERVATION)
                .where(RESERVATION.ID.eq(id))
                .fetchOne())
                .map(record -> record.into(Reservation.class))
                .map(this::convertReservationToProto);
    }

    /**
     * Get reservation which status are equal to input parameter status.
     *
     * @param status status of reservation.
     * @return Set of reservations.
     */
    @Nonnull
    @Override
    public Set<ReservationDTO.Reservation> getReservationsByStatus(
            @Nonnull final ReservationStatus status) {
        final List<Reservation> reservations = dsl.selectFrom(RESERVATION)
                .where(RESERVATION.STATUS.eq(ReservationStatusConverter.typeToDb(status)))
                .fetch()
                .into(Reservation.class);
        return convertReservationListToProto(reservations);
    }

    /**
     * Create a new reservation and it will create a new ID for the new reservation.
     *
     * @param reservation describe the contents of new reservation.
     * @return new created reservation.
     */
    @Nonnull
    @Override
    public ReservationDTO.Reservation createReservation(@Nonnull final ReservationDTO.Reservation reservation) {
        final ReservationDTO.Reservation newReservation = ReservationDTO.Reservation.newBuilder(reservation)
                .setId(IdentityGenerator.next())
                .build();
        final ReservationRecord newReservationRecord = dsl.newRecord(RESERVATION);

        updateReservationRecordWithStore(newReservation, newReservationRecord);
        return newReservation;
    }

    /**
     * Update a existing reservation with a new reservation.
     *
     * @param id The id of reservation needs to update.
     * @param reservation a new reservation need to store.
     * @return updated reservation object.
     * @throws NoSuchObjectException if can not find existing reservation.
     */
    @Nonnull
    @Override
    public ReservationDTO.Reservation updateReservation(
            final long id,
            @Nonnull final ReservationDTO.Reservation reservation) throws NoSuchObjectException {
        try {
            return dsl.transactionResult(configuration -> {
                final DSLContext transactionDsl = DSL.using(configuration);
                final ReservationRecord reservationRecord = Optional.ofNullable(transactionDsl.selectFrom(RESERVATION)
                        .where(RESERVATION.ID.eq(id))
                        .fetchOne())
                        .orElseThrow(() ->
                                new NoSuchObjectException("Reservation with id" + id + " not found"));
                final ReservationDTO.Reservation newReservation = reservation.toBuilder()
                        .setId(id)
                        .build();
                updateReservationRecordWithStore(newReservation, reservationRecord);
                return newReservation;
            });
        } catch (DataAccessException e) {
            if (e.getCause() instanceof NoSuchObjectException) {
                throw (NoSuchObjectException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Batch update existing reservations, if there are missing reservations, it will throw
     * {@link NoSuchObjectException}.
     *
     * @param reservations A set of new reservations.
     * @return A set of new reservaitons.
     * @throws NoSuchObjectException if can not find existing reservations.
     */
    @Nonnull
    @Override
    public Set<ReservationDTO.Reservation> updateReservationBatch(
            @Nonnull final Set<ReservationDTO.Reservation> reservations) throws NoSuchObjectException {
        try {
            dsl.transaction(configuration -> {
                DSLContext transactionDsl = DSL.using(configuration);
                final List<ReservationRecord> updateReservationRecords = new ArrayList<>();
                final Set<Long> reservationIds = reservations.stream()
                        .map(ReservationDTO.Reservation::getId)
                        .collect(Collectors.toSet());
                final List<ReservationRecord> reservationRecords = transactionDsl.selectFrom(RESERVATION)
                        .where(RESERVATION.ID.in(reservationIds))
                        .fetch();
                if (reservationRecords.size() != reservations.size()) {
                    throw new NoSuchObjectException("There are reservations missing, required: "
                            + reservations.size() + " but found: " + reservationRecords.size());
                }
                final Map<Long, ReservationDTO.Reservation> reservationMap = reservations.stream()
                        .collect(Collectors.toMap(ReservationDTO.Reservation::getId, Function.identity()));
                reservationRecords.stream()
                        .map(record -> updateReservationRecord(reservationMap.get(record.getId()), record))
                        .forEach(updateReservationRecords::add);
                transactionDsl.batchUpdate(updateReservationRecords).execute();
            });
        } catch (DataAccessException e) {
            if (e.getCause() instanceof NoSuchObjectException) {
                throw (NoSuchObjectException)e.getCause();
            } else {
                throw e;
            }
        }
        return reservations;
    }

    /**
     * Delete a existing reservation.
     *
     * @param id The id of reservation needs to delete.
     * @return deleted reservation object.
     * @throws NoSuchObjectException if can not find existing reservation.
     */
    @Nonnull
    @Override
    public ReservationDTO.Reservation deleteReservationById(final long id) throws NoSuchObjectException {
        try {
            return dsl.transactionResult(configuration -> {
                final DSLContext transactionDsl = DSL.using(configuration);
                final ReservationDTO.Reservation reservation = getReservationById(id)
                        .orElseThrow(() ->
                                new NoSuchObjectException("Reservation with id" + id + " not found"));
                transactionDsl.deleteFrom(RESERVATION).where(RESERVATION.ID.eq(id)).execute();
                return reservation;
            });
        } catch (DataAccessException e) {
            if (e.getCause() instanceof NoSuchObjectException) {
                throw (NoSuchObjectException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Convert a JOOQ-generated a list of {@link Reservation} to a {@link ReservationDTO.Reservation} object
     * that carries the same information.
     *
     * @param reservations A list of JOOQ-generated {@link Reservation}.
     * @return Set of {@link ReservationDTO.Reservation} that describes the reservation.
     */
    private Set<ReservationDTO.Reservation> convertReservationListToProto(
            @Nonnull final List<Reservation> reservations) {
        return reservations.stream()
                .map(reservation -> convertReservationToProto(reservation))
                .collect(Collectors.toSet());
    }

    private ReservationDTO.Reservation convertReservationToProto(
            @Nonnull final Reservation reservation) {
        return ReservationDTO.Reservation.newBuilder()
                .setId(reservation.getId())
                .setName(reservation.getName())
                .setStartDate(convertLocalDateToProto(reservation.getStartTime()))
                .setExpirationDate(convertLocalDateToProto(reservation.getExpireTime()))
                .setStatus(ReservationStatusConverter.typeFromDb(reservation.getStatus()))
                .setReservationTemplateCollection(reservation.getReservationTemplateCollection())
                .setConstraintInfoCollection(reservation.getConstraintInfoCollection())
                .build();
    }

    private ReservationDTO.Reservation.Date convertLocalDateToProto(@Nonnull final LocalDateTime time) {
        return ReservationDTO.Reservation.Date.newBuilder()
                .setYear(time.getYear())
                .setMonth(time.getMonthValue())
                .setDay(time.getDayOfMonth())
                .build();
    }

    /**
     * Update {@link ReservationRecord} with information from a {@link ReservationDTO.Reservation}
     * proto and stores updated record back to database.
     *
     * @param reservation The reservation that represents the new information.
     * @param record {@link ReservationRecord} need to update.
     */
    private void updateReservationRecordWithStore(@Nonnull final ReservationDTO.Reservation reservation,
                                                  @Nonnull final ReservationRecord record) {
        updateReservationRecord(reservation, record).store();
    }

    /**
     * Update {@link ReservationRecord} with information from a {@link ReservationDTO.Reservation}
     * proto. But it will not update to database.
     *
     * @param reservation The reservation that represents the new information.
     * @param record {@link ReservationRecord} need to update.
     * @return {@link ReservationRecord} after updated.
     */
    private ReservationRecord updateReservationRecord(@Nonnull final ReservationDTO.Reservation reservation,
                                                      @Nonnull final ReservationRecord record) {
        record.setId(reservation.getId());
        record.setName(reservation.getName());
        record.setStartTime(convertDateProtoToLocalDate(reservation.getStartDate()));
        record.setExpireTime(convertDateProtoToLocalDate(reservation.getExpirationDate()));
        record.setStatus(ReservationStatusConverter.typeToDb(reservation.getStatus()));
        record.setReservationTemplateCollection(reservation.getReservationTemplateCollection());
        record.setConstraintInfoCollection(reservation.getConstraintInfoCollection());
        return record;
    }

    private LocalDateTime convertDateProtoToLocalDate(@Nonnull ReservationDTO.Reservation.Date date) {
        return LocalDateTime.of(date.getYear(), date.getMonth(), date.getDay(), 0, 0);
    }
}

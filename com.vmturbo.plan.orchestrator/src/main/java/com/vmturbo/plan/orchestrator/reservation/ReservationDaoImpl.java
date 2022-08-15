package com.vmturbo.plan.orchestrator.reservation;

import static com.vmturbo.plan.orchestrator.db.Tables.RESERVATION;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.springframework.util.StopWatch;

import com.vmturbo.common.protobuf.plan.ReservationDTO;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.StringDiagnosable;
import com.vmturbo.plan.orchestrator.api.NoSuchValueException;
import com.vmturbo.plan.orchestrator.api.ReservationFieldsConverter;
import com.vmturbo.plan.orchestrator.db.tables.pojos.Reservation;
import com.vmturbo.plan.orchestrator.db.tables.records.ReservationRecord;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;

/**
 * Implementation of {@link ReservationDao}.
 */
public class ReservationDaoImpl implements ReservationDao {

    private final int reservationByIdMaxAttempts = 4;
    private long reservationByIdPollTime = 30000L;

    private final Logger logger = LogManager.getLogger();

    @VisibleForTesting
    static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    private final DSLContext dsl;

    private final List<ReservationDeletedListener> listeners =
            Collections.synchronizedList(new ArrayList<>());

    private final Set<ReservationStatus> finishedStatuses = new HashSet(
            Arrays.asList(ReservationStatus.RESERVED, ReservationStatus.FUTURE,
                    ReservationStatus.PLACEMENT_FAILED, ReservationStatus.INVALID));

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
        Optional<Reservation> res = Optional.ofNullable(dsl.selectFrom(RESERVATION)
                .where(RESERVATION.ID.eq(id))
                .fetchOne())
                .map(record -> record.into(Reservation.class));
        if (res.isPresent()) {
            try {
                return Optional.of(convertReservationToProto(res.get()));
            } catch (NoSuchValueException e) {
                logger.error("convertReservationToProto", e);
            }
        }
        return Optional.empty();
    }

    /**
     * Get the reservation by its id.
     *
     * @param id id of reservation.
     * @param apiCallBlock determines if the api call is blocking or not
     * @return Optional Reservation, if not found, will return Optional.empty().
     */
    @Nonnull
    @Override
    public Optional<ReservationDTO.Reservation> getReservationById(final long id, final boolean apiCallBlock) {
        if (!apiCallBlock) {
            return getReservationById(id);
        } else {
            /*
             * This polling system is only temporary.
             * We will get rid of this once we remove the apiCallBlock parameter.
             * We expect this to happen in slow release 8.9.1.
             * Noted in the following ticket OM-88470
             * */
            Optional<ReservationDTO.Reservation> reservationOptional;

            for (int attempts = 0; attempts < reservationByIdMaxAttempts; attempts++) {
                reservationOptional = getReservationById(id);
                if (!reservationOptional.isPresent()
                        || finishedStatuses.contains(reservationOptional.get().getStatus())) {
                    return reservationOptional;
                } else {
                    try {
                        Thread.sleep(reservationByIdPollTime);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Thread interrupted awaiting polling for get reservation by id : " + id, e);
                    }
                }
            }
            logger.warn("Waited for {} seconds. Could not get the reservation with id of {}.Returning empty",
                    ((reservationByIdPollTime / 1000) * reservationByIdMaxAttempts), id);
            return Optional.empty();
        }
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
        try {
            final List<Reservation> reservations = dsl.selectFrom(RESERVATION)
                    .where(RESERVATION.STATUS.eq(ReservationFieldsConverter.statusToDb(status)))
                    .fetch()
                    .into(Reservation.class);
            return convertReservationListToProto(reservations);
        } catch (NoSuchValueException e) {
            logger.error("getReservationsByStatus", e);
            return Collections.emptySet();
        }
    }

    /**
     * Create a new reservation and it will create a new ID for the new reservation.
     *
     * @param reservation describe the contents of new reservation.
     * @return new created reservation.
     */
    @Nonnull
    @Override
    public ReservationDTO.Reservation createReservation(
            @Nonnull final ReservationDTO.Reservation reservation) {
        return dsl.transactionResult(configuration -> {
            final ReservationDTO.Reservation newReservation = ReservationDTO.Reservation.newBuilder(
                    reservation).setId(IdentityGenerator.next()).setStatus(
                    ReservationStatus.INITIAL).build();
            final DSLContext transactionDsl = DSL.using(configuration);
            final ReservationRecord newReservationRecord = transactionDsl.newRecord(RESERVATION);
            updateReservationRecordWithStore(newReservation, newReservationRecord);
            return newReservation;
        });
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
    public ReservationDTO.Reservation updateReservation(final long id,
            @Nonnull final ReservationDTO.Reservation reservation) throws NoSuchObjectException {
        try {
            return dsl.transactionResult(configuration -> {
                final DSLContext transactionDsl = DSL.using(configuration);
                final ReservationRecord reservationRecord = Optional.ofNullable(
                        transactionDsl.selectFrom(RESERVATION)
                                .where(RESERVATION.ID.eq(id))
                                .fetchOne()).orElseThrow(
                        () -> new NoSuchObjectException("Reservation with id" + id + " not found"));
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
     * Batch update existing reservations, if there are missing reservations, it will throw.
     * {@link NoSuchObjectException}
     *
     * @param reservations A set of new reservations.
     * @return A set of new reservaitons.
     * @throws NoSuchObjectException if can not find existing reservations.
     */
    @Nonnull
    @Override
    public Set<ReservationDTO.Reservation> updateReservationBatch(
            @Nonnull final Set<ReservationDTO.Reservation> reservations)
            throws NoSuchObjectException {
        try {
            dsl.transaction(configuration -> {
                DSLContext transactionDsl = DSL.using(configuration);
                final List<ReservationRecord> updateReservationRecords = new ArrayList<>();
                final Set<Long> reservationIds = reservations.stream().map(
                        ReservationDTO.Reservation::getId).collect(Collectors.toSet());
                final List<ReservationRecord> reservationRecords = transactionDsl.selectFrom(
                        RESERVATION).where(RESERVATION.ID.in(reservationIds)).fetch();
                if (reservationRecords.size() != reservations.size()) {
                    throw new NoSuchObjectException(
                            "There are reservations missing, required: " + reservations.size()
                                    + " but found: " + reservationRecords.size());
                }
                final Map<Long, ReservationDTO.Reservation> reservationMap =
                        reservations.stream().collect(
                                Collectors.toMap(ReservationDTO.Reservation::getId,
                                        Function.identity()));
                reservationRecords.stream().map(record -> {
                    try {
                        return updateReservationRecord(reservationMap.get(record.getId()), record);
                    } catch (NoSuchValueException e) {
                        logger.error("updateReservationRecord", e);
                        return null;
                    }
                }).filter(Objects::nonNull).forEach(updateReservationRecords::add);
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
     * @param deployed true if the associated VM is deployed.
     * @param delayedDeletionTimeInMillis if deployed is true set expiration date based on
     *         delayedDeletionTimeInMillis
     * @return deleted reservation object.
     * @throws NoSuchObjectException if can not find existing reservation.
     */
    @Nonnull
    @Override
    public ReservationDTO.Reservation deleteReservationById(final long id, boolean deployed,
            long delayedDeletionTimeInMillis) throws NoSuchObjectException {
        ReservationDTO.Reservation reservation;
        final StopWatch stopWatch = new StopWatch("ReservationDaoImpl-deleteReservationByID");
        if (!deployed) {
            try {
                stopWatch.start("get and delete reservation");
                reservation = dsl.transactionResult(configuration -> {
                    final DSLContext transactionDsl = DSL.using(configuration);
                    final ReservationDTO.Reservation insideReservation = getReservationById(
                            id).orElseThrow(() -> new NoSuchObjectException(
                            "Reservation with id" + id + " not found"));
                    transactionDsl.deleteFrom(RESERVATION).where(RESERVATION.ID.eq(id)).execute();
                    return insideReservation;
                });
                stopWatch.stop();
            } catch (DataAccessException e) {
                if (e.getCause() instanceof NoSuchObjectException) {
                    throw (NoSuchObjectException)e.getCause();
                } else {
                    throw e;
                }
            }
        } else {
            // If delayed deletion. Just set the DelayedDeletionDate and DelayedDeletion flag.
            Optional<ReservationDTO.Reservation> originalReservation = getReservationById(id);
            if (originalReservation.isPresent()) {
                ReservationDTO.Reservation updatedReservation =
                        originalReservation.get().toBuilder().setDeployed(true).setExpirationDate(
                                System.currentTimeMillis() + delayedDeletionTimeInMillis).build();
                reservation = updateReservation(id, updatedReservation);
            } else {
                throw new NoSuchObjectException("Reservation with id" + id + " not found");
            }
        }
        stopWatch.start("listeners.forEach");
        listeners.forEach((listener) -> listener.onReservationDeleted(reservation, deployed));
        stopWatch.stop();
        logger.debug(stopWatch::prettyPrint);
        return reservation;
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
        return deleteReservationById(id, false, 0l);
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
                .map(reservation -> {
                    try {
                        return convertReservationToProto(reservation);
                    } catch (NoSuchValueException e) {
                        logger.error("convertReservationToProto", e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    private ReservationDTO.Reservation convertReservationToProto(
            @Nonnull final Reservation reservation) throws NoSuchValueException {
        return ReservationDTO.Reservation.newBuilder()
                .setId(reservation.getId())
                .setName(reservation.getName())
                .setStartDate(convertLocalDateToTimestamp(reservation.getStartTime()))
                .setExpirationDate(convertLocalDateToTimestamp(reservation.getExpireTime()))
                .setStatus(ReservationFieldsConverter.statusFromDb(reservation.getStatus()))
                .setReservationTemplateCollection(reservation.getReservationTemplateCollection())
                .setConstraintInfoCollection(reservation.getConstraintInfoCollection())
                .setDeployed(reservation.getDeployed() > 0)
                .setReservationMode(ReservationFieldsConverter.modeFromDb(reservation.getMode()))
                .setReservationGrouping(ReservationFieldsConverter.groupingFromDb(reservation.getGrouping()))
                .build();
    }

    private long convertLocalDateToTimestamp(@Nonnull final LocalDateTime time) {
        return time.atOffset(ZoneOffset.UTC).toInstant().toEpochMilli();
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
        try {
            updateReservationRecord(reservation, record).store();
        } catch (NoSuchValueException e) {
            logger.error("updateReservationRecord", e);
        }
    }

    /**
     * Update {@link ReservationRecord} with information from a {@link ReservationDTO.Reservation}
     * proto. But it will not update to database.
     *
     * @param reservation The reservation that represents the new information.
     * @param record {@link ReservationRecord} need to update.
     * @return {@link ReservationRecord} after updated.
     * @throws NoSuchValueException if invalid reservation value is specified.
     */
    private ReservationRecord updateReservationRecord(@Nonnull final ReservationDTO.Reservation reservation,
                                                      @Nonnull final ReservationRecord record)
                                                              throws NoSuchValueException {
        record.setId(reservation.getId());
        record.setName(reservation.getName());
        record.setStartTime(convertDateProtoToLocalDate(reservation.getStartDate()));
        record.setExpireTime(convertDateProtoToLocalDate(reservation.getExpirationDate()));
        record.setStatus(ReservationFieldsConverter.statusToDb(reservation.getStatus()));
        record.setReservationTemplateCollection(reservation.getReservationTemplateCollection());
        record.setConstraintInfoCollection(reservation.getConstraintInfoCollection());
        record.setDeployed(reservation.getDeployed() ? 1 : 0);
        record.setMode(ReservationFieldsConverter.modeToDb(reservation.getReservationMode()));
        record.setGrouping(ReservationFieldsConverter.groupingToDb(reservation.getReservationGrouping()));
        return record;
    }



    private LocalDateTime convertDateProtoToLocalDate(final long timestamp) {

        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp),
                TimeZone.getTimeZone("UTC").toZoneId());
    }


    /**
     * {@inheritDoc}
     *
     * This method retrieves all reservations and serializes them as JSON strings.
     *
     * @throws DiagnosticsException on diagnostics exceptions occurred
     */
    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        final Set<ReservationDTO.Reservation> reservations = getAllReservations();
        logger.info("Collecting diagnostics for {} reservations", reservations.size());
        for (ReservationDTO.Reservation reservation : reservations) {
            appender.appendString(GSON.toJson(reservation, ReservationDTO.Reservation.class));
        }
    }

    /**
     * {@inheritDoc}
     *
     * This method clears all existing reservations, then deserializes and adds a list of
     * serialized reservations from diagnostics.
     *
     * @param collectedDiags The diags collected from a previous call to
     *      {@link StringDiagnosable#collectDiags(DiagnosticsAppender)}. Must be in the same order.
     * @throws DiagnosticsException if the db already contains reservations, or in response
     *                              to any errors that may occur deserializing or restoring a
     *                              reservation.
     */
    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags, @Nullable Void context) throws DiagnosticsException {

        final List<String> errors = new ArrayList<>();

        final Set<ReservationDTO.Reservation> preexisting = getAllReservations();
        if (!preexisting.isEmpty()) {
            final int numPreexisting = preexisting.size();
            final String clearingMessage = "Clearing " + numPreexisting +
                " preexisting reservations: " + preexisting.stream()
                    .map(ReservationDTO.Reservation::getName)
                    .collect(Collectors.toList());
            errors.add(clearingMessage);
            logger.warn(clearingMessage);

            final int deleted = deleteAllReservations();
            if (deleted != numPreexisting) {
                final String deletedMessage = "Failed to delete " + (numPreexisting - deleted) +
                    " preexisting reservations: " + getAllReservations().stream()
                        .map(ReservationDTO.Reservation::getName)
                        .collect(Collectors.toList());
                logger.error(deletedMessage);
                errors.add(deletedMessage);
            }
        }

        logger.info("Restoring {} serialized reservations from diagnostics", collectedDiags.size());

        final long count = collectedDiags.stream().map(serial -> {
            try {
                return GSON.fromJson(serial, ReservationDTO.Reservation.class);
            } catch (JsonParseException e) {
                errors.add("Failed to restore reservation " + serial +
                    " because of parse exception" + e.getMessage());
                return null;
            }
        }).filter(Objects::nonNull).map(this::restoreReservation).filter(optional -> {
            optional.ifPresent(errors::add);
            return !optional.isPresent();
        }).count();

        logger.info("Loaded {} reservations from diagnostics", count);
        if (!errors.isEmpty()) {
            throw new DiagnosticsException(errors);
        }
    }

    @Nonnull
    @Override
    public String getFileName() {
        return "Reservations";
    }

    /**
     * Add a reservation to the database. Note that this is used for restoring reservations from
     * diagnostics and should NOT be used for normal operations.
     *
     * @param reservation the reservation to add.
     * @return an optional of a string representing any error that may have occurred
     */
    private Optional<String> restoreReservation(@Nonnull final ReservationDTO.Reservation reservation) {
        final ReservationDTO.Reservation newReservation = ReservationDTO.Reservation
            .newBuilder(reservation).setId(reservation.getId()).build();
        try {
            int r = dsl.transactionResult(configuration -> {
                final DSLContext transactionDsl = DSL.using(configuration);
                final ReservationRecord newReservationRecord = transactionDsl.newRecord(RESERVATION);
                return updateReservationRecord(newReservation, newReservationRecord).store();
            });
            return r == 1 ? Optional.empty() : Optional.of("Failed to restore reservation " + reservation);
        } catch (DataAccessException e) {
            return Optional.of("Could not restore reservation " + reservation +
                " because of DataAccessException "+ e.getMessage());
        }
    }

    /**
     * Deletes all reservations. Note: this is only used when restoring reservations
     * from diagnostics and should NOT be used during normal operations.
     *
     * @return the number of records deleted
     */
    private int deleteAllReservations() {
        try {
            return dsl.deleteFrom(RESERVATION).execute();
        } catch (DataAccessException e) {
            return 0;
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void addListener(@Nonnull ReservationDeletedListener listener) {
        listeners.add(Objects.requireNonNull(listener));
    }

    @VisibleForTesting
    void setReservationByIdPollTime(Long pollTime) {
        this.reservationByIdPollTime = pollTime;
    }
}

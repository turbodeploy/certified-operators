package com.vmturbo.plan.orchestrator.reservation;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.plan.ReservationDTO.CreateReservationRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.DeleteReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetAllReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetReservationByStatusRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateFutureAndExpiredReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateFutureAndExpiredReservationsResponse;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceImplBase;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;

/**
 * Implementation of gRpc service for Reservation.
 */
public class ReservationRpcService extends ReservationServiceImplBase {
    private final Logger logger = LogManager.getLogger();

    private final PlanDao planDao;

    private final ReservationDao reservationDao;

    private final TemplatesDao templatesDao;

    private final PlanRpcService planService;

    private final ReservationManager reservationManager;


    public ReservationRpcService(@Nonnull final PlanDao planDao,
                                 @Nonnull final TemplatesDao templateDao,
                                 @Nonnull final ReservationDao reservationDao,
                                 @Nonnull final PlanRpcService planRpcService,
                                 @Nonnull final ReservationManager reservationManager) {
        this.planDao = Objects.requireNonNull(planDao);
        this.templatesDao = Objects.requireNonNull(templateDao);
        this.reservationDao = Objects.requireNonNull(reservationDao);
        this.planService = Objects.requireNonNull(planRpcService);
        this.reservationManager = Objects.requireNonNull(reservationManager);

    }


    @Override
    public void getAllReservations(GetAllReservationsRequest request,
                                   StreamObserver<Reservation> responseObserver) {
        try {
            for (Reservation reservation : reservationDao.getAllReservations()) {
                responseObserver.onNext(reservation);
            }
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to get all reservations")
                    .asException());
        }
    }

    @Override
    public void getReservationByStatus(GetReservationByStatusRequest request,
                                       StreamObserver<Reservation> responseObserver) {
        try {
            for (Reservation reservation :
                    reservationDao.getReservationsByStatus(request.getStatus())) {
                responseObserver.onNext(reservation);
            }
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to get reservation by status: " + request.getStatus())
                    .asException());
        }
    }

    @Override
    public void getReservationById(GetReservationByIdRequest request,
                                   StreamObserver<Reservation> responseObserver) {
        if (!request.hasReservationId()) {
            logger.error("Missing reservation id for get Reservation.");
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Get Reservation by reservation id must provider an " +
                            "reservation id").asException());
            return;
        }
        try {
            final Optional<Reservation> reservationOptional =
                    reservationDao.getReservationById(request.getReservationId());
            if (reservationOptional.isPresent()) {
                responseObserver.onNext(reservationOptional.get());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("Reservation ID " + Long.toString(request.getReservationId())
                                + " not found.")
                        .asException());
            }
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to get reservation " + request.getReservationId() + ".")
                    .asException());
        }
    }

    @Override
    public void deleteReservationById(DeleteReservationByIdRequest request,
                                      StreamObserver<Reservation> responseObserver) {
        if (!request.hasReservationId()) {
            logger.error("Missing reservation id for delete Reservation.");
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Delete Reservation by reservation id must provider an " +
                            "reservation id").asException());
            return;
        }
        try {
            final Reservation reservation = reservationDao.deleteReservationById(request.getReservationId());
            responseObserver.onNext(reservation);
            responseObserver.onCompleted();
        } catch (NoSuchObjectException e) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Reservation ID " + Long.toString(request.getReservationId())
                            + " not found.")
                    .asException());
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to delete reservation " + request.getReservationId() + ".")
                    .asException());
        }
    }

    @Override
    public void updateReservationById(UpdateReservationByIdRequest request,
                                      StreamObserver<Reservation> responseObserver) {
        if (!request.hasReservationId()) {
            logger.error("Missing reservation id for update Reservation.");
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Update Reservation by reservation id must provider an " +
                            "reservation id").asException());
            return;
        }
        final Set<Long> templateIds = getTemplateIds(request.getReservation());
        // check input template ids are valid
        if (!isValidTemplateIds(templateIds)) {
            logger.error("Input templateIds are invalid: " + templateIds);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Template Ids are " +
                    "invalid").asException());
            return;
        }
        try {
            final Reservation reservation =
                    reservationDao.updateReservation(request.getReservationId(), request.getReservation());
            responseObserver.onNext(reservation);
            responseObserver.onCompleted();
        } catch (NoSuchObjectException e) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Reservation ID " + Long.toString(request.getReservationId())
                            + " not found.")
                    .asException());
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to update reservation " + request.getReservationId() + ".")
                    .asException());
        }
    }

    @Override
    public void updateFutureAndExpiredReservations(UpdateFutureAndExpiredReservationsRequest request,
                                        StreamObserver<UpdateFutureAndExpiredReservationsResponse> responseObserver) {
        try {
            final Set<Reservation> reservationsToStart = new HashSet<>();
            final Set<Reservation> reservationsToRemove = new HashSet<>();
            reservationDao.getAllReservations().forEach(reservation -> {
                // Check for expiration first.
                if (reservationManager.hasReservationExpired(reservation)) {
                    reservationsToRemove.add(reservation);
                } else if (reservation.getStatus() == ReservationStatus.FUTURE && reservationManager.isReservationActiveNow(reservation)) {
                    reservationsToStart.add(reservation);
                } else if (reservation.getStatus() == ReservationStatus.INVALID) {
                    // Reservations that are invalid may have become valid (e.g. if the entity
                    // they are constrained by was temporarily absent from the Topology).
                    reservationsToStart.add(reservation);
                }
            });

            for (Reservation reservation : reservationsToRemove) {
                reservationDao.deleteReservationById(reservation.getId());
                logger.info("Deleted Expired Reservation: " + reservation.getName());
            }

            if (reservationsToStart.size() > 0) {
                for (Reservation reservation : reservationsToStart) {
                    reservationManager.intializeReservationStatus(reservation);
                }
                logger.info("Starting {} newly active reservations.", reservationsToStart.size());
                reservationManager.checkAndStartReservationPlan();
            }
            responseObserver.onNext(UpdateFutureAndExpiredReservationsResponse.newBuilder()
                .setActivatedReservations(reservationsToStart.size())
                .setExpiredReservationsRemoved(reservationsToRemove.size())
                .build());
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to update reservations.")
                    .asException());
        } catch (NoSuchObjectException e) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Failed to delete expired reservation")
                    .asException());
        }
    }

    @Override
    public void updateReservations(UpdateReservationsRequest request,
                                   StreamObserver<Reservation> responseObserver) {
        final Set<Long> templateIds = request.getReservationList().stream()
                .map(this::getTemplateIds)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
        // check input template ids are valid
        if (!isValidTemplateIds(templateIds)) {
            logger.error("Input templateIds are invalid: " + templateIds);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Template Ids are " +
                    "invalid").asException());
            return;
        }
        try {
            final Set<Reservation> reservations = request.getReservationList().stream()
                    .collect(Collectors.toSet());
            for (Reservation reservation : reservationDao.updateReservationBatch(reservations)) {
                responseObserver.onNext(reservation);
            }
            responseObserver.onCompleted();
        } catch (NoSuchObjectException e) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription(e.getMessage())
                    .asException());
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to update reservations.")
                    .asException());
        }
    }

    @Override
    public void createReservation(CreateReservationRequest request,
                                  StreamObserver<Reservation> responseObserver) {
        final Set<Long> templateIds = getTemplateIds(request.getReservation());
        // check input template ids are valid
        if (!isValidTemplateIds(templateIds)) {
            logger.error("Input templateIds are invalid: " + templateIds);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Template Ids are " +
                    "invalid").asException());
        }
        try {
            final Reservation reservation = reservationDao.createReservation(request.getReservation());
            final Reservation queuedReservation = reservationManager.intializeReservationStatus(reservation);
            logger.info("Created Reservation: " + request.getReservation().getName());
            responseObserver.onNext(queuedReservation);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to create reservation.")
                    .asException());
        }
        reservationManager.checkAndStartReservationPlan();
    }



    /**
     * Check if input template ids are all valid template ids. If there is any id which can not find
     * related template, it return false. And also if input template ids is empty, will return false.
     *
     * @param templateIds a set of template ids.
     * @return boolean indicates if all input template ids are valid.
     */
    private boolean isValidTemplateIds(@Nonnull final Set<Long> templateIds) {
        if (templateIds.isEmpty()) {
            return false;
        }
        final long retrieveTemplatesCount = templatesDao.getTemplatesCount(templateIds);
        return retrieveTemplatesCount == templateIds.size();
    }

    private Set<Long> getTemplateIds(@Nonnull final Reservation reservation) {
        return reservation.getReservationTemplateCollection().getReservationTemplateList().stream()
                .map(ReservationTemplate::getTemplateId)
                .collect(Collectors.toSet());
    }
}

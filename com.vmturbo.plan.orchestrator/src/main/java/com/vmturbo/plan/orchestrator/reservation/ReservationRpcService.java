package com.vmturbo.plan.orchestrator.reservation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.ReservationDTO.CreateReservationRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.DeleteReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetAllReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetReservationByStatusRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.InitialPlacementRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.InitialPlacementResponse;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateReservationByIdRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceImplBase;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.plan.orchestrator.plan.IntegrityException;
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

    private final String DISABLED = "DISABLED";

    public ReservationRpcService(@Nonnull final PlanDao planDao,
                                 @Nonnull final TemplatesDao templateDao,
                                 @Nonnull final ReservationDao reservationDao,
                                 @Nonnull final PlanRpcService planRpcService) {
        this.planDao = Objects.requireNonNull(planDao);
        this.templatesDao = Objects.requireNonNull(templateDao);
        this.reservationDao = Objects.requireNonNull(reservationDao);
        this.planService = Objects.requireNonNull(planRpcService);
    }

    @Override
    public void initialPlacement(InitialPlacementRequest request,
                                 StreamObserver<InitialPlacementResponse> responseObserver) {
        if (!request.hasScenarioInfo()) {
            logger.error("Missing scenario info for initial placement.");
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Initial placement must have an scenario info").asException());
            return;
        }
        final List<ScenarioChange> scenarioChangeList = request.getScenarioInfo().getChangesList();
        final Set<Long> templateIds = scenarioChangeList.stream()
                .filter(ScenarioChange::hasTopologyAddition)
                .map(ScenarioChange::getTopologyAddition)
                .filter(TopologyAddition::hasTemplateId)
                .map(TopologyAddition::getTemplateId)
                .collect(Collectors.toSet());
        // check input template ids are valid
        if (!isValidTemplateIds(templateIds)) {
            logger.error("Input templateIds are invalid: " + templateIds);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Template Ids are " +
                    "invalid").asException());
            return;
        }
        final List<ScenarioChange> settingOverrides = createPlacementActionSettingOverride();
        PlanInstance planInstance = null;
        try {
            final Scenario scenario = Scenario.newBuilder()
                    .setScenarioInfo(ScenarioInfo.newBuilder(request.getScenarioInfo())
                            .clearChanges()
                            .addAllChanges(scenarioChangeList)
                            .addAllChanges(settingOverrides))
                        .build();
            planInstance = planDao.createPlanInstance(scenario, PlanProjectType.INITAL_PLACEMENT);
        } catch (IntegrityException e) {
            logger.error("Failed to create a plan instance for initial placement: ", e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
            return;
        }
        logger.info("Starting initial placement: {}", planInstance.getPlanId());
        runPlanInstanceForInitialPlacement(planInstance, responseObserver);
        responseObserver.onNext(InitialPlacementResponse.newBuilder()
                .setPlanId(planInstance.getPlanId())
                .build());
        responseObserver.onCompleted();
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
            return;
        }
        try {
            final Reservation reservation = reservationDao.createReservation(request.getReservation());
            responseObserver.onNext(reservation);
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to create reservation.")
                    .asException());
        }
    }

    /**
     * Send request to run initial placement plan.
     *
     * @param planInstance {@link PlanInstance} represent initial placement plan.
     * @param responseObserver stream observer for initial placement.
     */
    private void runPlanInstanceForInitialPlacement(
            @Nonnull final PlanInstance planInstance,
            @Nonnull final StreamObserver<InitialPlacementResponse> responseObserver) {
        planService.runPlan(
                PlanId.newBuilder()
                        .setPlanId(planInstance.getPlanId())
                        .build(),
                new StreamObserver<PlanInstance>() {
                    @Override
                    public void onNext(PlanInstance value) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        logger.error("Error occurred while executing plan {}.",
                                planInstance.getPlanId());
                        responseObserver.onError(
                                Status.INTERNAL.withDescription(t.getMessage()).asException());
                    }

                    @Override
                    public void onCompleted() {
                    }
                });
    }

    /**
     * Create settingOverride in order to disable move and provision for placed entity and only unplaced
     * entity could move.
     *
     * @return list of {@link ScenarioChange}.
     */
    @VisibleForTesting
    List<ScenarioChange> createPlacementActionSettingOverride() {
        // disable all actions
        List<EntitySettingSpecs> placementPlanSettingsToDisable = Arrays.asList(EntitySettingSpecs.Move,
                EntitySettingSpecs.Provision, EntitySettingSpecs.StorageMove,
                EntitySettingSpecs.Resize, EntitySettingSpecs.Suspend, EntitySettingSpecs.Reconfigure,
                EntitySettingSpecs.Activate);
        ArrayList<ScenarioChange> placementSettingOverrides = new ArrayList<>(placementPlanSettingsToDisable.size());
        placementPlanSettingsToDisable.forEach(settingSpec -> {
            // disable setting
            placementSettingOverrides.add(ScenarioChange.newBuilder()
                    .setSettingOverride(SettingOverride.newBuilder()
                            .setSetting(Setting.newBuilder()
                                    .setSettingSpecName(settingSpec.getSettingName())
                                    .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(DISABLED))))
                    .build());
        });
        return placementSettingOverrides;
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

package com.vmturbo.plan.orchestrator.reservation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ReservationDTO;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;

/**
 * Handle reservation related stuff. This is the place where we check if the current
 * reservation plan is running and block the subsequent reservations.
 * We also kickoff a new reservation plan if no other plan is running.
 * All the state transition logic is handled by this class.
 */
public class ReservationManager {
    private final Logger logger = LogManager.getLogger();

    private final ReservationDao reservationDao;

    private final PlanDao planDao;

    private final PlanRpcService planService;

    private static final String DISABLED = "DISABLED";

    private static final Object reservationSetLock = new Object();

    /**
     * constructor for ReservationManager.
     * @param planDao input plan dao
     * @param reservationDao input reservation dao
     * @param planRpcService input plan service
     */
    public ReservationManager(@Nonnull final PlanDao planDao,
                              @Nonnull final ReservationDao reservationDao,
                              @Nonnull final PlanRpcService planRpcService) {
        this.planDao = Objects.requireNonNull(planDao);
        this.reservationDao = Objects.requireNonNull(reservationDao);
        this.planService = Objects.requireNonNull(planRpcService);
    }

    public ReservationDao getReservationDao() {
        return reservationDao;
    }

    public PlanDao getPlanDao() {
        return planDao;
    }

    /**
     * Update all inprogress reservations to placement_failed on plan failure.
     * Also kick start a new reservation plan by calling checkAndStartReservationPlan.
     */
    public void updateReservationsOnPlanFailure() {
        logger.error("Reservation Plan failed. Updating all INPROGRESS reservations to INVALID.");
        synchronized (reservationSetLock) {
            final Set<Reservation> inProgressSet =
                    reservationDao.getAllReservations().stream()
                            .filter(res -> res.getStatus() == ReservationStatus.INPROGRESS)
                            .collect(Collectors.toSet());
            Set<Reservation> updatedReservation = new HashSet<>();
            for (ReservationDTO.Reservation reservation : inProgressSet) {
                updatedReservation.add(reservation.toBuilder()
                        .setStatus(ReservationStatus.INVALID).build());
            }
            try {
                reservationDao.updateReservationBatch(updatedReservation);
            } catch (NoSuchObjectException e) {
                logger.error("Reservation update failed" + e);
            }
        }
        checkAndStartReservationPlan();
    }

    /**
     * Checks if there is a reservation plan is currently running. If it is not running
     * then start a new reservation plan.
     */
    public void checkAndStartReservationPlan() {
        synchronized (reservationSetLock) {
            final Set<Reservation> inProgressSet = new HashSet<>();
            final Set<Reservation> unfulfilledSet = new HashSet<>();
            final Set<Reservation> allReservations = reservationDao.getAllReservations();
            for (Reservation reservation : allReservations) {
                if (reservation.getStatus() == ReservationStatus.INPROGRESS) {
                    inProgressSet.add(reservation);
                } else if (reservation.getStatus() == ReservationStatus.UNFULFILLED) {
                    unfulfilledSet.add(reservation);
                }
            }
            if (inProgressSet.size() == 0 && unfulfilledSet.size() > 0) {
                Set<Reservation> updatedReservation = new HashSet<>();
                for (ReservationDTO.Reservation reservation : unfulfilledSet) {
                    updatedReservation.add(reservation.toBuilder()
                            .setStatus(ReservationStatus.INPROGRESS).build());
                }
                try {
                    reservationDao.updateReservationBatch(updatedReservation);
                } catch (NoSuchObjectException e) {
                    logger.error("Reservation update failed" + e);
                }
                Set<Long> reservationPlanIDs = planDao.getAllPlanInstances().stream()
                        .filter(pl -> pl.getProjectType() ==
                                PlanProjectType.RESERVATION_PLAN)
                        .map(resPlan -> resPlan.getPlanId())
                        .collect(Collectors.toSet());
                for (Long planID : reservationPlanIDs) {
                    try {
                        planDao.deletePlan(planID);
                    } catch (NoSuchObjectException e) {
                        logger.error("Reservation plan : {} deletion failed" + e, planID);
                    }
                }
                runPlanForBatchReservation();
            }
        }
    }

    /**
     * Update the reservation to UNFULFILLED/FUTURE based on if the reservation is
     * currently active.
     * @param reservation the reservation of interest
     * @return reservation with updated status
     */
    public Reservation intializeReservationStatus(Reservation reservation) {
        synchronized (reservationSetLock) {
            Reservation updatedReservation = reservation.toBuilder()
                    .setStatus(isReservationActiveNow(reservation) ?
                            ReservationStatus.UNFULFILLED
                            : ReservationStatus.FUTURE)
                    .build();
            try {
                reservationDao.updateReservation(reservation.getId(), updatedReservation);
            } catch (NoSuchObjectException e) {
                logger.error("Reservation: {} failed to update." + e, reservation.getName());
            }
            return updatedReservation;
        }
    }

    /**
     * Check if the reservation is active now or has a future start date.
     * @param reservation reservation of interest
     * @return false if the reservation has a future start date.
     *         true  otherwise.
     */
    public boolean isReservationActiveNow(@Nonnull final ReservationDTO.Reservation reservation) {
        final DateTime today = DateTime.now(DateTimeZone.UTC);
        final DateTime reservationDate = new DateTime(reservation.getStartDate(), DateTimeZone.UTC);
        return reservationDate.isEqual(today) || reservationDate.isBefore(today);
    }

    /**
     * Update the reservation status from INPROGRESS to RESERVED/PLACEMENT_FIELD
     * based on if the reservation is successful.
     * @param reservationSet the set of reservations to be updated
     */
    public void updateReservationResult(Set<ReservationDTO.Reservation> reservationSet) {
        synchronized (reservationSetLock) {
            Set<ReservationDTO.Reservation> updatedReservation = new HashSet<>();
            for (ReservationDTO.Reservation reservation : reservationSet) {
                if (isReservationSuccessful(reservation)) {
                    updatedReservation.add(reservation.toBuilder()
                            .setStatus(ReservationStatus.RESERVED).build());
                } else {
                    updatedReservation.add(reservation.toBuilder()
                            .setStatus(ReservationStatus.PLACEMENT_FAILED).build());
                }
            }
            try {
                reservationDao.updateReservationBatch(updatedReservation);
            } catch (NoSuchObjectException e) {
                logger.error("Reservation update failed" + e);
            }
            return;
        }
    }

    /**
     * The method determines if the reservation was successfully placed. The reservation is
     * successful only if all the buyers have a provider
     * @param reservation the reservation of interest
     * @return true if all buyers have a provider. false otherwise.
     */
    private boolean isReservationSuccessful(@Nonnull final ReservationDTO.Reservation reservation) {
        List<ReservationTemplate> reservationTemplates =
                reservation.getReservationTemplateCollection().getReservationTemplateList();
        for (ReservationTemplate reservationTemplate : reservationTemplates) {
            List<ReservationInstance> reservationInstances =
                    reservationTemplate.getReservationInstanceList();
            for (ReservationInstance reservationInstance : reservationInstances) {
                if (reservationInstance.getPlacementInfoList().size() == 0) {
                    return false;
                } else {
                    for (PlacementInfo placementInfo : reservationInstance.getPlacementInfoList()) {
                        if (!placementInfo.hasProviderId()) {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    /**
     * Send request to run initial placement plan.
     */
    public void runPlanForBatchReservation() {
        try {
            final List<ScenarioChange> settingOverrides = createPlacementActionSettingOverride();
            ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
                    .build();
            final Scenario scenario = Scenario.newBuilder()
                    .setScenarioInfo(ScenarioInfo.newBuilder(scenarioInfo)
                            .clearChanges()
                            .addAllChanges(settingOverrides))
                    .build();

            final PlanInstance planInstance = planDao
                    .createPlanInstance(scenario, PlanProjectType.RESERVATION_PLAN);
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
                        }

                        @Override
                        public void onCompleted() {
                        }
                    });
        } catch (Exception e) {
            logger.error("Failed to create a plan instance for initial placement: ", e);
        }
    }

    /**
     * Create settingOverride in order to disable move and
     * provision for placed entity and only unplaced entity could move.
     *
     * @return list of {@link ScenarioChange}.
     */
    @VisibleForTesting
    List<ScenarioChange> createPlacementActionSettingOverride() {
        // disable all actions
        List<EntitySettingSpecs> placementPlanSettingsToDisable =
                Arrays.asList(EntitySettingSpecs.Move,
                EntitySettingSpecs.Provision, EntitySettingSpecs.StorageMove,
                EntitySettingSpecs.Resize, EntitySettingSpecs.Suspend,
                EntitySettingSpecs.Reconfigure,
                EntitySettingSpecs.Activate);
        ArrayList<ScenarioChange> placementSettingOverrides =
                new ArrayList<>(placementPlanSettingsToDisable.size());
        placementPlanSettingsToDisable.forEach(settingSpec -> {
            // disable setting
            placementSettingOverrides.add(ScenarioChange.newBuilder()
                    .setSettingOverride(SettingOverride.newBuilder()
                            .setSetting(Setting.newBuilder()
                                    .setSettingSpecName(settingSpec.getSettingName())
                                    .setEnumSettingValue(EnumSettingValue
                                            .newBuilder().setValue(DISABLED))))
                    .build());
        });
        return placementSettingOverrides;
    }

}

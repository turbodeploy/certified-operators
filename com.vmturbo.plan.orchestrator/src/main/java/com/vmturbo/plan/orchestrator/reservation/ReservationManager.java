package com.vmturbo.plan.orchestrator.reservation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.TemplateProtoUtil;
import com.vmturbo.common.protobuf.market.InitialPlacement.FindInitialPlacementRequest;
import com.vmturbo.common.protobuf.market.InitialPlacement.FindInitialPlacementResponse;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer.InitialPlacementCommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyerPlacementInfo;
import com.vmturbo.common.protobuf.market.InitialPlacementServiceGrpc.InitialPlacementServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ReservationDTO;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationChange;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationChanges;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo.Type;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.utils.ReservationProtoUtil;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;
import com.vmturbo.plan.orchestrator.plan.PlanStatusListener;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Handle reservation related stuff. This is the place where we check if the current
 * reservation plan is running and block the subsequent reservations.
 * We also kickoff a new reservation plan if no other plan is running.
 * All the state transition logic is handled by this class.
 */
public class ReservationManager implements PlanStatusListener, ReservationDeletedListener {
    private final Logger logger = LogManager.getLogger();

    private final ReservationDao reservationDao;

    private final TemplatesDao templatesDao;

    private final PlanDao planDao;

    private final PlanRpcService planService;

    private final ReservationNotificationSender reservationNotificationSender;

    private final InitialPlacementServiceBlockingStub initialPlacementServiceBlockingStub;

    private static final String DISABLED = "DISABLED";

    private static final Object reservationSetLock = new Object();

    /**
     * constructor for ReservationManager.
     * @param planDao input plan dao.
     * @param reservationDao input reservation dao.
     * @param planRpcService input plan service.
     * @param reservationNotificationSender the reservation notification sender.
     * @param initialPlacementServiceBlockingStub for grpc call to market component.
     * @param templatesDao  input template dao
     */
    public ReservationManager(@Nonnull final PlanDao planDao,
                              @Nonnull final ReservationDao reservationDao,
                              @Nonnull final PlanRpcService planRpcService,
                              @Nonnull final ReservationNotificationSender reservationNotificationSender,
                              @Nonnull final InitialPlacementServiceBlockingStub initialPlacementServiceBlockingStub,
                              @Nonnull final TemplatesDao templatesDao
    ) {
        this.planDao = Objects.requireNonNull(planDao);
        this.reservationDao = Objects.requireNonNull(reservationDao);
        this.planService = Objects.requireNonNull(planRpcService);
        this.reservationNotificationSender = Objects.requireNonNull(reservationNotificationSender);
        this.reservationDao.addListener(this);
        this.initialPlacementServiceBlockingStub = Objects.requireNonNull(initialPlacementServiceBlockingStub);
        this.templatesDao = templatesDao;
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
    private void updateReservationsOnPlanFailure() {
        synchronized (reservationSetLock) {
            final Set<Reservation> inProgressSet =
                    reservationDao.getAllReservations().stream()
                            .filter(res -> res.getStatus() == ReservationStatus.INPROGRESS)
                            .collect(Collectors.toSet());
            Set<Reservation> updatedReservation = new HashSet<>();
            for (ReservationDTO.Reservation reservation : inProgressSet) {
                updatedReservation.add(ReservationProtoUtil.invalidateReservation(reservation));
            }
            try {
                reservationDao.updateReservationBatch(updatedReservation);
                logger.info("Marked reservations {} as invalid.", () -> inProgressSet.stream()
                    .map(reservation -> reservation.getId() + "(" + reservation.getName() + ")")
                    .collect(Collectors.joining(",")));
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
        Set<Reservation> currentReservations = new HashSet<>();
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
                for (ReservationDTO.Reservation reservation : unfulfilledSet) {
                    currentReservations.add(reservation.toBuilder()
                            .setStatus(ReservationStatus.INPROGRESS)
                            .build());
                }
                try {
                    reservationDao.updateReservationBatch(currentReservations);
                } catch (NoSuchObjectException e) {
                    logger.error("Reservation update failed" + e);
                }
            }
        }
        if (!currentReservations.isEmpty()) {
            // TODO We should have a timeout on this.
            FindInitialPlacementRequest initialPlacementRequest =
                    buildIntialPlacementRequest(currentReservations);
            FindInitialPlacementResponse response = initialPlacementServiceBlockingStub
                    .findInitialPlacement(initialPlacementRequest);
            Set<Reservation> updatedReservations = updateProviderInfoForReservations(response);
            updateReservationResult(updatedReservations);
        }

    }

    /**
     * Update the provider info of the reservation using the initial placement response.
     *
     * @param response initial placement response from market.
     * @return all in-progress reservations with the provider info updated.
     */
    public Set<Reservation> updateProviderInfoForReservations(final FindInitialPlacementResponse response) {
        Map<Long, Reservation> updatedReservations = new HashMap<>();
        reservationDao.getAllReservations().stream()
                .filter(res -> res.getStatus() == ReservationStatus.INPROGRESS)
                .forEach(inProgressRes -> updatedReservations.put(inProgressRes.getId(), inProgressRes));
        Map<Long, Reservation> buyerIdToReservation = new HashMap<>();
        for (Reservation reservation : updatedReservations.values()) {
            for (ReservationTemplate reservationTemplate : reservation.getReservationTemplateCollection().getReservationTemplateList()) {
                for (ReservationInstance reservationInstance : reservationTemplate.getReservationInstanceList()) {
                    buyerIdToReservation.put(reservationInstance.getEntityId(), reservation);
                }
            }
        }
        for (InitialPlacementBuyerPlacementInfo initialPlacementBuyerPlacementInfo : response.getInitialPlacementBuyerPlacementInfoList()) {
            Reservation currentReservation = buyerIdToReservation.get(initialPlacementBuyerPlacementInfo.getBuyerId());
            Reservation reservation = (updatedReservations.get(currentReservation.getId()) == null)
                    ? currentReservation
                    : updatedReservations.get(currentReservation.getId());
            Reservation.Builder updatedReservation = reservation.toBuilder();
            ReservationTemplateCollection.Builder reservationTemplateCollection = updatedReservation.getReservationTemplateCollection().toBuilder();

            //find templateIndex, InstanceIndex and the placementInfoIndex. We have to update the
            // placementInfo based on initialPlacementBuyerPlacementInfo.
            int templateIndex = 0;
            int instanceIndex = 0;
            int placementInfoIndex = 0;
            boolean foundPlacementInfo = false;
            for (ReservationTemplate reservationTemplate : reservationTemplateCollection.getReservationTemplateList()) {
                instanceIndex = 0;
                for (ReservationInstance reservationInstance : reservationTemplate.getReservationInstanceList()) {
                    if (reservationInstance.getEntityId() == initialPlacementBuyerPlacementInfo.getBuyerId()) {
                        for (PlacementInfo placementInfo : reservationInstance.getPlacementInfoList()) {
                            if (placementInfo.getPlacementInfoId()
                                    == initialPlacementBuyerPlacementInfo
                                    .getCommoditiesBoughtFromProviderId()) {
                                foundPlacementInfo = true;
                                break;
                            }
                            placementInfoIndex++;
                        }
                        break;
                    }
                    instanceIndex++;
                }
                if (foundPlacementInfo) {
                    break;
                }
                templateIndex++;
            }

            if (!foundPlacementInfo) {
                continue;
            }

            ReservationTemplate.Builder reservationTemplate = updatedReservation.getReservationTemplateCollection().getReservationTemplateList()
                    .get(templateIndex).toBuilder();
            ReservationInstance.Builder reservationInstance = reservationTemplate.getReservationInstanceList()
                    .get(instanceIndex).toBuilder();
            PlacementInfo.Builder placementInfo = reservationInstance
                    .getPlacementInfo(placementInfoIndex)
                    .toBuilder().setProviderId(initialPlacementBuyerPlacementInfo.getProviderId());
            reservationInstance.setPlacementInfo(placementInfoIndex, placementInfo);
            reservationTemplate.setReservationInstance(instanceIndex, reservationInstance);
            reservationTemplateCollection.setReservationTemplate(templateIndex, reservationTemplate);
            updatedReservation.setReservationTemplateCollection(reservationTemplateCollection);
            updatedReservations.put(reservation.getId(), updatedReservation.build());
        }
        return updatedReservations.values().stream().collect(Collectors.toSet());
    }

    /**
     * build a initial placement request for the given set of reservations.
     *
     * @param reservations the input set of reservations.
     * @return the constructed initial placement request. buyerIdToReservation is updated.
     */
    public FindInitialPlacementRequest buildIntialPlacementRequest(Set<Reservation> reservations) {
        FindInitialPlacementRequest.Builder initialPlacementRequestBuilder = FindInitialPlacementRequest.newBuilder();
        for (Reservation reservation : reservations) {
            for (ReservationTemplate reservationTemplate : reservation.getReservationTemplateCollection()
                    .getReservationTemplateList()) {
                for (ReservationInstance reservationInstance : reservationTemplate.getReservationInstanceList()) {
                    long buyerId = reservationInstance.getEntityId();
                    InitialPlacementBuyer.Builder intialPlacementBuyerBuilder =
                            InitialPlacementBuyer.newBuilder().setBuyerId(buyerId);
                    for (PlacementInfo placementInfo : reservationInstance.getPlacementInfoList()) {
                        intialPlacementBuyerBuilder.addInitialPlacementCommoditiesBoughtFromProvider(
                                InitialPlacementCommoditiesBoughtFromProvider.newBuilder()
                                        .setCommoditiesBoughtFromProviderId(placementInfo
                                                .getPlacementInfoId())
                                        .setCommoditiesBoughtFromProvider(TopologyEntityDTO
                                                .CommoditiesBoughtFromProvider.newBuilder()
                                                .setProviderEntityType(placementInfo
                                                        .getProviderType())
                                                .addAllCommodityBought(placementInfo
                                                        .getCommodityBoughtList())));
                    }
                    initialPlacementRequestBuilder.addInitialPlacementBuyer(intialPlacementBuyerBuilder);
                }
            }
        }
        return initialPlacementRequestBuilder.build();
    }

    /**
     * add reservation instance/fake entity to the reservation.
     *
     * @param reservation the reservation of interest.
     * @return reservation with the reservation instance updated.
     */
    public Reservation addEntityToReservation(Reservation reservation) {
        List<ReservationTemplate> reservationTemplateList = new ArrayList<>();
        int entityNameSuffix = 0;
        for (ReservationTemplate reservationTemplate : reservation.getReservationTemplateCollection()
                .getReservationTemplateList()) {
            ReservationTemplate.Builder reservationTemplateBuilder = reservationTemplate.toBuilder();
            Optional<Template> templateOptional = templatesDao.getTemplate(reservationTemplateBuilder.getTemplate().getId());
            if (!templateOptional.isPresent()) {
                return reservation;
            }
            final Template template = templateOptional.get();
            reservationTemplateBuilder.setTemplate(template);
            final TemplateInfo templateInfo = template.getTemplateInfo();
            List<Double> diskSizes = new ArrayList<>();
            double memorySize = 0;
            double numOfCpu = 0;
            double cpuSpeed = 0;
            for (TemplateResource templateResource : templateInfo.getResourcesList()) {
                for (TemplateField templateField : templateResource.getFieldsList()) {
                    switch (templateField.getName()) {
                        case TemplateProtoUtil
                                .VM_STORAGE_DISK_SIZE:
                            diskSizes.add(Double.parseDouble(templateField.getValue()));
                            break;
                        case TemplateProtoUtil.VM_COMPUTE_VCPU_SPEED:
                            cpuSpeed = Double.parseDouble(templateField.getValue());
                            break;
                        case TemplateProtoUtil.VM_COMPUTE_MEM_SIZE:
                            memorySize = Double.parseDouble(templateField.getValue());
                            break;
                        case TemplateProtoUtil.VM_COMPUTE_NUM_OF_VCPU:
                            numOfCpu = Double.parseDouble(templateField.getValue());
                            break;
                        default:
                            break;
                    }
                }
            }
            List<PlacementInfo> placementInfos = new ArrayList<>();
            placementInfos.add(PlacementInfo.newBuilder().clearProviderId()
                    .addCommodityBought(createCommodityBoughtDTO(CommodityDTO
                            .CommodityType.CPU_PROVISIONED_VALUE, numOfCpu * cpuSpeed))
                    .addCommodityBought(createCommodityBoughtDTO(CommodityDTO
                            .CommodityType.MEM_PROVISIONED_VALUE, memorySize))
                    .setProviderType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .setPlacementInfoId(IdentityGenerator.next()).build());
            for (Double diskSize : diskSizes) {
                placementInfos.add(PlacementInfo
                        .newBuilder().clearProviderId()
                        .addCommodityBought(createCommodityBoughtDTO(CommodityDTO
                                .CommodityType.STORAGE_PROVISIONED_VALUE, diskSize))
                        .setProviderType(EntityType.STORAGE_VALUE)
                        .setPlacementInfoId(IdentityGenerator.next()).build());
            }

            for (int i = 0; i < reservationTemplateBuilder.getCount(); i++) {
                ReservationInstance.Builder reservationInstanceBuilder = ReservationInstance
                        .newBuilder().setEntityId(IdentityGenerator.next())
                        .setName(reservation.getName() + "_" + entityNameSuffix);
                entityNameSuffix++;
                reservationInstanceBuilder.addAllPlacementInfo(placementInfos);
                reservationTemplateBuilder.addReservationInstance(ReservationInstance
                        .newBuilder(reservationInstanceBuilder.build()));
            }

            reservationTemplateList.add(reservationTemplateBuilder.build());
        }

        Reservation updatedReservation = reservation.toBuilder()
                .setReservationTemplateCollection(ReservationTemplateCollection.newBuilder()
                        .addAllReservationTemplate(reservationTemplateList))
                .build();
        return updatedReservation;
    }

    /**
     * Add entities to the reservation. Update the reservation to UNFULFILLED/FUTURE
     * based on if the reservation is currently active.
     * @param reservation the reservation of interest
     * @return reservation with updated status
     */
    public Reservation intializeReservationStatus(Reservation reservation) {
        synchronized (reservationSetLock) {
            ReservationDTO.Reservation reservationWithEntity = addEntityToReservation(reservation);
            Reservation updatedReservation = reservationWithEntity.toBuilder()
                    .setStatus(isReservationActiveNow(reservationWithEntity)
                            ? ReservationStatus.UNFULFILLED
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
     * Check if the reservation has expired.
     * @param reservation reservation of interest
     * @return false if the reservation has not expired.
     *         true  otherwise.
     */
    public boolean hasReservationExpired(@Nonnull final ReservationDTO.Reservation reservation) {
        final DateTime today = DateTime.now(DateTimeZone.UTC);
        final DateTime expirationDate = new DateTime(reservation.getExpirationDate(), DateTimeZone.UTC);
        return expirationDate.isEqual(today) || expirationDate.isBefore(today);
    }

    /**
     * Update the reservation status from INPROGRESS to RESERVED/PLACEMENT_FIELD
     * based on if the reservation is successful.
     * @param reservationSet the set of reservations to be updated
     */
    public void updateReservationResult(Set<ReservationDTO.Reservation> reservationSet) {
        synchronized (reservationSetLock) {
            Set<ReservationDTO.Reservation> updatedReservations = new HashSet<>();
            Set<ReservationDTO.Reservation> statusUpdatedReservation = new HashSet<>();
            for (ReservationDTO.Reservation reservation : reservationSet) {
                boolean isSuccessful = isReservationSuccessful(reservation);
                ReservationDTO.Reservation.Builder updatedReservation = reservation.toBuilder();
                // We should call the updateReservationBatch for all reservations including the ones
                // whose status didn't change..Because the provider could have changed.
                if (isSuccessful) {
                    updatedReservation.setStatus(ReservationStatus.RESERVED);
                } else {
                    updatedReservation.setStatus(ReservationStatus.PLACEMENT_FAILED);
                }
                updatedReservations.add(updatedReservation.build());
                // we should not broadcast the reservation change to the UI if the status does not change.
                // The provider change happens only during the main market and that need not be updated
                // in the UI. A refresh of screen will get the user the new providers.
                if (updatedReservation.getStatus() != reservation.getStatus()) {
                    logger.info("Finished Reservation: " + reservation.getName());
                    statusUpdatedReservation.add(updatedReservation.build());
                }
            }
            try {
                if (!updatedReservations.isEmpty()) {
                    reservationDao.updateReservationBatch(updatedReservations);
                }
                if (!statusUpdatedReservation.isEmpty()) {
                    broadcastReservationChange(statusUpdatedReservation);
                }
            } catch (NoSuchObjectException e) {
                logger.error("Reservation update failed" + e);
            }
        }
        checkAndStartReservationPlan();
    }

    /**
     * Broadcast the {@link ReservationChanges} using the reservation notification sender and handle
     * any errors.
     *
     * @param reservations the reservations whose statuses have changed and will be broadcasted.
     */
    @VisibleForTesting
    protected void broadcastReservationChange(@Nonnull final Set<ReservationDTO.Reservation> reservations) {
        if (reservations.isEmpty()) {
            return;
        }

        final ReservationDTO.ReservationChanges.Builder resChangesBuilder =
            ReservationDTO.ReservationChanges.newBuilder();
        for (ReservationDTO.Reservation reservation : reservations) {
            resChangesBuilder.addReservationChange(buildReservationChange(reservation));
        }

        try {
            reservationNotificationSender.sendNotification(resChangesBuilder.build());
        } catch (CommunicationException e) {
            logger.error("Error sending reservation update notification for reservation changes.", e);
        }
    }

    /**
     * Builds a {@link ReservationDTO.ReservationChange} from a {@link ReservationDTO.Reservation}.
     * @param reservation the reservation.
     * @return a ReservationChange object built from the reservation.
     */
    private ReservationDTO.ReservationChange buildReservationChange(ReservationDTO.Reservation reservation) {
        return ReservationChange.newBuilder()
            .setId(reservation.getId())
            .setStatus(reservation.getStatus())
            .build();
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
            Map<Long, String> scopeIds = getAllScopeEntities();
            PlanScope.Builder planScopeBuilder = PlanScope.newBuilder();
            if (!scopeIds.isEmpty()) {
                for (Entry<Long, String> scopeid : scopeIds.entrySet()) {
                    PlanScopeEntry planScopeEntry = PlanScopeEntry
                            .newBuilder().setScopeObjectOid(scopeid.getKey())
                            .setClassName(scopeid.getValue()).build();
                    planScopeBuilder.addScopeEntries(planScopeEntry);
                }
            }
            final Scenario scenario = Scenario.newBuilder()
                    .setScenarioInfo(ScenarioInfo.newBuilder(scenarioInfo)
                            .clearChanges()
                            .addAllChanges(settingOverrides)
                            .setScope(planScopeBuilder))
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

    @Override
    public void onPlanStatusChanged(@Nonnull final PlanInstance plan) throws PlanStatusListenerException {
        if (plan.getProjectType() == PlanProjectType.RESERVATION_PLAN) {
            if (plan.getStatus() == PlanStatus.FAILED) {
                logger.error("Reservation plan {} failed. Message: {}." +
                    " Marking reservations as INVALID.", plan.getPlanId(), plan.getStatusMessage());
                // We know that the failed plan relates to the current in-progress
                // reservations because successful reservations delete the plan they relate
                // to while holding a lock.
                updateReservationsOnPlanFailure();
            }
        }
    }

    @Override
    public void onPlanDeleted(@Nonnull final PlanInstance plan) throws PlanStatusListenerException {
        // We ignore plan deletions for reservations, since we delete them very eagerly.
    }

    private CommodityBoughtDTO createCommodityBoughtDTO(int commodityType, double used) {
        return createCommodityBoughtDTO(commodityType, Optional.empty(), used);
    }

    private CommodityBoughtDTO createCommodityBoughtDTO(int commodityType, Optional<String> key, double used) {
        final CommodityType.Builder commType = CommodityType.newBuilder().setType(commodityType);
        key.ifPresent(commType::setKey);
        return CommodityBoughtDTO.newBuilder()
                .setUsed(used)
                .setActive(true)
                .setCommodityType(commType)
                .build();
    }

    /**
     * Get all the constraint ids of inprogress reservation.
     * If one of the in-progress reservation is un -constrained (full scope) the return empty list.
     *
     * @return Map of constraint id and the type of constraint.
     */
    private Map<Long, String> getAllScopeEntities() {
        Map<Long, String> scope = new HashMap<>();
        final Set<Reservation> reservations =
                reservationDao.getAllReservations().stream()
                        .filter(res -> res.getStatus() == ReservationStatus.INPROGRESS)
                        .collect(Collectors.toSet());
        for (Reservation reservation : reservations) {
            if (!reservation.hasConstraintInfoCollection() ||
                    reservation
                            .getConstraintInfoCollection()
                            .getReservationConstraintInfoList() == null ||
                    reservation.getConstraintInfoCollection()
                            .getReservationConstraintInfoList().isEmpty()) {
                return new HashMap<>();
            } else {
                List<ReservationConstraintInfo> reservationConstraintInfos = reservation
                        .getConstraintInfoCollection().getReservationConstraintInfoList();
                for (ReservationConstraintInfo reservationConstraintInfo
                        : reservationConstraintInfos) {
                    if (reservationConstraintInfo.getType() == Type.CLUSTER) {
                        scope.put(reservationConstraintInfo
                                .getConstraintId(), StringConstants.CLUSTER);
                    } else if (reservationConstraintInfo.getType() ==
                            Type.VIRTUAL_DATA_CENTER) {
                        scope.put(reservationConstraintInfo
                                .getConstraintId(), StringConstants.VDC);
                    } else if (reservationConstraintInfo
                            .getType() == Type.DATA_CENTER) {
                        scope.put(reservationConstraintInfo
                                .getConstraintId(), StringConstants.DATA_CENTER);
                    } else {
                        return new HashMap<>();
                    }
                }
            }
        }
        return scope;
    }

    @Override
    public void onReservationDeleted(@Nonnull final Reservation reservation) {
        checkAndStartReservationPlan();
    }
}

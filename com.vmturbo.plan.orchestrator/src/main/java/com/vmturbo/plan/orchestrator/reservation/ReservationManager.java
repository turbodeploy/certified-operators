package com.vmturbo.plan.orchestrator.reservation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

import com.vmturbo.common.protobuf.TemplateProtoUtil;
import com.vmturbo.common.protobuf.market.InitialPlacement.DeleteInitialPlacementBuyerRequest;
import com.vmturbo.common.protobuf.market.InitialPlacement.DeleteInitialPlacementBuyerResponse;
import com.vmturbo.common.protobuf.market.InitialPlacement.FindInitialPlacementRequest;
import com.vmturbo.common.protobuf.market.InitialPlacement.FindInitialPlacementResponse;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer.InitialPlacementCommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyerPlacementInfo;
import com.vmturbo.common.protobuf.market.InitialPlacementServiceGrpc.InitialPlacementServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ConstraintInfoCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationChange;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationChanges;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo.Type;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.SettingOverride;
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
public class ReservationManager implements ReservationDeletedListener {
    private final Logger logger = LogManager.getLogger();

    private final ReservationDao reservationDao;

    private final TemplatesDao templatesDao;

    private final ReservationNotificationSender reservationNotificationSender;

    private final InitialPlacementServiceBlockingStub initialPlacementServiceBlockingStub;

    private static final Object reservationSetLock = new Object();

    private Map<Long, ReservationConstraintInfo> constraintIDToCommodityTypeMap = new HashMap<>();

    /**
     * constructor for ReservationManager.
     * @param reservationDao input reservation dao.
     * @param reservationNotificationSender the reservation notification sender.
     * @param initialPlacementServiceBlockingStub for grpc call to market component.
     * @param templatesDao  input template dao
     */
    public ReservationManager(@Nonnull final ReservationDao reservationDao,
                              @Nonnull final ReservationNotificationSender reservationNotificationSender,
                              @Nonnull final InitialPlacementServiceBlockingStub initialPlacementServiceBlockingStub,
                              @Nonnull final TemplatesDao templatesDao
    ) {
        this.reservationDao = Objects.requireNonNull(reservationDao);
        this.reservationNotificationSender = Objects.requireNonNull(reservationNotificationSender);
        this.reservationDao.addListener(this);
        this.initialPlacementServiceBlockingStub = Objects.requireNonNull(initialPlacementServiceBlockingStub);
        this.templatesDao = templatesDao;
    }

    /**
     * Add the constraint id and reservationConstraintInfo send from TP to the map for future lookup.
     * @param id Constraint ID
     * @param reservationConstraintInfo the associated constraint info.
     */
    public void addToConstraintIDToCommodityTypeMap(Long id, ReservationConstraintInfo reservationConstraintInfo) {
        constraintIDToCommodityTypeMap.put(id, reservationConstraintInfo);
    }

    /**
     * Checks if the constraint id is valud and populated in constraintIDToCommodityTypeMap.
     * @param constraintId the constraint id of interest.
     * @return true if the constraintIDToCommodityTypeMap keyset contains constraintId.
     */
    public boolean isConstraintIdValid(Long constraintId) {
        return constraintIDToCommodityTypeMap.containsKey(constraintId);
    }

    public ReservationDao getReservationDao() {
        return reservationDao;
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
            Set<Reservation> updatedReservations = updateProviderInfoForReservations(response,
                    currentReservations.stream().map(res -> res.getId()).collect(Collectors.toSet()));
            updateReservationResult(updatedReservations);
        }

    }

    /**
     * Update the provider info of the reservation using the initial placement response.
     *
     * @param response initial placement response from market.
     * @param currentReservations reservations which are sent to the market. We have to make sure
     *                          we only update the reservations which are part of the request.
     * @return all in-progress reservations with the provider info updated.
     */
    public Set<Reservation> updateProviderInfoForReservations(final FindInitialPlacementResponse response,
                                                              Set<Long> currentReservations) {
        Map<Long, Reservation> updatedReservations = new HashMap<>();
        reservationDao.getAllReservations().stream()
                .filter(res -> res.getStatus() == ReservationStatus.INPROGRESS)
                .filter(res -> currentReservations.contains(res.getId()))
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
            if (currentReservation == null) {
                logger.error("The buyer id {} does not "
                                + "correspond to any reservation in the request",
                        initialPlacementBuyerPlacementInfo.getBuyerId());
                continue;
            }
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
                    .toBuilder();
            if (initialPlacementBuyerPlacementInfo.hasInitialPlacementSuccess()) {
                placementInfo
                        .setProviderId(
                                initialPlacementBuyerPlacementInfo.getInitialPlacementSuccess()
                                        .getProviderOid());
            } else {
                reservationInstance
                        .addAllFailureInfo(
                                initialPlacementBuyerPlacementInfo.getInitialPlacementFailure()
                                        .getFailureInfoList());
            }
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
                            InitialPlacementBuyer.newBuilder().setBuyerId(buyerId).setReservationId(reservation.getId());
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
            Optional<Template> templateOptional = templatesDao.getTemplate(reservationTemplateBuilder.getTemplateId());
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
            for (Double diskSize : diskSizes) {
                placementInfos.add(PlacementInfo
                        .newBuilder().clearProviderId()
                        .addCommodityBought(createCommodityBoughtDTO(CommodityDTO
                                .CommodityType.STORAGE_PROVISIONED_VALUE, diskSize))
                        .setProviderType(EntityType.STORAGE_VALUE).build());
            }

            PlacementInfo.Builder computePlacementInfo = PlacementInfo.newBuilder().clearProviderId()
                    .addCommodityBought(createCommodityBoughtDTO(CommodityDTO
                            .CommodityType.CPU_PROVISIONED_VALUE, numOfCpu * cpuSpeed))
                    .addCommodityBought(createCommodityBoughtDTO(CommodityDTO
                            .CommodityType.MEM_PROVISIONED_VALUE, memorySize))
                    .setProviderType(EntityType.PHYSICAL_MACHINE_VALUE);

            for (ReservationConstraintInfo constraintInfo
                    : reservation.getConstraintInfoCollection()
                            .getReservationConstraintInfoList()) {
                switch (constraintInfo.getType()) {
                    case DATA_CENTER:
                        computePlacementInfo.addCommodityBought(
                                createCommodityBoughtDTO(CommodityDTO
                                                .CommodityType.DATACENTER_VALUE,
                                        Optional.of(constraintInfo.getKey()), 1));
                        break;
                    case CLUSTER:
                        computePlacementInfo.addCommodityBought(
                                createCommodityBoughtDTO(CommodityDTO
                                                .CommodityType.CLUSTER_VALUE,
                                        Optional.of(constraintInfo.getKey()), 1));
                        break;
                    case POLICY:
                        computePlacementInfo.addCommodityBought(
                                createCommodityBoughtDTO(CommodityDTO
                                                .CommodityType.SEGMENTATION_VALUE,
                                        Optional.of(constraintInfo.getKey()), 1));
                        break;
                    default:
                        break;

                }
            }
            placementInfos.add(computePlacementInfo.build());


            for (int i = 0; i < reservationTemplateBuilder.getCount(); i++) {
                ReservationInstance.Builder reservationInstanceBuilder = ReservationInstance
                        .newBuilder().setEntityId(IdentityGenerator.next())
                        .setName(reservation.getName() + "_" + entityNameSuffix);
                entityNameSuffix++;
                placementInfos.forEach(p -> {
                    reservationInstanceBuilder.addPlacementInfo(p.toBuilder()
                            .setPlacementInfoId(IdentityGenerator.next()).build());
                });
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
            Reservation reservationWithConstraintInfo = addConstraintInfoDetails(reservation);
            ReservationDTO.Reservation reservationWithEntity =
                    addEntityToReservation(reservationWithConstraintInfo);
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
     * update the constraint info of the reservation.
     *
     * @param reservation the reservation of interest.
     * @return reservation with the constraintInfo updated.
     */
    public Reservation addConstraintInfoDetails(Reservation reservation) {
        ConstraintInfoCollection.Builder constraintInfoCollection = ConstraintInfoCollection.newBuilder();
        for (ReservationConstraintInfo reservationConstraintInfo
                : reservation.getConstraintInfoCollection().getReservationConstraintInfoList()) {
            ReservationConstraintInfo updatedReservationConstraintInfo =
                    constraintIDToCommodityTypeMap.get(reservationConstraintInfo.getConstraintId());
            if (updatedReservationConstraintInfo != null) {
                constraintInfoCollection.addReservationConstraintInfo(updatedReservationConstraintInfo);
            }
        }
        return reservation.toBuilder().setConstraintInfoCollection(constraintInfoCollection).build();
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
                ReservationDTO.Reservation updatedReservation;
                // We should call the updateReservationBatch for all reservations including the ones
                // whose status didn't change..Because the provider could have changed.
                if (isSuccessful) {
                    updatedReservation = ReservationProtoUtil.clearReservationInstance(
                            reservation,
                            false, true).toBuilder()
                            .setStatus(ReservationStatus.RESERVED).build();
                } else {
                    updatedReservation = reservation.toBuilder()
                            .setStatus(ReservationStatus.PLACEMENT_FAILED).build();
                }
                updatedReservations.add(updatedReservation);
                // we should not broadcast the reservation change to the UI if the status does not change.
                // The provider change happens only during the main market and that need not be updated
                // in the UI. A refresh of screen will get the user the new providers.
                if (updatedReservation.getStatus() != reservation.getStatus()) {
                    logger.info("Finished Reservation: " + reservation.getName());
                    statusUpdatedReservation.add(updatedReservation);
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

    @Override
    public void onReservationDeleted(@Nonnull final Reservation reservation) {
        DeleteInitialPlacementBuyerRequest.Builder deleteInitialPlacementBuyerRequest =
                DeleteInitialPlacementBuyerRequest.newBuilder();
        reservation.getReservationTemplateCollection().getReservationTemplateList()
                .stream().forEach(template -> template.getReservationInstanceList()
                .stream().forEach(buyer -> deleteInitialPlacementBuyerRequest
                        .addBuyerId(buyer.getEntityId())));
        DeleteInitialPlacementBuyerResponse response = initialPlacementServiceBlockingStub
                .deleteInitialPlacementBuyer(deleteInitialPlacementBuyerRequest.build());
        if (!response.getResult()) {
            logger.error("Deletion of Initial Placement Buyer failed");
        }
        checkAndStartReservationPlan();
    }
}

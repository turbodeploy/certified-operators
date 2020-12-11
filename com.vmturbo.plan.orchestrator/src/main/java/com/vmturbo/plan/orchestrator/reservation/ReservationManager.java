package com.vmturbo.plan.orchestrator.reservation;

import java.util.ArrayList;
import java.util.Collections;
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
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.utils.ReservationProtoUtil;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricCounter;

/**
 * Handle reservation related stuff. This is the place where we check if the current
 * reservation plan is running and block the subsequent reservations.
 * We also kickoff a new reservation plan if no other plan is running.
 * All the state transition logic is handled by this class.
 */
public class ReservationManager implements ReservationDeletedListener {
    private final Logger logger = LogManager.getLogger();

    private final String logPrefix = "FindInitialPlacement: ";

    private final ReservationDao reservationDao;

    private final TemplatesDao templatesDao;

    private final ReservationNotificationSender reservationNotificationSender;

    private final InitialPlacementServiceBlockingStub initialPlacementServiceBlockingStub;

    private static final Object reservationSetLock = new Object();

    private Map<Long, ReservationConstraintInfo> constraintIDToCommodityTypeMap = new HashMap<>();

    private Map<String, Long> clusterCommodityKeyToClusterID = new HashMap<>();

    private static final String DELETED_STATUS = "DELETED";

    /**
     * Track reservation counts.
     */
    private static final DataMetricCounter RESERVATION_STATUS_COUNTER = DataMetricCounter.builder()
            .withName(StringConstants.METRICS_TURBO_PREFIX + "reservations_total")
            .withHelp("Reservation status count that have been run in the system")
            .withLabelNames("status")
            .build()
            .register();

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
     * @param constraintIDToCommodityTypeMap the map from id to reservationConstraintInfo.
     */
    public void addToConstraintIDToCommodityTypeMap(Map<Long, ReservationConstraintInfo>
                                                            constraintIDToCommodityTypeMap) {
        synchronized (reservationSetLock) {
            this.constraintIDToCommodityTypeMap.clear();
            this.constraintIDToCommodityTypeMap.putAll(constraintIDToCommodityTypeMap);
            constraintIDToCommodityTypeMap.entrySet().stream().forEach(entry -> {
                if (entry.getValue().getType() == Type.CLUSTER
                        || entry.getValue().getType() == Type.STORAGE_CLUSTER) {
                    clusterCommodityKeyToClusterID.put(entry.getValue().getKey(), entry.getKey());
                }
            });
        }
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
                    logger.error(logPrefix + "Reservation update failed" + e);
                }
            } else {
                for (Reservation unfulfilled : unfulfilledSet) {
                    logger.info(logPrefix + "Reservation: " + unfulfilled.getId()
                            + " waiting for current reservations to finish. Currently running reservations: "
                            + inProgressSet.size());
                }
            }
        }
        if (!currentReservations.isEmpty()) {
            for (Reservation reservation : currentReservations) {
                logger.info(logPrefix + "sending reservation: " + reservation.getId()
                        + " for intial placement. Current Status: " + reservation.getStatus());
            }
            Set<Reservation> updatedReservations = currentReservations;
            try {
                // TODO We should have a timeout on this.
                FindInitialPlacementRequest initialPlacementRequest =
                        buildIntialPlacementRequest(currentReservations);
                FindInitialPlacementResponse response = initialPlacementServiceBlockingStub
                        .findInitialPlacement(initialPlacementRequest);
                updatedReservations = updateProviderInfoForReservations(response,
                        currentReservations.stream().map(res -> res.getId()).collect(Collectors.toSet()));
            } catch (Exception e) {
                logger.warn(logPrefix + "findInitialPlacement was not completed successfully");
            } finally {
                updateReservationResult(updatedReservations);
            }
        }

    }

    /**
     * Update the provider info of the reservation using the initial placement response.
     *
     * @param response            initial placement response from market.
     * @param currentReservations reservations which are sent to the market. We have to make sure
     *                            we only update the reservations which are part of the request.
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
                logger.error(logPrefix + "The buyer id {} does not "
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
                if (initialPlacementBuyerPlacementInfo
                        .getInitialPlacementSuccess().hasCluster()
                        && clusterCommodityKeyToClusterID
                                .containsKey(initialPlacementBuyerPlacementInfo
                                        .getInitialPlacementSuccess()
                                        .getCluster().getKey())) {
                    placementInfo
                            .setClusterId(clusterCommodityKeyToClusterID
                                    .get(initialPlacementBuyerPlacementInfo
                                            .getInitialPlacementSuccess().getCluster().getKey()));
                    placementInfo.addAllCommodityStats(initialPlacementBuyerPlacementInfo
                            .getInitialPlacementSuccess().getCommodityStatsList());
                }
                logger.info(logPrefix + "provider: " + initialPlacementBuyerPlacementInfo
                        .getInitialPlacementSuccess().getProviderOid()
                        + " found for entity: " + reservationInstance.getEntityId()
                        + " of reservation: " + updatedReservation.getId());
            } else {
                reservationInstance
                        .addAllUnplacedReason(
                                initialPlacementBuyerPlacementInfo.getInitialPlacementFailure()
                                        .getUnplacedReasonList());
                logger.info(logPrefix + "entity: " + reservationInstance.getEntityId()
                        + " of reservation: " + updatedReservation.getId() + " failed to get placed.");
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
     * @throws Exception if the templateID is not valid.
     */
    public Reservation addEntityToReservation(Reservation reservation) throws Exception {
        List<ReservationTemplate> reservationTemplateList = new ArrayList<>();
        int entityNameSuffix = 0;
        for (ReservationTemplate reservationTemplate : reservation.getReservationTemplateCollection()
                .getReservationTemplateList()) {
            ReservationTemplate.Builder reservationTemplateBuilder = reservationTemplate.toBuilder().clearReservationInstance();
            Optional<Template> templateOptional = templatesDao.getTemplate(reservationTemplateBuilder.getTemplateId());
            if (!templateOptional.isPresent()) {
                throw new Exception("Template not found for ID: " + reservationTemplateBuilder.getTemplateId());
            }
            final Template template = templateOptional.get();
            reservationTemplateBuilder.setTemplate(template);
            final TemplateInfo templateInfo = template.getTemplateInfo();
            List<Double> diskSizes = new ArrayList<>();
            List<Double> diskIOPs = new ArrayList<>();
            List<Double> diskConsumedFactors = new ArrayList<>();
            double memorySize = 0;
            double numOfCpu = 0;
            double cpuSpeed = 0;
            double cpuConsumedFactor = 0;
            double memoryConsumedFactor = 0;
            double ioThroughput = 0;
            double networkThroughput = 0;

            for (TemplateResource templateResource : templateInfo.getResourcesList()) {
                for (TemplateField templateField : templateResource.getFieldsList()) {
                    switch (templateField.getName()) {
                        case TemplateProtoUtil
                                .VM_STORAGE_DISK_SIZE:
                            diskSizes.add(Double.parseDouble(templateField.getValue()));
                            break;
                        case TemplateProtoUtil
                                .VM_STORAGE_DISK_IOPS:
                            diskIOPs.add(Double.parseDouble(templateField.getValue()));
                            break;
                        case TemplateProtoUtil
                                .VM_STORAGE_DISK_CONSUMED_FACTOR:
                            diskConsumedFactors.add(Double.parseDouble(templateField.getValue()));
                            break;
                        case TemplateProtoUtil.VM_COMPUTE_VCPU_SPEED:
                            cpuSpeed = Double.parseDouble(templateField.getValue());
                            break;
                        case TemplateProtoUtil.VM_COMPUTE_CPU_CONSUMED_FACTOR:
                            cpuConsumedFactor = Double.parseDouble(templateField.getValue());
                            break;
                        case TemplateProtoUtil.VM_COMPUTE_MEM_SIZE:
                            memorySize = Double.parseDouble(templateField.getValue());
                            break;
                        case TemplateProtoUtil.VM_COMPUTE_MEM_CONSUMED_FACTOR:
                            memoryConsumedFactor = Double.parseDouble(templateField.getValue());
                            break;
                        case TemplateProtoUtil.VM_COMPUTE_NUM_OF_VCPU:
                            numOfCpu = Double.parseDouble(templateField.getValue());
                            break;
                        case TemplateProtoUtil.VM_COMPUTE_IO_THROUGHPUT:
                            ioThroughput = Double.parseDouble(templateField.getValue());
                            break;
                        case TemplateProtoUtil.VM_COMPUTE_NETWORK_THROUGHPUT:
                            networkThroughput = Double.parseDouble(templateField.getValue());
                            break;
                        default:
                            break;
                    }
                }
            }
            List<PlacementInfo.Builder> storagePlacementInfos = new ArrayList<>();
            for (int i = 0; i < diskSizes.size(); i++) {
                storagePlacementInfos.add(PlacementInfo
                        .newBuilder().clearProviderId()
                        .addCommodityBought(createCommodityBoughtDTO(CommodityDTO
                                .CommodityType.STORAGE_PROVISIONED_VALUE, diskSizes.get(i)))
                        .addCommodityBought(createCommodityBoughtDTO(CommodityDTO
                                .CommodityType.STORAGE_AMOUNT_VALUE,
                                diskSizes.get(i) * diskConsumedFactors.get(i)))
                        .addCommodityBought(createCommodityBoughtDTO(CommodityDTO
                                .CommodityType.STORAGE_ACCESS_VALUE, diskIOPs.get(i)))
                        .setProviderType(EntityType.STORAGE_VALUE));
            }

            PlacementInfo.Builder computePlacementInfo = PlacementInfo.newBuilder().clearProviderId()
                    .addCommodityBought(createCommodityBoughtDTO(CommodityDTO
                            .CommodityType.CPU_PROVISIONED_VALUE, numOfCpu * cpuSpeed))
                    .addCommodityBought(createCommodityBoughtDTO(CommodityDTO
                            .CommodityType.CPU_VALUE, numOfCpu * cpuSpeed * cpuConsumedFactor))
                    .addCommodityBought(createCommodityBoughtDTO(CommodityDTO
                            .CommodityType.MEM_PROVISIONED_VALUE, memorySize))
                    .addCommodityBought(createCommodityBoughtDTO(CommodityDTO
                            .CommodityType.MEM_VALUE, memorySize * memoryConsumedFactor))
                    .addCommodityBought(createCommodityBoughtDTO(CommodityDTO
                            .CommodityType.IO_THROUGHPUT_VALUE, ioThroughput))
                    .addCommodityBought(createCommodityBoughtDTO(CommodityDTO
                            .CommodityType.NET_THROUGHPUT_VALUE, networkThroughput))
                    .setProviderType(EntityType.PHYSICAL_MACHINE_VALUE);

            for (ReservationConstraintInfo constraintInfo
                    : reservation.getConstraintInfoCollection()
                    .getReservationConstraintInfoList()) {
                if (constraintInfo.getProviderType() == EntityType.PHYSICAL_MACHINE_VALUE) {
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
                        case NETWORK:
                            computePlacementInfo.addCommodityBought(
                                    createCommodityBoughtDTO(CommodityDTO
                                                    .CommodityType.NETWORK_VALUE,
                                            Optional.of(constraintInfo.getKey()), 1));
                            break;
                        default:
                            break;

                    }
                } else if (constraintInfo.getProviderType() == EntityType.STORAGE_VALUE) {
                    switch (constraintInfo.getType()) {
                        case STORAGE_CLUSTER:
                            storagePlacementInfos.stream().forEach(storagePlacementInfo -> {
                                storagePlacementInfo.addCommodityBought(
                                        createCommodityBoughtDTO(CommodityDTO
                                                        .CommodityType.STORAGE_CLUSTER_VALUE,
                                                Optional.of(constraintInfo.getKey()), 1));
                            });
                            break;
                        case POLICY:
                            storagePlacementInfos.stream().forEach(storagePlacementInfo -> {
                                storagePlacementInfo.addCommodityBought(
                                        createCommodityBoughtDTO(CommodityDTO
                                                        .CommodityType.SEGMENTATION_VALUE,
                                                Optional.of(constraintInfo.getKey()), 1));
                            });
                            break;
                        default:
                            break;
                    }
                }
            }
            List<PlacementInfo.Builder> placementInfos = new ArrayList<>();
            placementInfos.addAll(storagePlacementInfos);
            placementInfos.add(computePlacementInfo);

            for (int i = 0; i < reservationTemplateBuilder.getCount(); i++) {
                ReservationInstance.Builder reservationInstanceBuilder = ReservationInstance
                        .newBuilder().setEntityId(IdentityGenerator.next())
                        .setName(reservation.getName() + "_" + entityNameSuffix);
                entityNameSuffix++;
                placementInfos.forEach(p -> {
                    reservationInstanceBuilder.addPlacementInfo(p.setPlacementInfoId(
                            IdentityGenerator.next()).build());
                });
                reservationTemplateBuilder.addReservationInstance(ReservationInstance
                        .newBuilder(reservationInstanceBuilder.build()));
                logger.info(logPrefix + "Added entity: " + reservationInstanceBuilder.getEntityId()
                        + " to reservation: " + reservation.getId());
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
            Reservation updatedReservation;
            try {
                Reservation reservationWithConstraintInfo = addConstraintInfoDetails(reservation);
                ReservationDTO.Reservation reservationWithEntity =
                        addEntityToReservation(reservationWithConstraintInfo);
                updatedReservation = reservationWithEntity.toBuilder()
                        .setStatus(isReservationActiveNow(reservationWithEntity)
                                ? ReservationStatus.UNFULFILLED
                                : ReservationStatus.FUTURE)
                        .build();
            } catch (Exception e) {
                logger.error(logPrefix + e.getMessage());
                updatedReservation = ReservationProtoUtil.invalidateReservation(reservation);
            }
            try {
                reservationDao.updateReservation(reservation.getId(), updatedReservation);
                logger.info(logPrefix + "Initialized Reservation: " + updatedReservation.getName()
                        + " Current Status: " + updatedReservation.getStatus()
                + " id: " + updatedReservation.getId());
            } catch (NoSuchObjectException e) {
                logger.error(logPrefix + "Reservation: {} failed to update." + e, reservation.getName());
            }
            return updatedReservation;
        }
    }

    /**
     * update the constraint info of the reservation.
     *
     * @param reservation the reservation of interest.
     * @return reservation with the constraintInfo updated.
     * @throws Exception if the constraint ID is not valid.
     */
    public Reservation addConstraintInfoDetails(Reservation reservation) throws Exception {
        ConstraintInfoCollection.Builder constraintInfoCollection = ConstraintInfoCollection.newBuilder();
        for (ReservationConstraintInfo reservationConstraintInfo
                : reservation.getConstraintInfoCollection().getReservationConstraintInfoList()) {
            ReservationConstraintInfo updatedReservationConstraintInfo =
                    constraintIDToCommodityTypeMap.get(reservationConstraintInfo.getConstraintId());
            if (updatedReservationConstraintInfo != null) {
                constraintInfoCollection.addReservationConstraintInfo(updatedReservationConstraintInfo);
                logger.info(logPrefix + "Added constraint: " + updatedReservationConstraintInfo.getConstraintId()
                        + " to reservation: " + reservation.getId());
            }
            else {
                throw new Exception("Constraint not found for ID: " + reservationConstraintInfo.getConstraintId());
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
                ReservationStatus reservationStatus = isReservationSuccessful(reservation);
                ReservationDTO.Reservation updatedReservation;
                // We should call the updateReservationBatch for all reservations including the ones
                // whose status didn't change..Because the provider could have changed.
                if (reservationStatus == ReservationStatus.RESERVED) {
                    // clear failure info.
                    updatedReservation = ReservationProtoUtil.clearReservationInstance(
                            reservation,
                            false, true).toBuilder()
                            .setStatus(ReservationStatus.RESERVED).build();
                } else if (reservationStatus == ReservationStatus.PLACEMENT_FAILED) {
                    // clear provider info
                    updatedReservation = ReservationProtoUtil.clearReservationInstance(
                            reservation,
                            true, false).toBuilder()
                            .setStatus(ReservationStatus.PLACEMENT_FAILED).build();
                } else {
                    // clear both failure and provider info
                    updatedReservation = ReservationProtoUtil.invalidateReservation(reservation);
                }
                updatedReservations.add(updatedReservation);
                // we should not broadcast the reservation change to the UI if the status does not change.
                // The provider change happens only during the main market and that need not be updated
                // in the UI. A refresh of screen will get the user the new providers.
                if (updatedReservation.getStatus() != reservation.getStatus()) {
                    logger.info(logPrefix + "Finished Reservation: " + updatedReservation.getName()
                            + " Current Status: " + updatedReservation.getStatus()
                            + " id: " + updatedReservation.getId());
                    statusUpdatedReservation.add(updatedReservation);
                    RESERVATION_STATUS_COUNTER.labels(updatedReservation.getStatus().toString()).increment();
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
                logger.error(logPrefix + "Reservation update failed" + e);
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
            logger.error(logPrefix + "Error sending reservation update notification for reservation changes.", e);
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
     * successful only if all the buyers have a provider. If failure info and provider info
     * is absent for all buyers then the reservation is invalid.
     * @param reservation the reservation of interest
     * @return the status of the reservation.
     */
    private ReservationStatus isReservationSuccessful(@Nonnull final ReservationDTO.Reservation reservation) {
        boolean isReservationValid = false;
        List<ReservationTemplate> reservationTemplates =
                reservation.getReservationTemplateCollection().getReservationTemplateList();
        for (ReservationTemplate reservationTemplate : reservationTemplates) {
            List<ReservationInstance> reservationInstances =
                    reservationTemplate.getReservationInstanceList();
            for (ReservationInstance reservationInstance : reservationInstances) {
                // if atleast one reservation instance has failure info then the
                // reservation is PLACEMENT_FAILED.
                if (!reservationInstance.getUnplacedReasonList().isEmpty()) {
                    return ReservationStatus.PLACEMENT_FAILED;
                }
                // if at least one reservation instance has placement info then the reservation is valid.
                if (reservationInstance.getPlacementInfoList() != null
                        && reservationInstance.getPlacementInfoList().size() != 0
                        && reservationInstance.getPlacementInfoList().stream()
                        .filter(a -> a.hasProviderId()).findAny().isPresent()) {
                    isReservationValid = true;
                }
            }
        }
        if (!isReservationValid) {
            return ReservationStatus.INVALID;
        } else {
            return ReservationStatus.RESERVED;
        }
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
     * Delete the reservation from the market Cache.
     * @param reservations the reservations to be deleted.
     */
    public void deleteReservationFromMarketCache(@Nonnull final Set<Reservation> reservations) {
        DeleteInitialPlacementBuyerRequest.Builder deleteInitialPlacementBuyerRequest =
                DeleteInitialPlacementBuyerRequest.newBuilder();
        for (Reservation reservation : reservations) {
            reservation.getReservationTemplateCollection().getReservationTemplateList()
                    .stream().forEach(template -> template.getReservationInstanceList()
                    .stream().forEach(buyer -> deleteInitialPlacementBuyerRequest
                            .addBuyerId(buyer.getEntityId())));
        }
        DeleteInitialPlacementBuyerResponse response = initialPlacementServiceBlockingStub
                .deleteInitialPlacementBuyer(deleteInitialPlacementBuyerRequest.build());
        if (!response.getResult()) {
            logger.error(logPrefix + "Deletion of Initial Placement Buyer failed");
        }
    }

    @Override
    public void onReservationDeleted(@Nonnull final Reservation reservation) {
        logger.info(logPrefix + " Deleted reservation: " + reservation.getName()
                + " id: " + reservation.getId());
        RESERVATION_STATUS_COUNTER.labels(DELETED_STATUS).increment();
        deleteReservationFromMarketCache(Collections.singleton(reservation));
        checkAndStartReservationPlan();
    }
}

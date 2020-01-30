package com.vmturbo.topology.processor.reservation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetAllReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateFutureReservationRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ReservationOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.template.TemplateConverterFactory;
import com.vmturbo.topology.processor.template.TemplatesNotFoundException;
import com.vmturbo.topology.processor.topology.TopologyEditorException;

/**
 * Responsible for convert Reservations to TopologyEntity and add them into live topology. And also
 * check if there are any Reservation should become active (start day is today or before and
 * status is FUTURE) and change its status and send request to update Reservation table.
 * <p>
 * Reservation entity will only buy provision commodity, for example, Virtual Machine reservation
 * entity only buy CpuProvision, MemProvision, StorageProvision commodity. And also the providers of
 * reservation entity will modify their commodity sold utilization based on reservation entity provision
 * value.
 */
public class ReservationManager {
    private static final Logger logger = LogManager.getLogger();

    // TODO: After OM-30576 is resolved, we can remove this epsilon check.
    // Because right now, in XL commodityDTO, it used double as data type, but in Market commodityDTO,
    // it use float as data type, it will cause commodity value precision loss between real time
    // topology with projected topology. The epsilon is 1.0e-7, because float number only have
    // about 7 decimal digits accuracy.
    private final double EPSILON = 1.0e-7;

    private final TemplateConverterFactory templateConverterFactory;

    private final ReservationServiceBlockingStub reservationService;

    public ReservationManager(@Nonnull final ReservationServiceBlockingStub reservationService,
                              @Nonnull final TemplateConverterFactory templateConverterFactory) {
        this.reservationService = Objects.requireNonNull(reservationService);
        this.templateConverterFactory = Objects.requireNonNull(templateConverterFactory);
    }

    /**
     * Convert all active and potential active Reservations to TopologyEntity and add them into live
     * topology.
     *
     * @param topology a Map contains live discovered topologyEntity.
     * @param planType the current planProjectType. Used to differentiate normal plan and
     *                 reservation plan
     * @param topologyType the current type. Used to differentiate realtime and plan
     * @return The number of reservation entities
     */
    public int applyReservation(@Nonnull final Map<Long, TopologyEntity.Builder> topology,
                                TopologyType topologyType,
                                PlanProjectType planType) {
        final GetAllReservationsRequest allReservationsRequest = GetAllReservationsRequest.newBuilder()
                .build();
        final Iterable<Reservation> allReservations = () ->
                reservationService.getAllReservations(allReservationsRequest);
        // Retrieve all active reservations.
        final Set<Reservation> reservedReservations =
                StreamSupport.stream(allReservations.spliterator(), false)
                        .filter(reservation -> reservation.getStatus() == ReservationStatus.RESERVED ||
                                reservation.getStatus() == ReservationStatus.PLACEMENT_FAILED)
                        .collect(Collectors.toSet());
        // Retrieve potential active reservations which start day is today or before and status is FUTURE.
        Set<Reservation> todayActiveReservations = new HashSet<>();
        if (topologyType == TopologyType.REALTIME) {
            final UpdateFutureReservationRequest request = UpdateFutureReservationRequest.newBuilder()
                    .build();
            reservationService.updateFutureReservation(request);
        }
        if (planType == PlanProjectType.RESERVATION_PLAN) {
            todayActiveReservations.addAll(StreamSupport.stream(allReservations.spliterator(), false)
                    .filter(reservation -> reservation.getStatus() == ReservationStatus.INPROGRESS)
                    .collect(Collectors.toSet()));
        }
        final List<TopologyEntity.Builder> reservationTopologyEntities = new ArrayList<>();

        final List<Reservation> updateReservations =
                handlePotentialActiveReservation(todayActiveReservations, reservationTopologyEntities, topology);
        handleReservedReservation(reservedReservations, reservationTopologyEntities, topology);
        // Update reservations which have just become active.
        if (!updateReservations.isEmpty()) {
            final UpdateReservationsRequest request = UpdateReservationsRequest.newBuilder()
                .addAllReservation(updateReservations)
                .build();
            reservationService.updateReservations(request);
        }

        //TODO: (OM-29676) update existing topologyEntity utilization based on placement information
        // and only buy provision commodities.
        int numAdded = 0;
        for (TopologyEntity.Builder reservedEntity : reservationTopologyEntities) {
            final TopologyEntity.Builder existingEntity =
                    topology.putIfAbsent(reservedEntity.getOid(), reservedEntity);
            if (existingEntity == null) {
                numAdded++;
            }
        }
        return numAdded;
    }

    /**
     * Convert reserved Reservations to topology entities.
     *
     * @param reservedReservations a set of {@link Reservation}.
     * @param reservationTopologyEntities a list of {@link TopologyEntity.Builder}.
     * @param topology a Map contains live discovered topologyEntity.
     */
    private void handleReservedReservation(
            @Nonnull final Set<Reservation> reservedReservations,
            @Nonnull final List<TopologyEntity.Builder> reservationTopologyEntities,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology) {
        for (Reservation reservation : reservedReservations) {
            final List<ReservationTemplate> reservationTemplates =
                    reservation.getReservationTemplateCollection()
                    .getReservationTemplateList();
            // give each reserved entity a ReservationOrigin
            Origin reservationOrigin = Origin.newBuilder()
                    .setReservationOrigin(ReservationOrigin.newBuilder()
                        .setReservationId(reservation.getId()))
                    .build();
            for (ReservationTemplate reservationTemplate : reservationTemplates) {
                List<TopologyEntity.Builder> entityBuilders = createTopologyEntities(reservationTemplate, topology);
                entityBuilders.forEach(builder -> {
                    builder.getEntityBuilder().setOrigin(reservationOrigin);
                    reservationTopologyEntities.add(builder);
                });
            }
        }
    }

    /**
     * Create a list of topology entities based on input {@link ReservationTemplate}.
     *
     * @param reservationTemplate {@link ReservationTemplate}.
     * @param topology a Map contains live discovered topologyEntity.
     * @return a list of topology entities.
     */
    private List<TopologyEntity.Builder> createTopologyEntities(
            @Nonnull ReservationTemplate reservationTemplate,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology) {
        // normally it should not happen, if it happens, will ignore this reservation template.
        if (reservationTemplate.getReservationInstanceCount() != reservationTemplate.getCount()) {
            logger.error("Count mismatch between instance count: " +
                    reservationTemplate.getReservationInstanceCount() + " with template count: " +
                    reservationTemplate.getCount());
            return Collections.emptyList();
        }
        final Map<Long, Long> templateCountMap =
                ImmutableMap.of(reservationTemplate.getTemplateId(),
                        reservationTemplate.getCount());
        //TODO: Handle the case that if templates are deleted, we should decide whether mark
        // Reservation inactive or delete reservations.
        try {
            final List<TopologyEntityDTO.Builder> topologyEntityDTOBuilder =
                    templateConverterFactory.generateReservationEntityFromTemplates(templateCountMap, topology)
                            .collect(Collectors.toList());
            final List<ReservationInstance> reservationInstances =
                    reservationTemplate.getReservationInstanceList();

            return convertTopologyEntityPairWithInstance(reservationInstances, topologyEntityDTOBuilder,
                    topology);
        } catch (TemplatesNotFoundException e) {
            logger.error("Can not find template: " + reservationTemplate.getTemplateId() +
            " and ignore the reservation templates.");
            return Collections.emptyList();
        }
    }

    private List<TopologyEntity.Builder> convertTopologyEntityPairWithInstance(
            @Nonnull final List<ReservationInstance> reservationInstances,
            @Nonnull final List<TopologyEntityDTO.Builder> topologyEntityDTOBuilder,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology) {
        // the size of reservationInstances and topologyEntityDTOBuilder has confirmed to be same.
        return IntStream.range(0, reservationInstances.size())
                .mapToObj(index -> modifyTopologyEntityWithInstance(reservationInstances.get(index),
                        topologyEntityDTOBuilder.get(index), topology))
                .collect(Collectors.toList());
    }

    /**
     * Update created topology entity builder based on input {@link ReservationInstance}, and change
     * topology entity oid to ReservationInstance's entity id in order to keep consistent.
     *
     * @param reservationInstance {@link ReservationInstance}.
     * @param topologyEntityBuilder {@link TopologyEntity.Builder}.
     * @param topology a Map contains live discovered topologyEntity.
     * @return a new updated {@link TopologyEntity.Builder}.
     */
    private TopologyEntity.Builder modifyTopologyEntityWithInstance(
            @Nonnull ReservationInstance reservationInstance,
            @Nonnull TopologyEntityDTO.Builder topologyEntityBuilder,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology) {
        topologyEntityBuilder.setOid(reservationInstance.getEntityId());
        topologyEntityBuilder.setDisplayName(reservationInstance.getName());
        // set suspendable to false in order to prevent Market from generating suspend action.
        disableSuspendAnalysisSetting(topologyEntityBuilder);
        final Set<PlacementInfo> placementInfos = reservationInstance.getPlacementInfoList().stream()
                .collect(Collectors.toSet());

        for (CommoditiesBoughtFromProvider.Builder commoditiesBoughtBuilder :
                topologyEntityBuilder.getCommoditiesBoughtFromProvidersBuilderList()) {
            placeProviderIdByEntityType(commoditiesBoughtBuilder, placementInfos, topology);
        }
        return TopologyEntity.newBuilder(topologyEntityBuilder);
    }

    /**
     * Add placement information's provider id to {@link CommoditiesBoughtFromProvider} based on
     * same provider entity type. And also will modify providers' commodity sold value utilization
     * based reservation entity commodity bought value.
     *
     * @param commoditiesBoughtBuilder {@link CommoditiesBoughtFromProvider.Builder}.
     * @param placementInfos a set of {@link PlacementInfo}.
     * @param topology a Map contains live discovered topologyEntity.
     * @return a new {@link CommoditiesBoughtFromProvider.Builder} contains provider id.
     */
    private CommoditiesBoughtFromProvider.Builder placeProviderIdByEntityType(
            @Nonnull CommoditiesBoughtFromProvider.Builder commoditiesBoughtBuilder,
            @Nonnull Set<PlacementInfo> placementInfos,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology) {
        placementInfos.stream()
                .filter(PlacementInfo::hasProviderType)
                .filter(PlacementInfo::hasProviderId)
                .filter(placementInfo ->
                        isCommodityBoughtGroupMatch(placementInfo, commoditiesBoughtBuilder))
                .findFirst()
                .ifPresent(placementInfoObj -> {
                    // if provider id is not available anymore, need to unplaced the commodity.
                    // It could happen when Market recommend to clone a Host and move Reserved VM to
                    // that cloned host, during next broadcast, the cloned host id is not available.
                    if (topology.containsKey(placementInfoObj.getProviderId())) {
                        commoditiesBoughtBuilder.setProviderId(placementInfoObj.getProviderId());
                        // modify provider entity utilization based on reservation entity bought
                        // commodity.
                        modifyProviderEntityCommodityBought(topology.get(placementInfoObj.getProviderId())
                                , commoditiesBoughtBuilder);
                        // remove this placementInfo from set, in order to avoid place same provider id to the
                        // following commodity bought group.
                        placementInfos.remove(placementInfoObj);
                    } else {
                        logger.warn("Provider id {} is not find for reservation",
                                placementInfoObj.getProviderId());
                    }
                });
        return commoditiesBoughtBuilder;
    }

    /**
     * Check if the new generated commodity bought group is same as {@link PlacementInfo} commodity
     * bought group. For example, input commodity bought group has provider entity is Storage and
     * have one StorageProvision commodity which used value is 100, and also in placementInfo, it's
     * provider entity type is Storage and have one StorageProvision commodity which used value is 100.
     * In this case two commodity bought group are matched.
     *
     * @param placementInfo {@link PlacementInfo}.
     * @param commoditiesBoughtBuilder {@link CommoditiesBoughtFromProvider.Builder}.
     * @return a boolean.
     */
    private boolean isCommodityBoughtGroupMatch(
            @Nonnull final PlacementInfo placementInfo,
            @Nonnull final CommoditiesBoughtFromProvider.Builder commoditiesBoughtBuilder) {
        if (placementInfo.getProviderType() != commoditiesBoughtBuilder.getProviderEntityType()) {
            return false;
        }
        // because reservation entity are created from template from scratch. Right now it doesn't
        // set commodity key and and doesn't contains same commodity type in one commodity bought group,
        // so it is ok to use commodity type as Map key.
        Map<Integer, CommodityBoughtDTO> projectedCommodityBoughtMap =
                placementInfo.getCommodityBoughtList().stream()
                        // filter out access commodity
                        .filter(commodityBoughtDTO -> !commodityBoughtDTO.getCommodityType().hasKey())
                        // filter out not used commodity
                        .filter(commodityBoughtDTO -> commodityBoughtDTO.getUsed() > 0.0)
                        .collect(Collectors.toMap(
                                commodityBought -> commodityBought.getCommodityType().getType(),
                                Function.identity()));
        return commoditiesBoughtBuilder.getCommodityBoughtList().stream()
                .filter(commodityBoughtDTO -> commodityBoughtDTO.getUsed() > 0.0)
                .allMatch(commodityBought ->
                        checkIfCommodityBoughtMatch(commodityBought, projectedCommodityBoughtMap));
    }

    /**
     * Check if there is any commodity bought in parameter map which matches with the input commodity
     * bought. And it only compare used and active value. Because for reservation entity, it doesn't
     * care peak value.
     *
     * @param commodityBought {@link CommodityBoughtDTO}.
     * @param projectedCommodityBoughtMap a Map key is commodity type, value is {@link CommodityBoughtDTO}.
     * @return a boolean.
     */
    private boolean checkIfCommodityBoughtMatch(
            @Nonnull final CommodityBoughtDTO commodityBought,
            @Nonnull final Map<Integer, CommodityBoughtDTO> projectedCommodityBoughtMap) {
        final CommodityBoughtDTO projectedCommodityBought =
                projectedCommodityBoughtMap.get(commodityBought.getCommodityType().getType());
        // only compare used and active here, because for peak value, market will set it to used's value.
        // the delta is used value multiply EPSILON, because the delta is not constant and it is based
        // on original double when converting double to float and convert back to double.
        return projectedCommodityBought != null && projectedCommodityBought.getActive() &&
                (Double.compare(Math.abs(projectedCommodityBought.getUsed() - commodityBought.getUsed()),
                        commodityBought.getUsed() * EPSILON) <= 0);
    }

    /**
     * Modify provider entity commodities sold based on created reservation entity's commodities bought.
     * It will only modify provider entity provision commodity sold "used" value, because reservation entity
     * only buy provision commodity. For example: a Reserved VM buy MemProvision commodity 100 value
     * from Host 1, this method will increase Host 1 MemProvision commodity sold "used" value by 100.
     *
     * @param providerEntity  provider entity of reservation instance.
     * @param commoditiesBoughtBuilder {@link CommoditiesBoughtFromProvider.Builder}.
     */
    @VisibleForTesting
    void modifyProviderEntityCommodityBought(
            @Nonnull final TopologyEntity.Builder providerEntity,
            @Nonnull CommoditiesBoughtFromProvider.Builder commoditiesBoughtBuilder) {
        // map only contains provision commodity and key is commodity type, value is used value of
        // provision commodity.
        final Map<Integer, Double> commodityBoughtTypeToUsed =
                commoditiesBoughtBuilder.getCommodityBoughtList().stream()
                        .filter(commodityBoughtDTO -> commodityBoughtDTO.getUsed() > 0)
                        .collect(Collectors.toMap(
                                commodityBought -> commodityBought.getCommodityType().getType(),
                                CommodityBoughtDTO::getUsed));
        final TopologyEntityDTO.Builder providerBuilder = providerEntity.getEntityBuilder();
        final Set<CommoditySoldDTO.Builder> commoditySoldBuilderSet =
                providerBuilder.getCommoditySoldListBuilderList().stream()
                        .filter(commoditySoldBuilder ->
                                commodityBoughtTypeToUsed.containsKey(
                                        commoditySoldBuilder.getCommodityType().getType()))
                        .collect(Collectors.toSet());
        // if any commodity type is missing in provider commodities sold list.
        if (commoditySoldBuilderSet.size() != commodityBoughtTypeToUsed.size()) {
            final Set<String> missingCommodityType = getMissingCommodityType(commoditySoldBuilderSet,
                    commodityBoughtTypeToUsed);
            logger.error("Provider {} is not selling commodities {} bought by reservation",
                    providerEntity.getOid(),
                    missingCommodityType.stream().collect(Collectors.joining(",")));
        } else {
            commoditySoldBuilderSet.forEach(commoditySoldBuilder -> {
                // adding the amount used by the reserved entity to the sold amount of the provider
                final double newUsed = commoditySoldBuilder.getUsed() +
                        commodityBoughtTypeToUsed.get(
                                commoditySoldBuilder.getCommodityType().getType());
                commoditySoldBuilder.setUsed(newUsed);
            });
        }
    }

    private Set<String> getMissingCommodityType(
            @Nonnull Set<CommoditySoldDTO.Builder> commoditySoldBuilderSet,
            @Nonnull final Map<Integer, Double> commodityBoughtTypeToUsed) {
        final Set<Integer> commoditySoldTypes = commoditySoldBuilderSet.stream()
                .map(commoditySoldDTOBuilder -> commoditySoldDTOBuilder.getCommodityType().getType())
                .collect(Collectors.toSet());
        final Set<Integer> missedCommodityTypes = commodityBoughtTypeToUsed.keySet().stream()
                .filter(commodityType -> !commoditySoldTypes.contains(commodityType))
                .collect(Collectors.toSet());
        return missedCommodityTypes.stream()
                .map(commodityType -> CommodityDTO.CommodityType.forNumber(commodityType))
                .map(commodityType -> commodityType.toString())
                .collect(Collectors.toSet());
    }

    /**
     * Create topology entities for those just become active Reservations. And also send update request
     * to keep latest Reservation information which contains created topology entity oids.
     *
     * @param todayActiveReservations a Set of {@link Reservation}.
     * @param reservationTopologyEntities a list of {@link TopologyEntity.Builder}.
     * @param topology The entities in the topology, arranged by ID.
     * @return a list of {@link Reservation} need to update.
     */
    @VisibleForTesting
    List<Reservation> handlePotentialActiveReservation(
            @Nonnull final Set<Reservation> todayActiveReservations,
            @Nonnull final List<TopologyEntity.Builder> reservationTopologyEntities,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology) {
        final List<Reservation> updateReservationsWithEntityOid = new ArrayList<>();
        // handle reservations which have just become active.
        final Set<Reservation.Builder> reservationsBuilder = todayActiveReservations.stream()
                .map(Reservation::toBuilder)
                .collect(Collectors.toSet());
        for (Reservation.Builder reservationBuilder : reservationsBuilder) {
            long instanceCount = 0;
            for (ReservationTemplate.Builder reservationTemplate : reservationBuilder
                    .getReservationTemplateCollectionBuilder()
                    .getReservationTemplateBuilderList()) {
                createNewReservationTemplate(reservationTopologyEntities, reservationTemplate,
                    topology, reservationBuilder.getName(), instanceCount);
                instanceCount += reservationTemplate.getCount();
            }
            updateReservationsWithEntityOid.add(reservationBuilder
                    .setStatus(ReservationStatus.INPROGRESS)
                    .build());
        }
        return updateReservationsWithEntityOid;
    }

    /**
     * Create new Reservation templates for those reservations which just be active (start day is today
     * or before and status is FUTURE).
     *
     * @param reservationTopologyEntities a list of {@link TopologyEntity.Builder} contains all
     *                                    reservation entities created from templates.
     * @param reservationTemplate {@link ReservationTemplate.Builder}.
     * @param topology The entities in the topology, arranged by ID.
     * @param reservationName name of reservation.
     * @param instanceCount cont index of reservation, used for create name for reservation instance.
     * @return new crated {@link ReservationTemplate}.
     */
    private void createNewReservationTemplate(
            @Nonnull final List<TopologyEntity.Builder> reservationTopologyEntities,
            @Nonnull final ReservationTemplate.Builder reservationTemplate,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology,
            @Nonnull final String reservationName,
            long instanceCount) {
        final Map<Long, Long> templateCountMap =
                ImmutableMap.of(reservationTemplate.getTemplateId(),
                        reservationTemplate.getCount());
        //TODO: Handle the case that if templates are deleted, we should decide whether mark
        // Reservation inactive or delete reservations.
        try {
            final List<TopologyEntityDTO.Builder> createdTopologyEntityDTO =
                    templateConverterFactory.generateReservationEntityFromTemplates(templateCountMap, topology)
                            .collect(Collectors.toList());

            final List<TopologyEntityDTO.Builder> updatedTopologyEntityDTO = new ArrayList<>();
            for (TopologyEntityDTO.Builder entityBuilder : createdTopologyEntityDTO) {
                final TopologyEntityDTO.Builder newEntityBuilder =
                        entityBuilder.setDisplayName(reservationName + "_" + instanceCount);
                // set suspendable to false in order to prevent Market from generating
                // suspend action.
                disableSuspendAnalysisSetting(newEntityBuilder);

                instanceCount++;
                updatedTopologyEntityDTO.add(newEntityBuilder);
            }
            // Add updated topology entity into reservation topology entities list.
            updatedTopologyEntityDTO.stream()
                    .map(TopologyEntity::newBuilder)
                    .forEach(reservationTopologyEntities::add);

            final List<ReservationInstance> createdReservationInstances =
                    updatedTopologyEntityDTO.stream()
                            .map(entityBuilder -> ReservationInstance.newBuilder()
                                    .setEntityId(entityBuilder.getOid())
                                    .setName(entityBuilder.getDisplayName())
                                    .build())
                            .collect(Collectors.toList());

             reservationTemplate
                    .clearReservationInstance()
                    .addAllReservationInstance(createdReservationInstances)
                    .build();
        } catch (TopologyEditorException e) {
            logger.error("Can not find template: " + reservationTemplate.getTemplateId() +
                    " and ignore the reservation templates.");
        }
    }

    /**
     * Only disable suspend for the input entity, and it will keep other analysis setting not changed.
     *
     * @param topologyEntityBuilder {@link TopologyEntity.Builder} need to disable suspend.
     */
    private void disableSuspendAnalysisSetting(@Nonnull TopologyEntityDTO.Builder topologyEntityBuilder) {
        if (topologyEntityBuilder.hasAnalysisSettings()) {
            topologyEntityBuilder.getAnalysisSettingsBuilder().setSuspendable(false);
        } else {
            topologyEntityBuilder.setAnalysisSettings(AnalysisSettings.newBuilder()
                    .setSuspendable(false));
        }
    }
}
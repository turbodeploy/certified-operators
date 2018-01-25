package com.vmturbo.topology.processor.reservation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.plan.ReservationDTO.GetAllReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.template.TemplateConverterFactory;
import com.vmturbo.topology.processor.template.TemplatesNotFoundException;
import com.vmturbo.topology.processor.topology.TopologyEditorException;

/**
 * Responsible for convert Reservations to TopologyEntity and add them into live topology. And also
 * check if there are any Reservation should become active (start day is today or before and
 * status is FUTURE) and change its status and send request to update Reservation table.
 */
public class ReservationManager {
    private static final Logger logger = LogManager.getLogger();

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
     */
    public void applyReservation(@Nonnull final Map<Long, TopologyEntity.Builder> topology) {
        final GetAllReservationsRequest allReservationsRequest = GetAllReservationsRequest.newBuilder()
                .build();
        final Iterable<Reservation> allReservations = () ->
                reservationService.getAllReservations(allReservationsRequest);
        // Retrieve all active reservations.
        final Set<Reservation> reservedReservations =
                StreamSupport.stream(allReservations.spliterator(), false)
                        .filter(reservation -> reservation.getStatus() == ReservationStatus.RESERVED)
                        .collect(Collectors.toSet());
        // Retrieve potential active reservations which start day is today or before and status is FUTURE.
        final Set<Reservation> todayActiveReservations =
                StreamSupport.stream(allReservations.spliterator(), false)
                        .filter(reservation -> reservation.getStatus() == ReservationStatus.FUTURE)
                        .filter(this::isReservationActiveNow)
                        .collect(Collectors.toSet());
        final List<TopologyEntity.Builder> reservationTopologyEntities = new ArrayList<>();

        final List<Reservation> updateReservations =
                handlePotentialActiveReservation(todayActiveReservations, reservationTopologyEntities);
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
        reservationTopologyEntities.stream()
                .forEach(topologyEntity -> topology.putIfAbsent(topologyEntity.getOid(),
                        topologyEntity));
    }

    /**
     * Check if reservation start day is today or before.
     *
     * @param reservation {@link Reservation}.
     * @return a Boolean.
     */
    @VisibleForTesting
    boolean isReservationActiveNow(@Nonnull final Reservation reservation) {
        final LocalDate today = LocalDate.now(DateTimeZone.UTC);
        final Reservation.Date startDate = reservation.getStartDate();
        final LocalDate reservationDate = new LocalDate(startDate.getYear(), startDate.getMonth(),
                startDate.getDay());
        return reservationDate.isEqual(today) || reservationDate.isBefore(today);
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
            for (ReservationTemplate reservationTemplate : reservationTemplates) {
                reservationTopologyEntities.addAll(createTopologyEntities(reservationTemplate, topology));
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
                    templateConverterFactory.generateTopologyEntityFromTemplates(templateCountMap,
                            ArrayListMultimap.create())
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
        // TODO: update topologyEntityBuilder from Project topology entity in order to keep correct
        // commodity type order. Current approach will not handle templates with multiple same
        // type fields, such as VM template with a list of Storage.
        topologyEntityBuilder.setOid(reservationInstance.getEntityId());
        topologyEntityBuilder.setDisplayName(reservationInstance.getName());
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
     * same provider entity type. Note that, it will have problem for templates which have a list of
     * same provider entity types.
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
        final int providerEntityType = commoditiesBoughtBuilder.getProviderEntityType();
        placementInfos.stream()
                .filter(PlacementInfo::hasProviderType)
                .filter(PlacementInfo::hasProviderId)
                .filter(placement -> placement.getProviderType() == providerEntityType)
                .findFirst()
                .ifPresent(placementInfoObj -> {
                    // if provider id is not available anymore, need to unplaced the commodity.
                    if (topology.containsKey(placementInfoObj.getProviderId())) {
                        commoditiesBoughtBuilder.setProviderId(placementInfoObj.getProviderId());
                        // remove this placementInfo from set, in order to avoid place same provider id to the
                        // following commodity bought group.
                        placementInfos.remove(placementInfoObj);
                    }
                });
        return commoditiesBoughtBuilder;
    }

    /**
     * Create topology entities for those just become active Reservations. And also send update request
     * to keep latest Reservation information which contains created topology entity oids.
     *
     * @param todayActiveReservations a Set of {@link Reservation}.
     * @param reservationTopologyEntities a list of {@link TopologyEntity.Builder}.
     * @return a list of {@link Reservation} need to update.
     */
    @VisibleForTesting
    List<Reservation> handlePotentialActiveReservation(
            @Nonnull final Set<Reservation> todayActiveReservations,
            @Nonnull final List<TopologyEntity.Builder> reservationTopologyEntities) {
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
                        reservationBuilder.getName(), instanceCount);
                instanceCount += reservationTemplate.getCount();
            }
            updateReservationsWithEntityOid.add(reservationBuilder
                    .setStatus(ReservationStatus.RESERVED)
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
     * @param reservationName name of reservation.
     * @param instanceCount cont index of reservation, used for create name for reservation instance.
     * @return new crated {@link ReservationTemplate}.
     */
    private void createNewReservationTemplate(
            @Nonnull final List<TopologyEntity.Builder> reservationTopologyEntities,
            @Nonnull final ReservationTemplate.Builder reservationTemplate,
            @Nonnull final String reservationName,
            long instanceCount) {
        final Map<Long, Long> templateCountMap =
                ImmutableMap.of(reservationTemplate.getTemplateId(),
                        reservationTemplate.getCount());
        //TODO: Handle the case that if templates are deleted, we should decide whether mark
        // Reservation inactive or delete reservations.
        try {
            final List<TopologyEntityDTO.Builder> createdTopologyEntityDTO =
                    templateConverterFactory.generateTopologyEntityFromTemplates(templateCountMap,
                            ArrayListMultimap.create())
                            .collect(Collectors.toList());

            final List<TopologyEntityDTO.Builder> updatedTopologyEntityDTO = new ArrayList<>();
            for (TopologyEntityDTO.Builder entityBuilder : createdTopologyEntityDTO) {
                final TopologyEntityDTO.Builder newEntityBuilder =
                        entityBuilder.setDisplayName(reservationName + "_" + instanceCount);
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
}
package com.vmturbo.plan.orchestrator.reservation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.client.RemoteIteratorDrain;
import com.vmturbo.plan.orchestrator.market.ProjectedTopologyProcessor;

/**
 * Update reservation placement information after receive notification from Repository. It will call
 * Repository to get latest project topology entities of reservation entities, and find out the
 * latest placement information.
 */
public class ReservationPlacementHandler implements ProjectedTopologyProcessor {
    private final Logger logger = LogManager.getLogger();

    private final ReservationManager reservationManager;

    private final RepositoryServiceBlockingStub repositoryService;

    public ReservationManager getReservationManager() {
        return reservationManager;
    }

    /**
     * constuctor for ReservationPlacementHandler.
     * @param reservationManager reservation manager.
     * @param repositoryService reservation service.
     */
    public ReservationPlacementHandler(@Nonnull final ReservationManager reservationManager,
                                       @Nonnull final RepositoryServiceBlockingStub repositoryService) {
        this.reservationManager = Objects.requireNonNull(reservationManager);
        this.repositoryService = Objects.requireNonNull(repositoryService);
    }

    /**
     * Get latest projected topology entities of reservation entities based on input topologyId and
     * contextId. Update reservation entities' placement information based on latest projected topology
     * entities.
     *
     * @param contextId context id of topology.
     * @param topologyId id of topology.
     */
    public void updateReservationsFromLiveTopology(final long contextId, final long topologyId) {
        // Get all RESERVED reservations, only RESERVED reservations have entities.
        Set<Reservation> reservationSet = getReservationSet(false);
        // if no reservations, don't bother updating.
        if (reservationSet.size() == 0) {
            return;
        }

        // Get projected topology entities of reservation entities.
        List<TopologyEntityDTO> reservationEntities =
            retrieveReservationEntities(contextId, topologyId, reservationSet);
        final List<TopologyEntityDTO> providerEntities =
            retrieveProviderEntities(contextId, topologyId, reservationEntities);
        doUpdate(reservationSet, reservationEntities, providerEntities);
    }

    private Set<Reservation> getReservationSet(boolean isReservationPlan) {
        // Get all RESERVED reservations, only RESERVED reservations have entities.
        final Set<Reservation> reservationSet;
        // We should update the inprogress reservation only in reservationPlan and
        // we should update the reserved and placement_failed only in realtime.
        // Note that updateReservations is only called for reservationPlan and real-time.
        if (isReservationPlan) {
            reservationSet = reservationManager.getReservationDao().getAllReservations().stream()
                .filter(res -> res.getStatus() == ReservationStatus.INPROGRESS)
                .collect(Collectors.toSet());
        } else {
            reservationSet = reservationManager.getReservationDao().getAllReservations().stream()
                .filter(res -> res.getStatus() == ReservationStatus.RESERVED ||
                    res.getStatus() == ReservationStatus.PLACEMENT_FAILED)
                .collect(Collectors.toSet());
        }
        return reservationSet;
    }

    private void doUpdate(Set<Reservation> reservationSet,
                          List<TopologyEntityDTO> reservationEntities,
                          List<TopologyEntityDTO> providerEntities) {
        if (reservationSet.size() == 0) {
            return;
        }

        final Map<Long, TopologyEntityDTO> entityIdToEntityMap = reservationEntities.stream()
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
        final Map<Long, Integer> providerIdToEntityType = providerEntities.stream()
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, TopologyEntityDTO::getEntityType));
        final Set<Reservation> updatedReservation = reservationSet.stream()
                .map(Reservation::toBuilder)
                .map(reservation -> updateReservationPlacement(reservation, entityIdToEntityMap,
                        providerIdToEntityType))
                .collect(Collectors.toSet());
        reservationManager.updateReservationResult(updatedReservation);

        reservationManager.checkAndStartReservationPlan();
    }

    private Set<Long> extractReservationEntityIds(@Nonnull final Set<Reservation> reservations) {
        return reservations.stream()
            .map(Reservation::getReservationTemplateCollection)
            .map(ReservationTemplateCollection::getReservationTemplateList)
            .flatMap(List::stream)
            .map(ReservationTemplate::getReservationInstanceList)
            .flatMap(List::stream)
            .map(ReservationInstance::getEntityId)
            .collect(Collectors.toSet());
    }

    /**
     * Get project topology entities of reservations.
     *
     * @param contextId context id of topology.
     * @param topologyId id of topology.
     * @param reservations a list of {@link Reservation}.
     * @return a list of {@link TopologyEntityDTO}.
     */
    private List<TopologyEntityDTO> retrieveReservationEntities(
            final long contextId,
            final long topologyId,
            @Nonnull final Set<Reservation> reservations) {
        final Set<Long> reservationEntityIds = extractReservationEntityIds(reservations);

        return retrieveTopologyEntities(RetrieveTopologyEntitiesRequest.newBuilder()
            .setTopologyContextId(contextId)
            .setTopologyId(topologyId)
            .addAllEntityOids(reservationEntityIds)
            .setTopologyType(TopologyType.PROJECTED));
    }

    private Stream<Long> extractProviders(TopologyEntityDTO entity) {
        return entity.getCommoditiesBoughtFromProvidersList().stream()
            .filter(CommoditiesBoughtFromProvider::hasProviderId)
            .map(CommoditiesBoughtFromProvider::getProviderId);
    }
    /**
     * Get projected topology entities of providers of Reservations. Because projected topology entities
     * not have projected entity type in {@link CommoditiesBoughtFromProvider}. It needs to request
     * another call to get entity types of those providers.
     *
     * @param contextId context id of topology.
     * @param topologyId id of topology.
     * @param reservationEntities a list of {@link TopologyEntityDTO}.
     * @return a list of {@link TopologyEntityDTO}.
     */
    private List<TopologyEntityDTO> retrieveProviderEntities(
            final long contextId,
            final long topologyId,
            @Nonnull List<TopologyEntityDTO> reservationEntities) {
        // Get provider ids of reservation entities.
        final Set<Long> providerIds = reservationEntities.stream()
                .flatMap(this::extractProviders)
                .collect(Collectors.toSet());


        return retrieveTopologyEntities(RetrieveTopologyEntitiesRequest.newBuilder()
            .setTopologyContextId(contextId)
            .setTopologyId(topologyId)
            .addAllEntityOids(providerIds)
            .setTopologyType(TopologyType.PROJECTED));
    }

    /**
     * Send request to Repository to retrieve project topology entities. Note that, those project
     * topology entities only contains partial fields of TopologyEntityDTO, it missing all Market
     * related settting fields.
     *
     * @param request {@link RetrieveTopologyEntitiesRequest}.
     * @return a list of {@link TopologyEntityDTO}.
     */
    private List<TopologyEntityDTO> retrieveTopologyEntities(
            @Nonnull final RetrieveTopologyEntitiesRequest.Builder request) {
        return RepositoryDTOUtil.topologyEntityStream(repositoryService.retrieveTopologyEntities(
                request.setReturnType(Type.FULL)
                    .build()))
            .map(PartialEntity::getFullEntity)
            .collect(Collectors.toList());
    }

    private Reservation updateReservationPlacement(
            @Nonnull final Reservation.Builder reservation,
            @Nonnull final Map<Long, TopologyEntityDTO> entityIdToEntityMap,
            @Nonnull final Map<Long, Integer> providerIdToEntityType) {
        reservation.getReservationTemplateCollectionBuilder()
                .getReservationTemplateBuilderList()
                .forEach(reservationTemplate ->
                        updateReservationTemplate(reservationTemplate, entityIdToEntityMap,
                                providerIdToEntityType));
        return reservation.build();
    }

    /**
     * Update reservation placement information for each type of template.
     *
     * @param reservationTemplate {@link ReservationTemplate} contains placement information for
     *                            each type templates.
     * @param entityIdToEntityMap a Map which key is reservation entity id, value is
     *                            {@link TopologyEntityDTO}
     * @param providerIdToEntityType a Map which key is provider entity id, value is provider entity
     *                               type.
     */
    private void updateReservationTemplate(
            @Nonnull final ReservationTemplate.Builder reservationTemplate,
            @Nonnull final Map<Long, TopologyEntityDTO> entityIdToEntityMap,
            @Nonnull final Map<Long, Integer> providerIdToEntityType) {
        reservationTemplate.getReservationInstanceBuilderList()
                .forEach(reservationInstance -> updateReservationInstance(reservationInstance,
                        entityIdToEntityMap, providerIdToEntityType));
    }

    /**
     * Update placement information of Reservation instances based on latest projected topology.
     * It will create a list of new {@link PlacementInfo} based on input reservation entity map and
     * provider entity map. The new placementInfo will replace old one in {@link ReservationInstance}.
     *
     * @param reservationInstance {@link ReservationInstance.Builder} contains entity id.
     * @param entityIdToEntityMap a Map key is reservation entity id, value is {@link TopologyEntityDTO}.
     * @param providerIdToEntityType a Map key is provider entity id, value is provider entity type.
     * @return
     */
    private void updateReservationInstance(
            @Nonnull final ReservationInstance.Builder reservationInstance,
            @Nonnull final Map<Long, TopologyEntityDTO> entityIdToEntityMap,
            @Nonnull final Map<Long, Integer> providerIdToEntityType) {
        final long entityId = reservationInstance.getEntityId();
        final List<PlacementInfo> placementInfos = new ArrayList<>();
        if (!entityIdToEntityMap.containsKey(entityId)) {
            logger.error("Can not found project topology entity for id: " + entityId);
            return ;
        }
        final TopologyEntityDTO topologyEntityDTO = entityIdToEntityMap.get(entityId);
        for (CommoditiesBoughtFromProvider commoditiesBoughtFromProvider :
                topologyEntityDTO.getCommoditiesBoughtFromProvidersList()) {
            final List<CommodityBoughtDTO> commodityBoughtDTOs =
                    commoditiesBoughtFromProvider.getCommodityBoughtList();
            PlacementInfo.Builder placementInfoBuilder = PlacementInfo.newBuilder()
                    .addAllCommodityBought(commodityBoughtDTOs);
            if (commoditiesBoughtFromProvider.hasProviderId()) {
                final long providerId = commoditiesBoughtFromProvider.getProviderId();
                placementInfoBuilder.setProviderId(providerId);
                if (!providerIdToEntityType.containsKey(providerId)) {
                    logger.error("Can not find project topology entity for id: " + providerId);
                } else {
                    placementInfoBuilder.setProviderType(providerIdToEntityType.get(providerId));
                }
            }
            placementInfos.add(placementInfoBuilder.build());
        }
        reservationInstance.clearPlacementInfo().addAllPlacementInfo(placementInfos);
    }

    @Override
    public boolean appliesTo(@Nonnull final TopologyInfo sourceTopologyInfo) {
        return sourceTopologyInfo.getPlanInfo().getPlanProjectType() == PlanProjectType.RESERVATION_PLAN;
    }

    @Override
    public void handleProjectedTopology(final long projectedTopologyId,
                                        @Nonnull final TopologyInfo sourceTopologyInfo,
                                        @Nonnull final RemoteIterator<ProjectedTopologyEntity> iterator)
            throws InterruptedException, TimeoutException, CommunicationException {
        Set<Reservation> reservations = getReservationSet(true);
        if (reservations.isEmpty()) {
            RemoteIteratorDrain.drainIterator(iterator,
                TopologyDTOUtil.getProjectedTopologyLabel(sourceTopologyInfo), false);
        } else {
            try {
                Set<Long> reservationEntityIds = extractReservationEntityIds(reservations);
                // We end up collecting all entities in memory, which is not ideal, but may be
                // acceptable in the reservation case since we greatly trim the topology before
                // broadcast.
                Map<Long, TopologyEntityDTO> entitiesById = new HashMap<>();
                while (iterator.hasNext()) {
                    iterator.nextChunk().forEach(e -> entitiesById.put(e.getEntity().getOid(), e.getEntity()));
                }
                List<TopologyEntityDTO> reservationEntities = reservationEntityIds.stream()
                    .map(entitiesById::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
                List<TopologyEntityDTO> providerEntities = reservationEntities.stream()
                    .flatMap(this::extractProviders)
                    .map(entitiesById::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
                doUpdate(reservations, reservationEntities, providerEntities);
            } finally {
                RemoteIteratorDrain.drainIterator(iterator,
                    TopologyDTOUtil.getProjectedTopologyLabel(sourceTopologyInfo), true);
            }
        }
    }
}

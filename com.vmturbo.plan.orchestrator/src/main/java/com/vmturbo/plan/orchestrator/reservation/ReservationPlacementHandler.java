package com.vmturbo.plan.orchestrator.reservation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesResponse;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Update reservation placement information after receive notification from Repository. It will call
 * Repository to get latest project topology entities of reservation entities, and find out the
 * latest placement information.
 */
public class ReservationPlacementHandler {
    private final Logger logger = LogManager.getLogger();

    private final ReservationDao reservationDao;

    private final RepositoryServiceBlockingStub repositoryService;

    public ReservationPlacementHandler(@Nonnull final ReservationDao reservationDao,
                                       @Nonnull final RepositoryServiceBlockingStub repositoryService) {
        this.reservationDao = Objects.requireNonNull(reservationDao);
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
    public void updateReservations(final long contextId, final long topologyId) {
        // Get all RESERVED reservations, only RESERVED reservations have entities.
        final Set<Reservation> reservationSet =
                reservationDao.getReservationsByStatus(ReservationStatus.RESERVED);

        // Get projected topology entities of reservation entities.
        final List<TopologyEntityDTO> reservationEntities =
                retrieveReservationEntities(contextId, topologyId, reservationSet);

        final List<TopologyEntityDTO> providerEntities =
                retrieveProviderEntities(contextId, topologyId, reservationEntities);

        final Map<Long, TopologyEntityDTO> entityIdToEntityMap = reservationEntities.stream()
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
        final Map<Long, Integer> providerIdToEntityType = providerEntities.stream()
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, TopologyEntityDTO::getEntityType));
        final Set<Reservation> updatedReservation = reservationSet.stream()
                .map(Reservation::toBuilder)
                .map(reservation -> updateReservationPlacement(reservation, entityIdToEntityMap,
                        providerIdToEntityType))
                .collect(Collectors.toSet());
        try {
            reservationDao.updateReservationBatch(updatedReservation);
        } catch (NoSuchObjectException e) {
            logger.error("Reservation update failed, contextId: " + contextId + " topologyId: "
            + topologyId + " error message: " + e);
        }
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
        final Set<Long> reservationEntityIds = reservations.stream()
                .map(Reservation::getReservationTemplateCollection)
                .map(ReservationTemplateCollection::getReservationTemplateList)
                .flatMap(List::stream)
                .map(ReservationTemplate::getReservationInstanceList)
                .flatMap(List::stream)
                .map(ReservationInstance::getEntityId)
                .collect(Collectors.toSet());

        final RetrieveTopologyEntitiesRequest retrieveReservationEntityRequest =
                RetrieveTopologyEntitiesRequest.newBuilder()
                        .setTopologyContextId(contextId)
                        .setTopologyId(topologyId)
                        .addAllEntityOids(reservationEntityIds)
                        .setTopologyType(TopologyType.PROJECTED)
                        .build();

        return retrieveTopologyEntities(retrieveReservationEntityRequest);
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
                .map(TopologyEntityDTO::getCommoditiesBoughtFromProvidersList)
                .flatMap(List::stream)
                .filter(CommoditiesBoughtFromProvider::hasProviderId)
                .map(CommoditiesBoughtFromProvider::getProviderId)
                .collect(Collectors.toSet());
        final RetrieveTopologyEntitiesRequest retrieveProviderEntityRequest =
                RetrieveTopologyEntitiesRequest.newBuilder()
                        .setTopologyContextId(contextId)
                        .setTopologyId(topologyId)
                        .addAllEntityOids(providerIds)
                        .setTopologyType(TopologyType.PROJECTED)
                        .build();

        return retrieveTopologyEntities(retrieveProviderEntityRequest);
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
            @Nonnull final RetrieveTopologyEntitiesRequest request) {
        final RetrieveTopologyEntitiesResponse response =
                repositoryService.retrieveTopologyEntities(request);
        return response.getEntitiesList();
    }

    private Reservation updateReservationPlacement(
            @Nonnull final Reservation.Builder reservation,
            @Nonnull final Map<Long, TopologyEntityDTO> entityIdToEntityMap,
            @Nonnull final Map<Long, Integer> providerIdToEntityType) {
        reservation.getReservationTemplateCollectionBuilder()
                .getReservationTemplateBuilderList()
                .stream()
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
        reservationTemplate.getReservationInstanceBuilderList().stream()
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
}

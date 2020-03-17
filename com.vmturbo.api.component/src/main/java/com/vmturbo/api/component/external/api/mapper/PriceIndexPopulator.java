package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.StatusRuntimeException;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStats;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse.TypeCase;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RequestDetails;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.common.stats.StatsUtils;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Class for fetching and setting the priceIndex on the service entity.
 */
public class PriceIndexPopulator {

    private static final Logger logger = LogManager.getLogger();

    private static final String PRICE_INDEX_COMMODITY = "priceIndex";

    private final StatsHistoryServiceBlockingStub statsHistoryRpc;

    private final RepositoryServiceBlockingStub repositoryRpcService;

    public PriceIndexPopulator(@Nonnull final StatsHistoryServiceBlockingStub statsHistoryRpc,
            @Nonnull final RepositoryServiceBlockingStub repositoryRpcService) {
        this.statsHistoryRpc = statsHistoryRpc;
        this.repositoryRpcService = repositoryRpcService;
    }

    /**
     * Populate priceIndex for real time entities.
     *
     * @param entities list of real time entities
     */
    public void populateRealTimeEntities(
            @Nonnull final List<ServiceEntityApiDTO> entities) {
        final Map<Long, Float> priceIndexEntityId = fetchPriceIndexForRealTimeEntities(entities);
        populatePriceIndex(entities, priceIndexEntityId);
    }

    /**
     * Populate priceIndex for plan entities.
     *
     * @param planTopologyId id of the plan topology
     * @param entities list of plan entities
     */
    public void populatePlanEntities(final long planTopologyId,
            @Nonnull final List<ServiceEntityApiDTO> entities) {
        final Map<Long, Float> priceIndexEntityId =
                fetchPriceIndexForPlanEntities(planTopologyId);
        populatePriceIndex(entities, priceIndexEntityId);
    }

    /**
     * Populate priceIndex for the given list of {@link ServiceEntityApiDTO}s, given the mapping
     * from entity oid to priceIndex value.
     *
     * @param entities list of {@link ServiceEntityApiDTO}s
     * @param priceIndexEntityId mapping from entity oid to priceIndex value
     */
    private void populatePriceIndex(@Nonnull final List<ServiceEntityApiDTO> entities,
            @Nonnull final Map<Long, Float> priceIndexEntityId) {
        entities.stream()
                // it's expected that these entities do not have priceIndex.
                .filter(entity -> !StatsUtils.SDK_ENTITY_TYPES_WITHOUT_SAVED_PRICES.contains(
                        ApiEntityType.fromStringToSdkType(entity.getClassName())))
                .forEach(entity -> {
                    Float priceIndex = priceIndexEntityId.get(Long.valueOf(entity.getUuid()));
                    if (priceIndex == null) {
                        logger.warn("Price index not found for entity {id: {}, name: {}}",
                                entity.getUuid(), entity.getDisplayName());
                    } else {
                        entity.setPriceIndex(priceIndex);
                    }
                });
    }

    /**
     * Fetch the priceIndex stats for the given list of entities. If an entity doesn't have
     * priceIndex or error when fetching stats in history component, it will not be included in the
     * result.
     *
     * @param serviceEntityApiDTOs the list of entities to get priceIndex stats for
     * @return mapping from entity oid to priceIndex value
     */
    private Map<Long, Float> fetchPriceIndexForRealTimeEntities(
            @Nonnull Collection<ServiceEntityApiDTO> serviceEntityApiDTOs) {
        // grouping entities by entity type, so it can get pagination stats for each type of
        // entities, note: we don't support pagination across entity types for now
        final Map<Integer, Set<Long>> entityTypesToOids = serviceEntityApiDTOs.stream()
                .collect(Collectors.groupingBy(se ->
                                ApiEntityType.fromString(se.getClassName()).typeNumber(),
                        Collectors.mapping(s -> Long.parseLong(s.getUuid()), Collectors.toSet())));
        // fetch price index stats of real market from history component
        final List<EntityStats> entityStats = new ArrayList<>();
        entityTypesToOids.entrySet().stream()
                // filter out entities which don't have price index
                .filter(entry -> !StatsUtils.SDK_ENTITY_TYPES_WITHOUT_SAVED_PRICES.contains(
                        entry.getKey()))
                .forEach(entry -> {
                    //todo: we should stream the grpc stats response
                    try {
                        // build scope and filter outside while loop, since only cursor changes
                        GetEntityStatsRequest.Builder request = GetEntityStatsRequest.newBuilder()
                                .setScope(EntityStatsScope.newBuilder()
                                        .setEntityList(EntityList.newBuilder()
                                                .addAllEntities(entry.getValue())))
                                .setFilter(StatsFilter.newBuilder()
                                        .addCommodityRequests(CommodityRequest.newBuilder()
                                                .setCommodityName(PRICE_INDEX_COMMODITY)));
                        String cursor = "";
                        do {
                            final GetEntityStatsResponse entityStatsResponse =
                                    statsHistoryRpc.getEntityStats(request.setPaginationParams(
                                            PaginationParameters.newBuilder()
                                                    .setCursor(cursor))
                                            .build());
                            cursor = entityStatsResponse.getPaginationResponse().getNextCursor();
                            entityStats.addAll(entityStatsResponse.getEntityStatsList());
                        } while (!StringUtils.isEmpty(cursor));
                    } catch (StatusRuntimeException e) {
                        logger.error("Error while fetching priceIndex for entities {} of type {}: ",
                                entry.getValue(), ApiEntityType.fromType(entry.getKey()).apiStr(), e);
                    }
                });
        return entityStats.stream()
                .filter(stat -> stat.getStatSnapshotsCount() > 0)
                .filter(stat -> stat.getStatSnapshots(0).getStatRecordsCount() > 0)
                .collect(Collectors.toMap(EntityStats::getOid,
                        stat -> stat.getStatSnapshots(0).getStatRecords(0).getUsed().getMax()));
    }

    /**
     * Fetch the priceIndex stats for all the entities in the given plan topology. If an entity
     * doesn't have priceIndex or error when fetching stats in history component, it will not be
     * included in the result.
     *
     * @param planTopologyId id of the plan topology
     * @return mapping from entity oid to priceIndex value
     */
    private Map<Long, Float> fetchPriceIndexForPlanEntities(final long planTopologyId) {
        // Reading the price index for the entities of plans
        List<PlanEntityStats> entityStats = new ArrayList<>();
        Iterator<PlanTopologyStatsResponse> resp =
            repositoryRpcService.getPlanTopologyStats(PlanTopologyStatsRequest.newBuilder()
                .setTopologyId(planTopologyId)
                .setRequestDetails(RequestDetails.newBuilder()
                    // Request no pagination on this response, since we want everything and
                    // streaming a single page is more efficient than retrieving multiple pages
                    .setPaginationParams(PaginationParameters.newBuilder().setEnforceLimit(false))
                    .setReturnType(Type.MINIMAL)
                    .setFilter(StatsFilter.newBuilder()
                        .setStartDate(System.currentTimeMillis())
                        .addCommodityRequests(CommodityRequest.newBuilder()
                            .setCommodityName(StringConstants.PRICE_INDEX))))
                .build());
        // This response processing assumes that the entire response will be a single page
        while (resp.hasNext()) {
            PlanTopologyStatsResponse chunk = resp.next();
            if (chunk.getTypeCase() != TypeCase.PAGINATION_RESPONSE) {
                entityStats.addAll(chunk.getEntityStatsWrapper().getEntityStatsList());
            }
        }

        return entityStats.stream()
                .filter(stat -> stat.getPlanEntityStats().getStatSnapshotsCount() > 0)
                .filter(stat -> stat.getPlanEntityStats().getStatSnapshots(0).getStatRecordsCount() > 0)
                .collect(Collectors.toMap(stat -> stat.getPlanEntity().getMinimal().getOid(),
                        stat -> stat.getPlanEntityStats().getStatSnapshots(0).getStatRecords(0)
                                .getUsed().getMax()));
    }
}

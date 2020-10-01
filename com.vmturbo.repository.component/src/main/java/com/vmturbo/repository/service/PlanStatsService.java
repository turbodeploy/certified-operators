package com.vmturbo.repository.service;

import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_VMS_PER_HOST;
import static com.vmturbo.common.protobuf.utils.StringConstants.NUM_VMS_PER_STORAGE;
import static com.vmturbo.common.protobuf.utils.StringConstants.PRICE_INDEX;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanCombinedStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityAndCombinedStats;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityAndCombinedStats.Builder;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityAndCombinedStatsChunk;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStats;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStatsChunk;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.StatEpoch;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.pagination.EntityStatsPaginator;
import com.vmturbo.components.common.pagination.EntityStatsPaginator.PaginatedStats;
import com.vmturbo.components.common.pagination.EntityStatsPaginator.SortCommodityValueGetter;
import com.vmturbo.components.common.stats.StatsUtils;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.repository.service.AbridgedSoldCommoditiesForProvider.AbridgedSoldCommodity;
import com.vmturbo.repository.service.PlanEntityStatsExtractor.DefaultPlanEntityStatsExtractor;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufReader;
import com.vmturbo.repository.topology.util.PlanEntityStatsExtractorUtil;

/**
 * A service for retrieving plan entity stats.
 */
public class PlanStatsService {

    private static final Logger logger = LogManager.getLogger();

    /**
     * A predicate to test whether a given {@link TopologyEntityDTO} was added to the plan via a
     * scenario change (i.e. as part of the plan configuration).
     */
    private static final Predicate<TopologyEntityDTO> ENTITY_ADDED_BY_SCENARIO =
        topologyEntityDTO -> topologyEntityDTO.hasOrigin()
        && topologyEntityDTO.getOrigin().hasPlanScenarioOrigin();

    /**
     * A predicate to test whether a given {@link TopologyEntityDTO} is suspended.
     */
    private static final Predicate<TopologyEntityDTO> ENTITY_SUSPENDED =
        topologyEntityDTO -> topologyEntityDTO.hasEntityState()
            && EntityState.SUSPENDED == topologyEntityDTO.getEntityState();

    /**
     * A predicate to test whether a given {@link TopologyEntityDTO} is an unplaced VM.
     */
    private static final Predicate<TopologyEntityDTO> UNPLACED_VM =
        topologyEntityDTO -> EntityType.VIRTUAL_MACHINE.getValue() == topologyEntityDTO.getEntityType()
            && !TopologyDTOUtil.isPlaced(topologyEntityDTO);

    /**
     * A factory for creating {@link EntityStatsPaginationParams}.
     */
    private final EntityStatsPaginationParamsFactory paginationParamsFactory;

    /**
     * To do in-memory pagination of entities.
     */
    private final EntityStatsPaginator entityStatsPaginator;

    /**
     * Extracts and converts requested stats from plan entities.
     */
    private final PlanEntityStatsExtractor planEntityStatsExtractor = new DefaultPlanEntityStatsExtractor();

    /**
     * Converts entities to partial entities with the appropriate detail levels.
     */
    private final PartialEntityConverter partialEntityConverter;

    /**
     * The max number of entities to send in a single message.
     */
    private final int maxEntitiesPerChunk;

    /**
     * Create a service for retrieving plan entity stats.
     *
     * @param paginationParamsFactory a factory for creating {@link EntityStatsPaginationParams}
     * @param entityStatsPaginator to do in-memory pagination of entities
     * @param partialEntityConverter converts entities to partial entities with the appropriate
     *                               detail levels
     * @param maxEntitiesPerChunk the max number of entities to send in a single message
     */
    public PlanStatsService(@Nonnull final EntityStatsPaginationParamsFactory paginationParamsFactory,
                            @Nonnull final EntityStatsPaginator entityStatsPaginator,
                            @Nonnull final PartialEntityConverter partialEntityConverter,
                            final int maxEntitiesPerChunk) {
        this.paginationParamsFactory = Objects.requireNonNull(paginationParamsFactory);
        this.entityStatsPaginator = Objects.requireNonNull(entityStatsPaginator);
        this.partialEntityConverter = Objects.requireNonNull(partialEntityConverter);
        this.maxEntitiesPerChunk = maxEntitiesPerChunk;
    }

    /**
     * Fetch stats from the requested Plan Topology.
     *
     * <p>This topology may be either plan source or plan projected (i.e. the output of Market Analysis).
     * This data is taken from the Arango DB "raw TopologyApiDTO" storage. The reason for using
     * this rather than the ArangoDB graph representation of the topology appears to be that the
     * graph representations of the entities does not contain all of the commodity information
     * needed to calculate these stats.</p>
     *
     * @param reader reads the raw topology protobufs containing the plan entities
     * @param statEpoch describes whether the plan topology is PLAN_SOURCE or PLAN_PROJECTED
     * @param statsFilter filters on the stats to be retrieved
     * @param entityPredicate entities that match this predicate will be included in the response
     * @param paginationParameters describes if and how to paginate the response
     * @param entityReturnType indicates what level of detail of entities to include in the response
     * @param responseObserver the sync for entity stats constructed here and returned to caller
     * @param relatedEntityType the {@link ApiEntityType} string version specifying entities targeted
     */
    public void getPlanTopologyStats(@Nonnull final Iterator<List<ProjectedTopologyEntity>> reader,
                                     @Nonnull final StatEpoch statEpoch,
                                     @Nonnull final StatsFilter statsFilter,
                                     @Nonnull final Predicate<TopologyEntityDTO> entityPredicate,
                                     @Nonnull final PaginationParameters paginationParameters,
                                     @Nonnull final Type entityReturnType,
                                     @Nonnull StreamObserver<PlanTopologyStatsResponse> responseObserver,
                                     @Nullable final  String relatedEntityType) {
        // We store them in memory first, and sort and paginate them after.
        // The better solution would be to do the sorting and pagination in the database, but:
        //  1) At the time of this writing we often restrict the number of entities we retrieve
        //     for projected plan stats.
        //  2) Making that change would mean changing how we store projected topology, so the
        //     effort is not worth it for now.

        // By convention, the requested start date is used to determine the timestamp
        // for single-topology snapshots. In order to get source stats, the
        // startDate must be less than or equal to the planStartTime, so any startDate that
        // retrieves such stats is a reasonable representation of the time for those snapshots.
        // Similar logic applies to projected stats, where to retrieve (only) projected
        // stats the startDate must be greater than planStartTime.
        long sourceSnapshotTime = statsFilter.getStartDate();

        // For source topologies, filter entities added via scenario changes
        Predicate<TopologyEntityDTO> updatedEntityPredicate =
            StatEpoch.PLAN_SOURCE == statEpoch
                ? entityPredicate.and(ENTITY_ADDED_BY_SCENARIO.negate())
                : entityPredicate;

        updatedEntityPredicate = updatedEntityPredicate
            // Filter suspended entities from both the source and projected topologies
            .and(ENTITY_SUSPENDED.negate())
            // Filter unplaced VMs from both the source and projected topologies (though we
            // generally only expect to encounter them in the projected topology)
            .and(UNPLACED_VM.negate());

        // Retrieve the entities and their stats from the data store
        final Map<Long, EntityAndStats> entities = retrieveTopologyEntitiesAndStats(
            reader, updatedEntityPredicate, statsFilter, statEpoch, sourceSnapshotTime,
                relatedEntityType);

        // Begin the sorting and pagination process
        final EntityStatsPaginationParams paginationParams =
            paginationParamsFactory.newPaginationParams(paginationParameters);
        final SortCommodityValueGetter sortCommodityValueGetter;
        if (paginationParams.getSortCommodity().equals(PRICE_INDEX)) {
            // Note: This sorting will only work for projected topologies. Source topologies entities
            // are stored as TopologyEntityDTOs and thus will not have the projectedPriceIndex or the
            // originalPriceIndex fields set.
            sortCommodityValueGetter = (entityId) ->
                Optional.of((float)entities.get(entityId).entity.getProjectedPriceIndex());
        } else {
            sortCommodityValueGetter = (entityId) ->
                entities.get(entityId).getCommodityUsedAvg(paginationParams.getSortCommodity());
        }

        final PaginatedStats paginatedStats =
            entityStatsPaginator.paginate(entities.keySet(), sortCommodityValueGetter, paginationParams);

        final PlanTopologyStatsResponse.Builder paginationResponseBuilder =
            PlanTopologyStatsResponse.newBuilder()
                .setPaginationResponse(paginatedStats.getPaginationResponse());
        responseObserver.onNext(paginationResponseBuilder.build());

        // It's important to preserve the order in the paginated stats page.
        for (List<Long> idsChunk :
            Lists.partition(paginatedStats.getNextPageIds(), maxEntitiesPerChunk)) {
            final PlanTopologyStatsResponse.Builder entityStatsResponseBuilder =
                PlanTopologyStatsResponse.newBuilder();
            final PlanEntityStatsChunk.Builder planEntityStatsChunkBuilder =
                PlanEntityStatsChunk.newBuilder();
            idsChunk.stream()
                .forEach(entityId -> {
                    final EntityAndStats entityAndStats = Objects.requireNonNull(entities.get(entityId));
                    final PlanEntityStats planEntityStat = PlanEntityStats.newBuilder()
                        .setPlanEntity(partialEntityConverter
                            .createPartialEntity(entityAndStats.entity.getEntity(), entityReturnType))
                        .setPlanEntityStats(entityAndStats.stats).build();
                    planEntityStatsChunkBuilder.addEntityStats(planEntityStat);
                });
            entityStatsResponseBuilder.setEntityStatsWrapper(planEntityStatsChunkBuilder);
            responseObserver.onNext(entityStatsResponseBuilder.build());
        }
        responseObserver.onCompleted();
    }

    /**
     * Fetch the combined stats (both source and projected) related to a given plan execution
     *
     * <p>The response will contain a paginated list containing both the entities and their
     * associated stats, representing both the source and projected plan topologies.</p>
     *
     * <p>The stats in each StatSnapshot returned will be restricted to commodities in the
     * commodity_name list in the StatsFilter parameter, if any. Otherwise, all stats will be
     * returned.</p>
     *
     * <p>The stats in each StatSnapshot returned will be restricted to the entity type specified
     * in the StatsFilter related_entity_type, if any. Otherwise, stats from entities of all types
     * will be returned.</p>
     *
     * <p>Returns (through the responseObserver) a sequence of PlanEntityStats, containing entities
     * and their associated stats derived from both the source and projected plan topologies</p>
     *
     * @param sourceReader reads the raw topology protobufs containing the plan entities from the
     *                     plan source topology
     * @param projectedReader reads the raw topology protobufs containing the plan entities from the
     *                     plan projected topology
     * @param statsFilter filters on the stats to be retrieved
     * @param entityPredicate entities that match this predicate will be included in the response
     * @param topologyToSortOn indicates which (source or projected) plan topology to sort on
     * @param paginationParameters describes if and how to paginate the response
     * @param entityReturnType indicates what level of detail of entities to include in the response
     * @param responseObserver stream for entity stats constructed here to be returned to the caller
     * @param relatedEntityType the {@link ApiEntityType} string version specifying entities targeted
     */
    public void getPlanCombinedStats(@Nonnull final Iterator<List<ProjectedTopologyEntity>> sourceReader,
                                     @Nonnull final Iterator<List<ProjectedTopologyEntity>> projectedReader,
                                     @Nonnull final StatsFilter statsFilter,
                                     @Nonnull final Predicate<TopologyEntityDTO> entityPredicate,
                                     @Nonnull final TopologyType topologyToSortOn,
                                     @Nonnull final PaginationParameters paginationParameters,
                                     @Nonnull final Type entityReturnType,
                                     @Nonnull StreamObserver<PlanCombinedStatsResponse> responseObserver,
                                     @Nullable final  String relatedEntityType) {
        // We don't store any timestamps within the plan data stored in ArangoDB. Instead, we have
        // a convention where the requested start and end date are used to determine the timestamp
        // for source and projected snapshots, respectively. In order to get source stats, the
        // startDate must be less than or equal to the planStartTime, so any startDate that
        // retrieves such stats is a reasonable representation of the time for those snapshots.
        // Similar logic applies to projected stats, where the endDate must be greater than planStartTime.
        long sourceSnapshotTime = statsFilter.getStartDate();
        long projectedSnapshotTime = statsFilter.getEndDate();
        final Predicate<TopologyEntityDTO> updatedEntityPredicate = entityPredicate
            // Filter suspended entities from both the source and projected topologies
            .and(ENTITY_SUSPENDED.negate())
            // Filter unplaced VMs from both the source and projected topologies (though we
            // generally only expect to encounter them in the projected topology)
            .and(UNPLACED_VM.negate());
        // Filter entities added via scenario changes from the source topology response
        final Predicate<TopologyEntityDTO> sourceEntityPredicate = updatedEntityPredicate
            .and(ENTITY_ADDED_BY_SCENARIO.negate());
        // Retrieve the entities and their stats from the data store
        final Map<Long, EntityAndStats> sourceEntities =
            retrieveTopologyEntitiesAndStats(sourceReader, sourceEntityPredicate, statsFilter,
                StatEpoch.PLAN_SOURCE, sourceSnapshotTime, relatedEntityType);
        final Map<Long, EntityAndStats> projectedEntities =
            retrieveTopologyEntitiesAndStats(projectedReader, updatedEntityPredicate, statsFilter,
                StatEpoch.PLAN_PROJECTED, projectedSnapshotTime, relatedEntityType);

        // Determine which topology to sort on
        // Retrieve from the request the primary topology type for this request, which will be used
        // for sorting
        final Map<Long, EntityAndStats> entitiesToSort =
            (topologyToSortOn == TopologyType.SOURCE) ?
                sourceEntities :
                projectedEntities;

        // Begin the sorting and pagination process
        final EntityStatsPaginationParams paginationParams =
            paginationParamsFactory.newPaginationParams(paginationParameters);

        // Determine how entities will be sorted
        // It's possible that some entities may only exist in one or the other topology. If an
        // entity exists in the topology that we are sorting on, there is no problem. If an entity
        // exists solely in the other topology, it will be treated as not having the commodity being
        // sorted on, giving it the lowest possible sort value. These entities will still be in the
        // paginated list, but they will come last (or first, depending on the pagination request).
        final SortCommodityValueGetter sortCommodityValueGetter;
        if (paginationParams.getSortCommodity().equals(PRICE_INDEX)) {
            // Note: This sorting will only work for projected topologies. Source topologies entities
            // are stored as TopologyEntityDTOs and thus will not have the projectedPriceIndex or the
            // originalPriceIndex fields set.
            sortCommodityValueGetter = (entityId) ->
                entitiesToSort.containsKey(entityId)
                    ? Optional.of((float)entitiesToSort.get(entityId).entity.getProjectedPriceIndex())
                    : Optional.empty();
        } else {
            sortCommodityValueGetter = (entityId) ->
                entitiesToSort.containsKey(entityId)
                    ? entitiesToSort.get(entityId).getCommodityUsedAvg(paginationParams.getSortCommodity())
                    : Optional.empty();
        }

        // Create a set containing the IDs of all entities found in either topology
        final Set<Long> combinedEntityIds = Sets.newHashSet();
        combinedEntityIds.addAll(sourceEntities.keySet());
        combinedEntityIds.addAll(projectedEntities.keySet());
        // Sort and paginate all the entity IDs, using the comparator created above
        final PaginatedStats paginatedStats =
            entityStatsPaginator.paginate(combinedEntityIds, sortCommodityValueGetter, paginationParams);

        // Send the pagination information as the first chunk of the response
        final PlanCombinedStatsResponse.Builder paginationResponseBuilder =
            PlanCombinedStatsResponse.newBuilder()
                .setPaginationResponse(paginatedStats.getPaginationResponse());
        responseObserver.onNext(paginationResponseBuilder.build());

        sendCombinedStatsResponse(paginatedStats, sourceEntities, projectedEntities,
            entityReturnType, responseObserver);

        // Signal that the page has been completely transmited.
        responseObserver.onCompleted();
    }

    /**
     * Filter and maps {@link CommodityRequest} collection to set of supported.
     *
     * @param commodityRequests collection of request to filter and map.
     * @param relatedEntityType the {@link ApiEntityType} string version specifying entities targeted
     * @return set of densityStats collected from {@link CommodityRequest}s
     */
    private Set<String> getFilteredDensityStats(@Nonnull List<CommodityRequest> commodityRequests,
            @Nullable final String relatedEntityType) {
        final String finalDensityName = getSupportedDensityStatNameForEnityType(ApiEntityType.fromString(relatedEntityType));

        if (finalDensityName == null) {
            return Collections.emptySet();
        }

        return commodityRequests.stream()
                .map(CommodityRequest::getCommodityName)
                .filter(stat -> stat.equals(finalDensityName))
                .collect(Collectors.toSet());
    }

    /**
     * Returns supported density stat name for {@link ApiEntityType}.
     *
     * @param relatedEntityType the entityType whos support density stats is wanted
     * @return the density stat name supported for entityType or null
     */
    private String getSupportedDensityStatNameForEnityType(@Nullable ApiEntityType relatedEntityType) {
        String densityName = null;
        switch (relatedEntityType) {
            case STORAGE:
                densityName = NUM_VMS_PER_STORAGE;
                break;
            case PHYSICAL_MACHINE:
                densityName = NUM_VMS_PER_HOST;
                break;
        }
        return densityName;
    }

    /**
     * A predicate to test whether a given {@link TopologyEntityDTO} is VM entity.
     */
    private Predicate<TopologyEntityDTO> VIRTUAL_MACHINE_ENTITY_TYPE() {
        return topologyEntityDTO ->
                EntityType.VIRTUAL_MACHINE.getValue() == topologyEntityDTO.getEntityType();
    }

    /**
     * A predicate to test whether if {@link TopologyEntityDTO} is appropriate for VM densityStats
     * consideration.
     */
    private Predicate<TopologyEntityDTO> getVMDensityStatsMatcher() {
        return VIRTUAL_MACHINE_ENTITY_TYPE() //Filter in VM entities
                .and(ENTITY_SUSPENDED.negate()) //Filter out suspended VMs
                .and(UNPLACED_VM.negate()); //Filter out Unplaced VMS
    }

    /**
     * Predicate over CommoditiesBoughtFromProvider returning true if providersEntityType matches
     * the {@link ApiEntityType} string version.
     *
     * @param relatedEntityType the {@link ApiEntityType} string version to match
     * @return  true if the provider entity type of the CommoditiesBoughtFromProvider
     *          matches the given relatedEntityType  OR if relatedEntityType is null
     */
    private Predicate<CommoditiesBoughtFromProvider> getEntityTypeMatcher(@Nullable final String relatedEntityType) {
        if (relatedEntityType == null) {
            return entityTypeValue -> true;
        }

        final Integer relatedTypeSDKValue = ApiEntityType.fromStringToSdkType(relatedEntityType);

        return commoditiesBoughtFromProvider ->
                commoditiesBoughtFromProvider.getProviderEntityType() == relatedTypeSDKValue;

    }

    /**
     * Gets set of providerIds selling to {@link TopologyEntityDTO} that match {@link ApiEntityType}
     * string version.
     *
     * @param entityDto the {@link TopologyEntityDTO} who providers we wish to match to relatedEntityType
     * @param relatedEntityType UIEntityType string version to match against entityDto
     * @return set of provider ids from TopologyEntityDTO matching relatedEntityType
     */
    Set<Long> getProvidersToIncrementVMDensityStats(
            @Nonnull final TopologyEntityDTO entityDto,
            @Nullable final String relatedEntityType) {
                return entityDto.getCommoditiesBoughtFromProvidersList().stream()
                        .filter(getEntityTypeMatcher(relatedEntityType))
                        .map(CommoditiesBoughtFromProvider::getProviderId)
                        .collect(Collectors.toSet());
    }

    /**
     * Updates and returns map of providerIds to vmDensityCounts based on collection providerIds.
     *
     * @param providerIds the provider ids to update vmCounts in map
     * @param providerToVmCount the map with current providerIds to vmDensityCounts
     * @return the update Map of providerIds to vmDensityCounts
     */
    private Map<Long, Integer> updateMapIncrementsWithProviderSet(
            @Nonnull final Set<Long> providerIds, @Nonnull Map<Long, Integer> providerToVmCount) {

        providerIds.stream().forEach( providerId -> {
            providerToVmCount.putIfAbsent(providerId, 0);
            providerToVmCount.merge(providerId, 1, Integer::sum);
        });

        return providerToVmCount;
    }

    /**
     * Retrieve entities and their stats from the requested topology.
     *
     * @param reader a topology reader to use to load entities from ArangoDB
     * @param entityMatcher a predicate that determines whether a given entity should be included
     * @param statsFilter determines which stats will be included
     * @param statEpoch the type of epoch to set on the stat snapshot
     * @param snapshotDate the date reported for the created stat snapshot
     * @param relatedEntityType the {@link ApiEntityType} string version specifying entities targeted
     * @return a mapping of entityId to an {@link EntityAndStats} containing the requested stats
     */
    Map<Long, EntityAndStats> retrieveTopologyEntitiesAndStats(
            @Nonnull final Iterator<List<ProjectedTopologyEntity>> reader,
            @Nonnull final Predicate<TopologyEntityDTO> entityMatcher,
            @Nonnull final StatsFilter statsFilter,
            @Nullable final StatEpoch statEpoch,
            final long snapshotDate,
            @Nullable final  String relatedEntityType) {
        final Map<Long, EntityAndStats> entities = new HashMap<>();
        final Map<Long, Integer> providerVMDensitycounts = new HashMap<>();
        final Set<String> requestedDensityStats =
                getFilteredDensityStats(statsFilter.getCommodityRequestsList(), relatedEntityType);
        final boolean hasDensityStats = !requestedDensityStats.isEmpty();
        final Predicate<TopologyEntityDTO> vmDensityStatMatcher = getVMDensityStatsMatcher();
        final List<ProjectedTopologyEntity> requestedEntities = new ArrayList<>();
        final Map<Long, AbridgedSoldCommoditiesForProvider> providerIdToSoldCommodities
            = new HashMap<>();
        final Set<Integer> requestedCommodities =
            StatsUtils.collectCommodityNames(statsFilter).stream()
            .map(commodityName -> UICommodityType.fromString(commodityName).typeNumber())
            .collect(Collectors.toSet());
        // process the chunks of TopologyEntityDTO protobufs as received
        while (reader.hasNext()) {
            List<ProjectedTopologyEntity> chunk = reader.next();
            logger.debug("chunk size: {}", chunk.size());
            for (ProjectedTopologyEntity entity : chunk) {
                final TopologyEntityDTO entityDto = entity.getEntity();

                // If VM density stats requested, we update map with providerIds to vmDensities
                // Records get added after all entities processed in while loop
                if (hasDensityStats && vmDensityStatMatcher.test(entityDto)) {
                    final Set<Long> providerIdsToIncrementDensityStats =
                        getProvidersToIncrementVMDensityStats(entityDto, relatedEntityType);
                    updateMapIncrementsWithProviderSet(providerIdsToIncrementDensityStats,
                        providerVMDensitycounts);
                }
                // apply the filtering predicate
                if (entityMatcher.test(entityDto)) {
                    requestedEntities.add(entity);
                }
                providerIdToSoldCommodities.put(entityDto.getOid(),
                    abridgeSoldCommodities(entityDto, requestedCommodities));
            }
        }
        final Map<String, Set<Integer>> commodityNameToProviderTypes = StatsUtils
            .commodityNameToProviderType(statsFilter);
        final Map<String, Set<String>> commodityNameToGroupByFilters =
            StatsUtils.commodityNameToGroupByFilters(statsFilter);
        for (final ProjectedTopologyEntity entity : requestedEntities) {
            // Calculate the stats for this entity
            final EntityStats.Builder stats =
                planEntityStatsExtractor.extractStats(entity, statEpoch,
                    providerIdToSoldCommodities, commodityNameToProviderTypes,
                    commodityNameToGroupByFilters, snapshotDate);
            entities.put(entity.getEntity().getOid(), new EntityAndStats(entity, stats));
        }
        //Add VMDensity stats to result if they were part of request
        if (hasDensityStats) {
            addVMDensityStatRecordsToEntities(entities, providerVMDensitycounts, requestedDensityStats);
        }

        return entities;
    }

    private AbridgedSoldCommoditiesForProvider abridgeSoldCommodities(
        final TopologyEntityDTO provider, final Set<Integer> requestedCommodities) {
        final List<AbridgedSoldCommodity> soldCommodities = provider.getCommoditySoldListList()
            .stream()
            .filter(commoditySold -> requestedCommodities
                .contains(commoditySold.getCommodityType().getType()))
            .map(commoditySold -> new AbridgedSoldCommodity(
                commoditySold.getCommodityType().getType(), commoditySold.getCapacity()))
            .collect(Collectors.toList());
        return new AbridgedSoldCommoditiesForProvider(provider.getEntityType(), soldCommodities);
    }

    /**
     * Adds vmDensity stats to entities.
     *
     * @param entities  map of entities stats
     * @param providerVMDensitycounts the map of providerIds to vmDensityCounts
     * @param requestedDensityStats the density commodities requested
     */
    private void addVMDensityStatRecordsToEntities(@Nonnull final Map<Long, EntityAndStats> entities,
            @Nonnull final Map<Long, Integer> providerVMDensitycounts,
            @Nonnull final Set<String> requestedDensityStats) {

        entities.entrySet().stream()
                .filter(entry -> providerVMDensitycounts.containsKey(entry.getKey()))
                .forEach(entry -> {
                    final Long providerId = entry.getKey();
                    final EntityAndStats entityAndStats = entry.getValue();
                    //PlanEntityStatsExtractor will only create 1 snapshot which we will
                    //append vmDensityStats to
                    final StatSnapshot.Builder statSnapshots =
                            entityAndStats.stats.getStatSnapshotsBuilder(0);
                    final float vmDensity = providerVMDensitycounts.get(providerId).floatValue();
                    requestedDensityStats.stream().forEach(commodityName -> {
                        final StatRecord densityStatRecord =
                                PlanEntityStatsExtractorUtil.buildVmDensityStatRecord(commodityName, vmDensity);
                        statSnapshots.addStatRecords(densityStatRecord);
                    });
                });
    }

    private void sendCombinedStatsResponse(@Nonnull final PaginatedStats paginatedStats,
                                           @Nonnull final Map<Long, EntityAndStats> sourceEntities,
                                           @Nonnull final Map<Long, EntityAndStats> projectedEntities,
                                           @Nonnull final Type returnType,
                                           @Nonnull final StreamObserver<PlanCombinedStatsResponse> responseObserver) {
        // Send the entities and stats as chunks, limited by our internal chunk sizes rather than
        // the configured page size. A single page may require multiple chunks to transmit.
        // It's important to preserve the order in the paginated stats page.
        for (List<Long> idsChunk :
            Lists.partition(paginatedStats.getNextPageIds(), maxEntitiesPerChunk)) {
            final PlanCombinedStatsResponse.Builder entityCombinedStatsResponseBuilder =
                PlanCombinedStatsResponse.newBuilder();
            final PlanEntityAndCombinedStatsChunk.Builder planEntityAndCombinedStatsChunkBuilder =
                PlanEntityAndCombinedStatsChunk.newBuilder();
            // Build a single chunk of the page
            idsChunk.stream()
                .forEach(entityId -> {
                    final EntityAndStats sourceEntityAndStats = sourceEntities.get(entityId);
                    final EntityAndStats projectedEntityAndStats = projectedEntities.get(entityId);
                    final Builder planEntityAndCombinedStatsBuilder =
                        PlanEntityAndCombinedStats.newBuilder();
                    EntityStats.Builder combinedStatsBuilder = EntityStats.newBuilder();
                    // A given entity may exist in (either) one or both plan topologies
                    if (sourceEntityAndStats != null) {
                        combinedStatsBuilder.mergeFrom(sourceEntityAndStats.stats.build());
                        planEntityAndCombinedStatsBuilder
                            .setPlanSourceEntity(partialEntityConverter
                                .createPartialEntity(sourceEntityAndStats.entity.getEntity(), returnType))
                            .setPlanCombinedStats(combinedStatsBuilder);
                    }
                    if (projectedEntityAndStats != null) {
                        combinedStatsBuilder.mergeFrom(projectedEntityAndStats.stats.build());
                        planEntityAndCombinedStatsBuilder
                            .setPlanProjectedEntity(partialEntityConverter
                                .createPartialEntity(projectedEntityAndStats.entity.getEntity(), returnType))
                            .setPlanCombinedStats(combinedStatsBuilder);
                    }
                    planEntityAndCombinedStatsChunkBuilder
                        .addEntityAndCombinedStats(planEntityAndCombinedStatsBuilder.build());
                });
            // Stream a single chunk of the page to the responseObserver
            entityCombinedStatsResponseBuilder
                .setEntityCombinedStatsWrapper(planEntityAndCombinedStatsChunkBuilder);
            responseObserver.onNext(entityCombinedStatsResponseBuilder.build());
        }
    }

    /**
     * Utility class to store a {@link ProjectedTopologyEntity} and it's associated
     * {@link EntityStats} during stat retrieval.
     *
     * <p>Only for use inside this class.</p>
     */
    @VisibleForTesting
    static class EntityAndStats {
        final ProjectedTopologyEntity entity;
        final EntityStats.Builder stats;

        private EntityAndStats(@Nonnull final ProjectedTopologyEntity entity,
                               @Nonnull final EntityStats.Builder stats) {
            this.entity = entity;
            this.stats = stats;
        }

        /**
         * Get the average used value of a commodity for this entity.
         *
         * @param commodityName The name of the commodity.
         * @return An optional containing the average used commodity value, or an empty optional if
         *         the entity does not buy/sell that commodity.
         */
        @Nonnull
        Optional<Float> getCommodityUsedAvg(@Nonnull final String commodityName) {
            return stats.getStatSnapshotsList().stream()
                // There should be at most one stat snapshot, because each stat snapshot represents
                // a point in time, and we are restoring a single ProjectedTopologyEntity
                // message - which is just the entity at the time that the source or projected
                // topology was generated.
                .findFirst()
                .flatMap(snapshot -> snapshot.getStatRecordsList().stream()
                    .filter(record -> record.getName().equals(commodityName))
                    // This is technically incorrect, because commodities may have keys.
                    // But in practice, we usually sort by sold commodities (e.g. CPU) or
                    // commodities from attributes (e.g. priceIndex) that don't
                    // have keys.
                    .findFirst())
                .map(record -> record.getUsed().getAvg());
        }
    }
}

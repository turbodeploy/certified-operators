package com.vmturbo.repository.service;

import static com.vmturbo.components.common.utils.StringConstants.PRICE_INDEX;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.grpc.stub.StreamObserver;

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
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.pagination.EntityStatsPaginator;
import com.vmturbo.components.common.pagination.EntityStatsPaginator.PaginatedStats;
import com.vmturbo.components.common.pagination.EntityStatsPaginator.SortCommodityValueGetter;
import com.vmturbo.components.common.stats.StatsUtils;
import com.vmturbo.repository.service.PlanStatsService.PlanEntityStatsExtractor.DefaultPlanEntityStatsExtractor;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufReader;

/**
 * A service for retrieving plan entity stats.
 */
public class PlanStatsService {

    private static final Logger logger = LogManager.getLogger();

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
     */
    public void getPlanTopologyStats(@Nonnull final TopologyProtobufReader reader,
                                     @Nonnull final StatEpoch statEpoch,
                                     @Nonnull final StatsFilter statsFilter,
                                     @Nonnull final Predicate<TopologyEntityDTO> entityPredicate,
                                     @Nonnull final PaginationParameters paginationParameters,
                                     @Nonnull final Type entityReturnType,
                                     @Nonnull StreamObserver<PlanTopologyStatsResponse> responseObserver) {
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

        // Retrieve the entities and their stats from the data store
        final Map<Long, EntityAndStats> entities = retrieveTopologyEntitiesAndStats(
            reader, entityPredicate, statsFilter, statEpoch, sourceSnapshotTime);

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
     */
    public void getPlanCombinedStats(@Nonnull final TopologyProtobufReader sourceReader,
                                     @Nonnull final TopologyProtobufReader projectedReader,
                                     @Nonnull final StatsFilter statsFilter,
                                     @Nonnull final Predicate<TopologyEntityDTO> entityPredicate,
                                     @Nonnull final TopologyType topologyToSortOn,
                                     @Nonnull final PaginationParameters paginationParameters,
                                     @Nonnull final Type entityReturnType,
                                     @Nonnull StreamObserver<PlanCombinedStatsResponse> responseObserver) {
        // We don't store any timestamps within the plan data stored in ArangoDB. Instead, we have
        // a convention where the requested start and end date are used to determine the timestamp
        // for source and projected snapshots, respectively. In order to get source stats, the
        // startDate must be less than or equal to the planStartTime, so any startDate that
        // retrieves such stats is a reasonable representation of the time for those snapshots.
        // Similar logic applies to projected stats, where the endDate must be greater than planStartTime.
        long sourceSnapshotTime = statsFilter.getStartDate();
        long projectedSnapshotTime = statsFilter.getEndDate();
        // Retrieve the entities and their stats from the data store
        final Map<Long, EntityAndStats> sourceEntities =
            retrieveTopologyEntitiesAndStats(sourceReader, entityPredicate, statsFilter,
                StatEpoch.PLAN_SOURCE, sourceSnapshotTime);
        final Map<Long, EntityAndStats> projectedEntities =
            retrieveTopologyEntitiesAndStats(projectedReader, entityPredicate, statsFilter,
                StatEpoch.PLAN_PROJECTED, projectedSnapshotTime);

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
     * Retrieve entities and their stats from the requested topology.
     *
     * @param reader a topology reader to use to load entities from ArangoDB
     * @param entityMatcher a predicate that determines whether a given entity should be included
     * @param statsFilter determines which stats will be included
     * @param statEpoch the type of epoch to set on the stat snapshot
     * @param snapshotDate the date reported for the created stat snapshot
     * @return a mapping of entityId to an {@link EntityAndStats} containing the requested stats
     */
    private Map<Long, EntityAndStats> retrieveTopologyEntitiesAndStats(
        @Nonnull final TopologyProtobufReader reader,
        @Nonnull final Predicate<TopologyEntityDTO> entityMatcher,
        @Nonnull final StatsFilter statsFilter,
        @Nullable final StatEpoch statEpoch,
        final long snapshotDate) {
        final Map<Long, EntityAndStats> entities = new HashMap<>();
        // process the chunks of TopologyEntityDTO protobufs as received
        while (reader.hasNext()) {
            List<ProjectedTopologyEntity> chunk = reader.nextChunk();
            logger.debug("chunk size: {}", chunk.size());
            for (ProjectedTopologyEntity entity : chunk) {
                // apply the filtering predicate
                if (!entityMatcher.test(entity.getEntity())) {
                    logger.trace("skipping {}", entity.getEntity().getDisplayName());
                    continue;
                }

                // Calculate the stats for this entity
                final EntityStats.Builder stats =
                    planEntityStatsExtractor.extractStats(entity, statsFilter, statEpoch, snapshotDate);
                entities.put(entity.getEntity().getOid(), new EntityAndStats(entity, stats));
            }
        }
        return entities;
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
     * A utility to convert extract requested stats from {@link TopologyEntityDTO}.
     * Split apart mostly for unit testing purposes, so that methods relying on this extraction
     * can be tested separately.
     */
    @FunctionalInterface
    interface PlanEntityStatsExtractor {

        /**
         * Extract the stats values from a given {@link ProjectedTopologyEntity} and add them to a new
         * {@link EntityStats} object.
         *
         * @param projectedEntity the {@link ProjectedTopologyEntity} to transform
         * @param statsFilter the stats filter to use to build the stat snapshot
         * @param statEpoch the type of epoch to set on the stat snapshot
         * @param snapshotDate the snapshot date to use for the stat snapshot
         * @return an {@link EntityStats} object populated from the current stats for the
         * given {@link ProjectedTopologyEntity}
         */
        @Nonnull
        EntityStats.Builder extractStats(@Nonnull ProjectedTopologyEntity projectedEntity,
                                         @Nonnull StatsFilter statsFilter,
                                         @Nullable StatEpoch statEpoch,
                                         long snapshotDate);

        /**
         * The default implementation of {@link PlanEntityStatsExtractor} for use in production.
         */
        class DefaultPlanEntityStatsExtractor implements PlanEntityStatsExtractor {
            @Nonnull
            @Override
            public EntityStats.Builder extractStats(@Nonnull final ProjectedTopologyEntity projectedEntity,
                                                    @Nonnull final StatsFilter statsFilter,
                                                    @Nullable final StatEpoch statEpoch,
                                                    final long snapshotDate) {
                Set<String> commodityNames = StatsUtils.collectCommodityNames(statsFilter);
                logger.debug("Extracting stats for commodities: {}", commodityNames);
                StatSnapshot.Builder snapshot = StatSnapshot.newBuilder();
                if (statEpoch != null) {
                    snapshot.setStatEpoch(statEpoch);
                }
                snapshot.setSnapshotDate(snapshotDate);

                // commodities bought - TODO: compute capacity of commodities bought = seller capacity
                for (CommoditiesBoughtFromProvider commoditiesBoughtFromProvider :
                    projectedEntity.getEntity().getCommoditiesBoughtFromProvidersList()) {
                    String providerOidString = Long.toString(commoditiesBoughtFromProvider.getProviderId());
                    logger.debug("   provider  id {}", providerOidString);
                    commoditiesBoughtFromProvider.getCommodityBoughtList().forEach(commodityBoughtDTO ->
                        buildStatRecord(commodityBoughtDTO.getCommodityType(), commodityBoughtDTO.getPeak(),
                            commodityBoughtDTO.getUsed(), 0, providerOidString, commodityNames)
                            .ifPresent(snapshot::addStatRecords));
                }
                // commodities sold
                String entityOidString = Long.toString(projectedEntity.getEntity().getOid());
                final List<CommoditySoldDTO> commoditySoldListList = projectedEntity.getEntity().getCommoditySoldListList();
                for (CommoditySoldDTO commoditySoldDTO : commoditySoldListList) {
                    buildStatRecord(commoditySoldDTO.getCommodityType(), commoditySoldDTO.getPeak(),
                        commoditySoldDTO.getUsed(), commoditySoldDTO.getCapacity(),
                        entityOidString, commodityNames)
                        .ifPresent(snapshot::addStatRecords);
                }

                if (commodityNames.contains(PRICE_INDEX)) {
                    final float projectedPriceIdx = (float)projectedEntity.getProjectedPriceIndex();
                    final StatRecord priceIdxStatRecord = StatRecord.newBuilder()
                        .setName(PRICE_INDEX)
                        .setCurrentValue(projectedPriceIdx)
                        .setUsed(buildStatValue(projectedPriceIdx))
                        .setPeak(buildStatValue(projectedPriceIdx))
                        .setCapacity(buildStatValue(projectedPriceIdx))
                        .build();
                    snapshot.addStatRecords(priceIdxStatRecord);
                }

                return EntityStats.newBuilder()
                    .setOid(projectedEntity.getEntity().getOid())
                    .addStatSnapshots(snapshot);
            }

            /**
             * If the commodityType is in the given list, return an Optional with a new StatRecord
             * with values populated.
             * If the commodityType is not in the given list, return Optional.empty().
             *
             * @param commodityType the numeric (SDK) type of the commodity
             * @param peak peak value recorded for one sample
             * @param used used (or current) value recorded for one sample
             * @param capacity the total capacity for the commodity
             * @param providerOidString the OID for the provider - either this SE for sold, or the 'other'
             *                          SE for bought commodities
             * @param commodityNames the Set of commodity names (DB String) that are to be included.
             * @return either an Optional containing a new StatRecord initialized from the given values, or
             * if the given commodity is not on the list, then return Optional.empty().
             */
            private Optional<StatRecord> buildStatRecord(TopologyDTO.CommodityType commodityType,
                                                         double peak, double used, double capacity,
                                                         String providerOidString,
                                                         Set<String> commodityNames) {
                final String commodityStringName =
                    UICommodityType.fromType(commodityType.getType()).apiStr();
                if (commodityNames.isEmpty() || commodityNames.contains(commodityStringName)) {
                    final String units = CommodityTypeUnits.fromString(commodityStringName).getUnits();
                    final String key = commodityType.getKey();
                    // create a stat record from the used and peak values
                    // todo: capacity value, which comes from provider, is not set - may not be needed
                    StatRecord statRecord = StatRecord.newBuilder()
                        .setName(commodityStringName)
                        .setUnits(units)
                        .setCurrentValue((float)used)
                        .setUsed(buildStatValue((float)used))
                        .setPeak(buildStatValue((float)peak))
                        .setCapacity(buildStatValue((float)capacity))
                        .setStatKey(key)
                        .setProviderUuid(providerOidString)
                        .build();
                    return Optional.of(statRecord);
                } else {
                    return Optional.empty();
                }
            }

            /**
             * Create a {@link StatRecord.StatValue} initialized from a single value. All the fields
             * are set to the same value.
             *
             * @param value the value to initialize the StatValue with
             * @return a {@link StatRecord.StatValue} initialized with all fields set from the given value
             */
            private StatRecord.StatValue buildStatValue(float value) {
                return StatRecord.StatValue.newBuilder()
                    .setAvg(value)
                    .setMin(value)
                    .setMax(value)
                    .setTotal(value)
                    .build();
            }
        }
    }

    /**
     * Utility class to store a {@link ProjectedTopologyEntity} and it's associated
     * {@link EntityStats} during stat retrieval.
     *
     * <p>Only for use inside this class.</p>
     */
    private static class EntityAndStats {
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

package com.vmturbo.repository.service;

import static com.vmturbo.components.common.stats.StatsUtils.collectCommodityNames;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javaslang.control.Either;

import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.repository.RepositoryDTO;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.DeleteTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStats;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStatsChunk;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponseCode;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceImplBase;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.pagination.EntityStatsPaginator;
import com.vmturbo.components.common.pagination.EntityStatsPaginator.PaginatedStats;
import com.vmturbo.components.common.pagination.EntityStatsPaginator.SortCommodityValueGetter;
import com.vmturbo.repository.service.ArangoRepositoryRpcService.PlanEntityStatsExtractor.DefaultPlanEntityStatsExtractor;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyDeletionException;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufReader;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufsManager;

/**
 * Implementation of RepositoryService that uses arango for both plan and realtime.
 */
public class ArangoRepositoryRpcService extends RepositoryServiceImplBase {

    private static final String PRICE_INDEX_STAT_NAME = "priceIndex";

    private static final Logger logger = LogManager.getLogger();

    private final TopologyLifecycleManager topologyLifecycleManager;

    private final TopologyProtobufsManager topologyProtobufsManager;

    private final GraphDBService graphDBService;

    private final EntityStatsPaginationParamsFactory paginationParamsFactory;

    private final EntityStatsPaginator entityStatsPaginator;

    private final PlanEntityStatsExtractor planEntityStatsExtractor;

    private final PartialEntityConverter partialEntityConverter;

    private final int maxEntitiesPerChunk; // the max number of entities to send in a single message

    public ArangoRepositoryRpcService(@Nonnull final TopologyLifecycleManager topologyLifecycleManager,
                                      @Nonnull final TopologyProtobufsManager topologyProtobufsManager,
                                      @Nonnull final GraphDBService graphDBService,
                                      @Nonnull final EntityStatsPaginationParamsFactory paginationParamsFactory,
                                      @Nonnull final EntityStatsPaginator entityStatsPaginator,
                                      @Nonnull final PartialEntityConverter partialEntityConverter,
                                      final int maxEntitiesPerChunk) {
        this(topologyLifecycleManager, topologyProtobufsManager, graphDBService,
            paginationParamsFactory, entityStatsPaginator, new DefaultPlanEntityStatsExtractor(),
            partialEntityConverter, maxEntitiesPerChunk);
    }

    ArangoRepositoryRpcService(@Nonnull final TopologyLifecycleManager topologyLifecycleManager,
                               @Nonnull final TopologyProtobufsManager topologyProtobufsManager,
                               @Nonnull final GraphDBService graphDBService,
                               @Nonnull final EntityStatsPaginationParamsFactory paginationParamsFactory,
                               @Nonnull final EntityStatsPaginator entityStatsPaginator,
                               @Nonnull final PlanEntityStatsExtractor planEntityStatsExtractor,
                               @Nonnull final PartialEntityConverter partialEntityConverter,
                               final int maxEntitiesPerChunk) {
        this.topologyLifecycleManager = Objects.requireNonNull(topologyLifecycleManager);
        this.topologyProtobufsManager = Objects.requireNonNull(topologyProtobufsManager);
        this.graphDBService = Objects.requireNonNull(graphDBService);
        this.paginationParamsFactory = Objects.requireNonNull(paginationParamsFactory);
        this.entityStatsPaginator = Objects.requireNonNull(entityStatsPaginator);
        this.planEntityStatsExtractor = Objects.requireNonNull(planEntityStatsExtractor);
        this.partialEntityConverter = partialEntityConverter;
        this.maxEntitiesPerChunk = maxEntitiesPerChunk;
    }

    private boolean validateDeleteTopologyRequest(DeleteTopologyRequest request,
            StreamObserver<RepositoryOperationResponse> responseObserver) {

        if (!request.hasTopologyId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Topology ID missing")
                .asException());
            return false;
        }

        if (!request.hasTopologyContextId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Topology Context ID missing")
                .asException());
            return false;
        }

        if (!request.hasTopologyType()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Topology Type missing")
                .asException());
            return false;
        }

        return true;
    }

    @Override
    public void deleteTopology(DeleteTopologyRequest request,
            StreamObserver<RepositoryOperationResponse> responseObserver) {
        if (!validateDeleteTopologyRequest(request, responseObserver)) {
            return;
        }

        // Map the topology type from the request to the enum used by TopologyId
        final TopologyType topologyType = TopologyType.mapTopologyType(request.getTopologyType());
        logger.debug("Deleting topology with id:{}, contextId:{} and type:{}.",
                request.getTopologyId(), request.getTopologyContextId(), topologyType);
        try {
            topologyLifecycleManager.deleteTopology(
                    new TopologyID(request.getTopologyContextId(),
                            request.getTopologyId(),
                            topologyType));
            final RepositoryOperationResponse responseBuilder =
                    RepositoryOperationResponse.newBuilder()
                        .setResponseCode(RepositoryOperationResponseCode.OK)
                        .build();
            responseObserver.onNext(responseBuilder);
            responseObserver.onCompleted();

        } catch (TopologyDeletionException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }

    }

    @Override
    public void retrieveTopology(final RepositoryDTO.RetrieveTopologyRequest topologyRequest,
                                 final StreamObserver<RepositoryDTO.RetrieveTopologyResponse> responseObserver) {
        final long topologyID = topologyRequest.getTopologyId();

        try {
            logger.debug("Retrieving topology for {} with filter {}", topologyID,
                    topologyRequest.getEntityFilter());
            final TopologyProtobufReader reader =
                    topologyProtobufsManager.createTopologyProtobufReader(
                        topologyID,
                        topologyRequest.hasEntityFilter() ?
                                Optional.of(topologyRequest.getEntityFilter()) : Optional.empty());
            while (reader.hasNext()) {
                for (List<ProjectedTopologyEntity> chunk :
                    Lists.partition(reader.nextChunk(), maxEntitiesPerChunk)) {
                    final RepositoryDTO.RetrieveTopologyResponse responseChunk =
                        RepositoryDTO.RetrieveTopologyResponse.newBuilder()
                            .addAllEntities(Collections2.transform(chunk, ProjectedTopologyEntity::getEntity))
                            .build();
                    responseObserver.onNext(responseChunk);
                }
            }
            responseObserver.onCompleted();
        } catch (NoSuchElementException nse) {
            responseObserver.onError(Status
                .NOT_FOUND
                .withDescription(String.format("Cannot find topology with ID %s", topologyID))
                .asException());
        } catch (RuntimeException e) {
            responseObserver.onError(Status.UNKNOWN.withCause(e)
                                                   .withDescription(e.getMessage())
                                                   .asException());
        }
    }

    @Override
    public void retrieveTopologyEntities(RetrieveTopologyEntitiesRequest request,
                                         StreamObserver<PartialEntityBatch> responseObserver) {

        if (!request.hasTopologyContextId() || !request.hasTopologyType()) {
            logger.error("Missing parameters for retrieve topology entities: " + request);
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Missing parameters for retrieve topology entities")
                    .asException());
            return;
        }

        final TopologyType topologyType = (request.getTopologyType() ==
            RepositoryDTO.TopologyType.PROJECTED) ? TopologyType.PROJECTED :
                        TopologyType.SOURCE;

        Optional<TopologyID> topologyIdOpt =
            topologyLifecycleManager.getTopologyId(request.getTopologyContextId(), topologyType);

        if (!topologyIdOpt.isPresent()) {
            logger.warn("No topology exists with context {} and type {}. Returning empty results.",
                request.getTopologyContextId(), topologyType);
            responseObserver.onCompleted();
            return;
        }

        final TopologyID topologyID = topologyIdOpt.get();
        final Either<String, Collection<TopologyEntityDTO>> result =
            graphDBService.retrieveTopologyEntities(topologyID,
                ImmutableSet.copyOf(request.getEntityOidsList()));

        Collection<TopologyEntityDTO> filteredEntities = result.isRight()
                ? filterEntityByType(request, result.get())
                : Collections.emptyList();
        // send the results in batches, if needed
        Iterators.partition(filteredEntities.iterator(), maxEntitiesPerChunk).forEachRemaining(chunk -> {
            PartialEntityBatch.Builder batch = PartialEntityBatch.newBuilder();
            chunk.forEach(e -> batch.addEntities(
                partialEntityConverter.createPartialEntity(e, request.getReturnType())));

            logger.debug("Sending entity batch of {} items", batch.getEntitiesCount());
            responseObserver.onNext(batch.build());
        });

        responseObserver.onCompleted();
    }

    private Collection<TopologyEntityDTO> filterEntityByType (RetrieveTopologyEntitiesRequest request,
                                                              Collection<TopologyEntityDTO> entities) {
        if (!request.getEntityTypeList().isEmpty()) {
            return entities.stream()
                    .filter(e -> request.getEntityTypeList().contains(e.getEntityType()))
                    .collect(Collectors.toList());
        } else {
            return entities;
        }
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
     * @param request the topologyId, time, entityType, and commodity filter to retrieve stats for
     * @param responseObserver the sync for entity stats constructed here and returned to caller
     */
    @Override
    public void getPlanTopologyStats(@Nonnull PlanTopologyStatsRequest request,
                          @Nonnull StreamObserver<PlanTopologyStatsResponse> responseObserver) {
        final long topologyId = request.getTopologyId();
        // create a filter on relatedEntityType
        final Stats.StatsFilter requestFilter = request.getFilter();
        logger.debug("fetch projected plan stats, entity filter {}, commodities {}",
                request.getRelatedEntityType(), collectCommodityNames(requestFilter));
        final Predicate<TopologyEntityDTO> entityPredicate = newEntityMatcher(request);
        final TopologyProtobufReader reader =
            topologyProtobufsManager.createTopologyProtobufReader(topologyId, Optional.empty());

        // process the chunks of TopologyEntityDTO protobufs as received
        //
        // We store them in memory first, and sort and paginate them after.
        // The better solution would be to do the sorting and pagination in the database, but:
        //  1) At the time of this writing we often restrict the number of entities we retrieve
        //     for projected plan stats.
        //  2) Making that change would mean changing how we store projected topology, so the
        //     effort is not worth it for now.
        final Map<Long, EntityAndStats> entities = new HashMap<>();
        while (reader.hasNext()) {
            try {
                List<ProjectedTopologyEntity> chunk = reader.nextChunk();
                logger.debug("chunk size: {}", chunk.size());
                for (ProjectedTopologyEntity entity : chunk) {
                    // apply the filtering predicate
                    if (!entityPredicate.test(entity.getEntity())) {
                        logger.trace("skipping {}", entity.getEntity().getDisplayName());
                        continue;
                    }

                    final EntityStats.Builder stats = planEntityStatsExtractor.extractStats(entity, request);
                    entities.put(entity.getEntity().getOid(), new EntityAndStats(entity, stats));
                }
            } catch (NoSuchElementException e) {
                final String errorMessage = "Topology with ID: " + topologyId + "not found.";
                logger.error(errorMessage, e.getMessage());
                responseObserver.onError(Status.INTERNAL
                        .withDescription(errorMessage)
                        .asException());
                return;
            }
        }

        final EntityStatsPaginationParams paginationParams =
                paginationParamsFactory.newPaginationParams(request.getPaginationParams());

        final SortCommodityValueGetter sortCommodityValueGetter;
        if (paginationParams.getSortCommodity().equals(PRICE_INDEX_STAT_NAME)) {
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
                        .setPlanEntity(entityAndStats.entity.getEntity())
                        .setPlanEntityStats(entityAndStats.stats).build();
                    planEntityStatsChunkBuilder.addEntityStats(planEntityStat);
                });
            entityStatsResponseBuilder.setEntityStats(planEntityStatsChunkBuilder);
            responseObserver.onNext(entityStatsResponseBuilder.build());
        }
        responseObserver.onCompleted();
    }



    /**
     * A predicate over a TopologyEntityDTO that will return true if the entity matches the
     * filters in the request.
     *
     * TODO: implement the filtering in the PropertyValueFilter component of the request - OM-33678
     *
     * @param request the External REST API version of the entity type to select
     * @return true if the entity type of the given TopologyEntityDTO matches the given relatedEntityType
     */
    @Nonnull
    private static Predicate<TopologyEntityDTO> newEntityMatcher(
            @Nonnull final PlanTopologyStatsRequest request) {
        final Optional<Set<Long>> requestedEntities = request.hasEntityFilter() ?
                Optional.of(Sets.newHashSet(request.getEntityFilter().getEntityIdsList())) :
                Optional.empty();
        final Predicate<TopologyEntityDTO> entityTypePredicate = request.hasRelatedEntityType() ?
                matchEntityType(request.getRelatedEntityType()) : noFilterPredicate();
        return (entity) -> {
            if (requestedEntities.isPresent() && !requestedEntities.get().contains(entity.getOid())) {
                return false;
            } else {
                return entityTypePredicate.test(entity);
            }
        };
    }

    /**
     * A predicate over a TopologyEntityDTO that will return true if the entity type matches
     * the String version of entity type passed from the External REST API.
     *
     * @param relatedEntityType the External REST API version of the entity type to select
     * @return true if the entity type of the given TopologyEntityDTO matches the given relatedEntityType
     */
    private static Predicate<TopologyEntityDTO> matchEntityType(String relatedEntityType) {
        return (entity) ->
            UIEntityType.fromString(relatedEntityType) == UIEntityType.fromEntity(entity);
    }

    private static Predicate<TopologyEntityDTO> noFilterPredicate() {
        return (entity) -> true;
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
         * @param request the parameters for this request
         * @return an {@link EntityStats} object populated from the current stats for the
         * given {@link ProjectedTopologyEntity}
         */
        @Nonnull
        EntityStats.Builder extractStats(@Nonnull final ProjectedTopologyEntity projectedEntity,
                                         @Nonnull final PlanTopologyStatsRequest request);

        /**
         * The default implementation of {@link PlanEntityStatsExtractor} for use in production.
         */
        class DefaultPlanEntityStatsExtractor implements PlanEntityStatsExtractor {
            @Nonnull
            @Override
            public EntityStats.Builder extractStats(@Nonnull final ProjectedTopologyEntity projectedEntity,
                                                    @Nonnull final PlanTopologyStatsRequest request) {
                Set<String> commodityNames = collectCommodityNames(request.getFilter());
                StatSnapshot.Builder snapshot = StatSnapshot.newBuilder();
                if (request.hasFilter() && request.getFilter().hasStartDate()) {
                    snapshot.setSnapshotDate(request.getFilter().getStartDate());
                }

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

                if (commodityNames.contains(PRICE_INDEX_STAT_NAME)) {
                    final float projectedPriceIdx = (float) projectedEntity.getProjectedPriceIndex();
                    final StatRecord priceIdxStatRecord = StatRecord.newBuilder()
                            .setName(PRICE_INDEX_STAT_NAME)
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
                            .setCurrentValue((float) used)
                            .setUsed(buildStatValue((float) used))
                            .setPeak(buildStatValue((float) peak))
                            .setCapacity(buildStatValue((float) capacity))
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
     * Only for use inside this class.
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

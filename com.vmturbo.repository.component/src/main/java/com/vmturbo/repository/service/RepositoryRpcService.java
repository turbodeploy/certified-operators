package com.vmturbo.repository.service;

import static com.vmturbo.components.common.ClassicEnumMapper.COMMODITY_TYPE_MAPPINGS;
import static com.vmturbo.components.common.ClassicEnumMapper.ENTITY_TYPE_MAPPINGS;
import static com.vmturbo.components.common.stats.StatsUtils.collectCommodityNames;
import static javaslang.API.$;
import static javaslang.API.Case;
import static javaslang.API.Match;
import static javaslang.Patterns.Left;
import static javaslang.Patterns.Right;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javaslang.control.Either;

import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.repository.RepositoryDTO;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.DeleteTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStats;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponseCode;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesResponse;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceImplBase;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.pagination.EntityStatsPaginator;
import com.vmturbo.components.common.pagination.EntityStatsPaginator.PaginatedStats;
import com.vmturbo.components.common.pagination.EntityStatsPaginator.SortCommodityValueGetter;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.repository.service.RepositoryRpcService.PlanEntityStatsExtractor.DefaultPlanEntityStatsExtractor;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyDeletionException;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufReader;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufsManager;

/**
 * Server side implementation of the repository gRPC calls.
 */
public class RepositoryRpcService extends RepositoryServiceImplBase {

    private static final String PRICE_INDEX_STAT_NAME = "priceIndex";

    private static final Logger logger = LoggerFactory.getLogger(RepositoryRpcService.class);

    private static final ImmutableBiMap<CommodityDTO.CommodityType, String> COMMODITY_TYPE_TO_STRING_MAPPER =
            ImmutableBiMap.copyOf(COMMODITY_TYPE_MAPPINGS).inverse();

    private final TopologyLifecycleManager topologyLifecycleManager;

    private final TopologyProtobufsManager topologyProtobufsManager;

    private final GraphDBService graphDBService;

    private final EntityStatsPaginationParamsFactory paginationParamsFactory;

    private final EntityStatsPaginator entityStatsPaginator;

    private final PlanEntityStatsExtractor planEntityStatsExtractor;

    public RepositoryRpcService(@Nonnull final TopologyLifecycleManager topologyLifecycleManager,
                                @Nonnull final TopologyProtobufsManager topologyProtobufsManager,
                                @Nonnull final GraphDBService graphDBService,
                                @Nonnull final EntityStatsPaginationParamsFactory paginationParamsFactory,
                                @Nonnull final EntityStatsPaginator entityStatsPaginator) {
        this(topologyLifecycleManager, topologyProtobufsManager, graphDBService,
            paginationParamsFactory, entityStatsPaginator, new DefaultPlanEntityStatsExtractor());
    }

    RepositoryRpcService(@Nonnull final TopologyLifecycleManager topologyLifecycleManager,
            @Nonnull final TopologyProtobufsManager topologyProtobufsManager,
            @Nonnull final GraphDBService graphDBService,
            @Nonnull final EntityStatsPaginationParamsFactory paginationParamsFactory,
            @Nonnull final EntityStatsPaginator entityStatsPaginator,
            @Nonnull final PlanEntityStatsExtractor planEntityStatsExtractor) {
        this.topologyLifecycleManager = Objects.requireNonNull(topologyLifecycleManager);
        this.topologyProtobufsManager = Objects.requireNonNull(topologyProtobufsManager);
        this.graphDBService = Objects.requireNonNull(graphDBService);
        this.paginationParamsFactory = Objects.requireNonNull(paginationParamsFactory);
        this.entityStatsPaginator = Objects.requireNonNull(entityStatsPaginator);
        this.planEntityStatsExtractor = Objects.requireNonNull(planEntityStatsExtractor);
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

        return true;
    }

    @Override
    public void deleteTopology(DeleteTopologyRequest request,
            StreamObserver<RepositoryOperationResponse> responseObserver) {
        if (!validateDeleteTopologyRequest(request, responseObserver)) {
            return;
        }

        logger.debug("Deleting topology with id:{} and contextId:{} ",
                request.getTopologyId(), request.getTopologyContextId());
        try {
            topologyLifecycleManager.getRealtimeTopologyId();
            topologyLifecycleManager.deleteTopology(
                    new TopologyID(request.getTopologyContextId(),
                            request.getTopologyId(),
                            TopologyType.PROJECTED));
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
                List<ProjectedTopologyEntity> chunk = reader.nextChunk();
                final RepositoryDTO.RetrieveTopologyResponse responseChunk =
                                RepositoryDTO.RetrieveTopologyResponse.newBuilder()
                                        .addAllEntities(Collections2.transform(chunk, ProjectedTopologyEntity::getEntity))
                                        .build();
                responseObserver.onNext(responseChunk);
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
                                         StreamObserver<RetrieveTopologyEntitiesResponse> responseObserver) {

        if (!request.hasTopologyContextId() || !request.hasTopologyType()) {
            logger.error("Missing parameters for retrieve topology entities: " + request);
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Missing parameters for retrieve topology entities")
                    .asException());
            return;
        }
        final TopologyType topologyType = (request.getTopologyType() ==
                RetrieveTopologyEntitiesRequest.TopologyType.PROJECTED) ? TopologyType.PROJECTED :
                        TopologyType.SOURCE;
        final Either<String, Collection<TopologyEntityDTO>> result = request.hasTopologyId() ?
                graphDBService.retrieveTopologyEntities(request.getTopologyContextId(),
                        request.getTopologyId(), ImmutableSet.copyOf(request.getEntityOidsList()),
                        topologyType) :
                graphDBService.retrieveRealTimeTopologyEntities(ImmutableSet.copyOf(request.getEntityOidsList()));
         final RetrieveTopologyEntitiesResponse response = Match(result).of(
                Case(Right($()), entities ->
                    RetrieveTopologyEntitiesResponse.newBuilder()
                            .addAllEntities(entities)
                            .build()),
                Case(Left($()), err -> RetrieveTopologyEntitiesResponse.newBuilder().build()));
         responseObserver.onNext(response);
         responseObserver.onCompleted();
    }

    /**
     * Fetch the stats related to a Plan topology. Depending on the 'startTime' of the
     * request: if there is a 'startTime' and it is in the future, then this request is
     * satisfied from the projected plan topology. If there is no 'startTime' or in the past, then
     * this request is to be to satisfied from the plan input topology (not yet implemented).
     *
     * @param request the parameters for this request, including the plan topology id and a StatsFilter
     *                object describing which stats to include in the result
     * @param responseObserver observer for the PlanTopologyResponse created here
     */
    @Override
    public void getPlanTopologyStats(@Nonnull final PlanTopologyStatsRequest request,
                      @Nonnull final StreamObserver<PlanTopologyStatsResponse> responseObserver) {
        // what is the timeframe for this stats request?
        if (request.hasFilter() &&
                request.getFilter().hasStartDate() &&
                request.getFilter().getStartDate() > Instant.now().toEpochMilli()) {
                // future = fetch from plan projected topology
                returnProjectedPlanStats(request, request.getTopologyId(), responseObserver);
        } else {
            // either no timeframe or timeframe is in the past - fetch from plan input topology
            // NOT IMPLEMENTED, and not required by the UI at the present; return empty result
            logger.warn("Plan stats request for 'now' = plan source topology; not implemented");
            responseObserver.onNext(PlanTopologyStatsResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }

    /**
     * Fetch stats from the Projected Plan Topology (i.e. the output of Market Analysis).
     * This data is taken from the Arango DB "raw TopologyApiDTO" storage.
     *  @param request the time, entityType, and commodity filter to apply to this request
     * @param projectedTopologyid the ID of the topology to fetch from.
     * @param responseObserver the sync for entity stats constructed here and returned to caller
     */
    private void returnProjectedPlanStats(@Nonnull PlanTopologyStatsRequest request,
                          long projectedTopologyid,
                          @Nonnull StreamObserver<PlanTopologyStatsResponse> responseObserver) {
        // create a filter on relatedEntityType
        final Stats.StatsFilter requestFilter = request.getFilter();
        logger.debug("fetch projected plan stats, entity filter {}, commodities {}",
                request.getRelatedEntityType(), collectCommodityNames(requestFilter));
        final Predicate<TopologyEntityDTO> entityPredicate = newEntityMatcher(request);
        final TopologyProtobufReader reader = topologyProtobufsManager.createTopologyProtobufReader(
                        projectedTopologyid, Optional.empty());

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
                logger.error("Topology with ID: " + projectedTopologyid + "not found.",
                        e.getMessage());
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Topology with ID: " + projectedTopologyid + "not found.")
                        .asException());
                return;
            }
        }

        final EntityStatsPaginationParams paginationParams =
                paginationParamsFactory.newPaginationParams(request.getPaginationParams());

        final SortCommodityValueGetter sortCommodityValueGetter;
        if (paginationParams.getSortCommodity().equals(PRICE_INDEX_STAT_NAME)) {
            sortCommodityValueGetter = (entityId) ->
                Optional.of((float)entities.get(entityId).entity.getProjectedPriceIndex());
        } else {
            sortCommodityValueGetter = (entityId) ->
                entities.get(entityId).getCommodityUsedAvg(paginationParams.getSortCommodity());
        }

        final PaginatedStats paginatedStats =
                entityStatsPaginator.paginate(entities.keySet(), sortCommodityValueGetter, paginationParams);

        final PlanTopologyStatsResponse.Builder responseBuilder = PlanTopologyStatsResponse.newBuilder()
                .setPaginationResponse(paginatedStats.getPaginationResponse());

        // It's important to preserve the order in the paginated stats page.
        paginatedStats.getNextPageIds().stream()
            .map(entityId -> {
                final EntityAndStats entityAndStats = Objects.requireNonNull(entities.get(entityId));
                return PlanEntityStats.newBuilder()
                        .setPlanEntity(entityAndStats.entity.getEntity())
                        .setPlanEntityStats(entityAndStats.stats);
            })
            .forEach(responseBuilder::addEntityStats);
        responseObserver.onNext(responseBuilder.build());
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
        return (entity) -> {
            final CommonDTO.EntityDTO.EntityType effectiveEntityType =
                    ENTITY_TYPE_MAPPINGS.get(relatedEntityType);
            if (effectiveEntityType == null) {
                logger.warn("unmapped relatedEntityType {}", relatedEntityType);
                return false;
            }
            return effectiveEntityType.getNumber() == entity.getEntityType();
        };
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
                    snapshot.setSnapshotDate(DateTimeUtil.toString(request.getFilter().getStartDate()));
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
                int commodityNum = commodityType.getType();
                CommodityDTO.CommodityType commonCommodityDtoType =
                        CommodityDTO.CommodityType.forNumber(commodityNum);
                final String commodityStringName =
                        COMMODITY_TYPE_TO_STRING_MAPPER.get(commonCommodityDtoType);
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
                    // message - which is just the entity at the time that the projected
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

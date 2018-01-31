package com.vmturbo.repository.service;

import static com.vmturbo.components.common.ClassicEnumMapper.COMMODITY_TYPE_MAPPINGS;
import static com.vmturbo.components.common.ClassicEnumMapper.ENTITY_TYPE_MAPPINGS;
import static javaslang.API.$;
import static javaslang.API.Case;
import static javaslang.API.Match;
import static javaslang.Patterns.Left;
import static javaslang.Patterns.Right;

import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
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

    private static final Logger logger = LoggerFactory.getLogger(RepositoryRpcService.class);

    private final TopologyLifecycleManager topologyLifecycleManager;

    private final TopologyProtobufsManager topologyProtobufsManager;
    private static final ImmutableBiMap<CommodityDTO.CommodityType, String> COMMODITY_TYPE_TO_STRING_MAPPER =
            ImmutableBiMap.copyOf(COMMODITY_TYPE_MAPPINGS).inverse();
    private final GraphDBService graphDBService;


    public RepositoryRpcService(@Nonnull final TopologyLifecycleManager topologyLifecycleManager,
                                @Nonnull final TopologyProtobufsManager topologyProtobufsManager,
                                @Nonnull final GraphDBService graphDBService) {
        this.topologyLifecycleManager = Objects.requireNonNull(topologyLifecycleManager);
        this.topologyProtobufsManager = Objects.requireNonNull(topologyProtobufsManager);
        this.graphDBService = Objects.requireNonNull(graphDBService);
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

        logger.info("Deleting topology with id:{} and contextId:{} ",
                request.getTopologyId(), request.getTopologyContextId());
        try {
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
            logger.info("Retrieving topology for {} with filter {}", topologyID,
                    topologyRequest.getEntityFilter());
            final TopologyProtobufReader reader =
                    topologyProtobufsManager.createTopologyProtobufReader(
                        topologyRequest.getTopologyId(),
                        topologyRequest.hasEntityFilter() ?
                                Optional.of(topologyRequest.getEntityFilter()) : Optional.empty());
            while (reader.hasNext()) {
                List<TopologyEntityDTO> chunk = reader.nextChunk();
                final RepositoryDTO.RetrieveTopologyResponse responseChunk =
                                RepositoryDTO.RetrieveTopologyResponse.newBuilder()
                                        .addAllEntities(chunk)
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
        if (!request.hasTopologyId() || !request.hasTopologyContextId() || !request.hasTopologyType()) {
            logger.error("Missing parameters for retrieve topology entities: " + request);
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Missing parameters for retrieve topology entities")
                    .asException());
            return;
        }
        if (request.getEntityOidsList().isEmpty()) {
            logger.error("Topology entities ids can not be empty: " + request);
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Topology entities ids can not be empty.")
                    .asException());
            return;
        }
        final TopologyType topologyType = (request.getTopologyType() ==
                RetrieveTopologyEntitiesRequest.TopologyType.PROJECTED) ? TopologyType.PROJECTED :
                        TopologyType.SOURCE;
        final Either<String, Collection<TopologyEntityDTO>> result =
                graphDBService.retrieveTopologyEntities(request.getTopologyContextId(),
                        request.getTopologyId(), ImmutableSet.copyOf(request.getEntityOidsList()),
                        topologyType);
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
     * request: if there is a 'startTime', then this request is assumed to be from the
     * projected plan topology. If there is no 'startTime' then this request is assumed
     * to be to refer to the plan input topology (not yet implemented).
     *
     * @param request the parameters for this request, including the plan topology id and a StatsFilter
     *                object describing which stats to include in the result
     * @param responseObserver observer for the PlanTopologyResponse created here
     */
    @Override
    public void getPlanTopologyStats (@Nonnull final PlanTopologyStatsRequest request,
                                      @Nonnull final StreamObserver<PlanEntityStats> responseObserver) {
        final Stats.StatsFilter requestFilter = request.getFilter();

        String relatedEntityType = requestFilter.getRelatedEntityType();

        long projectedTopologyid = request.getTopologyId();
        logger.info("get plan topology stats, topology id {}, relatedEntityType: {}",
                projectedTopologyid, relatedEntityType);

        // record whether these plan stats are source or projected topology for use in error msg
        String statsRequestScope = "?";

        try {
            long now = Instant.now(Clock.systemUTC()).toEpochMilli();
            // is the request for the future (plan projected topology) or now = plan input topology
            if (request.hasFilter() && request.getFilter().hasStartDate() &&
                    request.getFilter().getStartDate() > now) {
                // request is for the future
                statsRequestScope = "projected";
                returnProjectedPlanStats(request, projectedTopologyid, responseObserver);
            } else {
                // fetch from plan input topology - not implemented; return empty result
                statsRequestScope = "source";
                logger.warn("Plan stats request for 'now' = plan source topology; not implemented");
            }
        } catch (Exception e) {
            logger.error("Error getting plan " + statsRequestScope + " topology stats: ", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Error fetching projected topology " +
                            projectedTopologyid + ": " + e)
                    .asException());
            return;
        }
        responseObserver.onCompleted();
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
                                          @Nonnull StreamObserver<PlanEntityStats> responseObserver) {

        // fetch the topology for this projectedTopologyId from the Arango DB
        final Stats.StatsFilter requestFilter = request.getFilter();
        Predicate<TopologyEntityDTO> entityTypePredicate = requestFilter.hasRelatedEntityType() ?
                matchEntityType(requestFilter.getRelatedEntityType()) :
                noFilterPredicate();
        logger.info("fetch projected plan stats, entity filter {}, commodities {}",
                requestFilter.getRelatedEntityType(), requestFilter.getCommodityNameList());
        final TopologyProtobufReader reader = topologyProtobufsManager.createTopologyProtobufReader(
                        projectedTopologyid, Optional.empty());
        // process the chunks of TopologyEntityDTO protobufs as received
        while (reader.hasNext()) {
            try {
                List<TopologyEntityDTO> chunk = reader.nextChunk();
                logger.info("chunk size: {}", chunk.size());
                for (TopologyEntityDTO entityDTO : chunk) {
                    if (!entityTypePredicate.test(entityDTO)) {
                        logger.trace("skipping {}", entityDTO.getDisplayName());
                        continue;
                    }
                    logger.trace("entity: {}", entityDTO.getDisplayName());

                    // create a return stats record, including only the stats requested
                    EntityStats stats = getEntityStats(entityDTO, request);

                    PlanEntityStats entityStats = PlanEntityStats.newBuilder()
                            .setPlanEntity(entityDTO)
                            .setPlanEntityStats(stats)
                            .build();
                    responseObserver.onNext(entityStats);
                }
            } catch (NoSuchElementException e) {
                logger.error("Topology with ID: " + projectedTopologyid + "not found.",
                        e.getMessage());
                throw e;
            }

        }
    }

    /**
     * Extract the stats values from a given TopologyEntityDTO and add them to a new
     * EntityStats object.
     *
     * @param entityDTO the {@link TopologyEntityDTO} to transform
     * @param request the parameters for this request
     * @return an {@link EntityStats} object populated from the current stats for the
     * given {@link TopologyEntityDTO}
     */
    private EntityStats getEntityStats(TopologyEntityDTO entityDTO, PlanTopologyStatsRequest request) {
        Set<String> commodityNames = Sets.newHashSet(request.getFilter().getCommodityNameList());
        StatSnapshot.Builder snapshot = StatSnapshot.newBuilder();
        if (request.hasFilter() && request.getFilter().hasStartDate()) {
            snapshot.setSnapshotDate(DateTimeUtil.toString(request.getFilter().getStartDate()));
        }

        // commodities bought
        for (CommoditiesBoughtFromProvider commoditiesBoughtFromProvider :
                entityDTO.getCommoditiesBoughtFromProvidersList()) {
            String providerOidString = Long.toString(commoditiesBoughtFromProvider.getProviderId());
            logger.info("   provider  id {}", providerOidString);
            commoditiesBoughtFromProvider.getCommodityBoughtList().forEach(commodityBoughtDTO ->
                    buildStatRecord(commodityBoughtDTO.getCommodityType(), commodityBoughtDTO.getPeak(),
                            commodityBoughtDTO.getUsed(), providerOidString, commodityNames)
                            .ifPresent(snapshot::addStatRecords));
        }
        // commodities sold
        String entityOidString = Long.toString(entityDTO.getOid());
        final List<CommoditySoldDTO> commoditySoldListList = entityDTO.getCommoditySoldListList();
        for (CommoditySoldDTO commoditySoldDTO : commoditySoldListList) {
            buildStatRecord(commoditySoldDTO.getCommodityType(), commoditySoldDTO.getPeak(),
                    commoditySoldDTO.getUsed(), entityOidString, commodityNames
            )
                    .ifPresent(snapshot::addStatRecords);
        }
        return EntityStats.newBuilder()
                .setOid(entityDTO.getOid())
                .addStatSnapshots(snapshot)
                .build();
    }

    /**
     * If the commodityType is in the given list, return an Optional with a new StatRecord
     * with values populated.
     * If the commodityType is not in the given list, return Optional.empty().
     *
     * @param commodityType the numeric (SDK) type of the commodity
     * @param peak peak value recorded for one sample
     * @param used used (or current) value recorded for one sample
     * @param providerOidString the OID for the provider - either this SE for sold, or the 'other'
     *                          SE for bought commodities
     * @param commodityNames the Set of commodity names (DB String) that are to be included.
     * @return either an Optional containing a new StatRecord initialized from the given values, or
     * if the given commodity is not on the list, then return Optional.empty().
     */
    private Optional<StatRecord> buildStatRecord(TopologyDTO.CommodityType commodityType,
                                                 double peak, double used, String providerOidString,
                                                 Set<String> commodityNames) {
        int commodityNum = commodityType.getType();
        CommodityDTO.CommodityType commonCommodityDtoType =
                CommodityDTO.CommodityType.forNumber(commodityNum);
        final String commodityStringName =
                COMMODITY_TYPE_TO_STRING_MAPPER.get(commonCommodityDtoType);
        logger.info("      commodity name {}", commodityStringName);
        if (commodityNames.isEmpty() || commodityNames.contains(commodityStringName)) {
            final String units = CommodityTypeUnits.fromString(commodityStringName).getUnits();
            final String key = commodityType.getKey();
            logger.info("commodity name {}, used {}  peak {}  key {}",
                    commodityStringName, (float) used, (float) peak, key);
            // create a stat record from the used and peak values
            // todo: capacity value, which comes from provider, is not set - may not be needed
            StatRecord statRecord = StatRecord.newBuilder()
                    .setName(commodityStringName)
                    .setUnits(units)
                    .setCurrentValue((float) used)
                    .setUsed(buildStatValue((float) used))
                    .setPeak(buildStatValue((float) peak))
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

}

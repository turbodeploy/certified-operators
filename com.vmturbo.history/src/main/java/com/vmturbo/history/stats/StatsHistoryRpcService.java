package com.vmturbo.history.stats;

import static com.vmturbo.components.common.stats.StatsUtils.collectCommodityNames;
import static com.vmturbo.history.schema.abstraction.tables.ClusterStatsByDay.CLUSTER_STATS_BY_DAY;
import static org.apache.commons.lang.time.DateUtils.MILLIS_PER_DAY;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Table.Cell;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.prometheus.client.Summary;
import io.prometheus.client.Summary.Timer;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.exception.DataAccessException;
import org.springframework.util.CollectionUtils;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.DeletePlanStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.DeletePlanStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.EntityCommoditiesMaxValues;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityGroup;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityGroupList;
import com.vmturbo.common.protobuf.stats.Stats.GetAuditLogDataRetentionSettingRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetAuditLogDataRetentionSettingResponse;
import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityCommoditiesCapacityValuesRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityCommoditiesCapacityValuesResponse;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityCommoditiesMaxValuesRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityIdToEntityTypeMappingRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityIdToEntityTypeMappingResponse;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.GetMostRecentStatRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetMostRecentStatResponse;
import com.vmturbo.common.protobuf.stats.Stats.GetPercentileCountsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetStatsDataRetentionSettingsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GlobalFilter;
import com.vmturbo.common.protobuf.stats.Stats.PercentileChunk;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.SetAuditLogDataRetentionSettingRequest;
import com.vmturbo.common.protobuf.stats.Stats.SetAuditLogDataRetentionSettingResponse;
import com.vmturbo.common.protobuf.stats.Stats.SetPercentileCountsResponse;
import com.vmturbo.common.protobuf.stats.Stats.SetStatsDataRetentionSettingRequest;
import com.vmturbo.common.protobuf.stats.Stats.SetStatsDataRetentionSettingResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatEpoch;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.Builder;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoRequest;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.ProtobufSummarizers;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.stats.StatsAccumulator;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.schema.abstraction.tables.records.ClusterStatsByDayRecord;
import com.vmturbo.history.schema.abstraction.tables.records.MktSnapshotsStatsRecord;
import com.vmturbo.history.schema.abstraction.tables.records.ScenariosRecord;
import com.vmturbo.history.schema.abstraction.tables.records.SystemLoadRecord;
import com.vmturbo.history.stats.ClusterStatsReader.ClusterStatsRecordReader;
import com.vmturbo.history.stats.live.SystemLoadReader;
import com.vmturbo.history.stats.projected.ProjectedStatsStore;
import com.vmturbo.history.stats.readers.LiveStatsReader;
import com.vmturbo.history.stats.readers.LiveStatsReader.StatRecordPage;
import com.vmturbo.history.stats.readers.MostRecentLiveStatReader;
import com.vmturbo.history.stats.snapshots.StatSnapshotCreator;
import com.vmturbo.history.stats.writers.PercentileWriter;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * Handles incoming RPC calls to History Component to return Stats information.
 * Calls a {@link LiveStatsReader} to fetch the desired data, and then converts the resulting
 * DB {@link Record}s into a protobuf response value.
 **/
public class StatsHistoryRpcService extends StatsHistoryServiceGrpc.StatsHistoryServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private final LiveStatsReader liveStatsReader;
    private final HistorydbIO historydbIO;
    private final BulkLoader<ClusterStatsByDayRecord> clusterStatsByDayLoader;
    private final PlanStatsReader planStatsReader;
    private final long realtimeContextId;
    private final ClusterStatsReader clusterStatsReader;

    private final ProjectedStatsStore projectedStatsStore;

    private final EntityStatsPaginationParamsFactory paginationParamsFactory;

    private final StatSnapshotCreator statSnapshotCreator;

    private final StatRecordBuilder statRecordBuilder;

    private final SystemLoadReader systemLoadReader;
    private final RequestBasedReader<GetPercentileCountsRequest, PercentileChunk> percentileReader;
    private final MostRecentLiveStatReader mostRecentLiveStatReader;
    private final ExecutorService statsWriterExecutorService;

    private final int systemLoadRecordsPerChunk;

    // Cluster commodities for which to create projected headroom.
    private static final Set<String> CLUSTER_COMMODITY_STATS = ImmutableSet.of(
        StringConstants.CPU_HEADROOM,
        StringConstants.MEM_HEADROOM,
        StringConstants.STORAGE_HEADROOM,
        StringConstants.TOTAL_HEADROOM);

    private static final Summary GET_STATS_SNAPSHOT_DURATION_SUMMARY = Summary.build()
        .name("history_get_stats_snapshot_duration_seconds")
        .help("Duration in seconds it takes the history component to get a stats snapshot for a group or entity.")
        .labelNames("context_type")
        .register();

    private static final Summary GET_ENTITY_STATS_DURATION_SUMMARY = Summary.build()
        .name("history_get_entity_stats_duration_seconds")
        .help("Duration in seconds it takes the history component to retrieve per-entity stats.")
        .register();

    StatsHistoryRpcService(final long realtimeContextId,
            @Nonnull final LiveStatsReader liveStatsReader,
            @Nonnull final PlanStatsReader planStatsReader,
            @Nonnull final ClusterStatsReader clusterStatsReader,
            @Nonnull final BulkLoader<ClusterStatsByDayRecord> clusterStatsByDayLoader,
            @Nonnull final HistorydbIO historydbIO,
            @Nonnull final ProjectedStatsStore projectedStatsStore,
            @Nonnull final EntityStatsPaginationParamsFactory paginationParamsFactory,
            @Nonnull final StatSnapshotCreator statSnapshotCreator,
            @Nonnull final StatRecordBuilder statRecordBuilder,
            @Nonnull final SystemLoadReader systemLoadReader,
            final int systemLoadRecordsPerChunk,
            @Nonnull RequestBasedReader<GetPercentileCountsRequest, PercentileChunk> percentileReader,
            @Nonnull ExecutorService statsWriterExecutorService,
            @Nonnull MostRecentLiveStatReader mostRecentLiveStatReader) {
        this.realtimeContextId = realtimeContextId;
        this.liveStatsReader = Objects.requireNonNull(liveStatsReader);
        this.planStatsReader = Objects.requireNonNull(planStatsReader);
        this.clusterStatsReader = Objects.requireNonNull(clusterStatsReader);
        this.clusterStatsByDayLoader = Objects.requireNonNull(clusterStatsByDayLoader);
        this.historydbIO = Objects.requireNonNull(historydbIO);
        this.projectedStatsStore = Objects.requireNonNull(projectedStatsStore);
        this.paginationParamsFactory = Objects.requireNonNull(paginationParamsFactory);
        this.statSnapshotCreator = Objects.requireNonNull(statSnapshotCreator);
        this.statRecordBuilder = Objects.requireNonNull(statRecordBuilder);
        this.systemLoadReader = Objects.requireNonNull(systemLoadReader);
        this.systemLoadRecordsPerChunk = systemLoadRecordsPerChunk;
        this.percentileReader = Objects.requireNonNull(percentileReader);
        this.statsWriterExecutorService = Objects.requireNonNull(statsWriterExecutorService);
        this.mostRecentLiveStatReader = Objects.requireNonNull(mostRecentLiveStatReader);
    }

    /**
     * Get stats from the latest projected topology. Will return a {@link ProjectedStatsResponse}
     * with no data if the projected stats are not available.
     *
     * @param request gives the entities and stats to search for
     * @param responseObserver the sync for the result value {@link ProjectedStatsResponse}
     */
    @Override
    public void getProjectedStats(@Nonnull final ProjectedStatsRequest request,
                @Nonnull final StreamObserver<ProjectedStatsResponse> responseObserver) {
        final ProjectedStatsResponse.Builder builder = ProjectedStatsResponse.newBuilder();
        projectedStatsStore.getStatSnapshotForEntities(
                new HashSet<>(request.getEntitiesList()),
                new HashSet<>(request.getCommodityNameList()),
                new HashSet<>(request.getProvidersList()))
            .ifPresent(builder::setSnapshot);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    /**
     * Get stats from the latest projected topology on a per-entity basis. Will return
     * an {@link EntityStats} response with no stats for each entity in the request list
     * if the projected stats are not available.
     *
     * @param request gives entities and stats to search for
     * @param responseObserver the sync for {@link EntityStats}, one for each entity in the request
     */
    @Override
    public void getProjectedEntityStats(@Nonnull final ProjectedEntityStatsRequest request,
                @Nonnull final StreamObserver<ProjectedEntityStatsResponse> responseObserver) {
        final EntityStatsScope scope = request.getScope();
        // create mapping from seed entity to derived entities, if no derived entities specified,
        // it will only use the seed entity to fetch stats
        final Map<Long, Set<Long>> seedEntityToDerivedEntities;
        switch (scope.getScopeCase()) {
            case ENTITY_LIST:
                seedEntityToDerivedEntities = scope.getEntityList().getEntitiesList().stream()
                    .collect(Collectors.toMap(oid -> oid, Collections::singleton));
                break;
            case ENTITY_GROUP_LIST:
                seedEntityToDerivedEntities = scope.getEntityGroupList().getGroupsList().stream()
                .collect(Collectors.toMap(EntityGroup::getSeedEntity,
                    entityGroup -> entityGroup.getEntitiesCount() == 0
                        ? Collections.singleton(entityGroup.getSeedEntity())
                        : new HashSet<>(entityGroup.getEntitiesList())));
                break;
            default:
                logger.warn("Scope case {} is not supported, returning empty stats",
                    scope.getScopeCase());
                responseObserver.onError(new UnsupportedOperationException("Scope case: " +
                    scope.getScopeCase() + " is not supported, returning empty stats."));
                responseObserver.onCompleted();
                return;
        }
        responseObserver.onNext(projectedStatsStore.getEntityStats(
            seedEntityToDerivedEntities,
            new HashSet<>(request.getCommodityNameList()),
            new HashSet<>(request.getProvidersList()),
            paginationParamsFactory.newPaginationParams(request.getPaginationParams())));
        responseObserver.onCompleted();
    }

    /**
     * Fetch a sequence of StatSnapshot's based on the time range and filters in the StatsRequest.
     * The stats may be taken from the live topology history or the plan topology history depending
     * on the ID in the request.
     *
     * <p>Unfortunately, we need to determine, by a lookup, which type of entity ID(s) we are
     * to gather stats for. This call has been overloaded (ultimately) by the External REST API,
     * which doesn't distinguish between stats requests for groups vs. individual SEs, live
     * vs. plan topologies, etc.</p>
     *
     * <p>The 'entitiesList' in the {@link GetAveragedEntityStatsRequest} may be:</p>
     * <ul>
     *     <li>a single-element list containing the distinguished ID for the Real-Time Topology = "Market"</li>
     *     <li>a single-element list containing the OID of a plan topology - a numeric (long)</li>
     *     <li>a list of one or more {@link ServiceEntityApiDTO} OIDs - these are numeric (longs)</li>
     * </ul>
     *
     * @param request a set of parameters describing how to fetch the snapshot and what entities.
     * @param responseObserver the sync for each result value {@link StatSnapshot}
     */
    @Override
    public void getAveragedEntityStats(GetAveragedEntityStatsRequest request,
                               StreamObserver<StatSnapshot> responseObserver) {
        try {
            final List<Long> entitiesList = request.getEntitiesList();
            final StatsFilter filter = request.getFilter();
            // determine if this request is for stats for a plan topology; for efficiency, check
            // first for the special case ID of the entire live topology, "Market" (i.e. not a plan)
            boolean isPlan = entitiesList.size() == 1
                    && entitiesList.get(0) != realtimeContextId
                    && historydbIO.entityIdIsPlan(entitiesList.get(0));

            final Summary.Timer timer = GET_STATS_SNAPSHOT_DURATION_SUMMARY
                .labels(isPlan ?
                        SharedMetrics.PLAN_CONTEXT_TYPE_LABEL :
                        SharedMetrics.LIVE_CONTEXT_TYPE_LABEL)
                .startTimer();

            if (isPlan) {
                // read from plan topologies
                returnPlanTopologyStats(responseObserver, entitiesList.get(0),
                        filter.getCommodityRequestsList(), request.getGlobalFilter());
            } else {
                // read from live topologies
                getLiveMarketStats(filter, entitiesList, request.getGlobalFilter())
                    .forEach(responseObserver::onNext);
                responseObserver.onCompleted();
            }

            timer.observeDuration();
        } catch (Exception e) {
            final String requestSummary = ProtobufSummarizers.summarize(request);
            logger.error("Error getting stats snapshots for {}", requestSummary, e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(String.format("Internal Error fetching stats for: %s, cause: %s",
                            requestSummary, e.getMessage()))
                    .asException());
        }
    }

    /**
     * Get the comparator that can be used to sort EntityStats according to the passed-in pagination
     * parameters. We compare the getUtilization of the stat (used / capacity).
     *
     * @param entityStats list of entity stats to get comparator for
     * @param commodity name of the stat to compare
     * @param ascending whether to create comparator in the ascending order or descending order
     * @return comparator used to sort EntityStats
     */
    @VisibleForTesting
    protected Comparator<EntityStats> getEntityStatsComparator(@Nonnull List<EntityStats> entityStats,
                                                             @Nonnull String commodity,
                                                             final boolean ascending) {
        // pre calculate the getUtilization for each entity, to reduce the calculation during sorting
        final Map<Long, Float> utilizationByEntity = entityStats.stream()
            .collect(Collectors.toMap(EntityStats::getOid, stats ->
                getStatsUtilization(stats, commodity, ascending)));
        return (entityStats1, entityStats2) -> {
            float statVal1 = utilizationByEntity.get(entityStats1.getOid());
            float statVal2 = utilizationByEntity.get(entityStats2.getOid());
            int compareResult = Float.compare(statVal1, statVal2);
            // if stats values are same, compare entity oid to ensure stable sorting
            if (compareResult == 0) {
                compareResult = Long.compare(entityStats1.getOid(), entityStats2.getOid());
            }
            return ascending ? compareResult : -compareResult;
        };
    }

    /**
     * Get the getUtilization of the given commodity from the list of snapshots. If no no snapshots,
     * or no capacity, or commodity is not found, return max/min if it's ascending/descending so
     * it is placed at the end after sorting.
     *
     * @param entityStats entityStats containing entity id and a list of snapshots
     * @param commodity name of the commodity to calculate getUtilization for
     * @param ascending whether or not the pagination is ascending or descending
     * @return getUtilization of the given commodity
     */
    private Float getStatsUtilization(@Nonnull final EntityStats entityStats,
                                      @Nonnull final String commodity,
                                      final boolean ascending) {
        final List<StatSnapshot> snapshots = entityStats.getStatSnapshotsList();
        if (snapshots.isEmpty()) {
            // if no snapshots, return max/min if it's ascending/descending so it is placed
            // at the end after sorting
            return ascending ? Float.MAX_VALUE : Float.MIN_VALUE;
        }
        // take the first matching one for comparison
        return snapshots.get(0).getStatRecordsList().stream()
            .filter(statRecord -> StringUtils.equals(statRecord.getName(), commodity))
            // must have capacity to calculate getUtilization
            .filter(statRecord -> statRecord.hasCapacity() && statRecord.getCapacity().getAvg() != 0)
            .map(statRecord -> statRecord.getValues().getAvg() / statRecord.getCapacity().getAvg())
            .findFirst()
            // if no matching commodity, return max/min if it's ascending/descending so it is
            // placed at the end after sorting
            .orElse(ascending ? Float.MAX_VALUE : Float.MIN_VALUE);
    }

    @Override
    public void getEntityStats(@Nonnull GetEntityStatsRequest request,
                               @Nonnull StreamObserver<GetEntityStatsResponse> responseObserver) {
        final EntityStatsScope scope = request.getScope();
        if (scope.hasEntityList() &&
                scope.getEntityList().getEntitiesList().isEmpty()) {
            // It's not an error to request stats for "no" entities, but you will get no results :)
            responseObserver.onNext(GetEntityStatsResponse.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        } else if (!(scope.hasEntityList() || scope.hasEntityType() || scope.hasEntityGroupList())) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(
                "Scope must have either an entity type, a list of entity IDs or a list of " +
                    "entity groups.").asException());
        }
        try {
            final Timer timer = GET_ENTITY_STATS_DURATION_SUMMARY.startTimer();

            if (scope.hasEntityGroupList()) {
                // if the scope is a list of EntityGroups, we should handle it differently since
                // each stats is an aggregation on the derived entities for the seed entity
                returnStatsForEntityGroups(scope.getEntityGroupList(), request.getFilter(),
                    request.hasPaginationParams() ? Optional.of(request.getPaginationParams())
                        : Optional.empty(), responseObserver);
            } else {
                final StatRecordPage recordPage = liveStatsReader.getPaginatedStatsRecords(scope,
                    request.getFilter(),
                    paginationParamsFactory.newPaginationParams(request.getPaginationParams()));

                final List<EntityStats> entityStats = new ArrayList<>(
                    recordPage.getNextPageRecords().size());
                recordPage.getNextPageRecords().forEach((entityOid, recordList) -> {
                    final EntityStats.Builder statsForEntity = EntityStats.newBuilder()
                        .setOid(entityOid);
                    // organize the stats DB records for this entity into StatSnapshots and add to response
                    statSnapshotCreator.createStatSnapshots(recordList, false,
                        request.getFilter().getCommodityRequestsList())
                        .forEach(statsForEntity::addStatSnapshots);
                    entityStats.add(statsForEntity.build());
                });

                final PaginationResponse.Builder paginationResponse = PaginationResponse.newBuilder();
                recordPage.getNextCursor().ifPresent(paginationResponse::setNextCursor);
                recordPage.getTotalRecordCount().ifPresent(paginationResponse::setTotalRecordCount);

                responseObserver.onNext(GetEntityStatsResponse.newBuilder()
                    .addAllEntityStats(entityStats)
                    .setPaginationResponse(paginationResponse)
                    .build());
                responseObserver.onCompleted();
            }

            timer.observeDuration();
        } catch (VmtDbException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("DB Error fetching stats for " + scopeErrorDescription(scope))
                    .withCause(e)
                    .asException());
        } catch (IllegalArgumentException e) {
            final String errorMessage = "Invalid stats query: " + scopeErrorDescription(scope) +
                    ", " + e.getMessage();
            logger.error(errorMessage);
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(errorMessage)
                    .withCause(e)
                    .asException());
        } catch (RuntimeException e) {
            logger.error("Internal exception fetching stats for " + scopeErrorDescription(scope));
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal Error fetching stats for " + scopeErrorDescription(scope))
                    .withCause(e)
                    .asException());
        }
    }

    /**
     * Fetch and return a sequence of StatSnapshots for each of the provided entity group in the
     * request. For each seed entity (like: DC), it aggregates stats on its derived entities (like:
     * PMs) and use it as the stats for the seed entity.
     *
     * <p>Currently, we only support getting stats from live topology history, since there is no
     * use
     * case for plan topology. We can extend it to support plan topology history if necessary.
     *
     * <p>Note: This call is expected to be slower since we can't do pagination in DB level.
     *
     * @param entityGroupList      EntityGroupList containing list of EntityGroups
     * @param statsFilter          the requested stats names and filters
     * @param paginationParameters pagination parameters
     * @param responseObserver     the chunking channel on which the response should be returned
     * @throws VmtDbException if there is an error interacting with the database
     */
    @VisibleForTesting
    protected void returnStatsForEntityGroups(@Nonnull EntityGroupList entityGroupList,
                                            @Nonnull StatsFilter statsFilter,
                                            @Nonnull Optional<PaginationParameters> paginationParameters,
                                            @Nonnull StreamObserver<GetEntityStatsResponse> responseObserver)
            throws VmtDbException {
        final List<EntityGroup> entityGroups = entityGroupList.getGroupsList();
        final List<EntityStats> entityStatsList = new ArrayList<>(entityGroups.size());
        // get stats for all aggregated entities
        for (EntityGroup entityGroup : entityGroups) {
            // use derived entities if any, otherwise use seed entity
            final Collection<Long> entities = entityGroup.getEntitiesList().isEmpty()
                ? Collections.singleton(entityGroup.getSeedEntity())
                : entityGroup.getEntitiesList();
            final EntityStats.Builder entityStats = EntityStats.newBuilder()
                .setOid(entityGroup.getSeedEntity());
            // fetch aggregated stats
            getLiveMarketStats(statsFilter, entities, GlobalFilter.getDefaultInstance())
                .forEach(entityStats::addStatSnapshots);
            entityStatsList.add(entityStats.build());
        }

        // if no pagination parameters provided, return all
        if (!paginationParameters.isPresent()) {
            responseObserver.onNext(GetEntityStatsResponse.newBuilder()
                .addAllEntityStats(entityStatsList)
                    .build());
            responseObserver.onCompleted();
            return;
        }

        final PaginationParameters paginationParams = paginationParameters.get();
        // create stats comparator for sorting and pagination
        final Comparator<EntityStats> comparator = getEntityStatsComparator(entityStatsList,
            paginationParams.getOrderBy().getEntityStats().getStatName(), paginationParams.getAscending());
        // sort entityStatsList
        entityStatsList.sort(comparator);

        // get the start and end indexes for next page stats
        final int startIndex = paginationParams.hasCursor()
            ? Integer.parseInt(paginationParams.getCursor()) : 0;
        final int endIndex = Math.min(entityStatsList.size(),
            startIndex + paginationParams.getLimit());

        final GetEntityStatsResponse.Builder response = GetEntityStatsResponse.newBuilder()
                .addAllEntityStats(entityStatsList.subList(startIndex, endIndex));

        // set nextCursor on pagination response
        final PaginationResponse.Builder paginationResponse = PaginationResponse.newBuilder()
                .setTotalRecordCount(entityGroups.size());
        if (endIndex < entityStatsList.size()) {
            paginationResponse.setNextCursor(String.valueOf(endIndex));
        }
        response.setPaginationResponse(paginationResponse.build());
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    /**
     * Utility method to describe a scope (mostly for logging/error messages).
     *
     * @param scope The {@link EntityStatsScope}.
     * @return A string usable for error messages and logs.
     */
    @Nonnull
    private String scopeErrorDescription(@Nonnull final EntityStatsScope scope) {
        if (scope.hasEntityType()) {
            return "scope with global entity type " + scope.getEntityType();
        } else {
            return "scope with " + scope.getEntityList().getEntitiesCount() + " entities";
        }
    }

    @Override
    public void getClusterStats(@Nonnull Stats.ClusterStatsRequest request,
                                @Nonnull StreamObserver<ClusterStatsResponse> responseObserver) {
        try {
            for (ClusterStatsResponse responseChunk : clusterStatsReader.getStatsRecords(request)) {
                try {
                    responseObserver.onNext(responseChunk);
                } catch (IllegalArgumentException e) {
                    responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
                }
            }
        } catch (Throwable e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
        responseObserver.onCompleted();

    }

    /**
     * Get cluster stats as a part of processing a gRPC request.
     *
     * @param clusterId uuid of cluster to retrieve
     * @param filter    stats filter
     * @param timeFrame timeframe to use for query
     * @return retrieved records
     * @throws StatusException for specifically crafted errors to be sent to requester
     * @throws VmtDbException if a DB error occurs
     */
    private List<ClusterStatsRecordReader> getClusterStats(long clusterId, StatsFilter filter, TimeFrame timeFrame)
            throws StatusException, VmtDbException {

        long now = System.currentTimeMillis();
        long startDate = (filter.hasStartDate()) ? filter.getStartDate() : now;
        long endDate = filter.hasEndDate() ? filter.getEndDate() : now;
        if (startDate > endDate) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("Invalid date range for retrieving cluster statistics.")
                    .asException();
        }
        final Set<String> commodityNames = collectCommodityNames(filter);

        return clusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(clusterId,
                startDate, endDate, Optional.of(timeFrame), commodityNames);
    }

    /**
     * Get cluster stats for headroom plan, e.g. number of VMs.
     * We first query the monthly table. If there's not enough data (less than 2 rows) in the monthly
     * table between start date and end date, then we query the daily table.
     *
     * @param request a request to get cluster stats
     * @param responseObserver a stream observer on which to write cluster stats
     */
    @Override
    public void getClusterStatsForHeadroomPlan(@Nonnull Stats.ClusterStatsRequestForHeadroomPlan request,
                                               @Nonnull StreamObserver<StatSnapshot> responseObserver) {
        final long clusterId = request.getClusterId();
        StatsFilter statsFilter = request.getStats();
        try {
            List<ClusterStatsRecordReader> records = getClusterStats(clusterId, statsFilter, TimeFrame.MONTH);
            if (records.size() < 2) {
                records = getClusterStats(clusterId, statsFilter, TimeFrame.DAY);
            }
            sendClusterStatsResponse(request, records, responseObserver);
        } catch (Exception e) {
            responseObserver.onError(
                    (e instanceof StatusException) ? e : Status.INTERNAL
                            .withDescription("Cluster stats request failed")
                            .withCause(e)
                            .asException());
        }
    }

    /**
     * Postprocess cluster stats query results and package them into {@link StatSnapshot} instances,
     * and transmit them back to the requester.
     *
     * @param request          a cluster stats request
     * @param results          cluster stats records
     * @param responseObserver where to send the results
     * @throws StatusException if there's a problem packaging or sending results
     */
    private void sendClusterStatsResponse(@Nonnull final Stats.ClusterStatsRequestForHeadroomPlan request,
                                          @Nonnull final Collection<ClusterStatsRecordReader> results,
                                          @Nonnull final StreamObserver<StatSnapshot> responseObserver)
            throws StatusException {

        // group records according to timestamp and property type
        Map<Pair<Timestamp, String>, List<ClusterStatsRecordReader>> recordsByPropertyType =
                results.stream().collect(
                        Collectors.groupingBy(r -> Pair.of(r.getRecordedOn(), r.getPropertyType())));

        // collect results in a StatRecord per timestamp
        Multimap<Timestamp, StatRecord> resultMap = MultimapBuilder.hashKeys().arrayListValues().build();

        try {
            // a map that may contain counts of unexpected records by property type + subtype. For
            // error tracking
            final Map<String, AtomicInteger> unexpectedRecordCounts = new HashMap();

            // consolidate each db record list into a single stat record.
            recordsByPropertyType.forEach((key, records) -> {
                final Timestamp recordedOn = key.getLeft();
                final String propertyType = key.getRight();

                // we'll fill these in as we iterate through the records. They're nullable.
                String propertySubtype = null;
                Float value = null;
                Float capacity = null;

                // the record collection will generally be either 1 or 2 records long.
                if (records.size() == 1) {
                    // the stats with one record are single-value stats, so we are going to set the
                    // value and property sub-type.
                    ClusterStatsRecordReader record = records.get(0);
                    propertySubtype = record.getPropertySubtype();
                    value = record.getValue();
                } else {
                    // stats with multiple records are expected to be usage-related, with one
                    // record containing the "used" value and another containing "capacity". We will
                    // know which record is which based on the property sub-type.
                    for (ClusterStatsRecordReader record : records) {
                        switch (record.getPropertySubtype()) {
                            case StringConstants.USED:
                                value = record.getValue();
                                break;
                            case StringConstants.CAPACITY:
                                capacity = record.getValue();
                                break;
                            default:
                                // we don't expect to be here, but we also don't want to spam the log.
                                // let's count it in the warning map.
                                unexpectedRecordCounts.computeIfAbsent(
                                        propertyType + ":" + record.getPropertySubtype(),
                                        k -> new AtomicInteger()).incrementAndGet();
                        }
                    }
                }

                resultMap.put(recordedOn, statRecordBuilder.buildStatRecord(
                        propertyType,
                        propertySubtype,
                        capacity,
                        (Float)null,
                        null,
                        null,
                        value != null ? StatsAccumulator.singleStatValue(value) : null,
                        null,
                        null));

            });

            // Sort stats data by date.
            List<Timestamp> sortedStatsDates = Lists.newArrayList(resultMap.keySet());
            Collections.sort(sortedStatsDates);

            // A StatSnapshot will be created for each date.
            // Each snapshot may have several record types (e.g. "headroomVMs", "numVMs")
            for (Timestamp recordDate : sortedStatsDates) {
                Builder statSnapshotResponseBuilder = StatSnapshot.newBuilder();
                resultMap.get(recordDate).forEach(statSnapshotResponseBuilder::addStatRecords);
                statSnapshotResponseBuilder.setSnapshotDate(recordDate.getTime());
                // Currently, all cluster stats are historical
                statSnapshotResponseBuilder.setStatEpoch(StatEpoch.HISTORICAL);
                responseObserver.onNext(statSnapshotResponseBuilder.build());
            }

            if (request.getStats().getRequestProjectedHeadroom() && !sortedStatsDates.isEmpty()) {
                final Timestamp latestRecordDate = sortedStatsDates.get(sortedStatsDates.size() - 1);
                createProjectedHeadroomStats(responseObserver, request, latestRecordDate.getTime(),
                    resultMap.get(latestRecordDate));
            }

            // if we encountered any unexpected records, log them.
            if (!CollectionUtils.isEmpty(unexpectedRecordCounts)) {
                StringBuilder sb = new StringBuilder("Unexpected cluster stat records: ");
                unexpectedRecordCounts.forEach((recordKey, count) -> {
                    sb.append(recordKey).append("(").append(count).append(") ");
                });
                logger.warn(sb.toString());
            }

            // all done!
            responseObserver.onCompleted();
        } catch (Exception e) {
            throw (Status.INTERNAL
                    .withDescription("DB Error fetching stats for cluster " + request.getClusterId())
                    .withCause(e)
                    .asException());
        }
    }

    /**
     * If the date range spans over a month, get data from the CLUSTER_STATS_BY_MONTH table.
     * Otherwise, get data from CLUSTER_STATS_BY_DAY table.
     *
     * @param startDate start date
     * @param endDate end date
     * @return Return true if monthly data should be used. Return false otherwise.
     */
    private boolean isUseMonthlyData(@Nullable Long startDate, @Nullable Long endDate) {
        if (startDate != null && endDate != null) {
            final float numberOfDaysInDateRange = (endDate - startDate) / MILLIS_PER_DAY;
            return numberOfDaysInDateRange > 40;
        }
        return false;
    }

    /**
     * Create projected headroom stats for each day between snapshotDate and endDate
     * based on the latest headroom stats.
     *
     * <p>Suppose the daily VM growth is 1 and available CPU headroom is 10.
     * Available CPU headrooms for the next 3 days are 9, 8, 7 respectively.
     * Also, available headroom is always a non-negative integer.
     *
     * @param responseObserver a stream observer on which to write cluster stats
     * @param request a cluster stats request
     * @param latestRecordDate the date of the latest records
     * @param latestStatRecords the latest records
     */
    @VisibleForTesting
    void createProjectedHeadroomStats(
            @Nonnull final StreamObserver<StatSnapshot> responseObserver,
            @Nonnull final Stats.ClusterStatsRequestForHeadroomPlan request, final long latestRecordDate,
            @Nonnull final Collection<StatRecord> latestStatRecords) {
        final StatsFilter filter = request.getStats();
        final long startDate;
        final long endDate;
        if (latestStatRecords.isEmpty() || !filter.hasStartDate() || !filter.hasEndDate() ||
            (startDate = filter.getStartDate()) >= (endDate = filter.getEndDate()) ||
            latestRecordDate >= endDate) {
            return;
        }

        // Get latest headroom stats.
        final List<StatRecord> headroomCommodityRecords = latestStatRecords.stream().filter(record ->
            CLUSTER_COMMODITY_STATS.contains(record.getName())).collect(Collectors.toList());
        if (!headroomCommodityRecords.isEmpty()) {
            // Copy snapshot as current snapshot.
            responseObserver.onNext(StatSnapshot.newBuilder()
                .setSnapshotDate(System.currentTimeMillis())
                .setStatEpoch(StatEpoch.CURRENT)
                .addAllStatRecords(headroomCommodityRecords)
                .build());

            // Get monthly vm growth.
            final long clusterId = request.getClusterId();
            final List<ClusterStatsRecordReader> statDBRecords;
            try {
                // retrieve records, using timeframe computed from parameters
                statDBRecords = clusterStatsReader.getStatsRecordsForHeadRoomPlanRequest(clusterId,
                    startDate, endDate, Optional.empty(), Collections.singleton(StringConstants.VM_GROWTH));
            } catch (VmtDbException e) {
                responseObserver.onError(Status.INTERNAL
                        .withDescription("DB Error fetching stats for cluster " + clusterId)
                        .withCause(e)
                        .asException());
                return;
            }

            final Optional<ClusterStatsRecordReader> vmGrowthRecord = statDBRecords.stream()
                    .max(Comparator.comparingLong(record -> record.getRecordedOn().getTime()));
            if (vmGrowthRecord.isPresent()) {
                final int daysPerMonth = 30;
                final int dailyVMGrowth = (int)Math.ceil(vmGrowthRecord.get().getValue() / daysPerMonth);
                final long millisPerDay = TimeUnit.DAYS.toMillis(1);
                final long daysDifference = (endDate - latestRecordDate) / millisPerDay + 1;
                logger.info("Daily VM growth: {}. Days difference: {}.", dailyVMGrowth, daysDifference);

                // Create projected headroom stats for each day.
                for (int day = 1; day <= daysDifference; day++) {
                    StatSnapshot.Builder statSnapshotBuilder = StatSnapshot.newBuilder();
                    statSnapshotBuilder.setStatEpoch(StatEpoch.PROJECTED);
                    statSnapshotBuilder.setSnapshotDate(
                        Math.min(endDate, latestRecordDate + day * millisPerDay));

                    // Create projected headroom stats for each commodity.
                    final List<StatRecord> projectedStatRecords = new ArrayList<>(headroomCommodityRecords.size());
                    for (StatRecord statRecord : headroomCommodityRecords) {
                        final StatRecord.Builder projectedStatRecord = statRecord.toBuilder();
                        final float projectedHeadroom = Math.max(0,
                            statRecord.getUsed().getAvg() - dailyVMGrowth * day);
                        projectedStatRecord.setUsed(StatValue.newBuilder().setAvg(projectedHeadroom)
                            .setMin(projectedHeadroom).setMax(projectedHeadroom).setTotal(projectedHeadroom));
                        projectedStatRecord.clearCurrentValue();
                        projectedStatRecord.clearValues();
                        projectedStatRecord.clearPeak();
                        projectedStatRecords.add(projectedStatRecord.build());
                    }

                    statSnapshotBuilder.addAllStatRecords(projectedStatRecords);
                    responseObserver.onNext(statSnapshotBuilder.build());
                }
            } else {
                logger.warn("No VM growth record found between start date {} and end data {}" +
                        "in cluster {} table.", startDate, endDate,
                    isUseMonthlyData(startDate, endDate) ? "monthly" : "daily");
            }
        }
    }

    /**
     * Save computed cluster headroom to cluster_stat_by_day table.
     * This is an internal call, in the sense that user and external API request should never
     * trigger this method.
     *
     * @param request request with cluster id and headroom value
     * @param responseObserver response observer
     */
    @Override
    public void saveClusterHeadroom(@Nonnull Stats.SaveClusterHeadroomRequest request,
                                    @Nonnull StreamObserver<Stats.SaveClusterHeadroomResponse> responseObserver) {
        logger.debug("Got request to save cluster headroom: {}", request);

        Timestamp today = Timestamp.from(Instant.now().truncatedTo(ChronoUnit.DAYS));

        try {
            // Table represents : <PropertyType, SubPropertyType, Value>
            com.google.common.collect.Table<String, String, Double> headroomData =
                    HashBasedTable.create();

            headroomData.put(StringConstants.HEADROOM_VMS, StringConstants.HEADROOM_VMS,
                    (double)request.getHeadroom());
            headroomData.put(StringConstants.VM_GROWTH, StringConstants.VM_GROWTH,
                    (double)request.getMonthlyVMGrowth());
            // CPU related headroom stats.
            headroomData.put(StringConstants.CPU_HEADROOM, StringConstants.USED,
                    (double)request.getCpuHeadroomInfo().getHeadroom());
            headroomData.put(StringConstants.CPU_HEADROOM, StringConstants.CAPACITY,
                    (double)request.getCpuHeadroomInfo().getCapacity());
            // Don't save days to exhaustion when headroom capacity is 0,
            // because it doesn't make sense to have days to exhaustion in this case.
            if (request.getCpuHeadroomInfo().getCapacity() != 0) {
                headroomData.put(StringConstants.CPU_EXHAUSTION, StringConstants.EXHAUSTION_DAYS,
                        (double)request.getCpuHeadroomInfo().getDaysToExhaustion());
            }

            // Memory related headroom stats.
            headroomData.put(StringConstants.MEM_HEADROOM, StringConstants.USED,
                    (double)request.getMemHeadroomInfo().getHeadroom());
            headroomData.put(StringConstants.MEM_HEADROOM, StringConstants.CAPACITY,
                    (double)request.getMemHeadroomInfo().getCapacity());
            if (request.getMemHeadroomInfo().getCapacity() != 0) {
                headroomData.put(StringConstants.MEM_EXHAUSTION, StringConstants.EXHAUSTION_DAYS,
                        (double)request.getMemHeadroomInfo().getDaysToExhaustion());
            }

            // Storage related headroom stats.
            headroomData.put(StringConstants.STORAGE_HEADROOM, StringConstants.USED,
                    (double)request.getStorageHeadroomInfo().getHeadroom());
            headroomData.put(StringConstants.STORAGE_HEADROOM, StringConstants.CAPACITY,
                    (double)request.getStorageHeadroomInfo().getCapacity());
            if (request.getStorageHeadroomInfo().getCapacity() != 0) {
                headroomData.put(StringConstants.STORAGE_EXHAUSTION, StringConstants.EXHAUSTION_DAYS,
                        (double)request.getStorageHeadroomInfo().getDaysToExhaustion());
            }
            // save computed headrooms tats to cluster_stats_by_day table
            for (Cell<String, String, Double> cell : headroomData.cellSet()) {
                ClusterStatsByDayRecord record = CLUSTER_STATS_BY_DAY.newRecord();
                record.setRecordedOn(today);
                record.setInternalName(Long.toString(request.getClusterId()));
                record.setPropertyType(cell.getRowKey());
                record.setPropertySubtype(cell.getColumnKey());
                record.setValue(cell.getValue());
                record.setSamples(1);
                clusterStatsByDayLoader.insert(record);
            }
            clusterStatsByDayLoader.flush(true);
            responseObserver.onNext(Stats.SaveClusterHeadroomResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (InterruptedException e) {
            logger.error("Failed to save cluster headroom data in database for cluster {}: {}",
                    request.getClusterId(), e);
            responseObserver.onError(Status.INTERNAL.withDescription(
                    "Failed to save cluster headroom data in database.")
                    .withCause(e)
                    .asException());
        }
    }

    /**
     * Gather stats from the PLAN topology history for the given topologyContextID. There will be
     * only one.
     * Package the stats up in a StatSnapshot response protobuf and pass it back via the chunking
     * response "onNext()" cal.
     *  @param responseObserver the chunking channel on which the response should be returned
     * @param topologyContextId the context id for the plan topology to look up
     * @param commodityRequests the commodities to include.
     * @param globalFilter the global filter to apply to all returned stats
     */
    private void returnPlanTopologyStats(StreamObserver<StatSnapshot> responseObserver,
                                         long topologyContextId,
                                         List<CommodityRequest> commodityRequests,
                                         @Nonnull GlobalFilter globalFilter) {
        StatSnapshot.Builder beforePlanStatSnapshotResponseBuilder = StatSnapshot.newBuilder();
        StatSnapshot.Builder afterPlanStatSnapshotResponseBuilder = StatSnapshot.newBuilder();
        List<MktSnapshotsStatsRecord> dbSnapshotStatsRecords;
        try {
            dbSnapshotStatsRecords = planStatsReader.getStatsRecords(
                    topologyContextId, commodityRequests, globalFilter);
        } catch (VmtDbException e) {
            throw new RuntimeException("Error fetching plan market stats for "
                    + topologyContextId, e);
        }
        for (MktSnapshotsStatsRecord statsDBRecord : dbSnapshotStatsRecords) {
            String relatedEntityType = Optional.ofNullable(statsDBRecord.getEntityType())
                .map(ApiEntityType::fromType)
                .map(ApiEntityType::apiStr)
                .orElse(null);
            StatsAccumulator statsValue = new StatsAccumulator();
            statsValue.record(statsDBRecord.getMinValue(), statsDBRecord.getAvgValue(),
                    statsDBRecord.getMaxValue());
            final StatRecord statResponseRecord = statRecordBuilder.buildStatRecord(
                    statsDBRecord.getPropertyType(),
                    statsDBRecord.getPropertySubtype(),
                    statsDBRecord.getCapacity() == null ? null : statsDBRecord.getCapacity()
                            .floatValue(),
                    null /* effective capacity*/,
                    relatedEntityType,
                    null /* producerId */,
                    statsValue.toStatValue(),
                    null /* commodityKey */,
                    // (Jan 29, 2020) Now we do populate (and rely on) the 'relation' data for
                    // individual plan entities coming from the Repository. However, we still don't
                    // have a use case for using this data in the History response, which is
                    // aggregated data for the entire plan. In this context, relation makes no sense.
                    null /*relation*/);

            // Group all records with property type name that start with "current" in one group
            // (before plan) and others in another group (after plan).
            if (statsDBRecord.getPropertyType().startsWith(StringConstants.STAT_PREFIX_CURRENT)) {
                beforePlanStatSnapshotResponseBuilder.addStatRecords(statResponseRecord);
            } else {
                afterPlanStatSnapshotResponseBuilder.addStatRecords(statResponseRecord);
            }
        }
        Optional<ScenariosRecord> planStatsRecord = historydbIO.getScenariosRecord(topologyContextId);
        if (planStatsRecord.isPresent()) {
            final long planTime = planStatsRecord.get().getCreateTime().getTime();
            final Timestamp updateTime = planStatsRecord.get().getUpdateTime();
            final long planEndTime = updateTime == null ? planTime : updateTime.getTime();

            beforePlanStatSnapshotResponseBuilder.setSnapshotDate(planTime);
            beforePlanStatSnapshotResponseBuilder.setStatEpoch(StatEpoch.PLAN_SOURCE);
            responseObserver.onNext(beforePlanStatSnapshotResponseBuilder.build());

            afterPlanStatSnapshotResponseBuilder.setSnapshotDate(planEndTime);
            afterPlanStatSnapshotResponseBuilder.setStatEpoch(StatEpoch.PLAN_PROJECTED);
            responseObserver.onNext(afterPlanStatSnapshotResponseBuilder.build());

            responseObserver.onCompleted();
        } else {
            responseObserver.onError(Status.NOT_FOUND.withDescription(
                    "Unable to find scenarios record for " + topologyContextId).asException());
        }

    }

    /**
     * Read the stats DB for the live market, stored in xxx_stats_yyy table, organized by
     * ServiceEntity type (e.g. pm_stats_yyy and vm_stats_yyy) and by time frame, e.g.
     * xxx_stats_latest, xxx_stats__by_hour, xxx_stats_by_day, xxx_stats_by_month.
     *
     * <p>If the 'entities' list is empty then this is a request for averaged stats from the full
     * live market.</p>
     *
     * <p>If there is an empty 'entities' list and a 'relatedEntityType' is given, then only
     * entities of that type are included in the averaged stats returned.</p>
     *
     * <p>If there are entity OIDs specified in the 'entities' list, then the 'relatedEntityType' is
     * ignored and only those entities listed are included in the stats averages returned.</p>
     *
     * @param statsFilter The stats filter to apply.
     * @param entities A list of service entity OIDs; an empty list implies full market, which
     *                 may be filtered based on relatedEntityType
     * @param globalFilter The global filter to apply to all commodities requested by the filter.
     * @return stream of StatSnapshots from live market
     * @throws VmtDbException if error writing to the db.
     */
    private Stream<StatSnapshot> getLiveMarketStats(@Nonnull final StatsFilter statsFilter,
                                                    @Nonnull Collection<Long> entities,
                                                    @Nonnull GlobalFilter globalFilter) throws VmtDbException {
        // get a full list of stats that satisfy this request, depending on the entity request
        final List<Record> statDBRecords;
        final boolean fullMarket = entities.isEmpty();
        if (fullMarket) {
            statDBRecords = liveStatsReader.getFullMarketStatsRecords(statsFilter, globalFilter);
        } else {
            if (!globalFilter.equals(GlobalFilter.getDefaultInstance())) {
                logger.warn("'global filter' ({}) is ignored when specific entities listed",
                        globalFilter);
            }
            statDBRecords = liveStatsReader.getRecords(
                    entities.stream()
                            .map(id -> Long.toString(id))
                            .collect(Collectors.toSet()), statsFilter);
        }

        // organize the stats DB records into StatSnapshots and return them to the caller
        final List<Builder> statSnapshots = statSnapshotCreator
            .createStatSnapshots(statDBRecords, fullMarket, statsFilter.getCommodityRequestsList())
            .collect(Collectors.toList());
        // All live market stats are considered historical for now
        statSnapshots.stream().forEach(statSnapshot -> statSnapshot.setStatEpoch(StatEpoch.HISTORICAL));
        return statSnapshots.stream().map(Builder::build);
    }

    /**
     * Delete stats associated with a plan.
     *
     * @param request PlanId/TopologyContextId which identifies the plan
     * @param responseObserver channel for sending the response {@link DeletePlanStatsResponse}
     *
     */
    @Override
    public void deletePlanStats(DeletePlanStatsRequest request,
                               StreamObserver<DeletePlanStatsResponse> responseObserver) {
        if (!request.hasTopologyContextId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Missing required parameter topologyContextId.")
                .asException());
            return;
        }

        try {
            final DeletePlanStatsResponse.Builder responseBuilder = DeletePlanStatsResponse.newBuilder();
            historydbIO.deletePlanStats(request.getTopologyContextId());
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (VmtDbException e) {
            responseObserver.onError(Status.INTERNAL.withDescription("Error deleting plan stats with id: " + request.getTopologyContextId())
                    .withCause(e)
                    .asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getStatsDataRetentionSettings(
                    GetStatsDataRetentionSettingsRequest request,
                    StreamObserver<Setting> responseObserver) {

        try {
            historydbIO.getStatsRetentionSettings()
                .forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (DataAccessException | VmtDbException e) {
            responseObserver.onError(
                Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setStatsDataRetentionSetting(
                    SetStatsDataRetentionSettingRequest request,
                    StreamObserver<SetStatsDataRetentionSettingResponse> responseObserver) {

        try {
            SetStatsDataRetentionSettingResponse.Builder response =
                    SetStatsDataRetentionSettingResponse.newBuilder();
            if (!request.hasRetentionSettingName() || !request.hasRetentionSettingValue()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Missing arguments. Expecting both retentionSettingName" +
                        "and retentionSettingValue")
                    .asException());
                return;
            }

            Optional<Setting> result =
                historydbIO.setStatsDataRetentionSetting(
                                request.getRetentionSettingName(),
                                request.getRetentionSettingValue());

            if (result.isPresent()) {
                responseObserver.onNext(
                    response.setNewSetting(result.get())
                        .build());
            } else {
                responseObserver.onNext(response.build());
            }
            responseObserver.onCompleted();
        } catch (VmtDbException e) {
            responseObserver.onError(
                Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getAuditLogDataRetentionSetting(
                    GetAuditLogDataRetentionSettingRequest request,
                    StreamObserver<GetAuditLogDataRetentionSettingResponse> responseObserver) {

        try {
            GetAuditLogDataRetentionSettingResponse response =
                GetAuditLogDataRetentionSettingResponse.newBuilder()
                    .setAuditLogRetentionSetting(historydbIO.getAuditLogRetentionSetting())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (DataAccessException | VmtDbException e) {
            responseObserver.onError(
                Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAuditLogDataRetentionSetting(
                    SetAuditLogDataRetentionSettingRequest request,
                    StreamObserver<SetAuditLogDataRetentionSettingResponse> responseObserver) {

        try {
            SetAuditLogDataRetentionSettingResponse.Builder response =
                    SetAuditLogDataRetentionSettingResponse.newBuilder();
            if (!request.hasRetentionSettingValue()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Missing retention value argument")
                    .asException());
                return;
            }

            Optional<Setting> result =
                historydbIO.setAuditLogRetentionSetting(
                                request.getRetentionSettingValue());

            if (result.isPresent()) {
                responseObserver.onNext(
                    response.setNewSetting(result.get())
                        .build());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to set the retention value")
                    .asException());
            }
        } catch (VmtDbException e) {
            responseObserver.onError(
                Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getEntityCommoditiesMaxValues(
                    GetEntityCommoditiesMaxValuesRequest request,
                    StreamObserver<EntityCommoditiesMaxValues> responseObserver) {
        try {
            if (request.getEntityType() < 0 || EntityDTO.EntityType.forNumber(request.getEntityType()) == null) {
                logger.error("Invalid entity type in getEntityCommoditiesMaxValues request for entity {}",
                    request.getEntityType());
                responseObserver.onCompleted();
                return;
            }
            if (request.getCommodityTypesCount() <= 0) {
                logger.error("Invalid commodities count {} in getEntityCommoditiesMaxValues request for entity {}",
                    request.getCommodityTypesCount(), request.getEntityType());
                responseObserver.onCompleted();
                return;
            }
            if (request.getCommodityTypesList().stream().anyMatch(commNum ->
                CommodityDTO.CommodityType.forNumber(commNum) == null)) {
                logger.error("Invalid commodities {} in getEntityCommoditiesMaxValues request for entity {}",
                    request.getCommodityTypesList(), request.getEntityType());
                responseObserver.onCompleted();
                return;
            }
            historydbIO.getEntityCommoditiesMaxValues(
                    request.getEntityType(),
                    request.getCommodityTypesList(),
                    request.hasIsBought() && request.getIsBought(),
                    request.getUuidsList(),
                    request.hasUseHistoricalCommBoughtLookbackDays()
                            && request.getUseHistoricalCommBoughtLookbackDays()
            ).forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (VmtDbException | SQLException e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    /**
     * Getting the request for commodities capacity and getting the response from the db.
     *
     * @param request   contains the entities and commodities to get capacity for
     * @param responseObserver  for streaming the response
     */
    @Override
    public void getEntityCommoditiesCapacityValues( GetEntityCommoditiesCapacityValuesRequest request,
            StreamObserver<GetEntityCommoditiesCapacityValuesResponse> responseObserver) {
        final Map<Integer, List<Integer>> commsByType = new HashMap<>();
        request.getTargetCommsList().forEach(entityAndComms -> {
            commsByType.put(entityAndComms.getEntityType(), entityAndComms.getCommodityTypeList());
        });

        commsByType.forEach((entityType, commsToGet) -> {
            // Get data from the daily table. Daily table rollups happen hourly.
            try {
                final List<GetEntityCommoditiesCapacityValuesResponse> entityCommoditiesCapacityDayValues
                    = historydbIO.getEntityCommoditiesCapacityValues(entityType, commsToGet, TimeUnit.DAYS);
                entityCommoditiesCapacityDayValues.forEach(responseObserver::onNext);
            } catch (VmtDbException | SQLException e) {
                responseObserver.onError(
                        Status.INTERNAL.withDescription(e.getMessage()).asException());
            }
        });
        responseObserver.onCompleted();
    }

    @Override
    public void getPercentileCounts(GetPercentileCountsRequest request,
                    StreamObserver<PercentileChunk> responseObserver) {
        try {
            percentileReader.processRequest(request, responseObserver);
        } catch (VmtDbException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public StreamObserver<PercentileChunk> setPercentileCounts(
                    StreamObserver<SetPercentileCountsResponse> responseObserver) {
        return new PercentileWriter(responseObserver, historydbIO, statsWriterExecutorService);
    }

    /**
     * The method reads all the system load related records for all cluster ids.
     *
     * @param request It contains a list of cluster ids.
     * @param responseObserver An indication that the request has completed.
     */
    @Override
    public void getSystemLoadInfo(
                    final SystemLoadInfoRequest request,
                    final StreamObserver<SystemLoadInfoResponse> responseObserver) {
        if (request.getClusterIdList().size() == 0) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("No clusterId available")
                .asException());
            return;
        }

        for (final long clusterId : request.getClusterIdList()) {
            try {
                // Chunking messages in order not to exceed gRPC message maximum size,
                // which is 4MB by default. The size of each record is around 0.1KB.
                Iterators.partition(
                        systemLoadReader.getSystemLoadInfo(Long.toString(clusterId)).iterator(),
                        systemLoadRecordsPerChunk)
                    .forEachRemaining(chunkRecords -> {
                        final SystemLoadInfoResponse.Builder chunkResponse =
                            SystemLoadInfoResponse.newBuilder().setClusterId(clusterId);
                        for (SystemLoadRecord record : chunkRecords) {
                            chunkResponse.addRecord(systemLoadRecordToStatsSystemLoadRecord(record));
                        }
                        responseObserver.onNext(chunkResponse.build());
                    });
            } catch (RuntimeException e) {
                logger.error("Encountered error for clusterId {}. Error: {}",
                    clusterId, e.getMessage());
                responseObserver.onNext(SystemLoadInfoResponse.newBuilder()
                    .setClusterId(clusterId)
                    .setError(e.getMessage())
                    .build());
            }
        }

        responseObserver.onCompleted();
    }

    /**
     * Convert a {@link SystemLoadRecord} to a {@link Stats.SystemLoadRecord} for gRPC usage.
     *
     * @param record a {@link SystemLoadRecord}
     * @return a {@link Stats.SystemLoadRecord}
     */
    private Stats.SystemLoadRecord systemLoadRecordToStatsSystemLoadRecord(
            @Nonnull final SystemLoadRecord record) {
        return Stats.SystemLoadRecord.newBuilder()
            .setClusterId(record.getSlice() != null ? Long.parseLong(record.getSlice()) : 0L)
            .setSnapshotTime(record.getSnapshotTime() != null ? record.getSnapshotTime().getTime() : -1)
            .setUuid(record.getUuid() != null ? Long.parseLong(record.getUuid()) : 0L)
            .setProducerUuid(record.getProducerUuid() != null ? Long.parseLong(record.getProducerUuid()) : 0L)
            .setPropertyType(record.getPropertyType() != null ? record.getPropertyType() : "")
            .setPropertySubtype(record.getPropertySubtype() != null ? record.getPropertySubtype() : "")
            .setCapacity(record.getCapacity() != null ? record.getCapacity() : -1.0)
            .setAvgValue(record.getAvgValue() != null ? record.getAvgValue() : -1.0)
            .setMinValue(record.getMinValue() != null ? record.getMinValue() : -1.0)
            .setMaxValue(record.getMaxValue() != null ? record.getMaxValue() : -1.0)
            .setRelationType(record.getRelation() != null ? record.getRelation().ordinal() : -1)
            .setCommodityKey(record.getCommodityKey() != null ? record.getCommodityKey() : "").build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getEntityIdToEntityTypeMapping(
            GetEntityIdToEntityTypeMappingRequest request,
            StreamObserver<GetEntityIdToEntityTypeMappingResponse> responseObserver) {

        try {
            GetEntityIdToEntityTypeMappingResponse response =
                    GetEntityIdToEntityTypeMappingResponse.newBuilder()
                        .putAllEntityIdToEntityTypeMap(
                                historydbIO.getEntityIdToEntityTypeMap())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (VmtDbException e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getMostRecentStat(GetMostRecentStatRequest request,
                                  StreamObserver<GetMostRecentStatResponse> responseObserver) {
        final ServerCallStreamObserver streamObserver =
            responseObserver instanceof ServerCallStreamObserver ?
                (ServerCallStreamObserver)responseObserver : null;
        final Optional<GetMostRecentStatResponse.Builder> response = mostRecentLiveStatReader
            .getMostRecentStat(request.getEntityType(), request.getCommodityName(),
                request.getProviderId(), streamObserver);
        final GetMostRecentStatResponse returnObject = response.map(stat -> {
            final String entityDisplayName = liveStatsReader
                .getEntityDisplayNameForId(stat.getEntityUuid());
            if (entityDisplayName != null) {
                stat.setEntityDisplayName(entityDisplayName);
            }
            return stat.build();
        }).orElse(GetMostRecentStatResponse.newBuilder().build());
        logger.debug("Most recent stat response for request {}, is {}", () -> request,
            () -> returnObject);
        responseObserver.onNext(returnObject);
        responseObserver.onCompleted();
    }
}

package com.vmturbo.history.stats;

import static com.vmturbo.components.common.stats.StatsUtils.collectCommodityNames;
import static org.apache.commons.lang.time.DateUtils.MILLIS_PER_DAY;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.exception.DataAccessException;
import org.springframework.util.CollectionUtils;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.prometheus.client.Summary;
import io.prometheus.client.Summary.Timer;

import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.stats.Stats;
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
import com.vmturbo.common.protobuf.stats.Stats.GetEntityCommoditiesMaxValuesRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityIdToEntityTypeMappingRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityIdToEntityTypeMappingResponse;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.GetStatsDataRetentionSettingsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GlobalFilter;
import com.vmturbo.common.protobuf.stats.Stats.MultiSystemLoadInfoRequest;
import com.vmturbo.common.protobuf.stats.Stats.MultiSystemLoadInfoResponse;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.SetAuditLogDataRetentionSettingRequest;
import com.vmturbo.common.protobuf.stats.Stats.SetAuditLogDataRetentionSettingResponse;
import com.vmturbo.common.protobuf.stats.Stats.SetStatsDataRetentionSettingRequest;
import com.vmturbo.common.protobuf.stats.Stats.SetStatsDataRetentionSettingResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoRequest;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.tables.records.ClusterStatsByDayRecord;
import com.vmturbo.history.schema.abstraction.tables.records.ClusterStatsByMonthRecord;
import com.vmturbo.history.schema.abstraction.tables.records.MktSnapshotsStatsRecord;
import com.vmturbo.history.schema.abstraction.tables.records.ScenariosRecord;
import com.vmturbo.history.schema.abstraction.tables.records.SystemLoadRecord;
import com.vmturbo.history.stats.live.SystemLoadReader;
import com.vmturbo.history.stats.projected.ProjectedStatsStore;
import com.vmturbo.history.stats.readers.LiveStatsReader;
import com.vmturbo.history.stats.readers.LiveStatsReader.StatRecordPage;
import com.vmturbo.history.stats.writers.SystemLoadWriter;

/**
 * Handles incoming RPC calls to History Component to return Stats information.
 * Calls a {@link LiveStatsReader} to fetch the desired data, and then converts the resulting
 * DB {@link Record}s into a protobuf response value.
 **/
public class StatsHistoryRpcService extends StatsHistoryServiceGrpc.StatsHistoryServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private final LiveStatsReader liveStatsReader;
    private final HistorydbIO historydbIO;
    private final PlanStatsReader planStatsReader;
    private final long realtimeContextId;
    private final ClusterStatsReader clusterStatsReader;
    private final ClusterStatsWriter clusterStatsWriter;

    private final ProjectedStatsStore projectedStatsStore;

    private final EntityStatsPaginationParamsFactory paginationParamsFactory;

    private final StatSnapshotCreator statSnapshotCreator;

    private final StatRecordBuilder statRecordBuilder;

    private final SystemLoadReader systemLoadReader;
    private final SystemLoadWriter systemLoadWriter;

    private static final Set<String> HEADROOM_STATS =
                    ImmutableSet.of(StringConstants.CPU_HEADROOM,
                            StringConstants.MEM_HEADROOM,
                            StringConstants.STORAGE_HEADROOM,
                            StringConstants.CPU_EXHAUSTION,
                            StringConstants.MEM_EXHAUSTION,
                            StringConstants.STORAGE_EXHAUSTION,
                            StringConstants.VM_GROWTH,
                            StringConstants.HEADROOM_VMS,
                            StringConstants.NUM_VMS,
                            StringConstants.NUM_HOSTS,
                            StringConstants.NUM_STORAGES,
                            StringConstants.VM_GROWTH);

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
                           @Nonnull final ClusterStatsWriter clusterStatsWriter,
                           @Nonnull final HistorydbIO historydbIO,
                           @Nonnull final ProjectedStatsStore projectedStatsStore,
                           @Nonnull final EntityStatsPaginationParamsFactory paginationParamsFactory,
                           @Nonnull final StatSnapshotCreator statSnapshotCreator,
                           @Nonnull final StatRecordBuilder statRecordBuilder,
                           @Nonnull final SystemLoadReader systemLoadReader,
                           @Nonnull final SystemLoadWriter systemLoadWriter) {
        this.realtimeContextId = realtimeContextId;
        this.liveStatsReader = Objects.requireNonNull(liveStatsReader);
        this.planStatsReader = Objects.requireNonNull(planStatsReader);
        this.clusterStatsReader = Objects.requireNonNull(clusterStatsReader);
        this.clusterStatsWriter = Objects.requireNonNull(clusterStatsWriter);
        this.historydbIO = Objects.requireNonNull(historydbIO);
        this.projectedStatsStore = Objects.requireNonNull(projectedStatsStore);
        this.paginationParamsFactory = Objects.requireNonNull(paginationParamsFactory);
        this.statSnapshotCreator = Objects.requireNonNull(statSnapshotCreator);
        this.statRecordBuilder = Objects.requireNonNull(statRecordBuilder);
        this.systemLoadReader = Objects.requireNonNull(systemLoadReader);
        this.systemLoadWriter = Objects.requireNonNull(systemLoadWriter);
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
                new HashSet<>(request.getCommodityNameList()))
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
            paginationParamsFactory.newPaginationParams(request.getPaginationParams())));
        responseObserver.onCompleted();
    }

    /**
     * Fetch a sequence of StatSnapshot's based on the time range and filters in the StatsRequest.
     * The stats may be taken from the live topology history or the plan topology history depending
     * on the ID in the request.
     * <p>
     * Unfortunately, we need to determine, by a lookup, which type of entity ID(s) we are
     * to gather stats for. This call has been overloaded (ultimately) by the External REST API,
     * which doesn't distinguish between stats requests for groups vs. individual SEs, live
     * vs. plan topologies, etc.
     * <p>
     * The 'entitiesList' in the {@link GetAveragedEntityStatsRequest} may be:
     * <ul>
     * <li>a single-element list containing the distinguished ID for the Real-Time Topology = "Market"
     * <li>a single-element list containing the OID of a plan topology - a numeric (long)
     * <li>a list of one or more {@link .ServiceEntityApiDTO} OIDs - these are
     * numeric (longs)
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
                        filter.getCommodityRequestsList());
            } else {
                // read from live topologies
                getLiveMarketStats(filter, entitiesList, request.getGlobalFilter())
                    .forEach(responseObserver::onNext);
                responseObserver.onCompleted();
            }

            timer.observeDuration();
        } catch (Exception e) {
            logger.error("Error getting stats snapshots for {}", request);
            logger.error("    ", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal Error fetching stats for: " + request + ", cause: "
                            + e.getMessage())
                    .asException());
        }
    }

    /**
     * Get the comparator that can be used to sort EntityStats according to the passed-in pagination
     * parameters. We compare the utilization of the stat (used / capacity).
     *
     * @param entityStats list of entity stats to get comparator for
     * @param commodity name of the stat to compare
     * @param ascending whether to create comparator in the ascending order or descending order
     * @return comparator used to sort EntityStats
     */
    private Comparator<EntityStats> getEntityStatsComparator(@Nonnull List<EntityStats> entityStats,
                                                             @Nonnull String commodity,
                                                             final boolean ascending) {
        // pre calculate the utilization for each entity, to reduce the calculation during sorting
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
     * Get the utilization of the given commodity from the list of snapshots. If no no snapshots,
     * or no capacity, or commodity is not found, return max/min if it's ascending/descending so
     * it is placed at the end after sorting.
     *
     * @param entityStats entityStats containing entity id and a list of snapshots
     * @param commodity name of the commodity to calculate utilization for
     * @param ascending whether or not the pagination is ascending or descending
     * @return utilization of the given commodity
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
            // must have capacity to calculate utilization
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
     * request. For each seed entity (like: DC), it aggregates stats on its derived entities
     * (like: PMs) and use it as the stats for the seed entity.
     *
     * <p>Currently, we only support getting stats from live topology history, since there is no use
     * case for plan topology. We can extend it to support plan topology history if necessary.
     *
     * <p>Note: This call is expected to be slower since we can't do pagination in DB level.
     *
     * @param entityGroupList EntityGroupList containing list of EnityGroups
     * @param statsFilter the requested stats names and filters
     * @param paginationParameters pagination parameters
     * @param responseObserver the chunking channel on which the response should be returned
     * @throws VmtDbException if there is an error interacting with the database
     */
    private void returnStatsForEntityGroups(@Nonnull EntityGroupList entityGroupList,
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
                .addAllEntityStats(entityStatsList).build());
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
            ? Integer.valueOf(paginationParams.getCursor()) : 0;
        final int endIndex = Math.min(entityStatsList.size(),
            startIndex + paginationParams.getLimit());

        final GetEntityStatsResponse.Builder response = GetEntityStatsResponse.newBuilder()
                .addAllEntityStats(entityStatsList.subList(startIndex, endIndex));

        // set nextCursor on pagination response
        final PaginationResponse.Builder paginationResponse = PaginationResponse.newBuilder();
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
                                @Nonnull StreamObserver<StatSnapshot> responseObserver) {
        final long clusterId = request.getClusterId();
        final StatsFilter filter = request.getStats();

        long now = System.currentTimeMillis();
        long startDate = (filter.hasStartDate()) ? filter.getStartDate() : now;
        long endDate = filter.hasEndDate()? filter.getEndDate() : now;
        if (startDate > endDate) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Invalid date range for retrieving cluster statistics.")
                    .asException());
            return;
        }
        final Set<String> commodityNames = collectCommodityNames(filter);

        try {
            Multimap<java.sql.Date, StatRecord> resultMap = HashMultimap.create();

            if (isUseMonthlyData(startDate, endDate)) {
                final List<ClusterStatsByMonthRecord> statDBRecordsByMonth =
                        clusterStatsReader.getStatsRecordsByMonth(clusterId, startDate, endDate, commodityNames);

                // Group records by date.
                for (ClusterStatsByMonthRecord record : statDBRecordsByMonth) {
                    resultMap.put(record.getRecordedOn(), createStatRecordForClusterStatsByMonth(record));
                }
            } else {
                // If the date range is shorter than a month, or a date range is not provided,
                // get the most recent snapshot from the cluster_stats_by_day table.
                final List<ClusterStatsByDayRecord> statDBRecordsByDay =
                        clusterStatsReader.getStatsRecordsByDay(clusterId, startDate, endDate, commodityNames);

                // For CPUHeadroom, MemHeadroom and StorageHeadroom "property type" we have
                // rows with "sub-property type" : used and capacity. Both of them have
                // to be merged to create one stat record with read used and capacity.
                Map<String, List<ClusterStatsByDayRecord>> headroomStats = statDBRecordsByDay.stream()
                    .filter(record -> HEADROOM_STATS.contains(record.getPropertyType()))
                    .collect(Collectors.groupingBy(ClusterStatsByDayRecord::getPropertyType));

                // Handle Headroom stats
                headroomStats
                    .forEach((propertyType, recordsOnDifferentDates) -> {
                        // GroupBy records based on date
                        recordsOnDifferentDates.stream().collect(Collectors.groupingBy(ClusterStatsByDayRecord::getRecordedOn))
                            .values().forEach(records -> {
                                if (CollectionUtils.isEmpty(records))  {
                                    return;
                                }
                                switch (records.size()) {
                                    // CPUExhaustion, MemExhaustion and StorageExhaustion expected to have one record each.
                                    case 1 :
                                        resultMap.put(records.get(0).getRecordedOn(),
                                                        createStatRecordForClusterStatsByDay(records.get(0)));
                                        break;
                                     // CPUHeadroom, MemHeadroom and StorageHeadroom expected to have two records each.
                                    case 2 :
                                        ClusterStatsByDayRecord firstRecord =  records.get(0);
                                        ClusterStatsByDayRecord secondRecord =  records.get(1);
                                        if (firstRecord.getPropertySubtype().equals(StringConstants.CAPACITY)) {
                                            resultMap.put(secondRecord.getRecordedOn(),
                                                createStatRecordForClusterStatsByDay(secondRecord,
                                                                firstRecord.getValue().floatValue()));
                                        } else {
                                            resultMap.put(secondRecord.getRecordedOn(),
                                                createStatRecordForClusterStatsByDay(firstRecord,
                                                                secondRecord.getValue().floatValue()));
                                        }
                                    break;
                                    // Print an error if none of the cases satisfied above.
                                    default:
                                        logger.error("Skipping entry of headroom records because of unexpected  "
                                                        + "records size : " + records.size());
                                        break;
                                }
                                // Remove headroom records because we have already processed them.
                                statDBRecordsByDay.removeAll(records);
                        });
                    });

                // Group records by date.
                for (ClusterStatsByDayRecord record : statDBRecordsByDay) {
                    resultMap.put(record.getRecordedOn(), createStatRecordForClusterStatsByDay(record));
                }
            }

            // Sort stats data by date.
            List<java.sql.Date> sortedStatsDates = Lists.newArrayList(resultMap.keySet());
            Collections.sort(sortedStatsDates);

            // A StatSnapshot will be created for each date.
            // Each snapshot may have several record types (e.g. "headroomVMs", "numVMs")
            for (java.sql.Date recordDate : sortedStatsDates) {
                StatSnapshot.Builder statSnapshotResponseBuilder = StatSnapshot.newBuilder();
                resultMap.get(recordDate).forEach(statSnapshotResponseBuilder::addStatRecords);
                statSnapshotResponseBuilder.setSnapshotDate(recordDate.getTime());
                responseObserver.onNext(statSnapshotResponseBuilder.build());
            }

            responseObserver.onCompleted();
        } catch (VmtDbException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("DB Error fetching stats for cluster " + clusterId)
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

    private StatRecord createStatRecordForClusterStatsByDay(ClusterStatsByDayRecord dbRecord, float capacity) {
        return statRecordBuilder.buildStatRecord(
                        dbRecord.getPropertyType(),
                        dbRecord.getPropertySubtype(),
                        capacity,
                        (Float)null,
                        null,
                        null,
                        dbRecord.getValue().floatValue(),
                        dbRecord.getValue().floatValue(),
                        dbRecord.getValue().floatValue(),
                        null,
                        dbRecord.getValue().floatValue(),
                        null);
    }
    /**
     * Helper method to create StatRecord objects, which nest inside StatSnapshot objects.
     *
     * @param dbRecord the database record to begin with
     * @return a newly initialized {@link StatRecord} protobuf
     */
    private StatRecord createStatRecordForClusterStatsByDay(ClusterStatsByDayRecord dbRecord) {
        return statRecordBuilder.buildStatRecord(
                dbRecord.getPropertyType(),
                dbRecord.getPropertySubtype(),
                (Float)null,
                (Float)null,
                null,
                null,
                dbRecord.getValue().floatValue(),
                dbRecord.getValue().floatValue(),
                dbRecord.getValue().floatValue(),
                null,
                dbRecord.getValue().floatValue(),
                null);
    }

    private StatRecord createStatRecordForClusterStatsByMonth(ClusterStatsByMonthRecord dbRecord) {
        return statRecordBuilder.buildStatRecord(
                dbRecord.getPropertyType(),
                dbRecord.getPropertySubtype(),
                (Float)null,
                (Float)null,
                null,
                null,
                dbRecord.getValue().floatValue(),
                dbRecord.getValue().floatValue(),
                dbRecord.getValue().floatValue(),
                null,
                dbRecord.getValue().floatValue(),
                null);
    }
    /**
     * Calculate a Cluster Stats Rollup, if necessary, and persist to the appropriate stats table.
     *
     * The request contains all the clusters to be rolled up. For each cluster in the request
     * we get the ID, and the ID's of the cluster members.
     *
     * @param request a {link @ClusterRollupRequest} containing the cluster DTOs to be persisted.
     * @param responseObserver an indication that the request has completed - there is no data
     *                         returned with this response.
     */
    @Override
    public void computeClusterRollup(Stats.ClusterRollupRequest request,
                                     StreamObserver<Stats.ClusterRollupResponse> responseObserver) {
        try {
            // Filter by type as an extra precaution.
            clusterStatsWriter.rollupClusterStats(request.getClustersToRollupList().stream()
                .filter(group -> group.getType().equals(Type.CLUSTER))
                .collect(Collectors.toMap(Group::getId, Group::getCluster)));
            responseObserver.onNext(Stats.ClusterRollupResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            // acatch and handle any internal exception so we return a useful error to caller
            logger.error("Internal exception rolling up clusters, request: " + request, e);
            responseObserver.onError(Status.INTERNAL.withDescription("Error rolling up clusters")
                    .withCause(e)
                    .asException());
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
        logger.info("Got request to save cluster headroom: {}", request);
        try {

            // Table represents : <PropertyType, SubPropertyType, Value>
            com.google.common.collect.Table<String, String, BigDecimal> headroomData =
                            HashBasedTable.create();

            headroomData.put(StringConstants.HEADROOM_VMS, StringConstants.HEADROOM_VMS,
                            BigDecimal.valueOf(request.getHeadroom()));
            headroomData.put(StringConstants.NUM_VMS, StringConstants.NUM_VMS,
                            BigDecimal.valueOf(request.getNumVMs()));
            headroomData.put(StringConstants.NUM_HOSTS, StringConstants.NUM_HOSTS,
                            BigDecimal.valueOf(request.getNumHosts()));
            headroomData.put(StringConstants.NUM_STORAGES, StringConstants.NUM_STORAGES,
                            BigDecimal.valueOf(request.getNumStorages()));
            headroomData.put(StringConstants.VM_GROWTH, StringConstants.VM_GROWTH,
                            BigDecimal.valueOf(request.getMonthlyVMGrowth()));

            // CPU related headroom stats.
            headroomData.put(StringConstants.CPU_HEADROOM, StringConstants.USED,
                            BigDecimal.valueOf(request.getCpuHeadroomInfo().getHeadroom()));
            headroomData.put(StringConstants.CPU_HEADROOM, StringConstants.CAPACITY,
                            BigDecimal.valueOf(request.getCpuHeadroomInfo().getCapacity()));
            headroomData.put(StringConstants.CPU_EXHAUSTION, StringConstants.EXHAUSTION_DAYS,
                            BigDecimal.valueOf(request.getCpuHeadroomInfo().getDaysToExhaustion()));

            // Memory related headroom stats.
            headroomData.put(StringConstants.MEM_HEADROOM, StringConstants.USED,
                            BigDecimal.valueOf(request.getMemHeadroomInfo().getHeadroom()));
            headroomData.put(StringConstants.MEM_HEADROOM, StringConstants.CAPACITY,
                            BigDecimal.valueOf(request.getMemHeadroomInfo().getCapacity()));
            headroomData.put(StringConstants.MEM_EXHAUSTION, StringConstants.EXHAUSTION_DAYS,
                            BigDecimal.valueOf(request.getMemHeadroomInfo().getDaysToExhaustion()));

            // Storage related headroom stats.
            headroomData.put(StringConstants.STORAGE_HEADROOM, StringConstants.USED,
                            BigDecimal.valueOf(request.getStorageHeadroomInfo().getHeadroom()));
            headroomData.put(StringConstants.STORAGE_HEADROOM, StringConstants.CAPACITY,
                            BigDecimal.valueOf(request.getStorageHeadroomInfo().getCapacity()));
            headroomData.put(StringConstants.STORAGE_EXHAUSTION, StringConstants.EXHAUSTION_DAYS,
                            BigDecimal.valueOf(request.getStorageHeadroomInfo().getDaysToExhaustion()));

            clusterStatsWriter.batchInsertClusterStatsByDayRecord(Long.valueOf(request.getClusterId()), headroomData);
            responseObserver.onNext(Stats.SaveClusterHeadroomResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (VmtDbException e) {
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
     */
    private void returnPlanTopologyStats(StreamObserver<StatSnapshot> responseObserver,
                                         long topologyContextId,
                                         List<CommodityRequest> commodityRequests) {
        StatSnapshot.Builder beforePlanStatSnapshotResponseBuilder = StatSnapshot.newBuilder();
        StatSnapshot.Builder afterPlanStatSnapshotResponseBuilder = StatSnapshot.newBuilder();
        List<MktSnapshotsStatsRecord> dbSnapshotStatsRecords;
        try {
            dbSnapshotStatsRecords = planStatsReader.getStatsRecords(
                    topologyContextId, commodityRequests);
        } catch (VmtDbException e) {
            throw new RuntimeException("Error fetching plan market stats for "
                    + topologyContextId, e);
        }
        for (MktSnapshotsStatsRecord statsDBRecord : dbSnapshotStatsRecords) {
            final StatRecord statResponseRecord = statRecordBuilder.buildStatRecord(
                    statsDBRecord.getPropertyType(),
                    statsDBRecord.getPropertySubtype(),
                    statsDBRecord.getCapacity() == null ? null : statsDBRecord.getCapacity()
                            .floatValue(),
                    null /* effective capacity*/,
                    null /* relatedEntityType */,
                    null /* producerId */,
                    statsDBRecord.getAvgValue().floatValue(),
                    statsDBRecord.getMinValue().floatValue(),
                    statsDBRecord.getMaxValue().floatValue(),
                    null /* commodityKey */,
                    statsDBRecord.getAvgValue().floatValue(), /* Since there is only one value,
                                                             set the total equal to the average.*/
                    // (Feb 3, 2017) Currently, unlike in the real-time market, UI
                    // doesn't use the 'relation' data for the plan results. Here, set the
                    // property as "plan" to indicate this record is for a plan result.
                    "plan" /*relation*/);

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

            beforePlanStatSnapshotResponseBuilder.setSnapshotDate(planTime);
            responseObserver.onNext(beforePlanStatSnapshotResponseBuilder.build());

            afterPlanStatSnapshotResponseBuilder.setSnapshotDate(planTime);
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
     * If the 'entities' list is empty then this is a request for averaged stats from the full
     * live market.
     *
     * If there is an empty 'entities' list and a 'relatedEntityType' is given, then only
     * entities of that type are included in the averaged stats returned.
     *
     * If there are entity OIDs specified in the 'entities' list, then the 'relatedEntityType' is
     * ignored and only those entities listed are included in the stats averages returned.
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
        return statSnapshotCreator.createStatSnapshots(statDBRecords, fullMarket,
            statsFilter.getCommodityRequestsList()).map(StatSnapshot.Builder::build);
    }

    /**
     * Delete stats associated with a plan.
     *
     * @param request PlanId/TopologyContextId which indentifies the plan
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
            if (request.getEntityTypesCount() <= 0) {
                logger.warn("Invalid entities count in request: {}", request.getEntityTypesCount());
                responseObserver.onCompleted();
                return;
            }
            for (Integer entityType : request.getEntityTypesList()) {
                historydbIO.getEntityCommoditiesMaxValues(entityType).forEach(responseObserver::onNext);
            }
            responseObserver.onCompleted();
        } catch (VmtDbException|SQLException e) {
            responseObserver.onError(
                Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    /**
     * The method reads all the system load related records for a slice.
     *
     * @param request It contains the id of the slice.
     * @param responseObserver An indication that the request has completed.
     */
    @Override
    public void getSystemLoadInfo(
                    SystemLoadInfoRequest request,
                    final StreamObserver<SystemLoadInfoResponse> responseObserver) {
        final SystemLoadInfoResponse.Builder responseBuilder = SystemLoadInfoResponse.newBuilder();
        final Stats.SystemLoadRecord.Builder recordBuilder = Stats.SystemLoadRecord.newBuilder();

        List<SystemLoadRecord> records = systemLoadReader.getSystemLoadInfo(Long.toString(request.getClusterId()));
        for (SystemLoadRecord record : records) {
            Stats.SystemLoadRecord statsRecord = recordBuilder
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
            responseBuilder.addRecord(statsRecord);
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    /**
     * The method reads all the system load related records for all cluster ids.
     *
     * @param request It contains a list of cluster ids.
     * @param responseObserver An indication that the request has completed.
     */
    @Override
    public void getMultiSystemLoadInfo(
                    MultiSystemLoadInfoRequest request,
                    final StreamObserver<MultiSystemLoadInfoResponse> responseObserver) {
        // For now we essentially call the individual system load info RPC multiple times.
        // In the future we can try to optimize this.
        for (long clusterId : request.getClusterIdList()) {
            final SystemLoadInfoRequest systemLoadInfoRequest =
                SystemLoadInfoRequest.newBuilder().setClusterId(clusterId).build();

            getSystemLoadInfo(systemLoadInfoRequest, new StreamObserver<SystemLoadInfoResponse>() {
                private final SetOnce<SystemLoadInfoResponse> response = new SetOnce<>();

                @Override
                public void onNext(final SystemLoadInfoResponse systemLoadInfoResponse) {
                    this.response.trySetValue(systemLoadInfoResponse);
                }

                @Override
                public void onError(final Throwable throwable) {
                    logger.error("Encountered error for clusterId {}. Error: {}",
                        clusterId, throwable.getMessage());
                    responseObserver.onNext(MultiSystemLoadInfoResponse.newBuilder()
                        .setClusterId(clusterId)
                        .setError(throwable.getMessage())
                        .build());
                }

                @Override
                public void onCompleted() {
                    final MultiSystemLoadInfoResponse.Builder builder =
                        MultiSystemLoadInfoResponse.newBuilder().setClusterId(clusterId);

                    response.getValue()
                        .map(SystemLoadInfoResponse::getRecordList)
                        .ifPresent(builder::addAllRecord);

                    responseObserver.onNext(builder.build());
                }
            });
        }

        responseObserver.onCompleted();
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
}

package com.vmturbo.api.component.external.api.service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.grpc.StatusRuntimeException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.ExceptionMapper;
import com.vmturbo.api.component.external.api.mapper.MarketMapper;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.MagicScopeGateway;
import com.vmturbo.api.component.external.api.util.stats.PaginatedStatsExecutor;
import com.vmturbo.api.component.external.api.util.stats.PlanEntityStatsFetcher;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryExecutor;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IStatsService;
import com.vmturbo.api.utils.EncodingUtil;
import com.vmturbo.api.utils.UrlsHelp;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.EntityStatsOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Service implementation of Stats
 **/
public class StatsService implements IStatsService {

    /**
     * Market constant o match UI request, also used in test case
     */
    public static final String MARKET = "Market";

    /**
     * Cloud service constant to match UI request, also used in test cases
     */
    public static final String CLOUD_SERVICE = "cloudService";

    private static Logger logger = LogManager.getLogger(StatsService.class);

    private final StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub statsServiceRpc;

    private final PlanServiceBlockingStub planRpcService;

    private final StatsMapper statsMapper;

    private final GroupServiceBlockingStub groupServiceRpc;

    private final MagicScopeGateway magicScopeGateway;

    private final UserSessionContext userSessionContext;

    private final UuidMapper uuidMapper;

    private final StatsQueryExecutor statsQueryExecutor;

    /**
     * For fetching plan entities and their related stats.
     */
    private final PlanEntityStatsFetcher planEntityStatsFetcher;

    private final PaginatedStatsExecutor paginatedStatsExecutor;

    // CLUSTER_STATS is a collection of the cluster-headroom stats calculated from nightly plans.
    // TODO: we should share the enum instead of keeping a separate copy.
    private static final Set<String> CLUSTER_EXCLUSIVE_STATS = ImmutableSet.of(
                    StringConstants.CPU_HEADROOM,
                    StringConstants.MEM_HEADROOM,
                    StringConstants.STORAGE_HEADROOM,
                    StringConstants.TOTAL_HEADROOM,
                    StringConstants.CPU_EXHAUSTION,
                    StringConstants.MEM_EXHAUSTION,
                    StringConstants.STORAGE_EXHAUSTION,
                    StringConstants.HEADROOM_VMS,
                    StringConstants.VM_GROWTH);

    // stats that can be retrieved as cluster stats, but may also be available on other entities.
    private static final Set<String> CLUSTER_NONEXCLUSIVE_STATS = ImmutableSet.of(
                    StringConstants.CPU,
                    StringConstants.MEM,
                    StringConstants.NUM_SOCKETS,
                    StringConstants.NUM_CPUS,
                    StringConstants.NUM_VMS,
                    StringConstants.NUM_HOSTS,
                    StringConstants.HOST,
                    StringConstants.NUM_STORAGES);

    /**
     * Map Entity types to be expanded to the RelatedEntityType to retrieve. For example,
     * replace requests for stats for a DATACENTER entity with the PHYSICAL_MACHINEs
     * in that DATACENTER.
     */
    private static final Map<ApiEntityType, ApiEntityType> ENTITY_TYPES_TO_EXPAND = ImmutableMap.of(
            ApiEntityType.DATACENTER, ApiEntityType.PHYSICAL_MACHINE
    );

    StatsService(@Nonnull final StatsHistoryServiceBlockingStub statsServiceRpc,
                 @Nonnull final PlanServiceBlockingStub planRpcService,
                 @Nonnull final StatsMapper statsMapper,
                 @Nonnull final GroupServiceBlockingStub groupServiceRpc,
                 @Nonnull final MagicScopeGateway magicScopeGateway,
                 @Nonnull final UserSessionContext userSessionContext,
                 @Nonnull final UuidMapper uuidMapper,
                 @Nonnull final StatsQueryExecutor statsQueryExecutor,
                 @Nonnull final PlanEntityStatsFetcher planEntityStatsFetcher,
                 @Nonnull final PaginatedStatsExecutor paginatedStatsExecutor) {
        this.statsServiceRpc = Objects.requireNonNull(statsServiceRpc);
        this.planRpcService = planRpcService;
        this.groupServiceRpc = Objects.requireNonNull(groupServiceRpc);
        this.statsMapper = Objects.requireNonNull(statsMapper);
        this.magicScopeGateway = Objects.requireNonNull(magicScopeGateway);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
        this.statsQueryExecutor = Objects.requireNonNull(statsQueryExecutor);
        this.planEntityStatsFetcher = Objects.requireNonNull(planEntityStatsFetcher);
        this.paginatedStatsExecutor = Objects.requireNonNull(paginatedStatsExecutor);
    }

    /**
     * Create a simple response object containing the HATEOS links for the /stats service.
     *
     * @return a simple {@link BaseApiDTO} decorated with the HATEOS links for the /stats service.
     * @throws Exception if there's a problem constructing a URL
     */
    @Override
    public BaseApiDTO getStats() throws Exception {
        BaseApiDTO dto = new BaseApiDTO();

        UrlsHelp.setStatsHelp(dto);

        return dto;
    }

    /**
     * Provides a default stat period, to be used if no period is specified in a request
     * This default stat period includes "current" stats, i.e. no historical stats and no projections
     *
     * @return a default stat period
     */
    private static final StatPeriodApiInputDTO getDefaultStatPeriodApiInputDto() {
        return new StatPeriodApiInputDTO();
    }

    /**
     * Return stats for an Entity (ServiceEntity or Group) and a uuencoded
     * {@link StatPeriodApiInputDTO} query parameter which modifies the stats search. A group is
     * expanded and the stats averaged over the group contents.
     *
     * @param uuid         unique ID of the Entity for which the stats should be gathered
     * @param encodedQuery a uuencoded structure for the {@link StatPeriodApiInputDTO} to modify the
     *                     stats search
     * @return a List of {@link StatSnapshotApiDTO} responses containing the time-based stats
     * snapshots.
     * @throws Exception if there's an error enconding / deconding the string
     */
    @Override
    public List<StatSnapshotApiDTO> getStatsByEntityUuid(String uuid, String encodedQuery)
            throws Exception {

        StatPeriodApiInputDTO inputDto;
        if (encodedQuery != null && !encodedQuery.isEmpty()) {
            String jsonObject = EncodingUtil.decode(encodedQuery);
            ObjectMapper jsonMapper = new ObjectMapper();
            inputDto = jsonMapper.readValue(jsonObject, StatPeriodApiInputDTO.class);
        } else {
            inputDto = getDefaultStatPeriodApiInputDto();
        }

        return getStatsByEntityQuery(uuid, inputDto);
    }

    /**
     * Return stats for a {@link ServiceEntityApiDTO} given the UUID and an input
     * {@link StatPeriodApiInputDTO} object which modifies the stats search.
     * <p>
     * Note that the ServiceEntity may be a group. In that case, we expand the given UUID into
     * a list of ServiceEntity UUID's, and the results are averaged over the ServiceEntities
     * in the expanded list.
     * <p>
     * Some ServiceEntities, either in the input or as a result of group expansion, require
     * further expansion. For example, a DataCenter entity is replaced by the PMs in the
     * DataCenter. We use the Supply Chain fetcher to calculate that expansion.
     * <p>
     * The inputDto object can include a list of "filters" which are keys to search for records
     * in the statistics tables. (e.g. numHosts) For the purpose of getting
     * statistics before and after a plan execution, the stats values for both before and after
     * a plan execution should be returned. In the database, we denote the "before plan" value by
     * having a "current" prefix.  (e.g. currentNumHosts), and the projected value without the
     * prefix (e.g. numHosts). The UI will only send in one "filter" called "numHost". To work with
     * this UI requirement, we add a filter with the "current" prefix for each of the filters.
     * (e.g. for numHosts, we will add currentNumHosts.)
     * <p>
     * The results will be ordered such that projected stats come at the end of the list.
     *
     * @param originalUuid     the UUID of either a single ServiceEntity or a group.
     * @param inputDto the parameters to further refine this search.
     * @return a list of {@link StatSnapshotApiDTO}s one for each ServiceEntity in the expanded list,
     * with projected/future stats coming at the end of the list.
     */
    @Override
    public List<StatSnapshotApiDTO> getStatsByEntityQuery(String originalUuid, StatPeriodApiInputDTO inputDto)
            throws Exception {
        try {
            return getStatsByEntityQueryInternal(originalUuid, inputDto);
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
    }

    private List<StatSnapshotApiDTO> getStatsByEntityQueryInternal(
            String uuid, StatPeriodApiInputDTO inputDto) throws Exception {
        final ApiId apiId = uuidMapper.fromUuid(uuid);

        logger.debug("fetch stats for {} requestInfo: {}", uuid, inputDto);

        // Create a default Stat period, if one is not specified in the request
        if (inputDto == null) {
            inputDto = getDefaultStatPeriodApiInputDto();
        }

        return statsQueryExecutor.getAggregateStats(apiId, inputDto);
    }

    private Optional<PlanInstance> getRequestedPlanInstance(@Nonnull final StatScopesApiInputDTO inputDto) {
        // plan stats request must be the only uuid in the scopes list
        if (inputDto.getScopes().size() != 1) {
            return Optional.empty();
        }
        // check for a plan uuid
        String scopeUuid = inputDto.getScopes().iterator().next();
        return getRequestedPlanInstance(scopeUuid);
    }

    private Optional<PlanInstance> getRequestedPlanInstance(@Nonnull String possiblePlanUuid) {
        long oid;
        try {
            oid = Long.valueOf(possiblePlanUuid);
        } catch (NumberFormatException e) {
            // not a number, so cannot be a plan UUID
            return Optional.empty();
        }

        // fetch plan from plan orchestrator
        try {
            PlanDTO.OptionalPlanInstance planInstanceOptional =
                planRpcService.getPlan(PlanDTO.PlanId.newBuilder()
                    .setPlanId(oid)
                    .build());
            if (!planInstanceOptional.hasPlanInstance()) {
                return Optional.empty();
            }

            return Optional.of(planInstanceOptional.getPlanInstance());
        } catch (StatusRuntimeException e) {
            logger.error("Unable to reach plan orchestrator. Error: {}." +
                " Assuming ID {} is not a plan", e.getMessage(), possiblePlanUuid);
            return Optional.empty();
        }
    }

    /**
     * Return per-entity stats from a plan topology.
     * This is a helper method for
     * {@link StatsService#getStatsByUuidsQuery(StatScopesApiInputDTO, EntityStatsPaginationRequest)}.
     *
     * If the 'inputDto.period.startDate is before "now", then the stats returned will include stats
     * for the Plan Source Topology. If the 'inputDto.period.startDate is after "now", then  the
     * stats returned will include stats for the Plan Projected Topology.
     */
    @Nonnull
    private EntityStatsPaginationResponse getPlanEntityStats(
                @Nonnull final PlanInstance planInstance,
                @Nonnull final StatScopesApiInputDTO inputDto,
                @Nonnull final EntityStatsPaginationRequest paginationRequest) {
        // Clear the plan ID from the scope, so we treat it as an entity in the plan
        // topology stats request.
        inputDto.getScopes().clear();

        return planEntityStatsFetcher.getPlanEntityStats(planInstance, inputDto, paginationRequest);
    }

    @Nonnull
    private EntityStatsPaginationResponse getClusterEntityStats(
                @Nonnull final StatScopesApiInputDTO inputDto,
                @Nonnull final EntityStatsPaginationRequest paginationRequest)
            throws Exception {
        // translate pagination parameters
        final PaginationParameters.Builder paginationParametersBuilder = PaginationParameters.newBuilder();
        paginationRequest.getCursor()
                .map(paginationParametersBuilder::setCursor)
                .orElseGet(() -> paginationParametersBuilder.setCursor("0"));
        paginationRequest.getOrderByStat().ifPresent(orderBy ->
            setEntityStatsOrderByToPagination(paginationParametersBuilder, orderBy));
        paginationParametersBuilder
                .setAscending(paginationRequest.isAscending())
                .setLimit(paginationRequest.getLimit());

        // translate data request
        final StatsFilter statsFilter = statsMapper.newPeriodStatsFilter(inputDto.getPeriod());

        // if there is no order-by in the pagination request
        // we use the first stat requested in statsFilter
        if (!paginationParametersBuilder.hasOrderBy()) {
            statsFilter.getCommodityRequestsList().stream()
                .filter(CommodityRequest::hasCommodityName)
                .map(CommodityRequest::getCommodityName)
                .findFirst()
                .map(orderBy ->
                        setEntityStatsOrderByToPagination(paginationParametersBuilder, orderBy))
                .orElseThrow(() -> new IllegalArgumentException(
                                        "Cluster stats request does not specify stats to be fetched"));
        }

        // create internal request builder
        // add translation of data request
        // add pagination parameters
        final ClusterStatsRequest.Builder requestBuilder =
                ClusterStatsRequest.newBuilder()
                    .setStats(statsFilter)
                    .setPaginationParams(paginationParametersBuilder);

        // information about the scope of the call and the scope of the user
        final boolean marketScoped = !UuidMapper.hasLimitedScope(inputDto.getScopes());
        final boolean userScoped = userSessionContext.isUserScoped();

        // if the scope is not the market, fetch cluster information
        final Map<Long, GroupDefinition> clusters = new HashMap<>();
        if (!marketScoped) {
            // explicit scope: we bring the groups with all requested ids
            // and ensure that they are either clusters or groups of clusters

            // this variable holds the ids of the static members
            // of all encountered groups of clusters
            final Set<Long> idsOfMembers = new HashSet<>();
            final Set<Long> explicitlyRequestedIds = new HashSet<>(inputDto.getScopes().stream()
                                                                        .map(i -> Long.valueOf(i))
                                                                        .collect(Collectors.toSet()));
            // fetch related groups
            groupServiceRpc.getGroups(GetGroupsRequest.newBuilder()
                                        .setGroupFilter(GroupFilter.newBuilder()
                                                            .addAllId(explicitlyRequestedIds))
                                        .build())
                .forEachRemaining(grouping -> {
                    if (GroupProtoUtil.isCluster(grouping)) {
                        clusters.put(grouping.getId(), grouping.getDefinition());
                    } else if (GroupProtoUtil.isGroupOfClusters(grouping)) {
                        idsOfMembers.addAll(GroupProtoUtil.getAllStaticMembers(
                                                grouping.getDefinition()));
                    } else {
                        throw new IllegalArgumentException(
                            "The group with id " + grouping.getId()
                                    + " is neither a cluster nor a group of clusters");
                    }
                    explicitlyRequestedIds.remove(grouping.getId());
                });

            // if not all ids in the original scope have been found
            // then throw an exception
            if (!explicitlyRequestedIds.isEmpty()) {
                throw new UnknownObjectException("Cannot find the group(s) with ids: "
                                                        + explicitlyRequestedIds.toString());
            }

            // if groups of clusters were found, bring their members
            if (!idsOfMembers.isEmpty()) {
                groupServiceRpc.getGroups(GetGroupsRequest.newBuilder()
                                            .setGroupFilter(GroupFilter.newBuilder()
                                                                .addAllId(idsOfMembers))
                                            .build())
                        .forEachRemaining(grouping -> {
                            if (GroupProtoUtil.isCluster(grouping)) {
                                clusters.put(grouping.getId(), grouping.getDefinition());
                            } else {
                                throw new IllegalArgumentException(
                                    "The group with id " + grouping.getId() + " is not a cluster");
                            }
                        });
            }
        }

        // if the scope of the call is the whole market, but the user scope is limited,
        // we fetch all the clusters that the user is allowed to access
        // this set of clusters is going to be our scope in the internal call
        if (userScoped && marketScoped) {
            GroupProtoUtil.CLUSTER_GROUP_TYPES.stream()
                .map(type -> groupServiceRpc.getGroups(GetGroupsRequest.newBuilder()
                                                            .setGroupFilter(GroupFilter.newBuilder()
                                                                                .setGroupType(type))
                                                            .build()))
                .forEach(it -> it.forEachRemaining(g -> clusters.put(g.getId(), g.getDefinition())));
        }

        // if the scope is not the whole market, introduce scope to the request
        if (!marketScoped || userScoped) {
            requestBuilder.addAllClusterIds(clusters.keySet());
        }

        // run the service
        final ClusterStatsResponse response;
        try {
            response = statsServiceRpc.getClusterStats(requestBuilder.build());
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }

        // if the scope was the market,
        // now it is the time to fetch info about the relevant groups
        if (marketScoped && !userScoped) {
            groupServiceRpc.getGroups(GetGroupsRequest.newBuilder()
                                        .setGroupFilter(GroupFilter.newBuilder()
                                                            .addAllId(response.getSnapshotsList().stream()
                                                                            .map(EntityStats::getOid)
                                                                            .collect(Collectors.toSet())))
                                                    .build())
                    .forEachRemaining(g -> clusters.put(g.getId(), g.getDefinition()));
        }

        // construct the response
        final PaginationResponse paginationResponse = response.getPaginationResponse();
        final List<EntityStatsApiDTO> results =
            response.getSnapshotsList().stream()
                .map(entityStats -> {
                        final EntityStatsApiDTO entityStatsApiDTO = new EntityStatsApiDTO();
                        final GroupDefinition cluster = clusters.get(entityStats.getOid());
                        entityStatsApiDTO.setUuid(Long.toString(entityStats.getOid()));
                        entityStatsApiDTO.setStats(entityStats.getStatSnapshotsList().stream()
                                                        .limit(1)
                                                        .map(statsMapper::toStatSnapshotApiDTO)
                                                        .collect(Collectors.toList()));
                        if (cluster != null) {
                            entityStatsApiDTO.setDisplayName(cluster.getDisplayName());
                            switch (cluster.getType()) {
                                case COMPUTE_HOST_CLUSTER:
                                    entityStatsApiDTO.setClassName(StringConstants.CLUSTER);
                                    break;
                                case COMPUTE_VIRTUAL_MACHINE_CLUSTER:
                                    entityStatsApiDTO.setClassName(StringConstants.VIRTUAL_MACHINE_CLUSTER);
                                    break;
                                case STORAGE_CLUSTER:
                                    entityStatsApiDTO.setClassName(StringConstants.STORAGE_CLUSTER);
                                    break;
                                default:
                                    logger.error("Cluster stats contain entry about non-cluster");
                            }
                            entityStatsApiDTO.setEnvironmentType(
                                    cluster.getType() == GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER
                                            ? com.vmturbo.api.enums.EnvironmentType.HYBRID
                                            : com.vmturbo.api.enums.EnvironmentType.ONPREM);
                        } else {
                            logger.error("Could not get information about cluster with id "
                                                + entityStats.getOid());
                            entityStatsApiDTO.setDisplayName(Long.toString(entityStats.getOid()));
                        }
                        return entityStatsApiDTO;
                })
                .collect(Collectors.toList());
        if (paginationResponse.hasNextCursor()) {
            return paginationRequest.nextPageResponse(results, paginationResponse.getNextCursor(),
                                                      paginationResponse.getTotalRecordCount());
        } else {
            return paginationRequest.finalPageResponse(results, paginationResponse.getTotalRecordCount());
        }
    }

    // this auxiliary function returns a dummy value
    // the reason is that we want to use it in an Optional/map/orElse construct
    private boolean setEntityStatsOrderByToPagination(@Nonnull PaginationParameters.Builder pagination,
                                                      @Nonnull String orderBy) {
        pagination.setOrderBy(OrderBy.newBuilder()
                                .setEntityStats(EntityStatsOrderBy.newBuilder()
                                                    .setStatName(orderBy)));
        return true;
    }

    /**
     * Return stats for multiple entities by expanding the scopes field
     * of the {@link StatScopesApiInputDTO}.
     *
     * The input can request stats for entities, groups and/or clusters. The results will vary
     * depending on what stats are being requested, and whether or not they are cluster-level
     * stats or not.
     *
     * 1) If any "headroom stats" are requested, then this method will return a list of clusters
     * with their headroom stats. All ids in the request scopes are expected to be cluster ids, and
     * this method will log a warning if a non-cluster is requested when fetching headroom stats.
     * 2) If no "headroom stats" are requested, then this method will expand any group/cluster
     * id's in the scopes list into their member entities, and fetch stats on the entity-level. The
     * result will contain a combination of all entities in the scope list and all members of any
     * groups or clusters in the scope list, as well as all the requested (non-headroom) stats for
     * each entity.
     *
     * TODO: this conversion does not (yet) handle the "realtimeMarketReference" field of the
     * EntityStatsApiDTO.
     *
     * TODO: this method should probably be refactored (or broken into two) so the headroom and
     * cluster-stats are handled separately, rather than mixed into the same method.
     *
     * @param inputDto contains the query arguments; the 'scopes' property indicates a
     *                 list of items to query - might be Group, Cluster, or ServiceEntity.
     * @param paginationRequest the pagination request object
     * @return a list of {@link EntityStatsApiDTO} objects representing the entities in the search
     * with the commodities values filled in
     */
    @Override
    public EntityStatsPaginationResponse getStatsByUuidsQuery(@Nonnull StatScopesApiInputDTO inputDto,
                                                  EntityStatsPaginationRequest paginationRequest)
            throws Exception {
        try {
            return getStatsByUuidsQueryInternal(inputDto, paginationRequest);
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
    }

    private EntityStatsPaginationResponse getStatsByUuidsQueryInternal(
            @Nonnull StatScopesApiInputDTO inputDto,
            @Nonnull EntityStatsPaginationRequest paginationRequest)
                throws Exception {
        // All scopes must enter the magic gateway before they can proceed!
        inputDto.setScopes(magicScopeGateway.enter(inputDto.getScopes()));

        final Optional<PlanInstance> planInstance = getRequestedPlanInstance(inputDto);
        if (planInstance.isPresent()) {
            return getPlanEntityStats(planInstance.get(), inputDto, paginationRequest);
        } else if (isClusterStatsRequest(inputDto)) {
            return getClusterEntityStats(inputDto, paginationRequest);
        } else {
            return paginatedStatsExecutor.getLiveEntityStats(inputDto, paginationRequest);
        }
    }

    /**
     * Does the input request contain any cluster stats?
     *
     * @param inputDto the {@link StatScopesApiInputDTO} to check.
     * @return true if any of the input stats are headroom stats, false otherwise.
     */
    @VisibleForTesting
    protected boolean isClusterStatsRequest(StatScopesApiInputDTO inputDto) {
        if (inputDto.getPeriod() == null
                || CollectionUtils.isEmpty(inputDto.getPeriod().getStatistics())) {
            return false;
        }

        List<String> statsRequested = inputDto.getPeriod().getStatistics().stream()
                .map(StatApiInputDTO::getName)
                .collect(Collectors.toList());
        List<String> scopes = inputDto.getScopes();

        // the request MIGHT want cluster stats if it requests any stats available for clusters
        // but that are not exclusive to clusters. If we detect any of these, then we will check
        // the inputs to see if they are clusters or not.
        boolean mightContainClusterStats = false;
        // we'll set this if we find any non-cluster stats in the set. Retrieval of these stats would
        // require expansion of the cluster since they are definitely not available on the cluster
        // directly. If we find any of these, and there are no cluster-exclusive stats, we will
        // mark the whole request as not containing cluster stats so that the host stat aggregation
        // will take place. (ideally, we could handle both cluster-direct stats and cluster-derived
        // stats together in the same call, but that will require a larger refactoring)
        boolean containsNonClusterStats = false;
        if (CollectionUtils.isEmpty(statsRequested)) {
            return false;
        }
        // check for any cluster stats
        for (String stat : statsRequested) {
            if (CLUSTER_EXCLUSIVE_STATS.contains(stat)) {
                // this stat is only available on clusters -- this request is DEFINITELY a
                // cluster stats request.
                return true;
            }
            if (CLUSTER_NONEXCLUSIVE_STATS.contains(stat)) {
                // this stat is available on clusters. So MAYBE this request is a cluster stats
                // request.
                mightContainClusterStats = true;
            } else {
                containsNonClusterStats = true;
            }
        }
        // if the request is asking for ANY stats that are not available directly on the cluster,
        // mark this request as "no cluster stats"
        if (containsNonClusterStats) {
            return false;
        }
        // if the request is for stats that MIGHT be cluster stats, check if the request scope
        // actually contains clusters. If it does, then we'll mark this as a cluster stats request.
        if (mightContainClusterStats) {
            if (StringUtils.isNotEmpty(inputDto.getRelatedType())
                    && inputDto.getRelatedType().equals(StringConstants.CLUSTER)) {
                return true;
            }

            if (!CollectionUtils.isEmpty(scopes)) {
                for (String scopeEntry : scopes) {
                    try {
                        long oid = Long.valueOf(scopeEntry);
                        ApiId apiId = uuidMapper.fromOid(oid);
                        if (apiId.isGroup()) {
                            Optional<GroupType> groupType = apiId.getGroupType();
                            if (groupType.isPresent()
                                    && GroupProtoUtil.CLUSTER_GROUP_TYPES.contains(groupType.get())) {
                                return true;
                            }
                        }
                    } catch (NumberFormatException e) {
                        // not a number, this is not a cluster.
                        return false;
                    }
                }
            }
        }

        // this was not a cluster stats request after all.
        return false;
    }

    /**
     * Is the {@link StatScopesApiInputDTO} a market-scoped request?
     *
     * @param inputDto the request object ot check
     * @return true, if the request is found to be a market-scoped rqeuest. false otherwise.
     */
    public static boolean isMarketScoped(StatScopesApiInputDTO inputDto) {
        return (inputDto.getScopes().isEmpty() || inputDto.getScopes().stream()
                .anyMatch(scope -> scope.equals(MarketMapper.MARKET)));
    }

    /**
     * Check if group is a temporary group with global scope. And if temp group entity need to expand,
     * it should use expanded entity type instead of group entity type. If it is a temporary group
     * with global scope, we can speed up query using pre-aggregate market stats table.
     *
     * @param groupOptional a optional of group need to check.
     * @return return a optional of entity type, if input group is a temporary global scope group,
     *         otherwise return empty option.
     */
    @VisibleForTesting
    protected Optional<Integer> getGlobalTempGroupEntityType(@Nonnull final Optional<Grouping> groupOptional) {
        if (!groupOptional.isPresent() || !groupOptional.get().getDefinition().getIsTemporary()
                        || !groupOptional.get().getDefinition().hasStaticGroupMembers()
                        || groupOptional.get().getDefinition()
                                .getStaticGroupMembers().getMembersByTypeCount() != 1) {
            return Optional.empty();
        }

        final GroupDefinition tempGroup = groupOptional.get().getDefinition();
        final boolean isGlobalTempGroup = tempGroup.hasOptimizationMetadata()
                        && tempGroup.getOptimizationMetadata().getIsGlobalScope()
                // the global scope optimization.
                && (!tempGroup.getOptimizationMetadata().hasEnvironmentType()
                            || tempGroup.getOptimizationMetadata().getEnvironmentType()
                                    == EnvironmentType.HYBRID);

        int entityType = tempGroup.getStaticGroupMembers()
                        .getMembersByType(0)
                        .getType()
                        .getEntity();

        // if it is global temp group and need to expand, should return target expand entity type.
        if (isGlobalTempGroup && ENTITY_TYPES_TO_EXPAND.containsKey(
                ApiEntityType.fromType(entityType))) {
            return Optional.of(ENTITY_TYPES_TO_EXPAND.get(
                ApiEntityType.fromType(entityType)).typeNumber());
        } else if (isGlobalTempGroup) {
            // if it is global temp group and not need to expand.
            return Optional.of(entityType);
        } else {
            return Optional.empty();
        }
    }
}

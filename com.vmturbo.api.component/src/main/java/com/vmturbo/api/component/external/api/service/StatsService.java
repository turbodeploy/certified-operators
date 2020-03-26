package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.grpc.StatusRuntimeException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
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
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IStatsService;
import com.vmturbo.api.utils.EncodingUtil;
import com.vmturbo.api.utils.UrlsHelp;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
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
     * Cloud target constant to match UI request, also used in test case
     */
    public static final String TARGET = "target";

    /**
     * Cloud service constant to match UI request, also used in test cases
     */
    public static final String CLOUD_SERVICE = "cloudService";

    /**
     * Cloud service provider constant to match UI request, also used in test cases
     */
    public static final String CSP = "CSP";

    private static final String COSTCOMPONENT = "costComponent";

    // Internally generated stat name when stats period are not set.
    private static final String CURRENT_COST_PRICE = "currentCostPrice";

    private static final Set<String> NUM_WORKLOADS_STATS_SET = ImmutableSet.of(
        StringConstants.NUM_WORKLOADS, "currentNumWorkloads",
        StringConstants.NUM_VMS, "currentNumVMs",
        StringConstants.NUM_DBS, "currentNumDBs",
        StringConstants.NUM_DBSS, "currentNumDBSs");

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

    private static final String STAT_FILTER_PREFIX = "current";

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

    private static boolean isUtilizationStatName(@Nonnull String name) {
        return StringConstants.MEM.equalsIgnoreCase(name) || StringConstants.CPU.equalsIgnoreCase(name);
    }

    private static StatApiDTO getRelevantStatsRecord(@Nonnull EntityStatsApiDTO entityStatsApiDTO,
                                                     @Nonnull String commodity) {
        return entityStatsApiDTO.getStats().stream()
                    .flatMap(s -> s.getStatistics().stream())
                    .filter(s -> commodity.equalsIgnoreCase(s.getName()))
                    .findAny()
                    .orElseGet(() -> {
                        logger.error("No stat name " + commodity + " found on record: " + entityStatsApiDTO);
                        return new StatApiDTO();
                    });
    }

    private static double getUtilization(@Nonnull StatApiDTO statsRecord) {
        final double epsilon = 0.1;
        if (statsRecord.getCapacity() == null || statsRecord.getCapacity().getAvg() == null
                || statsRecord.getCapacity().getAvg() < epsilon) {
            return 0.0;
        }
        if (statsRecord.getValues() == null || statsRecord.getValues().getAvg() == null) {
            return 0.0;
        }
        return statsRecord.getValues().getAvg() / statsRecord.getCapacity().getAvg();
    }

    private static double getDefault(@Nonnull StatApiDTO statsRecord) {
        if (statsRecord.getValues() == null || statsRecord.getValues().getAvg() == null) {
            return 0.0;
        }
        return statsRecord.getValues().getAvg();
    }

    /**
     * Get per-cluster stats, modelled as per-entity stats (where each cluster is an entity).
     * This is a helper method to
     * {@link StatsService#getStatsByUuidsQuery(StatScopesApiInputDTO, EntityStatsPaginationRequest)}.
     */
    @Nonnull
    private EntityStatsPaginationResponse getClusterEntityStats(
                @Nonnull final StatScopesApiInputDTO inputDto,
                @Nonnull final EntityStatsPaginationRequest paginationRequest) {
        // we are being asked for cluster stats -- we'll retrieve these from getClusterStats()
        // without expanding the scopes.
        logger.debug("Request is for cluster stats -- will not expand clusters");

        // NOTE: if cluster stats are being requested, we expect that all uuids are clusters,
        // since these stats are only relevant for clusters. If any non-clusters are detected,
        // a warning will be logged and that entry will not be included in the results.
        final Iterator<Grouping> groups;

        // if the input scope is "Market" and is asking for related entity type "cluster", then
        // we'll find all clusters the user has access to and ask for stats on those.
        // otherwise, the scope contains specific id's and we'll request stats for the groups
        // specifically requested.
        if (isMarketScoped(inputDto)) {
            // a market scope request for cluster entity stats is only valid if the related entity
            // type is set to cluster
            if (!StringConstants.CLUSTER.equalsIgnoreCase(inputDto.getRelatedType())) {
                throw new IllegalArgumentException("Request with invalid scope : " +
                        (inputDto.getScopes().isEmpty() ? "empty" :  MarketMapper.MARKET));
            }

            // unfortunately it doesn't seem like we can get all clusters in one fetch, so we have
            // to fetch one type at a time.
            // TODO: update the group retrieval to support multiple group types in the request.
            final List<Grouping> allClusters = new ArrayList<>();
            GroupProtoUtil.CLUSTER_GROUP_TYPES
                    .stream()
                    .map(type -> groupServiceRpc.getGroups(
                                    GetGroupsRequest.newBuilder()
                                            .setGroupFilter(GroupFilter.newBuilder()
                                                    .setGroupType(type))
                                            .build())
                    ).forEach(it -> {
                it.forEachRemaining(allClusters::add);
            });

            groups = allClusters.iterator();

        } else {
            // get the specified groups by id
            groups = groupServiceRpc.getGroups(GetGroupsRequest.newBuilder()
                    .setGroupFilter(GroupFilter.newBuilder()
                        .addAllId(inputDto.getScopes().stream()
                            .map(Long::valueOf)
                            .collect(Collectors.toList())))
                    .build());
        }

        // request the cluster stats for each group
        // TODO: We are fetching the cluster stats one-at-a-time here. We should consider
        // batching the requests for better performance. See OM-33182.
        final List<EntityStatsApiDTO> results = new ArrayList<>();
        groups.forEachRemaining(group -> {
            if (GroupProtoUtil.CLUSTER_GROUP_TYPES.contains(group.getDefinition().getType())) {
                // verify the user has access to the cluster members. If any are inaccessible, this
                // method will propagate an AccessDeniedException.
                if (userSessionContext.isUserScoped() && group.getDefinition().hasStaticGroupMembers()) {
                    UserScopeUtils.checkAccess(userSessionContext.getUserAccessScope(),
                            GroupProtoUtil.getAllStaticMembers(group.getDefinition()));
                }
                final EntityStatsApiDTO statsDTO = new EntityStatsApiDTO();
                final String uuid = String.valueOf(group.getId());
                statsDTO.setUuid(uuid);
                statsDTO.setClassName(StringConstants.CLUSTER);
                statsDTO.setDisplayName(group.getDefinition().getDisplayName());
                statsDTO.setStats(new ArrayList<>());

                // Call Stats service to retrieve cluster related stats.
                final ClusterStatsRequest clusterStatsRequest =
                        statsMapper.toClusterStatsRequest(uuid, inputDto.getPeriod(), true);
                final Iterator<StatSnapshot> statSnapshotIterator =
                        statsServiceRpc.getClusterStats(clusterStatsRequest);
                while (statSnapshotIterator.hasNext()) {
                    statsDTO.getStats().add(statsMapper.toStatSnapshotApiDTO(statSnapshotIterator.next()));
                }

                results.add(statsDTO);
            }
        });
        // did we get all the groups we expected?
        if (results.size() != inputDto.getScopes().size()) {
            logger.warn("Not all headroom stats were retrieved. {} scopes requested, {} stats received.",
                    inputDto.getScopes().size(), results.size());
        }

        // sort according to pagination argument
        final Function<EntityStatsApiDTO, Double> comparisonFunction;
        if (paginationRequest.getOrderBy() == null
                || StringUtils.isEmpty(paginationRequest.getOrderBy().name())) {
            logger.warn("No order specified in cluster stats request");
            comparisonFunction = s -> 0.0; // arbitrary order
        } else {
            final String orderByName = paginationRequest.getOrderByStat().orElse("");
            if (isUtilizationStatName(orderByName)) {
                comparisonFunction = s -> getUtilization(getRelevantStatsRecord(s, orderByName));
            } else {
                comparisonFunction = s -> getDefault(getRelevantStatsRecord(s, orderByName));
            }
        }
        final Comparator<EntityStatsApiDTO> comparator;
        if (paginationRequest.isAscending()) {
            comparator = Comparator.comparing(comparisonFunction);
        } else {
            comparator = Comparator.comparing(comparisonFunction).reversed();
        }
        results.sort(comparator);

        int startIdx = paginationRequest.getCursor()
                            .map(cursor -> NumberUtils.toInt(cursor, 0))
                            .orElse(0);
        int endIdx = startIdx + paginationRequest.getLimit();

        // check for pagination response:
        // note we subtract one because if 0 items are remaining then this should be
        // a final page response
        if (endIdx >= (results.size() - 1)) {
            return paginationRequest.finalPageResponse(results.subList(startIdx, results.size()),
                                                       results.size());
        } else {
            return paginationRequest.nextPageResponse(results.subList(startIdx, endIdx),
                                                      Integer.toString(endIdx), endIdx - startIdx);
        }
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
                throws OperationFailedException {
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

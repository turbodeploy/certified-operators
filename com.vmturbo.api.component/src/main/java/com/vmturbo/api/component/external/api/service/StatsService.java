package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType.DATACENTER;
import static com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType.PHYSICAL_MACHINE;
import static com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.toServiceEntityApiDTO;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IStatsService;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.api.utils.EncodingUtil;
import com.vmturbo.api.utils.StatsUtils;
import com.vmturbo.api.utils.UrlsHelp;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.Pagination;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.reports.db.StringConstants;


/**
 * Service implementation of Stats
 **/
public class StatsService implements IStatsService {

    private static Logger logger = LogManager.getLogger(StatsService.class);

    // the threshold (relative to the current time in milliseconds) under which a stats request
    // start date will be treated as a request for the latest live stats, rather than as a strict
    // start date.
    private static final int LATEST_LIVE_STATS_RETRIEVAL_START_DATE_THRESHOLD_MS = 60000;

    private final StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub statsServiceRpc;

    private final PlanServiceBlockingStub planRpcService;

    private final StatPeriodApiInputDTO DEFAULT_STAT_API_INPUT_DTO = new StatPeriodApiInputDTO();

    private final Clock clock;

    private final RepositoryApi repositoryApi;

    private final RepositoryServiceBlockingStub repositoryRpcService;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final GroupExpander groupExpander;

    private final StatsMapper statsMapper;

    private final GroupServiceBlockingStub groupServiceRpc;

    private final TargetsService targetsService;

    // CLUSTER_STATS is a collection of the cluster-headroom stats calculated from nightly plans.
    // this set is mostly copied from com.vmturbo.platform.gateway.util.StatsUtils.headRoomMetrics,
    // with an extra entry for headroom vm's, which is also a cluster-level stat calculated in the
    // nightly plans.
    //
    // Although the UI requests them, the stats other than 'headroom vms' are not getting tracked
    // yet in XL. Support for these would be added in OM-33084
    // TODO: we should share the enum instead of keeping a separate copy.
    public static final Set<String> CLUSTER_STATS =
            ImmutableSet.of(StringConstants.CPU_HEADROOM,
                    StringConstants.MEM_HEADROOM,
                    StringConstants.STORAGE_HEADROOM,
                    StringConstants.CPU_EXHAUSTION,
                    StringConstants.MEM_EXHAUSTION,
                    StringConstants.STORAGE_EXHAUSTION,
                    StringConstants.VM_GROWTH,
                    StringConstants.HEADROOM_VMS);

    private static final String STAT_FILTER_PREFIX = "current";

    /**
     * Map Entity types to be expanded to the RelatedEntityType to retrieve. For example,
     * replace requests for stats for a DATACENTER entity with the PHYSICAL_MACHINEs
     * in that DATACENTER.
     */
    private static final Map<String, String> ENTITY_TYPES_TO_EXPAND = ImmutableMap.of(
            DATACENTER.getValue(), PHYSICAL_MACHINE.getValue()
    );

    StatsService(@Nonnull final StatsHistoryServiceBlockingStub statsServiceRpc,
                 @Nonnull final PlanServiceBlockingStub planRpcService,
                 @Nonnull final RepositoryApi repositoryApi,
                 @Nonnull final RepositoryServiceBlockingStub repositoryRpcService,
                 @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                 @Nonnull final StatsMapper statsMapper,
                 @Nonnull final GroupExpander groupExpander,
                 @Nonnull final Clock clock,
                 @Nonnull final TargetsService targetsService,
                 @Nonnull final GroupServiceBlockingStub groupServiceRpc) {
        this.statsServiceRpc = Objects.requireNonNull(statsServiceRpc);
        this.planRpcService = planRpcService;
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.repositoryRpcService = Objects.requireNonNull(repositoryRpcService);
        this.supplyChainFetcherFactory = supplyChainFetcherFactory;
        this.clock = Objects.requireNonNull(clock);
        this.groupExpander = groupExpander;
        this.targetsService = Objects.requireNonNull(targetsService);
        this.groupServiceRpc = Objects.requireNonNull(groupServiceRpc);
        this.statsMapper = Objects.requireNonNull(statsMapper);
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
     * Return stats for an Entity (ServiceEntity or Group) and a uuencoded
     * {@link StatPeriodApiInputDTO} query parameter which modifies the stats search. A group is
     * expanded and the stats averaged over the group contents.
     *
     * @param uuid unique ID of the Entity for which the stats should be gathered
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
            inputDto = DEFAULT_STAT_API_INPUT_DTO;
        }

        return getStatsByEntityQuery(uuid, inputDto);
    }

    /**
     * Return stats for a {@link ServiceEntityApiDTO} given the UUID and an input
     * {@link StatPeriodApiInputDTO} object which modifies the stats search.
     *
     * Note that the ServiceEntity may be a group. In that case, we expand the given UUID into
     * a list of ServiceEntity UUID's, and the results are averaged over the ServiceEntities
     * in the expanded list.
     *
     * Some ServiceEntities, either in the input or as a result of group expansion, require
     * further expansion. For example, a DataCenter entity is replaced by the PMs in the
     * DataCenter. We use the Supply Chain fetcher to calculate that expansion.
     *
     * The inputDto object can include a list of "filters" which are keys to search for records
     * in the statistics tables. (e.g. numHosts) For the purpose of getting
     * statistics before and after a plan execution, the stats values for both before and after
     * a plan execution should be returned. In the database, we denote the "before plan" value by
     * having a "current" prefix.  (e.g. currentNumHosts), and the projected value without the
     * prefix (e.g. numHosts). The UI will only send in one "filter" called "numHost". To work with
     * this UI requirement, we add a filter with the "current" prefix for each of the filters.
     * (e.g. for numHosts, we will add currentNumHosts.)
     *
     * The results will be ordered such that projected stats come at the end of the list.
     *
     * @param uuid the UUID of either a single ServiceEntity or a group.
     * @param inputDto the parameters to further refine this search.
     * @return a list of {@link StatSnapshotApiDTO}s one for each ServiceEntity in the expanded list,
     * with projected/future stats coming at the end of the list.
     */
    @Override
    public List<StatSnapshotApiDTO> getStatsByEntityQuery(String uuid, StatPeriodApiInputDTO inputDto)
            throws Exception {

        logger.debug("fetch stats for {} requestInfo: {}", uuid, inputDto);

        // choose LinkedList to make appending more efficient. This list will only be read once.
        final List<StatSnapshotApiDTO> stats = Lists.newLinkedList();
        // If the endDate is in the future, read from the projected stats
        final long clockTimeNow = clock.millis();

        // If uuid belongs to a cluster and the request is for getting VM headroom data,
        // get the stats from the cluster stats tables.
        // Currently, it is the only use case that reads stats data from the cluster table
        // and it is handled as a special case.  If more use cases need to get cluster level
        // statistics in the future, the conditions for calling getClusterStats will need to change.
        final List<StatApiInputDTO> statsFilters = inputDto.getStatistics();
        if (isClusterUuid(uuid) && containsAnyClusterStats(statsFilters)) {
            // uuid belongs to a cluster. Call Stats service to retrieve cluster related stats.
            ClusterStatsRequest clusterStatsRequest = statsMapper.toClusterStatsRequest(uuid, inputDto);
            Iterator<StatSnapshot> statSnapshotIterator = statsServiceRpc.getClusterStats(clusterStatsRequest);
            while (statSnapshotIterator.hasNext()) {
                stats.add(statsMapper.toStatSnapshotApiDTO(statSnapshotIterator.next()));
            }
        } else {
            final boolean fullMarketRequest = UuidMapper.isRealtimeMarket(uuid);
            final Optional<Group> groupOptional = groupExpander.getGroup(uuid);

            // determine the list of entity OIDs to query for this operation
            final Set<Long> entityStatOids;
            if (fullMarketRequest) {
                // An empty set means a request for the full market.
                entityStatOids = Collections.emptySet();
            } else {
                final Set<Long> expandedOidsList = groupOptional.isPresent() ?
                        groupExpander.expandUuid(uuid) : Sets.newHashSet(Long.valueOf(uuid));
                // expand any ServiceEntities that should be replaced by related ServiceEntities,
                // e.g. DataCenter is replaced by the PhysicalMachines in the DataCenter
                entityStatOids = expandGroupingServiceEntities(expandedOidsList);

                // if empty expansion and not "Market", must be an empty group/expandable entity
                // quick return.
                // If we don't return here, we'll end up requesting stats for the full market!
                if (entityStatOids.isEmpty()) {
                    return Collections.emptyList();
                }
            }

            if (inputDto.getEndDate() != null
                    && DateTimeUtil.parseTime(inputDto.getEndDate()) > clockTimeNow) {
                ProjectedStatsResponse response =
                        statsServiceRpc.getProjectedStats(statsMapper.toProjectedStatsRequest(entityStatOids,
                                inputDto));
                // create a StatSnapshotApiDTO from the ProjectedStatsResponse
                final StatSnapshotApiDTO projectedStatSnapshot = statsMapper.toStatSnapshotApiDTO(
                        response.getSnapshot());
                // set the time of the snapshot to "future" using the "endDate" of the request
                projectedStatSnapshot.setDate(
                    DateTimeUtil.toString(DateTimeUtil.parseTime(inputDto.getEndDate())));
                // add to the list of stats to return
                stats.add(projectedStatSnapshot);
            }
            // if the startDate is in the past, read from the history (and combine with projected, if any)
            Long startTime = null;
            if (inputDto.getStartDate() == null
                    || (startTime = DateTimeUtil.parseTime(inputDto.getStartDate())) < clockTimeNow) {
                // Because of issues like https://vmturbo.atlassian.net/browse/OM-36537, where a UI
                // request expecting stats between "now" and "future" fails to get "now" results
                // because the specified start time is often (but not always) later than the latest
                // available live topology broadcast, we are adding a special clause for the
                // case where, if a "now" start date is specified, the request will be treated as a
                // request for the latest available live topology stats.
                //
                // A "now" request is identified as one with a start date falling within
                // (LATEST_LIVE_STATS_RETRIEVAL_START_DATE_THRESHOLD_MS) of the current system time.
                //
                // The "special request" is handled by adjusting the historical stat request to have
                // neither a start nor end date set. This triggers a special case in the History
                // component, which interprets the null start/end as a request to get the most recent
                // live stats, regardless of current time.
                //
                // In the future, we would probably prefer an explicit parameter be able to handle
                // this case instead of using this special case for start date. But for now, this is
                // a small change we are making so we can avoid also having to change the stats API,
                // UI code and classic API to support a new param or special value.
                if (startTime != null
                    && (startTime > clockTimeNow - LATEST_LIVE_STATS_RETRIEVAL_START_DATE_THRESHOLD_MS)) {
                    logger.trace("Clearing start and end date since start time is {}ms ago.", (clockTimeNow - startTime));
                    inputDto.setStartDate(null);
                    inputDto.setEndDate(null);
                }
                if (statsFilters != null) {
                    // Duplicate the set of stat filters and add "current" as a prefix to the filter name.
                    List<StatApiInputDTO> currentStatFilters =
                            statsFilters.stream()
                                    .filter(filter -> filter.getName() != null)
                                    .map(filter -> STAT_FILTER_PREFIX +
                                        CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, filter.getName()))
                                    .map(filterName -> {
                                        StatApiInputDTO currentFilter = new StatApiInputDTO();
                                        currentFilter.setName(filterName);
                                        return currentFilter;
                                    })
                                    .collect(Collectors.toList());
                    statsFilters.addAll(currentStatFilters);
                }
                final Optional<Integer> tempGroupEntityType = getGlobalTempGroupEntityType(groupOptional);
                final GetAveragedEntityStatsRequest request =
                        statsMapper.toAveragedEntityStatsRequest(entityStatOids, inputDto, tempGroupEntityType);
                final Iterable<StatSnapshot> statsIterator = () ->
                        statsServiceRpc.getAveragedEntityStats(request);

                // convert the stats snapshots to the desired ApiDTO and return them. Also, insert
                // them before any projected stats in the output collection, since the UI expects
                // the projected stats to always come last.
                // (We are still fetching projected stats first, since we may have mutated
                // the inputDTO in this clause that fetches live/historical stats)
                stats.addAll(0, StreamSupport.stream(statsIterator.spliterator(), false)
                        .map(statsMapper::toStatSnapshotApiDTO)
                        .collect(Collectors.toList()));
            }
        }

        List<TargetApiDTO> targets = null;
        try {
            targets = targetsService.getTargets(null);
        } catch (RuntimeException e) {
            logger.error("Unable to get targets list due to error: {}." +
                    " Not using targets list for stat filtering.", e.getMessage());
        }

        // filter out those commodities listed in BLACK_LISTED_STATS in StatsUtils
        return StatsUtils.filterStats(stats, targets);
    }

    /**
     * Check if the uuid belongs to a cluster. Includes a specific check for the non-numeric
     * string "Market".
     *
     * @param uuid UUID of an entity
     * @return true if it is a cluster, false otherwise
     */
    private boolean isClusterUuid(@Nonnull String uuid) {
        if (uuid.equals(UuidMapper.UI_REAL_TIME_MARKET_STR)) {
            return false;
        }
        try {
            GetGroupResponse response = groupServiceRpc.getGroup(GroupID.newBuilder()
                    .setId(Long.parseLong(uuid))
                    .build());
            Type type = response.getGroup().getType();
            if (type.equals(Type.CLUSTER)) {
                return true;
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Cluster uuid is invalid: " + uuid);
        }
        return false;
    }

    private Optional<PlanInstance> getRequestedPlanInstance(@Nonnull final StatScopesApiInputDTO inputDto) {
        // plan stats request must be the only uuid in the scopes list
        if (inputDto.getScopes().size() != 1) {
            return Optional.empty();
        }
        // check for a plan uuid
        String scopeUuid = inputDto.getScopes().iterator().next();
        long scopeOid;
        try {
            scopeOid = Long.valueOf(scopeUuid);
        } catch (NumberFormatException e) {
            // not a number, so cannot be a plan UUID
            return Optional.empty();
        }

        // fetch plan from plan orchestrator
        PlanDTO.OptionalPlanInstance planInstanceOptional =
                planRpcService.getPlan(PlanDTO.PlanId.newBuilder()
                        .setPlanId(scopeOid)
                        .build());
        if (!planInstanceOptional.hasPlanInstance()) {
            return Optional.empty();
        }

        return Optional.of(planInstanceOptional.getPlanInstance());
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

        // definitely a plan instance; fetch the plan stats from the Repository client.
        final PlanTopologyStatsRequest planStatsRequest = statsMapper.toPlanTopologyStatsRequest(
                planInstance, inputDto, paginationRequest);
        final PlanTopologyStatsResponse response =
                repositoryRpcService.getPlanTopologyStats(planStatsRequest);

        // It's important to respect the order of entities in the returned stats, because
        // they're arranged according to the pagination request.
        final List<EntityStatsApiDTO> entityStatsList = response.getEntityStatsList().stream()
            .map(entityStats -> {
                final EntityStatsApiDTO entityStatsApiDTO = new EntityStatsApiDTO();
                final TopologyDTO.TopologyEntityDTO planEntity = entityStats.getPlanEntity();
                final ServiceEntityApiDTO serviceEntityApiDTO = toServiceEntityApiDTO(planEntity);
                entityStatsApiDTO.setUuid(Long.toString(planEntity.getOid()));
                entityStatsApiDTO.setDisplayName(planEntity.getDisplayName());
                entityStatsApiDTO.setClassName(ServiceEntityMapper.toUIEntityType(
                        planEntity.getEntityType()));
                entityStatsApiDTO.setRealtimeMarketReference(serviceEntityApiDTO);
                final List<StatSnapshotApiDTO> statSnapshotsList = entityStats.getPlanEntityStats()
                        .getStatSnapshotsList()
                        .stream()
                        .map(statsMapper::toStatSnapshotApiDTO)
                        .collect(Collectors.toList());
                entityStatsApiDTO.setStats(statSnapshotsList);
                return entityStatsApiDTO;
            })
            .collect(Collectors.toList());

        if (response.hasPaginationResponse()) {
            return PaginationProtoUtil.getNextCursor(response.getPaginationResponse())
                    .map(nextCursor -> paginationRequest.nextPageResponse(entityStatsList, nextCursor))
                    .orElseGet(() -> paginationRequest.finalPageResponse(entityStatsList));
        } else {
            return paginationRequest.allResultsResponse(entityStatsList);
        }
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
        // we are being asked for headroom stats -- we'll retrieve these from getClusterStats()
        // without expanding the scopes.
        logger.debug("Request is for headroom stats -- will not expand clusters");
        // NOTE: if headroom stats are being requested, we expect that all uuids are clusters,
        // since these stats are only relevant for clusters. If any non-clusters are detected,
        // a warning will be logged and that entry will not be included in the results.

        final Iterator<Group> groups = groupServiceRpc.getGroups(GetGroupsRequest.newBuilder()
                .addAllId(inputDto.getScopes().stream()
                        .map(Long::valueOf)
                        .collect(Collectors.toList()))
                .build());

        // request the cluster stats for each group
        // TODO: We are fetching the cluster stats one-at-a-time here. We should consider
        // batching the requests for better performance. See OM-33182.
        final List<EntityStatsApiDTO> results = new ArrayList<>();
        groups.forEachRemaining(group -> {
            if (group.hasCluster()) {
                final EntityStatsApiDTO statsDTO = new EntityStatsApiDTO();
                final String uuid = String.valueOf(group.getId());
                statsDTO.setUuid(String.valueOf(uuid));
                statsDTO.setClassName(GroupMapper.CLUSTER);
                statsDTO.setDisplayName(group.getCluster().getName());
                statsDTO.setStats(new ArrayList<>());

                // Call Stats service to retrieve cluster related stats.
                final ClusterStatsRequest clusterStatsRequest =
                        statsMapper.toClusterStatsRequest(uuid, inputDto.getPeriod());
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
            logger.warn("Not all headroom stats were retrieved. {} scopes requested, {} stats recieved.",
                    inputDto.getScopes().size(), results.size());
        }

        // The history component does not do pagination for per-cluster stats, because the number
        // of clusters is usually very small compared to the number of entities. We rely on
        // the default pagination mechanism.
        return paginationRequest.allResultsResponse(results);
    }

    /**
     * Get the scope that a {@link StatScopesApiInputDTO} is trying to select. This includes,
     * for example, expanding groups, or getting the IDs of related entities. Calling this method
     * may result in one or more remote calls to other components.
     * m
     * @param inputDto The {@link StatScopesApiInputDTO}.
     * @return A set of entity OIDs of entities in the scope.
     * @throws OperationFailedException If any part of the operation failed.
     */
    @Nonnull
    private Set<Long> getExpandedScope(@Nonnull final StatScopesApiInputDTO inputDto)
                throws OperationFailedException {
        final Set<Long> expandedUuids;
        final Optional<String> relatedType = Optional.ofNullable(inputDto.getRelatedType())
                // Treat unknown type as non-existent.
                .filter(type -> !type.equals(UIEntityType.UNKNOWN.getValue()))
                // This part is important, to expand types such as DATACENTER into the member
                // types (i.e. hosts)
                .map(statsMapper::normalizeRelatedType);
        // Market stats request must be the only uuid in the scopes list
        if (inputDto.getScopes().size() == 1 && (inputDto.getScopes().get(0).equals(UuidMapper.UI_REAL_TIME_MARKET_STR))) {
            // 'relatedType' is required for full market entity stats
            if (!relatedType.isPresent()) {
                throw new IllegalArgumentException("Cannot request individual stats for full " +
                        "Market without specifying 'relatedType'");
            }

            final Map<String, SupplyChainNode> result = supplyChainFetcherFactory.newNodeFetcher()
                    .entityTypes(Collections.singletonList(relatedType.get()))
                    .fetch();
            final SupplyChainNode relatedTypeNode = result.get(relatedType.get());
            expandedUuids = relatedTypeNode == null ?
                    Collections.emptySet() : RepositoryDTOUtil.getAllMemberOids(relatedTypeNode);
        } else {
            // Expand scopes list to determine the list of entity OIDs to query for this operation
            final Set<String> seedUuids = Sets.newHashSet(inputDto.getScopes());

            // This shouldn't happen, because we handle the full market case earlier on.
            Preconditions.checkArgument(UuidMapper.hasLimitedScope(seedUuids));

            if (relatedType.isPresent()) {
                // If the UI sets a related type, we need to do a supply chain query to get
                // the entities of that type related to the seed uuids.
                final Map<String, SupplyChainNode> result = supplyChainFetcherFactory.newNodeFetcher()
                        .addSeedUuids(seedUuids)
                        .entityTypes(Collections.singletonList(relatedType.get()))
                        .fetch();
                final SupplyChainNode relatedTypeNode = result.get(relatedType.get());
                expandedUuids = relatedTypeNode == null ?
                        Collections.emptySet() : RepositoryDTOUtil.getAllMemberOids(relatedTypeNode);
            } else {
                expandedUuids = groupExpander.expandUuids(seedUuids);
            }
        }
        return expandedUuids;
    }

    /**
     * Get the {@link EntityStatsScope} scope that a {@link StatScopesApiInputDTO} is trying to
     * select. This includes everything implied by {@link StatsService#getExpandedScope(StatScopesApiInputDTO)}
     * as well as optimizations to handle requesting stats for all entities from the global
     * market or a global temp group.
     *
     * @param inputDto The {@link StatScopesApiInputDTO}.
     * @return A {@link EntityStatsScope} object to use for the query.
     * @throws OperationFailedException If any part of the operation failed.
     */
    @Nonnull
    private EntityStatsScope createEntityStatsScope(@Nonnull final StatScopesApiInputDTO inputDto)
            throws OperationFailedException {
        final EntityStatsScope.Builder entityStatsScope = EntityStatsScope.newBuilder();
        final Optional<String> relatedType = Optional.ofNullable(inputDto.getRelatedType())
                // Treat unknown type as non-existent.
                .filter(type -> !type.equals(UIEntityType.UNKNOWN.getValue()))
                // This part is important, to expand types such as DATACENTER into the member
                // types (i.e. hosts)
                .map(statsMapper::normalizeRelatedType);
        // Market stats request must be the only uuid in the scopes list
        if (inputDto.getScopes().size() == 1 &&
                (inputDto.getScopes().get(0).equals(UuidMapper.UI_REAL_TIME_MARKET_STR))) {
            // 'relatedType' is required for full market entity stats
            if (!relatedType.isPresent()) {
                throw new IllegalArgumentException("Cannot request individual stats for full " +
                        "Market without specifying 'relatedType'");
            }

            entityStatsScope.setEntityType(ServiceEntityMapper.fromUIEntityType(relatedType.get()));
        } else if (inputDto.getScopes().size() == 1) {
            // Check if we can do the global entity type optimization.
            final Optional<Integer> globalEntityType =
                    getGlobalTempGroupEntityType(groupExpander.getGroup(inputDto.getScopes().get(0)));
            final Optional<Integer> relatedTypeInt =
                    relatedType.map(ServiceEntityMapper::fromUIEntityType);
            if (globalEntityType.isPresent() &&
                    // We can only do the global entity type optimization if the related type
                    // is unset, or is the same as the global entity type. Why? Because the
                    // topology graph may not be connected. For example, suppose global entity type
                    // is PM and related type is VM. We can't just set the entity stats scope to
                    // VM, because there may be VMs not connected to PMs (e.g. in the cloud).
                    (!relatedTypeInt.isPresent() || relatedTypeInt.equals(globalEntityType))) {
                entityStatsScope.setEntityType(globalEntityType.get());
            } else {
                entityStatsScope.setEntityList(EntityList.newBuilder()
                        .addAllEntities(getExpandedScope(inputDto)));
            }
        } else {
            entityStatsScope.setEntityList(EntityList.newBuilder()
                    .addAllEntities(getExpandedScope(inputDto)));
        }
        Preconditions.checkArgument(entityStatsScope.hasEntityList() || entityStatsScope.hasEntityType());
        return entityStatsScope.build();
    }

    /**
     * Get per-entity stats of live entities (i.e. entities in the real topology, as opposed
     * to plan entities).
     */
    @Nonnull
    private EntityStatsPaginationResponse getLiveEntityStats(
                    @Nonnull final StatScopesApiInputDTO inputDto,
                    @Nonnull final EntityStatsPaginationRequest paginationRequest)
                throws OperationFailedException {

        // 1. Request the next page of entity stats from the proper service.

        // This variable will be set to the next page of per-entity stats.
        final List<EntityStats> nextStatsPage;
        // This variable will be set to the pagination response.
        final Optional<Pagination.PaginationResponse> paginationResponseOpt;

        final long clockTimeNow = clock.millis();
        // is the startDate in the past?
        final boolean historicalStatsRequest = inputDto.getPeriod().getStartDate() == null ||
                DateTimeUtil.parseTime(inputDto.getPeriod().getStartDate()) < clockTimeNow;
        // is the endDate in the future?
        final boolean projectedStatsRequest = inputDto.getPeriod().getEndDate() != null &&
                DateTimeUtil.parseTime(inputDto.getPeriod().getEndDate()) > clockTimeNow;

        if (historicalStatsRequest) {
            // For historical requests, we use the EntityStatsScope object
            // which has some optimizations to deal with global requests.
            final EntityStatsScope entityStatsScope = createEntityStatsScope(inputDto);

            // fetch the historical stats for the given entities using the given search spec
            final GetEntityStatsResponse statsResponse = statsServiceRpc.getEntityStats(
                    statsMapper.toEntityStatsRequest(entityStatsScope, inputDto.getPeriod(),
                            paginationRequest));
            nextStatsPage = statsResponse.getEntityStatsList();
            paginationResponseOpt = statsResponse.hasPaginationResponse() ?
                    Optional.of(statsResponse.getPaginationResponse()) : Optional.empty();
        } else if (projectedStatsRequest) {
            // The projected stats service doesn't support the global entity type optimization,
            // because at the time of this writing there are no requests for projected
            // per-entity stats on the global scope, and it would require modifications
            // to the way we store projected stats.
            final Set<Long> expandedUuids = getExpandedScope(inputDto);
            final ProjectedEntityStatsResponse projectedStatsResponse = statsServiceRpc.getProjectedEntityStats(
                    statsMapper.toProjectedEntityStatsRequest(
                            expandedUuids, inputDto.getPeriod(), paginationRequest));

            nextStatsPage = projectedStatsResponse.getEntityStatsList();
            paginationResponseOpt = projectedStatsResponse.hasPaginationResponse() ?
                    Optional.of(projectedStatsResponse.getPaginationResponse()) : Optional.empty();
        } else {
            throw new OperationFailedException("Invalid start and end date combination.");
        }

        // 2. Get additional display info for all entities in the page from the repository.
        final Set<Long> entitiesWithStats = nextStatsPage.stream()
                .map(EntityStats::getOid)
                .collect(Collectors.toSet());

        final Map<Long, Optional<ServiceEntityApiDTO>> entities =
            repositoryApi.getServiceEntitiesById(
                ServiceEntitiesRequest.newBuilder(entitiesWithStats).build());

        // 3. Combine the results of 1. and 2. and return.
        final List<EntityStatsApiDTO> dto = nextStatsPage.stream()
            .map(entityStats -> {
                final Optional<ServiceEntityApiDTO> apiDto = entities.get(entityStats.getOid());
                return apiDto.map(seApiDto -> constructEntityStatsDto(seApiDto,
                        projectedStatsRequest, entityStats, inputDto));
            })
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

        return paginationResponseOpt
            .map(paginationResponse ->
                PaginationProtoUtil.getNextCursor(paginationResponse)
                    .map(nextCursor -> paginationRequest.nextPageResponse(dto, nextCursor))
                    .orElseGet(() -> paginationRequest.finalPageResponse(dto)))
            .orElseGet(() -> paginationRequest.allResultsResponse(dto));
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
     * @return a list of {@link EntityStatsApiDTO} objects representing the entities in the search
     * with the commodities values filled in
     */
    @Override
    public EntityStatsPaginationResponse getStatsByUuidsQuery(StatScopesApiInputDTO inputDto,
                                                  EntityStatsPaginationRequest paginationRequest)
            throws Exception {
        final Optional<PlanInstance> planInstance = getRequestedPlanInstance(inputDto);
        if (planInstance.isPresent()) {
            return getPlanEntityStats(planInstance.get(), inputDto, paginationRequest);
        } else if (containsAnyClusterStats(inputDto.getPeriod().getStatistics())) {
            return getClusterEntityStats(inputDto, paginationRequest);
        } else {
            return getLiveEntityStats(inputDto, paginationRequest);
        }
    }

    /**
     * Does the input request contain any headroom stats?
     *
     * @return true if any of the input stats are headroom stats, false otherwise.
     */
    private boolean containsAnyClusterStats(List<StatApiInputDTO> statsRequested) {
        return CollectionUtils.isNotEmpty(statsRequested) && statsRequested.stream()
                .map(StatApiInputDTO::getName)
                .anyMatch(CLUSTER_STATS::contains);
    }

    /**
     * Create a {@link EntityStatsApiDTO} that can be returned to the client, given all the
     * necessary information retrieved from history/repository.
     *
     * @param serviceEntity The {@link ServiceEntityApiDTO} from which to get the entity-based fields.
     * @param projectedStatsRequest Whether or not the request was for projected stats. Note - we
     *                              pass this in instead of re-determining it from the inputDto
     *                              because time will have passed from the start of the API call
     *                              to the point at which we construct {@link EntityStatsApiDTO}s.
     * @param entityStats The {@link EntityStats} from which to get stats values.
     * @param inputDto The {@link StatScopesApiInputDTO} for the stats query.
     * @return A fully configured {@link EntityStatsApiDTO}.
     */
    @Nonnull
    private EntityStatsApiDTO constructEntityStatsDto(ServiceEntityApiDTO serviceEntity,
                                                      final boolean projectedStatsRequest,
                                                      final EntityStats entityStats,
                                                      final StatScopesApiInputDTO inputDto) {
        final EntityStatsApiDTO entityStatsApiDTO = new EntityStatsApiDTO();
        entityStatsApiDTO.setUuid(serviceEntity.getUuid());
        entityStatsApiDTO.setClassName(serviceEntity.getClassName());
        entityStatsApiDTO.setDisplayName(serviceEntity.getDisplayName());
        entityStatsApiDTO.setStats(new ArrayList<>());
        if (projectedStatsRequest && entityStats.getStatSnapshotsCount() > 0) {
            // we expect either zero or one snapshot for each entity
            if (entityStats.getStatSnapshotsCount() > 1) {
                // this indicates a bug in History Component
                logger.error("Too many entity stats ({}) for: {} -> {}; taking the first.",
                        entityStats.getStatSnapshotsCount(),
                        entityStats.getOid(),
                        entityStats.getStatSnapshotsList());
            }
            entityStats.getStatSnapshotsList().stream()
                    .findFirst()
                    .map(statsMapper::toStatSnapshotApiDTO)
                    .map(statApiDto -> {
                        // set the time of the snapshot to "future" using the "endDate" of the request
                        statApiDto.setDate(inputDto.getPeriod().getEndDate());
                        return statApiDto;
                    })
                    .ifPresent(statApiDto -> entityStatsApiDTO.getStats().add(statApiDto));
        } else {
            entityStatsApiDTO.getStats().addAll(statsMapper.toStatsSnapshotApiDtoList(entityStats));
        }
        return entityStatsApiDTO;
    }

    /**
     * Replace specific types of ServiceEntities with "constituents". For example, a DataCenter SE
     * is replaced by the PhysicalMachine SE's related to that DataCenter.
     *<p>
     * ServiceEntities of other types not to be expanded are copied to the output result set.
     *<p>
     * See 6.x method SupplyChainUtils.getUuidsFromScopesByRelatedType() which uses the
     * marker interface EntitiesProvider to determine which Service Entities to expand.
     *<p>
     * Errors fetching the supplychain are logged and ignored - the input OID will be copied
     * to the output in case of an error or missing relatedEntityType info in the supplychain.
     *<p>
     * First, it will fetch entities which need to expand, then check if any input entity oid is
     * belong to those entities. Because if input entity set is large, it will cost a lot time to
     * fetch huge entity from Repository. Instead, if first fetch those entities which need to expand
     * , the amount will be much less than the input entity set size since right now only DataCenter
     * could expand.
     *
     * @param entityOidsToExpand a set of ServiceEntity OIDs to examine
     * @return a set of ServiceEntity OIDs with types that should be expanded replaced by the
     * "constituent" ServiceEntity OIDs as computed by the supply chain.
     */
    private Set<Long> expandGroupingServiceEntities(Set<Long> entityOidsToExpand) {
        // Early return if the input is empty, to prevent making
        // the initial RPC call.
        if (entityOidsToExpand.isEmpty()) {
            return Collections.emptySet();
        }

        final Set<Long> expandedEntityOids = Sets.newHashSet();
        // get all service entities which need to expand.
        final Set<ServiceEntityApiDTO> expandServiceEntities = ENTITY_TYPES_TO_EXPAND.keySet().stream()
                .flatMap(entityType ->
                        repositoryApi.getSearchResults(null, Collections.singletonList(entityType),
                                UuidMapper.UI_REAL_TIME_MARKET_STR, null, null).stream())
                .collect(Collectors.toSet());

        final Map<Long, ServiceEntityApiDTO> expandServiceEntityMap = expandServiceEntities.stream()
                .collect(Collectors.toMap(entity -> Long.valueOf(entity.getUuid()), Function.identity()));
        // go through each entity and check if it needs to expand.
        for (Long oidToExpand : entityOidsToExpand) {
            try {
                // if expandServiceEntityMap contains oid, it means current oid entity needs to expand.
                if (expandServiceEntityMap.containsKey(oidToExpand)) {
                    final ServiceEntityApiDTO expandEntity = expandServiceEntityMap.get(oidToExpand);
                    final String relatedEntityType =
                            ENTITY_TYPES_TO_EXPAND.get(expandEntity.getClassName());
                    // fetch the supply chain map:  entity type -> SupplyChainNode
                    Map<String, SupplyChainNode> supplyChainMap = supplyChainFetcherFactory
                            .newNodeFetcher()
                            .entityTypes(Collections.singletonList(relatedEntityType))
                            .addSeedUuid(expandEntity.getUuid())
                            .fetch();
                    SupplyChainNode relatedEntities = supplyChainMap.get(relatedEntityType);
                    if (relatedEntities != null) {
                        expandedEntityOids.addAll(RepositoryDTOUtil.getAllMemberOids(relatedEntities));
                    } else {
                        logger.warn("RelatedEntityType {} not found in supply chain for {}; " +
                                "the entity is discarded", relatedEntityType, expandEntity.getUuid());
                    }
                } else {
                    expandedEntityOids.add(oidToExpand);
                }
            } catch (OperationFailedException e) {
                logger.warn("Error fetching supplychain for {}: ", oidToExpand, e.getMessage());
                // include the OID unexpanded
                expandedEntityOids.add(oidToExpand);
            }
        }
        return expandedEntityOids;
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
    private Optional<Integer> getGlobalTempGroupEntityType(@Nonnull final Optional<Group> groupOptional) {
        final boolean isGlobalTempGroup = groupOptional.isPresent() && groupOptional.get().hasTempGroup()
                && groupOptional.get().getTempGroup().getIsGlobalScopeGroup();
        // if it is global temp group and need to expand, should return target expand entity type.
        if (isGlobalTempGroup && ENTITY_TYPES_TO_EXPAND.containsKey(ServiceEntityMapper.toUIEntityType(
                groupOptional.get().getTempGroup().getEntityType()))) {
            return Optional.of(ServiceEntityMapper.fromUIEntityType(
                    ENTITY_TYPES_TO_EXPAND.get(ServiceEntityMapper.toUIEntityType(
                            groupOptional.get().getTempGroup().getEntityType()))));
        } else if (isGlobalTempGroup) {
            // if it is global temp group and not need to expand.
            return Optional.of(groupOptional.get().getTempGroup().getEntityType());
        } else {
            return Optional.empty();
        }
    }
}

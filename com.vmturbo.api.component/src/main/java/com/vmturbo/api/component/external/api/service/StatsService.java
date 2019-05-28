package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.toServiceEntityApiDTO;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.component.external.api.mapper.MarketMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.DefaultCloudGroupProducer;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.MagicScopeGateway;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IStatsService;
import com.vmturbo.api.utils.CompositeEntityTypesSpec;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.api.utils.EncodingUtil;
import com.vmturbo.api.utils.StatsUtils;
import com.vmturbo.api.utils.UrlsHelp;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.common.Pagination;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.Builder;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.EntityTypeFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudExpenseStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudExpenseStatsRequest.GroupByType;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.TempGroupInfo;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchPlanTopologyEntityDTOsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsResponse;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;


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

    private static final Set<String> COST_STATS_SET = ImmutableSet.of(StringConstants.COST_PRICE, CURRENT_COST_PRICE);

    private static final Set<String> NUM_WORKLOADS_STATS_SET = ImmutableSet.of(
        StringConstants.NUM_WORKLOADS, "currentNumWorkloads",
        StringConstants.NUM_VMS, "currentNumVMs",
        StringConstants.NUM_DBS, "currentNumDBs",
        StringConstants.NUM_DBSS, "currentNumDBSs");

    private static Logger logger = LogManager.getLogger(StatsService.class);

    // the +/- threshold (relative to the current time) in which a stats request specifying a time
    // window will be treated as a request for the latest live stats, rather than as a historical or
    // projected stats request. As an example, if the threshold is 60 seconds, all stats with start
    // or end time falling within server now +/- 60 seconds will be treated as requesting the
    // "current" stats.
    private final Duration liveStatsRetrievalWindow;

    private final StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub statsServiceRpc;

    private final PlanServiceBlockingStub planRpcService;

    private final Clock clock;

    private final RepositoryApi repositoryApi;

    private final RepositoryServiceBlockingStub repositoryRpcService;

    private final SearchServiceBlockingStub searchServiceClient;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final GroupExpander groupExpander;

    private final StatsMapper statsMapper;

    private final GroupServiceBlockingStub groupServiceRpc;

    private final TargetsService targetsService;

    private final CostServiceBlockingStub costServiceRpc;

    private final MagicScopeGateway magicScopeGateway;

    private final UserSessionContext userSessionContext;

    private final ReservedInstancesService riService;

    private final long realtimeTopologyContextId;

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
            UIEntityType.DATACENTER.apiStr(), UIEntityType.PHYSICAL_MACHINE.apiStr()
    );

    // set of stats passed from UI, which are for the number of entities grouped by tier
    private static final Set<String> CLOUD_PLAN_ENTITIES_BY_TIER_STATS = ImmutableSet.of(
        StringConstants.NUM_VIRTUAL_DISKS,
        StringConstants.NUM_WORKLOADS
    );

    // list of entity types which are counted as workload for cloud
    public static final List<String> ENTITY_TYPES_COUNTED_AS_WORKLOAD = ImmutableList.of(
        UIEntityType.VIRTUAL_MACHINE.apiStr(),
        UIEntityType.DATABASE.apiStr(),
        UIEntityType.DATABASE_SERVER.apiStr()
    );

    private static final Map<String, List<String>> WORKLOAD_NAME_TO_ENTITY_TYPES = ImmutableMap.of(
        StringConstants.NUM_VMS, Collections.singletonList(UIEntityType.VIRTUAL_MACHINE.apiStr()),
        StringConstants.NUM_DBS, Collections.singletonList(UIEntityType.DATABASE.apiStr()),
        StringConstants.NUM_DBSS, Collections.singletonList(UIEntityType.DATABASE_SERVER.apiStr()),
        StringConstants.NUM_WORKLOADS, ENTITY_TYPES_COUNTED_AS_WORKLOAD
    );

    // set of plan types which are counted as cloud plans
    private static final Set<String> CLOUD_PLAN_TYPES = ImmutableSet.of(
        StringConstants.OPTIMIZE_CLOUD_PLAN_TYPE, StringConstants.CLOUD_MIGRATION_PLAN_TYPE);

    // the function of how to get the tier id from a given TopologyEntityDTO, this is used
    // for the stats of the number of entities by tier type
    private static final Map<String, Function<TopologyEntityDTO, Long>> ENTITY_TYPE_TO_GET_TIER_FUNCTION = ImmutableMap.of(
        UIEntityType.VIRTUAL_MACHINE.apiStr(), topologyEntityDTO ->
            topologyEntityDTO.getCommoditiesBoughtFromProvidersList().stream()
                .filter(commodityBought -> commodityBought.getProviderEntityType() == EntityType.COMPUTE_TIER_VALUE)
                .map(CommoditiesBoughtFromProvider::getProviderId)
                .findAny().get(),
        UIEntityType.DATABASE.apiStr(), topologyEntityDTO ->
            topologyEntityDTO.getCommoditiesBoughtFromProvidersList().stream()
                .filter(commodityBought -> commodityBought.getProviderEntityType() == EntityType.DATABASE_TIER_VALUE)
                .map(CommoditiesBoughtFromProvider::getProviderId)
                .findAny().get(),
        UIEntityType.DATABASE_SERVER.apiStr(), topologyEntityDTO ->
            topologyEntityDTO.getCommoditiesBoughtFromProvidersList().stream()
                .filter(commodityBought -> commodityBought.getProviderEntityType() == EntityType.DATABASE_SERVER_TIER_VALUE)
                .map(CommoditiesBoughtFromProvider::getProviderId)
                .findAny().get(),
        UIEntityType.VIRTUAL_VOLUME.apiStr(), topologyEntityDTO ->
            topologyEntityDTO.getConnectedEntityListList().stream()
                .filter(connectedEntity -> connectedEntity.getConnectedEntityType() == EntityType.STORAGE_TIER_VALUE)
                .map(ConnectedEntity::getConnectedEntityId)
                .findFirst().get()
    );

    StatsService(@Nonnull final StatsHistoryServiceBlockingStub statsServiceRpc,
                 @Nonnull final PlanServiceBlockingStub planRpcService,
                 @Nonnull final RepositoryApi repositoryApi,
                 @Nonnull final RepositoryServiceBlockingStub repositoryRpcService,
                 @Nonnull final SearchServiceBlockingStub searchServiceClient,
                 @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                 @Nonnull final StatsMapper statsMapper,
                 @Nonnull final GroupExpander groupExpander,
                 @Nonnull final Clock clock,
                 @Nonnull final TargetsService targetsService,
                 @Nonnull final GroupServiceBlockingStub groupServiceRpc,
                 @Nonnull final Duration liveStatsRetrievalWindow,
                 @Nonnull final CostServiceBlockingStub costService,
                 @Nonnull final MagicScopeGateway magicScopeGateway,
                 @Nonnull final UserSessionContext userSessionContext,
                 @Nonnull final ReservedInstancesService riService,
                 final long realtimeTopologyContextId) {
        this.statsServiceRpc = Objects.requireNonNull(statsServiceRpc);
        this.planRpcService = planRpcService;
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.repositoryRpcService = Objects.requireNonNull(repositoryRpcService);
        this.searchServiceClient = Objects.requireNonNull(searchServiceClient);
        this.supplyChainFetcherFactory = supplyChainFetcherFactory;
        this.clock = Objects.requireNonNull(clock);
        this.groupExpander = groupExpander;
        this.targetsService = Objects.requireNonNull(targetsService);
        this.groupServiceRpc = Objects.requireNonNull(groupServiceRpc);
        this.liveStatsRetrievalWindow = liveStatsRetrievalWindow;
        logger.debug("Live Stats Retrieval Window is {}sec", liveStatsRetrievalWindow.getSeconds());
        this.statsMapper = Objects.requireNonNull(statsMapper);
        this.costServiceRpc = Objects.requireNonNull(costService);
        this.magicScopeGateway = Objects.requireNonNull(magicScopeGateway);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
        this.riService = Objects.requireNonNull(riService);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
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
        final long currentTimeStamp = Clock.systemUTC().millis();
        // NOTE: to anyone updating this method, Gary Z. is offering a *generous* reward to any who can
        // refactor this function into smaller and simpler pieces, as it is getting cumbersome. Please
        // submit proof of your success by adding him to your review request.

        final String uuid = magicScopeGateway.enter(originalUuid);

        logger.debug("fetch stats for {} requestInfo: {}", uuid, inputDto);

        // Create a default Stat period, if one is not specified in the request
        if (inputDto == null) {
            inputDto = getDefaultStatPeriodApiInputDto();
        }

        // choose LinkedList to make appending more efficient. This list will only be read once.
        final List<StatSnapshotApiDTO> stats = Lists.newLinkedList();

        // put all requested stats filters into a new list
        final List<StatApiInputDTO> statsFilters = inputDto.getStatistics() == null
            ? Collections.emptyList() : new ArrayList<>(inputDto.getStatistics());
        Optional<PlanInstance> optPlan = getRequestedPlanInstance(uuid);

        // If it is a request for reserved instance coverage or utilization, then route it to the
        // ReservedInstancesService to handle, which will get the stats from cost component
        if (isRequestForRICoverageOrUtilizationStats(statsFilters)) {
            return riService.getRICoverageOrUtilizationStats(uuid, inputDto, optPlan);
        }

        // fetch the number of entities by tier stats for cloud plan
        Set<String> numEntitiesByTierStatNames = collectCloudPlanNumEntitiesByTierStatNames(optPlan, statsFilters);
        if (!numEntitiesByTierStatNames.isEmpty()) {
            stats.addAll(fetchNumEntitiesByTierStatsForCloudPlan(optPlan.get(), numEntitiesByTierStatNames));
        }

        // If the endDate is in the future, read from the projected stats
        final long clockTimeNow = clock.millis();

        List<TargetApiDTO> targets = null;

        // If uuid belongs to a cluster and the request is for getting VM headroom data,
        // get the stats from the cluster stats tables.
        // Currently, it is the only use case that reads stats data from the cluster table
        // and it is handled as a special case.  If more use cases need to get cluster level
        // statistics in the future, the conditions for calling getClusterStats will need to change.
        final boolean isDefaultCloudGroupUuid = isDefaultCloudGroupUuid(uuid);
        if (!isDefaultCloudGroupUuid && isClusterUuid(uuid) && containsAnyClusterStats(statsFilters)) {
            // uuid belongs to a cluster. Call Stats service to retrieve cluster related stats.
            // we are enforcing the user scope here, because we have the handy groupExpander at our
            // disposal and it may avoid an extra batch of network calls. If necessary in the future,
            // we can move this check into the history component stats service instead.
            if (userSessionContext.isUserScoped()) {
                // this will throw an "access denied" exception if any cluster members are out of scope
                UserScopeUtils.checkAccess(userSessionContext.getUserAccessScope(), groupExpander.expandUuid(uuid));
            }
            ClusterStatsRequest clusterStatsRequest = statsMapper.toClusterStatsRequest(uuid, inputDto);
            Iterator<StatSnapshot> statSnapshotIterator = statsServiceRpc.getClusterStats(clusterStatsRequest);
            while (statSnapshotIterator.hasNext()) {
                stats.add(statsMapper.toStatSnapshotApiDTO(statSnapshotIterator.next()));
            }
        } else {
            final boolean fullMarketRequest = UuidMapper.isRealtimeMarket(uuid);
            final Optional<Group> groupOptional = groupExpander.getGroup(uuid);

            // track if this is a plan or not.
            final boolean isPlanRequest;

            // determine the list of entity OIDs to query for this operation
            final Set<Long> entityStatOids;

            if (fullMarketRequest || isDefaultCloudGroupUuid) {
                // if the user is scoped, request their scope group members
                if (userSessionContext.isUserScoped()) {
                    entityStatOids = userSessionContext.getUserAccessScope().getScopeGroupMembers().toSet();
                } else {
                    // default case for market and cloud group - request all entities
                    entityStatOids = Collections.emptySet();
                }
                isPlanRequest = false;
            } else {
                // this request should be for either a group or a specific entity
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

                // check if this is a plan
                isPlanRequest = optPlan.isPresent();
                // if the user is scoped and this is not a plan, we need to check if the user has
                // access to the resulting entity set.
                if (!isPlanRequest) {
                    UserScopeUtils.checkAccess(userSessionContext, entityStatOids);
                }
            }

            final StatRequestParsedResultTuple requestStatsParsedResultPair = parseStats(inputDto);
            // OM-37484: give the startTime a +/- 60 second window for delineating between "current"
            // and "projected" stats requests. Without this window, the stats retrieval is too
            // sensitive to clock skew issues between the browser and the server, leading to incorrect
            // results in the UI.
            long currentStatsTimeWindowStart = clockTimeNow - liveStatsRetrievalWindow.toMillis();
            long currentStatsTimeWindowEnd = clockTimeNow + liveStatsRetrievalWindow.toMillis();
            if (requestStatsParsedResultPair.hasNonCostStat
                    && inputDto.getEndDate() != null
                    && DateTimeUtil.parseTime(inputDto.getEndDate()) > currentStatsTimeWindowEnd
                    && (!isPlanRequest)) {
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
                    || (startTime = DateTimeUtil.parseTime(inputDto.getStartDate())) < currentStatsTimeWindowEnd) {
                // Because of issues like https://vmturbo.atlassian.net/browse/OM-36537, where a UI
                // request expecting stats between "now" and "future" fails to get "now" results
                // because the specified start time is often (but not always) later than the latest
                // available live topology broadcast, we are adding a special clause for the
                // case where, if a "now" start date is specified, the request will be treated as a
                // request for the latest available live topology stats.
                //
                // A "now" request is identified as one with a start date falling within
                // (LATEST_LIVE_STATS_RETRIEVAL_TIME_THRESHOLD_MS) of the current system time.
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
                        && (startTime >= currentStatsTimeWindowStart)) {
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
                final Optional<Integer> globalTempGroupEntityType = getGlobalTempGroupEntityType(groupOptional);

                boolean isCloudEntityAndNotFullMarket = !fullMarketRequest && isCloudServiceEntity(uuid);

                // convert the stats snapshots to the desired ApiDTO and return them. Also, insert
                // them before any projected stats in the output collection, since the UI expects
                // the projected stats to always come last.
                // (We are still fetching projected stats first, since we may have mutated
                // the inputDTO in this clause that fetches live/historical stats)

                // move the get targets here, so they can be passed to building Cloud expense API dto
                final List<TargetApiDTO> finalTargets = targets = getTargets();
                //TODO refactor conditions (probably Strategy pattern)
                if (requestStatsParsedResultPair.hasCostStat) {
                    final Set<String> requestGroupBySet = parseGroupBy(inputDto);
                    if (isTopDownRequest(requestGroupBySet) && finalTargets != null && !finalTargets.isEmpty()) {
                        final Function<TargetApiDTO, String> valueFunction = getValueFunction(requestGroupBySet);
                        final Function<TargetApiDTO, String> typeFunction = getTypeFunction(requestGroupBySet);
                        // for group by Cloud services, we need to find all the services and
                        // stitch with expenses in Cost component
                        final List<BaseApiDTO> cloudServiceDTOs = requestGroupBySet.contains(CLOUD_SERVICE) ?
                                getDiscoveredServiceDTO() : Collections.emptyList();

                        final List<CloudCostStatRecord> cloudStatRecords =
                                getCloudExpensesRecordList(inputDto, uuid, requestGroupBySet,
                                        cloudServiceDTOs.stream()
                                                .map(BaseApiDTO::getUuid)
                                                .map(Long::valueOf)
                                                .collect(Collectors.toSet()));
                        stats.addAll(cloudStatRecords.stream()
                                .map(snapshot -> statsMapper.toStatSnapshotApiDTO(snapshot,
                                        finalTargets,
                                        typeFunction,
                                        valueFunction,
                                        cloudServiceDTOs,
                                        targetsService))
                                .collect(Collectors.toList()));

                    } else if ((uuid != null && isCloudEntityAndNotFullMarket) || isDefaultCloudGroupUuid || fullMarketRequest) {
                        final Set<Long> cloudEntityOids;
                        if (optPlan.isPresent()) {
                            cloudEntityOids = fetchRelatedEntitiesForScopes(MarketMapper.getPlanScopeIds(optPlan.get()),
                                ENTITY_TYPES_COUNTED_AS_WORKLOAD, null).values().stream()
                                .flatMap(Set::stream)
                                .collect(Collectors.toSet());
                        } else {
                            cloudEntityOids = entityStatOids;
                        }
                        List<CloudCostStatRecord> cloudCostStatRecords = getCloudStatRecordList(
                            inputDto, uuid, cloudEntityOids, requestGroupBySet);
                        if (!isGroupByComponentRequest(requestGroupBySet)) {
                            // have to aggregate as needed.
                            cloudCostStatRecords = aggregate(cloudCostStatRecords, statsFilters);
                        }
                        List<StatSnapshotApiDTO> statSnapshots = cloudCostStatRecords.stream()
                                .map(statsMapper::toCloudStatSnapshotApiDTO)
                                .collect(Collectors.toList());
                        // remove COST_STATS_SET since it has already been fetched
                        statsFilters.removeIf(statApiInputDTO -> COST_STATS_SET.contains(statApiInputDTO.getName()));
                        // add numWorkloads, numVMs, numDBs, numDBSs if requested
                        final List<StatApiDTO> numWorkloadStats = getNumWorkloadStatSnapshot(
                            statsFilters, entityStatOids, optPlan);
                        if (!numWorkloadStats.isEmpty()) {
                            // add the numWorkloads to the same timestamp it it exists.
                            if (statSnapshots.isEmpty()) {
                                StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
                                statSnapshotApiDTO.setDate(inputDto.getEndDate());
                                statSnapshotApiDTO.setStatistics(numWorkloadStats);
                                statSnapshots.add(statSnapshotApiDTO);
                            } else {
                                // add numWorkloads to all snapshots
                                statSnapshots.forEach(statSnapshotApiDTO -> {
                                    List<StatApiDTO> statApiDTOs = new ArrayList<>();
                                    statApiDTOs.addAll(statSnapshotApiDTO.getStatistics());
                                    statApiDTOs.addAll(numWorkloadStats);
                                    statSnapshotApiDTO.setStatistics(statApiDTOs);
                                });
                            }
                            // remove NUM_WORKLOADS_STATS_SET since it has already been fetched
                            statsFilters.removeIf(statApiInputDTO ->
                                NUM_WORKLOADS_STATS_SET.contains(statApiInputDTO.getName()));
                        }
                        stats.clear();
                        stats.addAll(statSnapshots);
                    }
                }

                // Return if the input DTO has stats other than "cloudCost", retrieve them from other components, e.g. History
                // TODO: combine both Cloud cost and non-Cloud stats for plan when plan is enabled in Cost component.
                if (((uuid != null && !isDefaultCloudGroupUuid) || requestStatsParsedResultPair.hasNonCostStat)
                        // should execute if left filters are not empty, or original input is empty (which means everything)
                        && (!CollectionUtils.isEmpty(statsFilters) || CollectionUtils.isEmpty(inputDto.getStatistics()))) {
                    // we are only passing the possible global temp group type if the user is not scoped
                    // -- scoped users need to retrieve based on specific entities, rather than a global
                    // temp group
                    final GetAveragedEntityStatsRequest request =
                            statsMapper.toAveragedEntityStatsRequest(entityStatOids, inputDto,
                                    userSessionContext.isUserScoped() ? Optional.empty() : globalTempGroupEntityType);

                    final Iterable<StatSnapshot> statsIterator = () -> statsServiceRpc.getAveragedEntityStats(request);
                    final List<StatSnapshot> statsList = new ArrayList<>();
                    statsIterator.forEach(s -> statsList.add(s));
                    // Only do it in realtime: find the latest record in the past and clone it, use the new clone as the snapshot
                    // for current. The reason to do it is that DB may not necessarily have records that
                    // matches with the time point when API queries stats data, therefore we decide to use the
                    // value from the latest record in history to represent it.
                    if (!isPlanRequest) {
                        StatSnapshot latestSnapshot = getLatestSnapShotInPast(statsList, currentTimeStamp);
                        if (latestSnapshot != null &&  currentTimeStamp != DateTimeUtil.parseTime(latestSnapshot.getSnapshotDate())) {
                            statsList.add(latestSnapshot.toBuilder().clone()
                                          .setSnapshotDate(DateTimeUtil.toString(currentTimeStamp)).build());
                        }
                    }
                    stats.addAll(0, statsList.stream().map(statsMapper::toStatSnapshotApiDTO)
                            .collect(Collectors.toList()));
                }
            }
        }

        // filter out those commodities listed in BLACK_LISTED_STATS in StatsUtils
        return StatsUtils.filterStats(stats, targets != null ? targets : getTargets());
    }

    /**
     * A helper method to find if the entity with a specific uuid is a cloud entity.
     *
     * @param uuid The uuid of the service entity
     * @return true if it is a cloud service entity, false otherwise
     * @throws UnknownObjectException
     */
    public boolean isCloudServiceEntity(final String uuid) throws UnknownObjectException {
        // Finding if the service entity is a cloud service entity
        ServiceEntityApiDTO entity = repositoryApi.getServiceEntityForUuid(Long.valueOf(uuid));

        if (entity != null) {
            return entity.getEnvironmentType()
                    == com.vmturbo.api.enums.EnvironmentType.CLOUD;
        } else {
            return false;
        }
    }

    /**
     * A helper method to find the stats snapshot with the latest time stamp in history.
     *
     * @param statsSnapshots a list of snapshot builders
     * @param currentTimeStamp the current time stamp which is used to decide snapshots in history
     * @return a Stats.StatSnapshot.Builder
     */
    private @Nullable StatSnapshot getLatestSnapShotInPast(Iterable<StatSnapshot> statsSnapshots, long currentTimeStamp) {
        StatSnapshot latestRecordInPast = null;
        long latestTimeStamp = 0;
        for (StatSnapshot snapshot : statsSnapshots) {
            long snapShotTimeStamp = DateTimeUtil.parseTime(snapshot.getSnapshotDate());
            if (snapShotTimeStamp > latestTimeStamp && snapShotTimeStamp <= currentTimeStamp) {
                latestTimeStamp = snapShotTimeStamp;
                latestRecordInPast = snapshot;
            }
        }
        return latestRecordInPast;
    }

    // aggregate to one StatRecord per related entity type per CloudCostStatRecord
    private List<CloudCostStatRecord> aggregate(@Nonnull final List<CloudCostStatRecord> cloudStatRecords,
                                                @Nonnull final List<StatApiInputDTO> statsFilters) {
        boolean hasRiCostRequest = hasRequestedRICompute(statsFilters);
        return cloudStatRecords.stream().map(cloudCostStatRecord -> {
            final Builder builder = CloudCostStatRecord.newBuilder();
            builder.setSnapshotDate(cloudCostStatRecord.getSnapshotDate());

            final Map<CostCategory, Map<Integer, List<StatRecord>>> statRecordsMap = Maps.newHashMap();
            cloudCostStatRecord.getStatRecordsList().forEach(statRecord ->
                statRecordsMap.computeIfAbsent(statRecord.getCategory(), k -> new HashMap<>())
                    .computeIfAbsent(statRecord.getAssociatedEntityType(), a -> Lists.newArrayList())
                    .add(statRecord));

            // add ri stat record
            final List<StatRecord> riRecords = statRecordsMap.getOrDefault(CostCategory.RI_COMPUTE,
                Collections.emptyMap()).get(EntityType.VIRTUAL_MACHINE_VALUE);
            if (hasRiCostRequest && riRecords != null) {
                builder.addStatRecords(aggregate(riRecords, Optional.empty(), true));
            }

            statsFilters.stream()
                .filter(statApiInputDTO -> StringConstants.COST_PRICE.equals(statApiInputDTO.getName()))
                .filter(statApiInputDTO -> statApiInputDTO.getRelatedEntityType() != null)
                .forEach(statApiInputDTO -> {
                    final String relatedEntityType = statApiInputDTO.getRelatedEntityType();
                    final Set<String> filters = CollectionUtils.emptyIfNull(statApiInputDTO.getFilters()).stream()
                        .map(StatFilterApiDTO::getValue).collect(Collectors.toSet());

                    // two types of request for storage:
                    //   {"name":"costPrice","relatedEntityType":"Storage"}
                    //   {"filters":[{"type":"costComponent","value":"STORAGE"}]
                    if ((relatedEntityType.equals(UIEntityType.VIRTUAL_MACHINE.apiStr()) && filters.contains("STORAGE"))
                            || relatedEntityType.equals(UIEntityType.STORAGE.apiStr())) {
                        // for the category storage, only get the record of entity type VM, since
                        // the cost of entity type volume is included in the vm record
                        final List<StatRecord> statRecordsList = statRecordsMap.getOrDefault(
                            CostCategory.STORAGE, Collections.emptyMap()).get(EntityType.VIRTUAL_MACHINE_VALUE);
                        if (!CollectionUtils.isEmpty(statRecordsList)) {
                            builder.addStatRecords(aggregate(statRecordsList, Optional.of(EntityType.STORAGE_VALUE), false));
                        }
                    } else if (relatedEntityType.equals(CompositeEntityTypesSpec.WORKLOAD_ENTITYTYPE)) {
                        final List<StatRecord> statRecordsList;
                        if (filters.isEmpty()) {
                            // add all if no filters
                            statRecordsList = cloudCostStatRecord.getStatRecordsList();
                        } else {
                            // add on demand compute
                            statRecordsList = statRecordsMap.getOrDefault(
                                CostCategory.ON_DEMAND_COMPUTE, Collections.emptyMap()).values()
                                .stream().flatMap(List::stream).collect(Collectors.toList());
                            if (filters.contains(StringConstants.ON_DEMAND_COMPUTE_LICENSE_COST)) {
                                // add license cost
                                statRecordsList.addAll(statRecordsMap.getOrDefault(
                                    CostCategory.LICENSE, Collections.emptyMap()).values().stream()
                                    .flatMap(List::stream).collect(Collectors.toList()));
                            }
                        }
                        if (!statRecordsList.isEmpty()) {
                            builder.addStatRecords(aggregate(statRecordsList, Optional.empty(), false));
                        }
                    } else {
                        int entityType = UIEntityType.fromString(relatedEntityType).typeNumber();
                        final List<StatRecord> statRecordsList;
                        if (filters.isEmpty()) {
                            // add all if no filters
                            statRecordsList = statRecordsMap.values().stream()
                                .filter(map -> map.containsKey(entityType))
                                .flatMap(map -> map.get(entityType).stream())
                                .collect(Collectors.toList());
                        } else {
                            // add on demand compute
                            statRecordsList = ListUtils.emptyIfNull(statRecordsMap.getOrDefault(
                                CostCategory.ON_DEMAND_COMPUTE, Collections.emptyMap()).get(entityType));
                            if (filters.contains(StringConstants.ON_DEMAND_COMPUTE_LICENSE_COST)) {
                                // add license cost
                                statRecordsList.addAll(ListUtils.emptyIfNull(statRecordsMap.getOrDefault(
                                    CostCategory.LICENSE, Collections.emptyMap()).get(entityType)));
                            }
                        }
                        if (!statRecordsList.isEmpty()) {
                            builder.addStatRecords(aggregate(statRecordsList, Optional.of(entityType), false));
                        }
                    }
                });
            return builder.build();
        }).collect(Collectors.toList());
    }

    /**
     * Aggregate a list of StatRecord into one StatRecord. Set relatedEntityType if provided.
     */
    private StatRecord aggregate(@Nonnull List<StatRecord> statRecordsList,
                                 @Nonnull Optional<Integer> relatedEntityType,
                                 final boolean isRiCost) {
        final StatRecord.Builder statRecordBuilder = CloudCostStatRecord.StatRecord.newBuilder();
        final StatRecord.StatValue.Builder statValueBuilder = CloudCostStatRecord.StatRecord.StatValue.newBuilder();
        statValueBuilder.setAvg((float) statRecordsList.stream().map(record -> record.getValues().getAvg())
            .mapToDouble(v -> v).average().orElse(0));
        statValueBuilder.setMax((float) statRecordsList.stream().map(record -> record.getValues().getAvg())
            .mapToDouble(v -> v).max().orElse(0));
        statValueBuilder.setMin((float) statRecordsList.stream().map(record -> record.getValues().getAvg())
            .mapToDouble(v -> v).min().orElse(0));
        statValueBuilder.setTotal((float) statRecordsList.stream().map(record -> record.getValues().getAvg())
            .mapToDouble(v -> v).sum());
        statRecordBuilder.setValues(statValueBuilder.build());
        if (isRiCost) {
            statRecordBuilder.setName(StringConstants.RI_COST);
        } else {
            statRecordBuilder.setName(StringConstants.COST_PRICE);
        }
        statRecordBuilder.setUnits(StringConstants.DOLLARS_PER_HOUR);
        relatedEntityType.ifPresent(statRecordBuilder::setAssociatedEntityType);
        return statRecordBuilder.build();
    }

    /**
     * Collect the names of the stats (for number of entities by tier type) from original input
     * stats into a new set, and remove from original list.
     */
    private Set<String> collectCloudPlanNumEntitiesByTierStatNames(@Nonnull Optional<PlanInstance> optPlan,
                                                                   @Nullable List<StatApiInputDTO> stats) {
        if (!optPlan.isPresent() || CollectionUtils.isEmpty(stats)) {
            return Collections.emptySet();
        }
        final String type = optPlan.get().getScenario().getScenarioInfo().getType();
        if (!CLOUD_PLAN_TYPES.contains(type)) {
            return Collections.emptySet();
        }
        Set<String> statsNames = new HashSet<>();
        Iterator<StatApiInputDTO> statsIterator = stats.iterator();
        while (statsIterator.hasNext()) {
            StatApiInputDTO statApiInputDTO = statsIterator.next();
            // this is for "Cloud Template Summary By Type", but ccc chart also passes numWorkload
            // as stat name, we need to handle them differently, so check filters since ccc chart
            // passes filters here, we might need to change UI to handle it better
            if (CLOUD_PLAN_ENTITIES_BY_TIER_STATS.contains(statApiInputDTO.getName()) &&
                    statApiInputDTO.getFilters() == null) {
                statsNames.add(statApiInputDTO.getName());
                statsIterator.remove();
            }
        }
        return statsNames;
    }

    /**
     * For cloud plan, fetch stats for the number of entities grouped by tier.
     *
     * @param planInstance the plan instance to fetch stats for
     * @param statsNames list of stats to fetch
     * @return list of StatSnapshotApiDTOs
     * @throws Exception
     */
    private List<StatSnapshotApiDTO> fetchNumEntitiesByTierStatsForCloudPlan(
            @Nonnull PlanInstance planInstance,
            @Nonnull Set<String> statsNames) throws Exception {
        final Long planTopologyContextId = planInstance.getPlanId();
        // find plan scope ids
        Set<Long> scopes = MarketMapper.getPlanScopeIds(planInstance);
        // return two snapshot, one for before plan, one for after plan
        StatSnapshotApiDTO statSnapshotBeforePlan = new StatSnapshotApiDTO();
        StatSnapshotApiDTO statSnapshotAfterPlan = new StatSnapshotApiDTO();
        List<StatApiDTO> statsBeforePlan = new ArrayList<>();
        List<StatApiDTO> statsAfterPlan = new ArrayList<>();

        for (String statName : statsNames) {
            if (StringConstants.NUM_VIRTUAL_DISKS.equals(statName)) {
                statsBeforePlan.addAll(getNumVirtualDisksStats(scopes, null));
                statsAfterPlan.addAll(getNumVirtualDisksStats(scopes, planTopologyContextId));
            } else if (StringConstants.NUM_WORKLOADS.equals(statName)) {
                statsBeforePlan.addAll(getNumWorkloadsByTierStats(scopes, null));
                statsAfterPlan.addAll(getNumWorkloadsByTierStats(scopes, planTopologyContextId));
            }
        }

        // set stats
        statSnapshotBeforePlan.setStatistics(statsBeforePlan);
        statSnapshotAfterPlan.setStatistics(statsAfterPlan);
        // set stats time, use plan start time as startDate, use one day later as endDate
        long planTime = planInstance.getStartTime();
        statSnapshotBeforePlan.setDate(DateTimeUtil.toString(planTime));
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(planTime);
        c.add(Calendar.DATE, 1);
        statSnapshotAfterPlan.setDate(DateTimeUtil.toString(c.getTimeInMillis()));

        return Lists.newArrayList(statSnapshotBeforePlan, statSnapshotAfterPlan);
    }

    /**
     * Get the stats of number of virtual disks by tier.
     *
     * @param scopes a set of volume ids
     * @param planTopologyContextId null or plan context id
     * @return list of stats for volumes
     * @throws Exception
     */
    private List<StatApiDTO> getNumVirtualDisksStats(@Nonnull Set<Long> scopes,
                                                     @Nullable Long planTopologyContextId) throws Exception {
        String volumeEntityType = UIEntityType.VIRTUAL_VOLUME.apiStr();
        // get all volumes ids in the plan scope, using supply chain fetcher
        Set<Long> volumeIds = fetchRelatedEntitiesForScopes(scopes,
            Lists.newArrayList(volumeEntityType), null).get(volumeEntityType);
        return fetchNumEntitiesByTierStats(volumeIds, planTopologyContextId,
            StringConstants.NUM_VIRTUAL_DISKS, StringConstants.TIER, ENTITY_TYPE_TO_GET_TIER_FUNCTION.get(volumeEntityType));
    }

    /**
     * Fetch the stats for the number of entities grouped by tier.
     *
     * @param entityIds ids of entities to fetch stats for
     * @param planTopologyContextId plan TopologyContextId, or null if for real time
     * @param statName the name of the stat to fetch
     * @param filterType is this a tier or template
     * @param getTierId the function to get tier id from the entity
     * @return list of StatApiDTOs
     */
    private List<StatApiDTO> fetchNumEntitiesByTierStats(@Nonnull Set<Long> entityIds,
                                                         @Nullable Long planTopologyContextId,
                                                         @Nonnull String statName,
                                                         @Nonnull String filterType,
                                                         @Nonnull Function<TopologyEntityDTO, Long> getTierId) {
        // fetch entities
        List<TopologyEntityDTO> entities = fetchTopologyEntityDTOs(entityIds, planTopologyContextId);
        // tier id --> number of entities using the tier
        Map<Long, Long> tierIdToNumEntities = entities.stream()
            .collect(Collectors.groupingBy(getTierId, Collectors.counting()));
        // tier id --> tier name
        Map<Long, String> tierIdToName = fetchTopologyEntityDTOs(tierIdToNumEntities.keySet(), planTopologyContextId).stream()
            .collect(Collectors.toMap(TopologyEntityDTO::getOid, TopologyEntityDTO::getDisplayName));

        return tierIdToNumEntities.entrySet().stream()
            .map(entry -> createStatApiDTOForPlan(statName, entry.getValue(),
                filterType, tierIdToName.get(entry.getKey()), planTopologyContextId == null))
            .collect(Collectors.toList());
    }

    /**
     * Create StatApiDTO based on given parameters, if beforePlan is true, then it is a stat for
     * real time; if false, it is a stat for after plan. Related filter is created to indicate
     * whether this is a stat before plan or after plan.
     */
    private StatApiDTO createStatApiDTOForPlan(String statName, Long statValue, String filterType,
                                               String filterValue, boolean beforePlan) {
        StatApiDTO statApiDTO = new StatApiDTO();
        statApiDTO.setName(statName);
        statApiDTO.setValue(Float.valueOf(statValue));
        // filters
        List<StatFilterApiDTO> statFilters = new ArrayList<>();
        // tier filter
        StatFilterApiDTO tierFilter = new StatFilterApiDTO();
        tierFilter.setType(filterType);
        tierFilter.setValue(filterValue);
        statFilters.add(tierFilter);
        // only add if it's real time market
        if (beforePlan) {
            StatFilterApiDTO planFilter = new StatFilterApiDTO();
            planFilter.setType(StringConstants.RESULTS_TYPE);
            planFilter.setValue(StringConstants.BEFORE_PLAN);
            statFilters.add(planFilter);
        }
        // set filters
        statApiDTO.setFilters(statFilters);
        return statApiDTO;
    }

    private List<StatApiDTO> getNumWorkloadsByTierStats(@Nonnull Set<Long> scopes,
                                                        @Nullable Long planTopologyContextId) throws Exception {
        // fetch related entities ids for given scopes
        final Map<String, Set<Long>> idsByEntityType = fetchRelatedEntitiesForScopes(scopes,
            ENTITY_TYPES_COUNTED_AS_WORKLOAD, null);
        return idsByEntityType.entrySet().stream()
            .flatMap(entry -> fetchNumEntitiesByTierStats(entry.getValue(), planTopologyContextId,
                StringConstants.NUM_WORKLOADS, StringConstants.TEMPLATE,
                ENTITY_TYPE_TO_GET_TIER_FUNCTION.get(entry.getKey())).stream()
            ).collect(Collectors.toList());
    }

    /**
     * Fetch the related entities for a given scope list.
     *
     * @param scopes ids of scope to fetch related entities for
     * @param relatedEntityTypes list of related entity types to fetch
     * @return map from related entity type to entities
     * @throws Exception
     */
    public Map<String, Set<Long>> fetchRelatedEntitiesForScopes(@Nonnull Set<Long> scopes,
                                                                @Nonnull List<String> relatedEntityTypes,
                                                                @Nullable EnvironmentType environmentType) throws Exception {
        // get all VMs ids in the plan scope, using supply chain fetcher
        SupplychainApiDTO supplychain = supplyChainFetcherFactory.newApiDtoFetcher()
            .topologyContextId(realtimeTopologyContextId)
            .addSeedUuids(scopes.stream().map(String::valueOf).collect(Collectors.toList()))
            .entityTypes(relatedEntityTypes)
            .entityDetailType(EntityDetailType.entity)
            .environmentType(environmentType)
            .fetch();
        return supplychain.getSeMap().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry ->
                entry.getValue().getInstances().keySet().stream()
                    .map(Long::valueOf)
                    .collect(Collectors.toSet())));
    }

    /**
     * Fetch the TopologyEntityDTOs from given plan TopologyContextId. If context id is not
     * provided, it fetch from real time topology.
     *
     * @param entityIds ids of entities to fetch
     * @param planTopologyContextId context id of the plan topology to fetch entities from,
     *                              or empty if it's for real time topology
     * @return list of TopologyEntityDTOs
     */
    private List<TopologyEntityDTO> fetchTopologyEntityDTOs(@Nonnull Set<Long> entityIds,
                                                            @Nullable Long planTopologyContextId) {
        if (entityIds.isEmpty()) {
            return Collections.emptyList();
        }

        if (planTopologyContextId != null) {
            SearchPlanTopologyEntityDTOsRequest request = SearchPlanTopologyEntityDTOsRequest.newBuilder()
                .setTopologyContextId(planTopologyContextId)
                .addAllEntityOid(entityIds)
                .build();
            return searchServiceClient.searchPlanTopologyEntityDTOs(request).getTopologyEntityDtosList();
        } else {
            SearchTopologyEntityDTOsRequest request = SearchTopologyEntityDTOsRequest.newBuilder()
                .addAllEntityOid(entityIds)
                .build();
            return searchServiceClient.searchTopologyEntityDTOs(request).getTopologyEntityDtosList();
        }
    }

    private List<CloudCostStatRecord> getCloudStatRecordList(@Nonnull final StatPeriodApiInputDTO inputDto,
                                                             @Nonnull final String uuid,
                                                             @Nonnull final Set<Long> entityStatOids,
                                                             @Nonnull final Set<String> requestGroupBySet) {
        final GetCloudCostStatsRequest.Builder builder = GetCloudCostStatsRequest.newBuilder();
        // set entity types filter
        final Set<Integer> relatedEntityTypes = getRelatedEntityTypes(inputDto.getStatistics());
        if (!relatedEntityTypes.isEmpty()) {
            builder.setEntityTypeFilter(EntityTypeFilter.newBuilder()
                .addAllEntityTypeId(relatedEntityTypes)
                .build());
        }
        //TODO consider create default Cloud group (probably in Group component). Currently the only group
        if (!isDefaultCloudGroupUuid(uuid) && !entityStatOids.isEmpty()) {
            builder.getEntityFilterBuilder().addAllEntityId(entityStatOids);
        }
        if (isGroupByComponentRequest(requestGroupBySet)) {
            builder.setGroupBy(GetCloudCostStatsRequest.GroupByType.COSTCOMPONENT);
        }
        // assuming that if endDate is not set, startDate is not set
        if (inputDto.getEndDate() != null) {
            builder.setEndDate(Long.valueOf(inputDto.getEndDate()));
            if (inputDto.getStartDate() != null) {
                builder.setStartDate(Long.valueOf(inputDto.getStartDate()));
            } else {
                // for CCC chart, the startDate is not set by UI after refactor of CCC
                // set it to 10 min earlier from now
                builder.setStartDate(
                    Instant.ofEpochMilli(clock.millis()).minus(Duration.ofMinutes(10)).toEpochMilli());
            }
        }

        return costServiceRpc.getCloudCostStats(builder.build()).getCloudStatRecordList();
    }

    private boolean isGroupByComponentRequest(Set<String> requestGroupBySet) {
        return requestGroupBySet.contains(COSTCOMPONENT);
    }

    /**
     * Return the count of VMs + Database + DatabaseServers in the cloud.
     */
    private List<StatApiDTO> getNumWorkloadStatSnapshot(@Nonnull List<StatApiInputDTO> statsFilters,
                                                        @Nonnull Set<Long> entityStatOids,
                                                        @Nonnull Optional<PlanInstance> optPlan) throws Exception {
        final Set<Long> scopeIds = optPlan.isPresent()
            ? MarketMapper.getPlanScopeIds(optPlan.get())
            : entityStatOids;
        List<StatApiDTO> stats = Lists.newArrayList();
        for (StatApiInputDTO statApiInputDTO : statsFilters) {
            List<String> entityTypes = WORKLOAD_NAME_TO_ENTITY_TYPES.get(statApiInputDTO.getName());
            if (entityTypes != null) {
                Map<String, Set<Long>> relatedEntities = fetchRelatedEntitiesForScopes(scopeIds,
                    entityTypes, EnvironmentType.CLOUD);
                final float numWorkloads = relatedEntities.values().stream()
                    .flatMap(Set::stream).count();
                final StatApiDTO statApiDTO = new StatApiDTO();
                statApiDTO.setName(statApiInputDTO.getName());
                statApiDTO.setValue(numWorkloads);
                final StatValueApiDTO statValueApiDTO = new StatValueApiDTO();
                statValueApiDTO.setAvg(numWorkloads);
                statValueApiDTO.setMax(numWorkloads);
                statValueApiDTO.setMin(numWorkloads);
                statValueApiDTO.setTotal(numWorkloads);
                statApiDTO.setValues(statValueApiDTO);
                stats.add(statApiDTO);
            }
        }
        return stats;
    }

    private List<CloudCostStatRecord> getCloudExpensesRecordList(@Nonnull final StatPeriodApiInputDTO inputDto,
                                                                 @Nonnull final String uuid,
                                                                 @Nonnull final Set<String> requestGroupBySet,
                                                                 @Nonnull final Set<Long> entityStatOids) {
        final GetCloudExpenseStatsRequest.Builder builder = GetCloudExpenseStatsRequest.newBuilder();
        if (inputDto.getStartDate() != null && inputDto.getEndDate() != null) {
            builder.setStartDate(Long.valueOf(inputDto.getStartDate()));
            builder.setEndDate(Long.valueOf(inputDto.getEndDate()));
        }
        if (!isDefaultCloudGroupUuid(uuid) && !entityStatOids.isEmpty()) {
            builder.getEntityFilterBuilder().addAllEntityId(entityStatOids);
        }
        if (requestGroupBySet.contains(TARGET)) {
            builder.setGroupBy(GroupByType.TARGET);
        } else if (requestGroupBySet.contains(CSP)){
            builder.setGroupBy(GroupByType.CSP);
        } else if (requestGroupBySet.contains(CLOUD_SERVICE)) {
            builder.setGroupBy(GroupByType.CLOUD_SERVICE);
        }

        return costServiceRpc.getAccountExpenseStats(
                builder.build()).getCloudStatRecordList();
    }

    private Set<Integer> getRelatedEntityTypes(@Nullable List<StatApiInputDTO> statApiInputDTOs) {
        if (CollectionUtils.isEmpty(statApiInputDTOs)) {
            return Collections.emptySet();
        }

        final Set<Integer> relatedEntityTypes = new HashSet<>();
        statApiInputDTOs.stream()
            .filter(statApiInputDTO -> statApiInputDTO.getRelatedEntityType() != null)
            .forEach(statApiInputDTO -> {
                String entityType = statApiInputDTO.getRelatedEntityType();
                if (CompositeEntityTypesSpec.WORKLOAD_ENTITYTYPE.equals(entityType)) {
                    relatedEntityTypes.addAll(ENTITY_TYPES_COUNTED_AS_WORKLOAD.stream()
                        .map(UIEntityType::fromString)
                        .map(UIEntityType::typeNumber)
                        .collect(Collectors.toSet())
                    );
                } else {
                    relatedEntityTypes.add(UIEntityType.fromString(entityType).typeNumber());
                }
            });
        return relatedEntityTypes;
    }

    // It seems there is no easy way to distinguish top down (expense) or bottom up (cost) requests.
    // Since both using {"statistics":[{"name":"costPrice" ...
    // Now, it seems following group by are top down requests.
    private boolean isTopDownRequest(final Set<String> requestGroupBySet) {
        return requestGroupBySet.contains(TARGET)
                || requestGroupBySet.contains(CSP)
                || requestGroupBySet.contains(CLOUD_SERVICE);
    }

    // Search discovered Cloud services.
    private List<BaseApiDTO> getDiscoveredServiceDTO() {
        try {
            // find all cloud services
            final SearchTopologyEntityDTOsResponse response =
                searchServiceClient.searchTopologyEntityDTOs(SearchTopologyEntityDTOsRequest.newBuilder()
                    .addSearchParameters(SearchParameters.newBuilder()
                        .setStartingFilter(SearchProtoUtil.entityTypeFilter(
                            UIEntityType.CLOUD_SERVICE.apiStr())))
                    .build());
            return response.getTopologyEntityDtosList().stream()
                .map(topologyEntity -> ServiceEntityMapper.toServiceEntityApiDTO(topologyEntity, null))
                .collect(Collectors.toList());
        } catch (Exception e) {
            logger.error("Failed to search Cloud service");
            return Collections.emptyList();
        }
    }

    // function to populate the statistics -> filters -> type for API DTO
    private Function<TargetApiDTO, String> getTypeFunction(@Nonnull final Set<String> requestGroupBySet) {
        if (requestGroupBySet.contains(TARGET)) return s -> TARGET;
        if (requestGroupBySet.contains(CSP)) return s -> CSP;
        if (requestGroupBySet.contains(CLOUD_SERVICE)) return s -> CLOUD_SERVICE;
        throw ApiUtils.notImplementedInXL();
    }

    // function to populate the statistics -> filters -> value for API DTO
    private Function<TargetApiDTO, String> getValueFunction(@Nonnull final Set<String> requestGroupBySet) {
        if (requestGroupBySet.contains(TARGET)) return s -> s.getDisplayName();
        if (requestGroupBySet.contains(CSP)) return s -> s.getType();
        if (requestGroupBySet.contains(CLOUD_SERVICE)) return s -> CLOUD_SERVICE;
        throw ApiUtils.notImplementedInXL();
    }

    // get all the discovered targets
    private List<TargetApiDTO> getTargets() {
        List<TargetApiDTO> targets = null;
        try {
            targets = targetsService.getTargets(null);
        } catch (RuntimeException e) {
            logger.error("Unable to get targets list due to error: {}." +
                    " Not using targets list for stat filtering.", e.getMessage());
        }
        return targets;
    }

    /**
     * Parse input DTO to get:
     * 1. Does it requires Cost stats?
     * 2. Does it requires Non-Cost stats?
     * @param inputDto StatPeriodApiInputDTO
     * @return both answer in Pair
     */
    private StatRequestParsedResultTuple parseStats(@Nonnull final StatPeriodApiInputDTO inputDto) {
        final Collection<StatApiInputDTO> collection = CollectionUtils.emptyIfNull(inputDto.getStatistics());
        boolean hasCostStat = false;
        boolean hasNonCostStat = false;
        for (StatApiInputDTO dto: collection) {
            if (COST_STATS_SET.contains(dto.getName())) {
                hasCostStat = true;
            } else {
                hasNonCostStat = true;
            }
            if (hasCostStat && hasNonCostStat) {
                break;
            }
        }
        return new StatRequestParsedResultTuple(hasCostStat, hasNonCostStat);
    }

    /**
     * Return true if the request DTO has RICost or else return false.
     */
    private boolean hasRequestedRICompute(@Nonnull final List<StatApiInputDTO> statsFilters) {
        return statsFilters.stream()
                .anyMatch(dto -> StringConstants.RI_COST.equals(dto.getName()));
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

    // check if the UUID is a predefined default Cloud group
    private boolean isDefaultCloudGroupUuid(@Nonnull final String uuid) {
        return DefaultCloudGroupProducer.getDefaultCloudGroup().stream()
            .anyMatch(group -> group.getUuid().equals(uuid));
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
                final ServiceEntityApiDTO serviceEntityApiDTO = toServiceEntityApiDTO(planEntity, null);
                entityStatsApiDTO.setUuid(Long.toString(planEntity.getOid()));
                entityStatsApiDTO.setDisplayName(planEntity.getDisplayName());
                entityStatsApiDTO.setClassName(UIEntityType.fromType(planEntity.getEntityType()).apiStr());
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
                // verify the user has access to the cluster members. If any are inaccessible, this
                // method will propagate an AccessDeniedException.
                if (userSessionContext.isUserScoped() && group.getCluster().hasMembers()) {
                    UserScopeUtils.checkAccess(userSessionContext.getUserAccessScope(),
                            group.getCluster().getMembers().getStaticMemberOidsList());
                }
                final EntityStatsApiDTO statsDTO = new EntityStatsApiDTO();
                final String uuid = String.valueOf(group.getId());
                statsDTO.setUuid(uuid);
                statsDTO.setClassName(StringConstants.CLUSTER);
                statsDTO.setDisplayName(group.getCluster().getDisplayName());
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
                .filter(type -> !type.equals(UIEntityType.UNKNOWN.apiStr()))
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

            // if the user is scoped, we will request the subset of entity oids in the user scope
            // that match the related type.
            if (userSessionContext.isUserScoped()) {
                expandedUuids = userSessionContext.getUserAccessScope()
                        .getAccessibleOidsByEntityType(relatedType.get())
                        .toSet();
                logger.debug("getExpandedScope() using cached oids for scoped user. found {} oids for related entity type {}",
                        expandedUuids.size(), relatedType.get());
            } else {
                // otherwise, use a supply chain query to get them
                final Map<String, SupplyChainNode> result = supplyChainFetcherFactory.newNodeFetcher()
                        .entityTypes(Collections.singletonList(relatedType.get()))
                        .fetch();
                final SupplyChainNode relatedTypeNode = result.get(relatedType.get());
                expandedUuids = relatedTypeNode == null ?
                        Collections.emptySet() : RepositoryDTOUtil.getAllMemberOids(relatedTypeNode);
            }
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
            // if the user is scoped, we will filter the entities according to their scope
            if (userSessionContext.isUserScoped()) {
                return userSessionContext.getUserAccessScope().filter(expandedUuids);
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
                .filter(type -> !type.equals(UIEntityType.UNKNOWN.apiStr()))
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

            // if the user is scoped, set this request to the set of in-scope entities of the
            // requested 'relatedType'
            if (userSessionContext.isUserScoped()) {
                entityStatsScope.setEntityList(EntityList.newBuilder()
                        .addAllEntities(userSessionContext.getUserAccessScope()
                                .getAccessibleOidsByEntityType(relatedType.get()))
                        .build());
            } else {
                // Otherwise, just set the entityType field on the stats scope.
                entityStatsScope.setEntityType(UIEntityType.fromString(relatedType.get()).typeNumber());
            }
        } else if (inputDto.getScopes().size() == 1) {
            // Check if we can do the global entity type optimization.
            final Optional<Integer> globalEntityType =
                    getGlobalTempGroupEntityType(groupExpander.getGroup(inputDto.getScopes().get(0)));
            final Optional<Integer> relatedTypeInt = relatedType
                .map(UIEntityType::fromString)
                .map(UIEntityType::typeNumber);
            if (globalEntityType.isPresent()
                    // We can only do the global entity type optimization if the related type
                    // is unset, or is the same as the global entity type. Why? Because the
                    // topology graph may not be connected. For example, suppose global entity type
                    // is PM and related type is VM. We can't just set the entity stats scope to
                    // VM, because there may be VMs not connected to PMs (e.g. in the cloud).
                    && (!relatedTypeInt.isPresent() || relatedTypeInt.equals(globalEntityType))
            ) {
                // if the user is scoped, set this request to the set of in-scope entities of the
                // requested 'relatedType'. Note that this is different than classic, which applies
                // the regular scope check to the global temp group, resulting in "no access".
                // TODO: Perhaps we should do the same, but it feels like a bug.
                if (userSessionContext.isUserScoped()) {
                    entityStatsScope.setEntityList(EntityList.newBuilder()
                        .addAllEntities(userSessionContext.getUserAccessScope()
                            .getAccessibleOidsByEntityType(
                                UIEntityType.fromType(globalEntityType.get()).apiStr()))
                        .build());
                } else {
                    entityStatsScope.setEntityType(globalEntityType.get());
                }
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

        // Is the startDate in the past?
        // If so (or if no startDate is provided), historical stats will be retrieved
        final Optional<StatPeriodApiInputDTO> period = Optional.ofNullable(inputDto.getPeriod());
        final boolean historicalStatsRequest = period
                .map(StatPeriodApiInputDTO::getStartDate)
                .map(startDate -> DateTimeUtil.parseTime(startDate) < clockTimeNow)
                .orElse(true);

        // Is the endDate in the future?
        // If so (and if historical stats are not being retrieved), projections will be retrieved
        // If no end date is provided, projections will not be retrieved under any circumstances
        final boolean projectedStatsRequest = period
                .map(StatPeriodApiInputDTO::getEndDate)
                .map(endDate -> DateTimeUtil.parseTime(endDate) > clockTimeNow)
                .orElse(false);

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
    public EntityStatsPaginationResponse getStatsByUuidsQuery(@Nonnull StatScopesApiInputDTO inputDto,
                                                  EntityStatsPaginationRequest paginationRequest)
            throws Exception {
        // All scopes must enter the magic gateway before they can proceed!
        inputDto.setScopes(magicScopeGateway.enter(inputDto.getScopes()));

        final Optional<PlanInstance> planInstance = getRequestedPlanInstance(inputDto);
        if (planInstance.isPresent()) {
            return getPlanEntityStats(planInstance.get(), inputDto, paginationRequest);
        } else if (containsAnyClusterStats(Optional.ofNullable(inputDto.getPeriod())
                                            .map(StatPeriodApiInputDTO::getStatistics)
                                            .orElse(Collections.emptyList()))) {
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
                                                      @Nonnull final StatScopesApiInputDTO inputDto) {
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
                        // in the
                        Optional.ofNullable(inputDto.getPeriod())
                                .map(StatPeriodApiInputDTO::getEndDate)
                                .ifPresent(statApiDto::setDate);
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
    private Set<Long> expandGroupingServiceEntities(Collection<Long> entityOidsToExpand) {
        // Early return if the input is empty, to prevent making
        // the initial RPC call.
        if (entityOidsToExpand.isEmpty()) {
            return Collections.emptySet();
        }

        final Set<Long> expandedEntityOids = Sets.newHashSet();
        // get all service entities which need to expand.
        final Set<ServiceEntityApiDTO> expandServiceEntities = ENTITY_TYPES_TO_EXPAND.keySet().stream()
                .flatMap(entityType -> repositoryApi.getSearchResults(
                    null, Collections.singletonList(entityType), null).stream())
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
        if (!groupOptional.isPresent() || !groupOptional.get().hasTempGroup()) {
            return Optional.empty();
        }

        final TempGroupInfo tempGroup = groupOptional.get().getTempGroup();
        final boolean isGlobalTempGroup = tempGroup.getIsGlobalScopeGroup()
                // TODO (roman, Nov 21 2018) OM-40569: Add proper support for environment type for
                // the global scope optimization.
                //
                // Right now we treat "on-prem" groups as "global" groups to avoid
                // performance regressions in large customer environments. However, this will give
                // incorrect results for entity types that can appear in both cloud an on-prem
                // environments (e.g. VMs). Since no customers of XL currently use the cloud
                // capabilities, this is ok as a short-term fix.
                && (!tempGroup.hasEnvironmentType() || tempGroup.getEnvironmentType() == EnvironmentTypeEnum.EnvironmentType.ON_PREM);

        // if it is global temp group and need to expand, should return target expand entity type.
        if (isGlobalTempGroup && ENTITY_TYPES_TO_EXPAND.containsKey(
                UIEntityType.fromType(tempGroup.getEntityType()).apiStr())) {
            return Optional.of(UIEntityType.fromString(ENTITY_TYPES_TO_EXPAND.get(
                UIEntityType.fromType(tempGroup.getEntityType()))).typeNumber());
        } else if (isGlobalTempGroup) {
            // if it is global temp group and not need to expand.
            return Optional.of(tempGroup.getEntityType());
        } else {
            return Optional.empty();
        }
    }

    private Set<String> parseGroupBy(final StatPeriodApiInputDTO inputDto) {
        return inputDto.getStatistics().stream()
                .filter(dto -> dto.getGroupBy() != null)
                .flatMap(apiInputDTO -> apiInputDTO.getGroupBy().stream())
                .collect(Collectors.toSet());
    }

    /**
     * Check if the input request is for reserved instance coverage or not.
     *
     * @param inputStats a list of {@link StatApiInputDTO}.
     * @return return true if request is for reserved instance coverage, otherwise return false;
     */
    private boolean isRequestForRICoverageOrUtilizationStats(
            @Nullable final List<StatApiInputDTO> inputStats) {
        if (inputStats != null && inputStats.size() == 1 &&
            (StringConstants.RI_COUPON_COVERAGE.equals(inputStats.get(0).getName()) ||
                StringConstants.RI_COUPON_UTILIZATION.equals(inputStats.get(0).getName()))) {
            return true;
        }
        return false;
    }

    private class StatRequestParsedResultTuple {
        final boolean hasCostStat;

        final boolean hasNonCostStat;

        public StatRequestParsedResultTuple(final boolean hasCostStat, final boolean hasNonCostStat) {
            this.hasCostStat = hasCostStat;
            this.hasNonCostStat = hasNonCostStat;
        }
    }
}

package com.vmturbo.api.component.external.api.service;

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

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ExceptionMapper;
import com.vmturbo.api.component.external.api.mapper.MarketMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.MagicScopeGateway;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryExecutor;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IStatsService;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.api.utils.EncodingUtil;
import com.vmturbo.api.utils.UrlsHelp;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.common.Pagination;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.TempGroupInfo;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;


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

    private final StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub statsServiceRpc;

    private final PlanServiceBlockingStub planRpcService;

    private final Clock clock;

    private final RepositoryApi repositoryApi;

    private final RepositoryServiceBlockingStub repositoryRpcService;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final GroupExpander groupExpander;

    private final StatsMapper statsMapper;

    private final GroupServiceBlockingStub groupServiceRpc;

    private final MagicScopeGateway magicScopeGateway;

    private final UserSessionContext userSessionContext;

    private final ServiceEntityMapper serviceEntityMapper;

    private final UuidMapper uuidMapper;

    private final StatsQueryExecutor statsQueryExecutor;

    // CLUSTER_STATS is a collection of the cluster-headroom stats calculated from nightly plans.
    // TODO: we should share the enum instead of keeping a separate copy.
    private static final Set<String> CLUSTER_STATS =
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
                    StringConstants.NUM_STORAGES);

    private static final String STAT_FILTER_PREFIX = "current";

    /**
     * Map Entity types to be expanded to the RelatedEntityType to retrieve. For example,
     * replace requests for stats for a DATACENTER entity with the PHYSICAL_MACHINEs
     * in that DATACENTER.
     */
    private static final Map<UIEntityType, UIEntityType> ENTITY_TYPES_TO_EXPAND = ImmutableMap.of(
            UIEntityType.DATACENTER, UIEntityType.PHYSICAL_MACHINE
    );

    // list of entity types which are counted as workload for cloud
    public static final List<String> ENTITY_TYPES_COUNTED_AS_WORKLOAD = ImmutableList.of(
        UIEntityType.VIRTUAL_MACHINE.apiStr(),
        UIEntityType.DATABASE.apiStr(),
        UIEntityType.DATABASE_SERVER.apiStr()
    );

    StatsService(@Nonnull final StatsHistoryServiceBlockingStub statsServiceRpc,
                 @Nonnull final PlanServiceBlockingStub planRpcService,
                 @Nonnull final RepositoryApi repositoryApi,
                 @Nonnull final RepositoryServiceBlockingStub repositoryRpcService,
                 @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                 @Nonnull final StatsMapper statsMapper,
                 @Nonnull final GroupExpander groupExpander,
                 @Nonnull final Clock clock,
                 @Nonnull final GroupServiceBlockingStub groupServiceRpc,
                 @Nonnull final MagicScopeGateway magicScopeGateway,
                 @Nonnull final UserSessionContext userSessionContext,
                 @Nonnull final ServiceEntityMapper serviceEntityMapper,
                 @Nonnull final UuidMapper uuidMapper,
                 @Nonnull final StatsQueryExecutor statsQueryExecutor) {
        this.statsServiceRpc = Objects.requireNonNull(statsServiceRpc);
        this.planRpcService = planRpcService;
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.repositoryRpcService = Objects.requireNonNull(repositoryRpcService);
        this.supplyChainFetcherFactory = supplyChainFetcherFactory;
        this.clock = Objects.requireNonNull(clock);
        this.groupExpander = groupExpander;
        this.groupServiceRpc = Objects.requireNonNull(groupServiceRpc);
        this.statsMapper = Objects.requireNonNull(statsMapper);
        this.magicScopeGateway = Objects.requireNonNull(magicScopeGateway);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
        this.serviceEntityMapper = Objects.requireNonNull(serviceEntityMapper);
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
        this.statsQueryExecutor = Objects.requireNonNull(statsQueryExecutor);
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

    /**
     * When a stats query contains related entity types and a non-empty scope, then
     * a supply chain traversal should bring all entities of the related entity types
     * that are transitively connected to the scope.  This is the role of this method.
     * If the list of related entity types is empty, then the method will return
     * without traversing the supply chain.
     *
     * @param entityIds scope of the stats query.
     * @param relatedEntityTypes a list of related entity types.
     * @return the set of all entity ids after the supply chain traversal
     * @throws OperationFailedException supply chain traversal failed
     */
    @Nonnull
    private Set<Long> performSupplyChainTraversal(
            @Nonnull Set<Long> entityIds,
            @Nonnull List<String> relatedEntityTypes
    ) throws OperationFailedException {
        return relatedEntityTypes.isEmpty() ? entityIds
                    : supplyChainFetcherFactory.expandScope(entityIds, relatedEntityTypes);
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
                final ServiceEntityApiDTO serviceEntityApiDTO =
                        serviceEntityMapper.toServiceEntityApiDTO(planEntity);
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

        if (inputDto.getScopes().isEmpty() || inputDto.getScopes().stream()
                .anyMatch(scope -> scope.equals(MarketMapper.MARKET))) {
            throw new IllegalArgumentException("Request with invalid scope : " +
                (inputDto.getScopes().isEmpty() ? "empty" :  MarketMapper.MARKET));
        }

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
     * @throws InterruptedException a gRPC call has been interrupted.
     */
    @Nonnull
    private Set<Long> getExpandedScope(@Nonnull final StatScopesApiInputDTO inputDto)
                throws OperationFailedException, InterruptedException {
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

            // Collect related entity types: if there are any we must perform supply chain traversal
            final List<String> relatedEntityTypes = new ArrayList<>();
            relatedType.ifPresent(relatedEntityTypes::add);
            if (inputDto.getPeriod() != null && inputDto.getPeriod().getStatistics() != null) {
                relatedEntityTypes.addAll(
                    inputDto.getPeriod().getStatistics().stream()
                        .map(StatApiInputDTO::getRelatedEntityType)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
            }

            // Expand groups and perform supply chain traversal with the resulting seed
            // Note that supply chain traversal will not happen if the list of related entity
            // types is empty
            expandedUuids =
                performSupplyChainTraversal(groupExpander.expandUuids(seedUuids), relatedEntityTypes);

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
     * @throws InterruptedException a gRPC call has been interrupted
     */
    @Nonnull
    private EntityStatsScope createEntityStatsScope(@Nonnull final StatScopesApiInputDTO inputDto)
            throws OperationFailedException, InterruptedException {
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
                throws OperationFailedException, InterruptedException {
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

        final Map<Long, MinimalEntity> entities = repositoryApi.entitiesRequest(entitiesWithStats)
            .getMinimalEntities()
            .collect(Collectors.toMap(MinimalEntity::getOid, Function.identity()));

        // 3. Combine the results of 1. and 2. and return.
        final List<EntityStatsApiDTO> dto = nextStatsPage.stream()
            .map(entityStats -> {
                final Optional<MinimalEntity> apiDto = Optional.ofNullable(entities.get(entityStats.getOid()));
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
        try {
            return getStatsByUuidsQueryInternal(inputDto, paginationRequest);
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
    }

    private EntityStatsPaginationResponse getStatsByUuidsQueryInternal(
            @Nonnull StatScopesApiInputDTO inputDto,
            EntityStatsPaginationRequest paginationRequest)
            throws OperationFailedException, InterruptedException {
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
    private EntityStatsApiDTO constructEntityStatsDto(MinimalEntity serviceEntity,
                                                      final boolean projectedStatsRequest,
                                                      final EntityStats entityStats,
                                                      @Nonnull final StatScopesApiInputDTO inputDto) {
        final EntityStatsApiDTO entityStatsApiDTO = new EntityStatsApiDTO();
        entityStatsApiDTO.setUuid(Long.toString(serviceEntity.getOid()));
        entityStatsApiDTO.setClassName(UIEntityType.fromType(serviceEntity.getEntityType()).apiStr());
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
                UIEntityType.fromType(tempGroup.getEntityType()))) {
            return Optional.of(ENTITY_TYPES_TO_EXPAND.get(
                UIEntityType.fromType(tempGroup.getEntityType())).typeNumber());
        } else if (isGlobalTempGroup) {
            // if it is global temp group and not need to expand.
            return Optional.of(tempGroup.getEntityType());
        } else {
            return Optional.empty();
        }
    }
}

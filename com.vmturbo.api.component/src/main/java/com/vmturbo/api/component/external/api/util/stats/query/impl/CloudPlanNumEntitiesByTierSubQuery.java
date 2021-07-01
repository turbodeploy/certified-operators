package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static com.vmturbo.common.protobuf.GroupProtoUtil.WORKLOAD_ENTITY_TYPES_API_STR;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest.ActionQuery;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Sub-query responsible for getting specific entity counts, grouped by tiers, for cloud plans
 * results.
 */
public class CloudPlanNumEntitiesByTierSubQuery implements StatsSubQuery {
    private final Logger logger = LogManager.getLogger();

    /**
     * Mapping of all 'numEntity' stat types to a Set of their respective API strings.
     */
    private static final Map<String, Set<String>> NUM_STAT_TYPES_TO_ENTITIES = ImmutableMap.of(
        StringConstants.NUM_WORKLOADS, WORKLOAD_ENTITY_TYPES_API_STR,
        StringConstants.NUM_VMS, ImmutableSet.of(ApiEntityType.VIRTUAL_MACHINE.apiStr()),
        StringConstants.NUM_DBSS, ImmutableSet.of(ApiEntityType.DATABASE_SERVER.apiStr()),
        StringConstants.NUM_VIRTUAL_DISKS, ImmutableSet.of(ApiEntityType.VIRTUAL_VOLUME.apiStr())
    );

    /**
     * Mapping of numEntity stat types used by by-tier queries to the corresponding entity type.
     */
    private static final ImmutableMap<String, Integer> NUM_STAT_TYPES_TO_ENTITY_TYPE =
            ImmutableMap.of(
                    StringConstants.NUM_VMS, EntityType.COMPUTE_TIER_VALUE,
                    StringConstants.NUM_VIRTUAL_DISKS, EntityType.STORAGE_TIER_VALUE
            );

    // the function of how to get the tier id from a given TopologyEntityDTO, this is used
    // for the stats of the number of entities by tier type
    @VisibleForTesting
    static final Map<String, Function<ApiPartialEntity, Optional<Long>>> ENTITY_TYPE_TO_GET_TIER_FUNCTION = ImmutableMap.of(
          ApiEntityType.VIRTUAL_MACHINE.apiStr(), topologyEntityDTO ->
              topologyEntityDTO.getProvidersList().stream()
              .filter(provider -> provider.getEntityType() == EntityType.COMPUTE_TIER_VALUE)
              .map(RelatedEntity::getOid)
              .findAny(),
          ApiEntityType.DATABASE.apiStr(), topologyEntityDTO ->
              topologyEntityDTO.getProvidersList().stream()
              .filter(provider -> provider.getEntityType() == EntityType.DATABASE_TIER_VALUE)
              .map(RelatedEntity::getOid)
              .findAny(),
          ApiEntityType.DATABASE_SERVER.apiStr(), topologyEntityDTO ->
              topologyEntityDTO.getProvidersList().stream()
              .filter(provider -> provider.getEntityType() == EntityType.DATABASE_SERVER_TIER_VALUE)
              .map(RelatedEntity::getOid)
              .findAny(),
          ApiEntityType.VIRTUAL_VOLUME.apiStr(), topologyEntityDTO ->
              topologyEntityDTO.getProvidersList().stream()
              .filter(provider -> provider.getEntityType() == EntityType.STORAGE_TIER_VALUE)
              .map(RelatedEntity::getOid)
              .findFirst()
      );

    private final RepositoryApi repositoryApi;
    private final SupplyChainFetcherFactory supplyChainFetcherFactory;
    private final ActionsServiceBlockingStub actionsServiceBlockingStub;

    /**
     * A 10-minute cache to keep mapping of destination (VM/Volume) tierIds to the number of
     * entities in that tier. This is used to populate the VM/Volume mapping tables when viewing
     * the output of MPC plan. Cache was needed as we were making multiple calls to get complete
     * action list that is used to populate the tier -> count mapping table, having the cache
     * reduces calls to AO.
     *
     * Key: <plan-id>-<statsKey>
     * Value: Map of tier oid to count of entities with that tier.
     */
    private final Cache<String, Map<Optional<Long>, Long>> countsByTierCache;

    public CloudPlanNumEntitiesByTierSubQuery(@Nonnull final RepositoryApi repositoryApi,
            @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
            ActionsServiceBlockingStub actionsServiceBlockingStub) {
        this.repositoryApi = repositoryApi;
        this.supplyChainFetcherFactory = supplyChainFetcherFactory;
        this.actionsServiceBlockingStub = actionsServiceBlockingStub;
        this.countsByTierCache = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES).build();
    }

    @Override
    public boolean applicableInContext(@Nonnull final StatsQueryContext context) {
        // Check if it's not a cloud plan type.
        Optional<PlanInstance> planInstanceOpt = context.getPlanInstance();
        return planInstanceOpt.isPresent() &&
            StringConstants.CLOUD_PLAN_TYPES.contains(
                planInstanceOpt.get().getScenario().getScenarioInfo().getType());
    }

    @Override
    public SubQuerySupportedStats getHandledStats(@Nonnull final StatsQueryContext context) {
        final Set<StatApiInputDTO> stats = context.findStats(NUM_STAT_TYPES_TO_ENTITIES.keySet()).stream()
            // this is for "Cloud Template Summary By Type", but ccc chart also passes numWorkload
            // as stat name, we need to handle them differently, so check filters since ccc chart
            // passes filters here, we might need to change UI to handle it better
            .filter(stat -> CollectionUtils.isEmpty(stat.getFilters()))
            .collect(Collectors.toSet());

        return SubQuerySupportedStats.some(stats);
    }

    @Nonnull
    @Override
    public List<StatSnapshotApiDTO> getAggregateStats(@Nonnull final Set<StatApiInputDTO> requestedStats,
                                                      @Nonnull final StatsQueryContext context)
            throws OperationFailedException {
        // Check if it's not a cloud plan type.
        Optional<PlanInstance> planInstanceOpt = context.getPlanInstance();
        if (!planInstanceOpt.isPresent()) {
            return Collections.emptyList();
        }

        final PlanInstance planInstance = planInstanceOpt.get();

        final long planTopologyContextId = context.getInputScope().oid();

        // find plan scope ids
        Set<Long> scopes = context.getQueryScope().getExpandedOids();
        // return two snapshot, one for before plan, one for after plan
        List<StatApiDTO> statsBeforePlan = new ArrayList<>();
        List<StatApiDTO> statsAfterPlan = new ArrayList<>();

        final Optional<String> foundNumStat = getNumStatFromRequestedStats(requestedStats);
        if (foundNumStat.isPresent()) {
            final String stat = foundNumStat.get();
            if (StringConstants.NUM_VIRTUAL_DISKS.equals(stat)) {
                statsBeforePlan.addAll(getNumVirtualDisksStats(scopes, planTopologyContextId,
                        false, planInstance.getProjectType()));
                statsAfterPlan.addAll(getNumVirtualDisksStats(scopes, planTopologyContextId, true,
                        planInstance.getProjectType()));
            } else {
                statsBeforePlan.addAll(getNumEntitiesByTierStats(scopes, planTopologyContextId,
                        false, stat, NUM_STAT_TYPES_TO_ENTITIES.get(stat),
                        planInstance.getProjectType()));
                statsAfterPlan.addAll(getNumEntitiesByTierStats(scopes, planTopologyContextId,
                        true, stat, NUM_STAT_TYPES_TO_ENTITIES.get(stat),
                        planInstance.getProjectType()));
            }
        }

        // set stats
        // set stats time, use plan start time as startDate, use plan end time as endDate
        final String beforeTime = DateTimeUtil.toString(planInstance.getStartTime());
        final String afterTime = DateTimeUtil.toString(planInstance.getEndTime());

        final StatSnapshotApiDTO beforeSnapshot = new StatSnapshotApiDTO();
        beforeSnapshot.setDate(beforeTime);
        beforeSnapshot.setEpoch(Epoch.PLAN_SOURCE);
        beforeSnapshot.setStatistics(statsBeforePlan);

        final StatSnapshotApiDTO afterSnapshot = new StatSnapshotApiDTO();
        afterSnapshot.setDate(afterTime);
        afterSnapshot.setEpoch(Epoch.PLAN_PROJECTED);
        afterSnapshot.setStatistics(statsAfterPlan);

        return ImmutableList.of(beforeSnapshot, afterSnapshot);
    }

    private Optional<String> getNumStatFromRequestedStats(Set<StatApiInputDTO> requestedStats) {
        for (String stat : NUM_STAT_TYPES_TO_ENTITIES.keySet()) {
            if (containsStat(stat, requestedStats)) {
                return Optional.of(stat);
            }
        }
        return Optional.empty();
    }

    private List<StatApiDTO> getNumVirtualDisksStats(@Nonnull Set<Long> scopes, long contextId,
            boolean projectedTopology, PlanProjectType planProjectType)
            throws OperationFailedException {
        String volumeEntityType = ApiEntityType.VIRTUAL_VOLUME.apiStr();

        if (planProjectType == PlanProjectType.CLOUD_MIGRATION) {
            // The repository doesn't currently provide accurate results for newly-migrated
            // entities, so parse the action list to determine which entities were migrated.
            return fetchNumEntitiesByTierStats(getEntitiesFromActionList(contextId, StringConstants.NUM_VIRTUAL_DISKS),
                    projectedTopology, StringConstants.NUM_VIRTUAL_DISKS, StringConstants.TIER, contextId);
        }

        // get all volumes ids in the plan scope, using supply chain fetcher
        // get all VMs ids in the plan scope, using supply chain fetcher
        Set<Long> volumeIds = getRelatedEntities(scopes, Collections.singletonList(volumeEntityType))
            .getOrDefault(volumeEntityType, ImmutableSet.of());
        return fetchNumEntitiesByTierStats(getEntitiesFromRepository(volumeIds, projectedTopology, contextId,
                ENTITY_TYPE_TO_GET_TIER_FUNCTION.get(volumeEntityType)), projectedTopology,
            StringConstants.NUM_VIRTUAL_DISKS, StringConstants.TIER, contextId);
    }

    @Nonnull
    private Map<String, Set<Long>> getRelatedEntities(@Nonnull final Set<Long> scopes, @Nonnull final List<String> types)
            throws OperationFailedException {
        return supplyChainFetcherFactory.newNodeFetcher()
            .addSeedUuids(scopes.stream().map(String::valueOf).collect(Collectors.toList()))
            .entityTypes(types)
            .environmentType(null)
            .fetch()
            .values()
            .stream()
            .collect(Collectors.toMap(n -> ApiEntityType.fromSdkTypeToEntityTypeString(n.getEntityType()), RepositoryDTOUtil::getAllMemberOids));
    }

    private List<StatApiDTO> getNumEntitiesByTierStats(@Nonnull Set<Long> scopes,
                               long contextId, boolean projectedTopology,
                               @Nonnull String statName,
                               @Nonnull Set<String> entityTypes,
                               PlanProjectType planProjectType) throws OperationFailedException {
        if (planProjectType == PlanProjectType.CLOUD_MIGRATION) {
            // The repository doesn't currently provide accurate results for newly-migrated
            // entities, so parse the action list to determine which entities were migrated.
            return fetchNumEntitiesByTierStats(getEntitiesFromActionList(contextId, statName),
                    projectedTopology, statName, StringConstants.TEMPLATE, contextId);
        }

        // fetch related entities ids for given scopes
        final Map<String, Set<Long>> idsByEntityType = getRelatedEntities(scopes,
            new ArrayList<>(entityTypes));

        return idsByEntityType.entrySet().stream()
            .flatMap(entry ->
                fetchNumEntitiesByTierStats(getEntitiesFromRepository(entry.getValue(), projectedTopology, contextId,
                        ENTITY_TYPE_TO_GET_TIER_FUNCTION.get(entry.getKey())), projectedTopology, statName,
                        StringConstants.TEMPLATE, contextId).stream()
            ).collect(Collectors.toList());
    }

    private List<StatApiDTO> fetchNumEntitiesByTierStats(@Nonnull Map<Optional<Long>, Long> tierIdToNumEntities,
                                                         boolean projectedTopology, @Nonnull String statName,
                                                         @Nonnull String filterType, long contextId) {
        // tier id --> tier name
        final Map<Long, String> tierIdToName = repositoryApi.entitiesRequest(tierIdToNumEntities.keySet()
                .stream().filter(Optional::isPresent)
                .map(Optional::get).collect(Collectors.toSet()))
                .contextId(contextId)
                .getMinimalEntities()
                .collect(Collectors.toMap(MinimalEntity::getOid,
                        MinimalEntity::getDisplayName));

        return tierIdToNumEntities.entrySet().stream()
                .filter(entry -> entry.getKey().isPresent())
                .map(entry -> createStatApiDTOForPlan(statName, entry.getValue(),
                        filterType, tierIdToName.get(entry.getKey().get()), projectedTopology))
                .collect(Collectors.toList());
    }

    /**
     * Return a map of tier ID to the number of entities on that tier.  The source of information
     * is retrieved from the repository.
     * @param entityIds entity IDs to search for
     * @param projectedTopology true if requesting stats for the projected topology
     * @param contextId context ID
     * @param getTierId function to use to extract tier ID
     * @return map of tier IDs to number of entities on that tier.
     */
    @Nonnull
    private Map<Optional<Long>, Long> getEntitiesFromRepository(@Nonnull Set<Long> entityIds,
            boolean projectedTopology, long contextId,
            @Nonnull Function<ApiPartialEntity, Optional<Long>> getTierId) {
        Map<Optional<Long>, Long> tierIdToNumEntities;
        final MultiEntityRequest request = createEntitiesRequest(entityIds, projectedTopology,
                contextId);
        // fetch entities
        final Map<Long, ApiPartialEntity> entities = request.getEntities()
                .collect(Collectors.toMap(ApiPartialEntity::getOid, Function.identity()));

        // tier id --> number of entities using the tier. Only include entities that are connected
        // to a region.  If they are not, they are unplaced and should be omitted.
        tierIdToNumEntities = entities.values().stream()
                .collect(Collectors.groupingBy(getTierId, Collectors.counting()));
        // Unplaced entities have a provider (tier ID) of 0.  We don't want to include these in the
        // number of entities stats, so remove them here.
        tierIdToNumEntities.remove(Optional.of(0L));
        return tierIdToNumEntities;
    }

    /**
     * Return a map of tier ID to the number of entities on that tier.  The source of information
     * is extracted from the actions list.
     * @param contextId context ID
     * @param statName stat name to build map for. Only numVMs or numVirtualDisks are supported.
     *                  Any other stat name will return an empty map.
     * @return map of tier IDs to number of entities on that tier
     */
    @Nonnull
    private Map<Optional<Long>, Long> getEntitiesFromActionList(long contextId,
            @Nonnull String statName) {
        final Integer requiredDestinationType = NUM_STAT_TYPES_TO_ENTITY_TYPE.get(statName);
        if (requiredDestinationType == null) {
            // Invalid entity type requested
            return Collections.emptyMap();
        }
        final String cacheKey = String.format("%s-%s", contextId, statName);
        final Map<Optional<Long>, Long> tierToCounts = countsByTierCache.getIfPresent(cacheKey);
        if (tierToCounts != null) {
            logger.info("{} Returning {} cached entities from actions for {}.",
                    TopologyDTOUtil.formatPlanLogPrefix(contextId), tierToCounts.size(), statName);
            return tierToCounts;
        }
        Map<Optional<Long>, Long> tierIdToNumEntities = new HashMap<>();

        logger.info("{} Start getting entities from actions for {}.",
                TopologyDTOUtil.formatPlanLogPrefix(contextId), statName);

        ActionQueryFilter actionQueryFilter = ActionQueryFilter.newBuilder()
                .setVisible(true)
                .addTypes(ActionType.MOVE)
                .addEntityType(requiredDestinationType)
                .build();
        AtomicReference<String> cursor = new AtomicReference<>("0");
        do {
            FilteredActionRequest filteredActionRequest =
                    FilteredActionRequest.newBuilder()
                            .setTopologyContextId(contextId)
                            .setPaginationParams(PaginationParameters.newBuilder()
                                    .setLimit(TopologyDTOUtil.CLOUD_MIGRATION_ACTION_QUERY_CURSOR_LIMIT)
                                    .setCursor(cursor.getAndSet("")))
                            .addActionQuery(ActionQuery.newBuilder()
                                .setQueryFilter(actionQueryFilter))
                            .build();
            actionsServiceBlockingStub.getAllActions(filteredActionRequest)
                    .forEachRemaining(rsp -> {
                // These are the actions related to the queried entities.  Only include entities with
                // an associated action.
                if (rsp.hasActionChunk()) {
                    for (ActionOrchestratorAction action : rsp.getActionChunk().getActionsList()) {
                        // Make sure the action's destination is a region
                        boolean hasDestinationRegion = false;
                        Long destinationTier = null;
                        // If the destination type is region and the entity is of the
                        // requested type then add the entity to the tier counts map.
                        for (ChangeProvider change : action.getActionSpec()
                                .getRecommendation()
                                .getInfo()
                                .getMove()
                                .getChangesList()) {
                            ActionEntity destination = change.getDestination();
                            int type = destination.getType();
                            if (type == requiredDestinationType) {
                                destinationTier = destination.getId();
                            } else if (type == EntityType.REGION_VALUE) {
                                hasDestinationRegion = true;
                            }
                            if (hasDestinationRegion && destinationTier != null) {
                                Optional<Long> key = Optional.of(destinationTier);
                                Long currentCount = tierIdToNumEntities.getOrDefault(key, 0L);
                                tierIdToNumEntities.put(key, currentCount + 1);
                                break;
                            }
                        }
                    }
                } else if (rsp.hasPaginationResponse()) {
                    cursor.set(rsp.getPaginationResponse().getNextCursor());
                }
            });
        } while (!StringUtils.isEmpty(cursor.get()));
        logger.info("{} Completed getting entities from actions for {}.",
                TopologyDTOUtil.formatPlanLogPrefix(contextId), statName);
        countsByTierCache.put(cacheKey, tierIdToNumEntities);
        return tierIdToNumEntities;
    }

    private MultiEntityRequest createEntitiesRequest(Set<Long> entityIds, boolean projectedTopology,
                                                     long contextId) {
        MultiEntityRequest request = repositoryApi.entitiesRequest(entityIds)
                        .contextId(contextId);
        if (projectedTopology) {
            request = request.projectedTopology();
        }
        return request;
    }

    /**
     * Create StatApiDTO based on given parameters, if beforePlan is true, then it is a stat for
     * real time; if false, it is a stat for after plan. Related filter is created to indicate
     * whether this is a stat before plan or after plan.
     *
     * @param statName name of statistic
     * @param statValue value of statistic
     * @param filterType type of the filter
     * @param filterValue value of the filter
     * @param projectedTopology true if statistic for the projected topology
     * @return statistic DTO
     */
    private static StatApiDTO createStatApiDTOForPlan(String statName, Long statValue, String filterType,
                                               String filterValue, boolean projectedTopology) {
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
        // only add if we need source topology DTOs
        if (!projectedTopology) {
            StatFilterApiDTO planFilter = new StatFilterApiDTO();
            planFilter.setType(StringConstants.RESULTS_TYPE);
            planFilter.setValue(StringConstants.BEFORE_PLAN);
            statFilters.add(planFilter);
        }
        // set filters
        statApiDTO.setFilters(statFilters);
        return statApiDTO;
    }
}

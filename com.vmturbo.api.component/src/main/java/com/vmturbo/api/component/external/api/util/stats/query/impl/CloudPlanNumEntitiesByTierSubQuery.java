package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static com.vmturbo.common.protobuf.GroupProtoUtil.WORKLOAD_ENTITY_TYPES_API_STR;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.CollectionUtils;

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
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Sub-query responsible for getting specific entity counts, grouped by tiers, for cloud plans
 * results.
 */
public class CloudPlanNumEntitiesByTierSubQuery implements StatsSubQuery {
    private static final Set<String> CLOUD_PLAN_TYPES = ImmutableSet.of(
        StringConstants.OPTIMIZE_CLOUD_PLAN, StringConstants.CLOUD_MIGRATION_PLAN);

    /**
     * Mapping of all 'numEntity' stat types to a Set of their respective API strings.
     */
    private static final Map<String, Set<String>> NUM_STAT_TYPES_TO_ENTITIES = ImmutableMap.of(
        StringConstants.NUM_WORKLOADS, WORKLOAD_ENTITY_TYPES_API_STR,
        StringConstants.NUM_VMS, ImmutableSet.of(ApiEntityType.VIRTUAL_MACHINE.apiStr()),
        StringConstants.NUM_DBSS, ImmutableSet.of(ApiEntityType.DATABASE_SERVER.apiStr()),
        StringConstants.NUM_VIRTUAL_DISKS, ImmutableSet.of(ApiEntityType.VIRTUAL_VOLUME.apiStr())
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

    public CloudPlanNumEntitiesByTierSubQuery(@Nonnull final RepositoryApi repositoryApi,
                                              @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory) {
        this.repositoryApi = repositoryApi;
        this.supplyChainFetcherFactory = supplyChainFetcherFactory;
    }

    @Override
    public boolean applicableInContext(@Nonnull final StatsQueryContext context) {
        // Check if it's not a cloud plan type.
        Optional<PlanInstance> planInstanceOpt = context.getPlanInstance();
        return planInstanceOpt.isPresent() &&
            CLOUD_PLAN_TYPES.contains(planInstanceOpt.get().getScenario().getScenarioInfo().getType());
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
                statsBeforePlan.addAll(getNumVirtualDisksStats(scopes, planTopologyContextId, false));
                statsAfterPlan.addAll(getNumVirtualDisksStats(scopes, planTopologyContextId, true));
            } else {
                statsBeforePlan.addAll(getNumEntitiesByTierStats(scopes, planTopologyContextId, false, stat, NUM_STAT_TYPES_TO_ENTITIES.get(stat)));
                statsAfterPlan.addAll(getNumEntitiesByTierStats(scopes, planTopologyContextId, true, stat, NUM_STAT_TYPES_TO_ENTITIES.get(stat)));
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

    private List<StatApiDTO> getNumVirtualDisksStats(@Nonnull Set<Long> scopes,
                                                     long contextId, boolean projectedTopology) throws OperationFailedException {
        String volumeEntityType = ApiEntityType.VIRTUAL_VOLUME.apiStr();
        // get all volumes ids in the plan scope, using supply chain fetcher
        // get all VMs ids in the plan scope, using supply chain fetcher
        Set<Long> volumeIds = getRelatedEntities(scopes, Collections.singletonList(volumeEntityType))
            .get(volumeEntityType);
        return fetchNumEntitiesByTierStats(volumeIds, projectedTopology, contextId,
            StringConstants.NUM_VIRTUAL_DISKS, StringConstants.TIER, ENTITY_TYPE_TO_GET_TIER_FUNCTION.get(volumeEntityType));
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
            .collect(Collectors.toMap(SupplyChainNode::getEntityType, RepositoryDTOUtil::getAllMemberOids));
    }

    private List<StatApiDTO> getNumEntitiesByTierStats(@Nonnull Set<Long> scopes,
                                                       long contextId, boolean projectedTopology,
                                                       @Nonnull String statName,
                                                       @Nonnull Set<String> entityTypes) throws OperationFailedException {
        // fetch related entities ids for given scopes
        final Map<String, Set<Long>> idsByEntityType = getRelatedEntities(scopes,
            new ArrayList<>(entityTypes));
        return idsByEntityType.entrySet().stream()
            .flatMap(entry -> fetchNumEntitiesByTierStats(entry.getValue(), projectedTopology, contextId,
                statName, StringConstants.TEMPLATE,
                ENTITY_TYPE_TO_GET_TIER_FUNCTION.get(entry.getKey())).stream()
            ).collect(Collectors.toList());
    }


    private List<StatApiDTO> fetchNumEntitiesByTierStats(@Nonnull Set<Long> entityIds,
                                                         boolean projectedTopology,
                                                         long contextId,
                                                         @Nonnull String statName,
                                                         @Nonnull String filterType,
                                                         @Nonnull Function<ApiPartialEntity, Optional<Long>> getTierId) {
        final MultiEntityRequest request = createEntitiesRequest(entityIds, projectedTopology, contextId);
        // fetch entities
        final Map<Long, ApiPartialEntity> entities = request.getEntities()
            .collect(Collectors.toMap(ApiPartialEntity::getOid, Function.identity()));
        // tier id --> number of entities using the tier
        final Map<Optional<Long>, Long> tierIdToNumEntities = entities.values().stream()
            .collect(Collectors.groupingBy(getTierId, Collectors.counting()));
        // tier id --> tier name
        // Looking up display name in real-time topology, as some (like NVME_SSD StorageTier -
        // which don't participate in market) are not in plan topology.
        // It should be safe to look up the tiers in the real-time topology because
        // Storage tiers available in plan are always going to be a subset of tiers available
        // in real-time, and the properties (like displayName) of the tiers won't change.
        final Map<Long, String> tierIdToName = repositoryApi.entitiesRequest(tierIdToNumEntities.keySet()
                        .stream().filter(key -> key.isPresent())
                        .map(Optional::get).collect(Collectors.toSet()))
                        .getMinimalEntities()
                        .collect(Collectors.toMap(MinimalEntity::getOid,
                                                  MinimalEntity::getDisplayName));

        return tierIdToNumEntities.entrySet().stream()
                        .filter(entry -> entry.getKey().isPresent())
            .map(entry -> createStatApiDTOForPlan(statName, entry.getValue(),
                filterType, tierIdToName.get(entry.getKey().get()), projectedTopology))
            .collect(Collectors.toList());
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

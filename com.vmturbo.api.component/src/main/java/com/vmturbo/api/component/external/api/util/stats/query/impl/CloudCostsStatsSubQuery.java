package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static com.vmturbo.api.component.external.api.util.stats.query.impl.StorageStatsSubQuery.NUM_VOL;
import static com.vmturbo.api.component.external.api.util.stats.query.impl.StorageStatsSubQuery.toCountStatApiDtos;
import static com.vmturbo.common.protobuf.GroupProtoUtil.WORKLOAD_ENTITY_TYPES_API_STR;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors.EnumValueDescriptor;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.CloudTypeMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedGroupInfo;
import com.vmturbo.api.component.external.api.service.StatsService;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext.TimeWindow;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.GlobalScope;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenseQueryScope;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenseQueryScope.IdList;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.Builder;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.CostSourceFilter;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostCategoryFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntityTypeFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudExpenseStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudExpenseStatsRequest.GroupByType;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Sub-query responsible for getting top down and bottom-up costs from the cost component.
 */
public class CloudCostsStatsSubQuery implements StatsSubQuery {

    private static final Logger logger = LogManager.getLogger();

    private static final Set<String> NUM_WORKLOADS_STATS_SET = ImmutableSet.of(
        StringConstants.NUM_WORKLOADS, "currentNumWorkloads",
        StringConstants.NUM_VMS, "currentNumVMs",
        StringConstants.NUM_DBS, "currentNumDBs",
        StringConstants.NUM_DBSS, "currentNumDBSs",
        StringConstants.NUM_VIRTUAL_VOLUMES, "currentNumVolumes");

    private static final Map<String, List<String>> WORKLOAD_NAME_TO_ENTITY_TYPES = ImmutableMap.of(
        StringConstants.NUM_VMS, Collections.singletonList(ApiEntityType.VIRTUAL_MACHINE.apiStr()),
        StringConstants.NUM_DBS, Collections.singletonList(ApiEntityType.DATABASE.apiStr()),
        StringConstants.NUM_DBSS, Collections.singletonList(ApiEntityType.DATABASE_SERVER.apiStr()),
        StringConstants.NUM_WORKLOADS, new ArrayList<>(WORKLOAD_ENTITY_TYPES_API_STR)
    );

    private static final Set<ApiEntityType> ENTITY_TYPES_TO_GET_COST_BY_FILTER = ImmutableSet.of(
        ApiEntityType.BUSINESS_ACCOUNT, ApiEntityType.REGION, ApiEntityType.AVAILABILITY_ZONE
    );

    // Internally generated stat name when stats period are not set.
    private static final String CURRENT_COST_PRICE = "currentCostPrice";

    static final String COST_COMPONENT = "costComponent";

    /**
     *Collection of cloud cost stats metrics.
     */
    public static final Set<String> COST_STATS_SET = ImmutableSet.of(StringConstants.COST_PRICE,
        CURRENT_COST_PRICE);

    private final RepositoryApi repositoryApi;

    private final CostServiceBlockingStub costServiceRpc;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final CloudStatRecordAggregator recordAggregator = new CloudStatRecordAggregator();

    private final ThinTargetCache thinTargetCache;

    private final BuyRiScopeHandler buyRiScopeHandler;

    private final CloudTypeMapper cloudTypeMapper;

    private final StorageStatsSubQuery storageStatsSubQuery;

    private  final UserSessionContext userSessionContext;

    public CloudCostsStatsSubQuery(@Nonnull final RepositoryApi repositoryApi,
                                   @Nonnull final CostServiceBlockingStub costServiceRpc,
                                   @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                                   @Nonnull final ThinTargetCache thinTargetCache,
                                   @Nonnull final BuyRiScopeHandler buyRiScopeHandler,
                                   @Nonnull final StorageStatsSubQuery storageStatsSubQuery,
                                   @Nonnull final UserSessionContext userSessionContext) {
        this.repositoryApi = repositoryApi;
        this.costServiceRpc = costServiceRpc;
        this.supplyChainFetcherFactory = supplyChainFetcherFactory;
        this.thinTargetCache = thinTargetCache;
        this.buyRiScopeHandler = buyRiScopeHandler;
        this.storageStatsSubQuery = storageStatsSubQuery;
        this.userSessionContext = userSessionContext;
        this.cloudTypeMapper = new CloudTypeMapper();
    }

    @Override
    public boolean applicableInContext(@Nonnull final StatsQueryContext context) {
        // If the query scope is going to be a non-CLOUD global group, we don't need to run
        // this sub-query.
        final Optional<EnvironmentType> globalScopeEnvType = context.getQueryScope().getGlobalScope()
            .flatMap(GlobalScope::environmentType);
        return !globalScopeEnvType.isPresent() || globalScopeEnvType.get() == EnvironmentType.CLOUD;
    }

    @Override
    public SubQuerySupportedStats getHandledStats(@Nonnull final StatsQueryContext context) {
        Set<StatApiInputDTO> costStats = context.findStats(COST_STATS_SET);
        if (costStats.isEmpty()) {
            return SubQuerySupportedStats.none();
        }

        // If there are cost stat requests we handle some of the count stats here too.
        // This is ugly, but seems in line with what the UI expects. :(
        final Set<StatApiInputDTO> countStats = context.findStats(NUM_WORKLOADS_STATS_SET);

        return SubQuerySupportedStats.some(Sets.union(costStats, countStats));
    }

    @Nonnull
    @Override
    public List<StatSnapshotApiDTO> getAggregateStats(@Nonnull final Set<StatApiInputDTO> requestedStats,
                                                      @Nonnull final StatsQueryContext context) throws OperationFailedException {
        final Optional<GlobalScope> globalScope = context.getQueryScope().getGlobalScope();
        final Set<String> requestGroupBySet = requestedStats.stream()
            .filter(dto -> dto.getGroupBy() != null)
            .flatMap(apiInputDTO -> apiInputDTO.getGroupBy().stream())
            .collect(toSet());
        final ApiId inputScope = context.getInputScope();

        final List<StatSnapshotApiDTO> statsResponse;
        if (isTopDownRequest(requestGroupBySet)) {
            // get the type function and value function according to the "groupBy" part of the query.
            // they will be used to populate the filters in StatApiDTO
            final Supplier<String> typeFunction = getTypeFunction(requestGroupBySet);
            final BiFunction<Long, Map<Long, MinimalEntity>, Optional<String>> valueFunction =
                    getValueFunction(requestGroupBySet);

            // get entities to use later to create the stat filters.
            final Map<Long, MinimalEntity> entityDTOs;
            if (requestGroupBySet.contains(StatsService.CLOUD_SERVICE)) {
                // for group by Cloud services, we need to find all the services and
                // stitch with expenses in Cost component
                entityDTOs = getDiscoveredEntityDTOs(ApiEntityType.CLOUD_SERVICE);
            } else if (requestGroupBySet.contains(StringConstants.BUSINESS_UNIT)) {
                // get business accounts
                entityDTOs = getDiscoveredEntityDTOs(ApiEntityType.BUSINESS_ACCOUNT);
            } else {
                entityDTOs = Collections.emptyMap();
            }

            final List<CloudCostStatRecord> cloudStatRecords =
                    getCloudExpensesRecordList(requestGroupBySet,
                            // if groupBy = cloudService, get only the service costs from the DB.
                            requestGroupBySet.contains(StatsService.CLOUD_SERVICE) ?
                                    entityDTOs.keySet() :
                                    Collections.emptySet(),
                            context);
            statsResponse = cloudStatRecords.stream()
                    .map(snapshot -> toStatSnapshotApiDTO(snapshot,
                            typeFunction,
                            valueFunction,
                            entityDTOs))
                    .collect(toList());
        } else if (inputScope.isRealtimeMarket() || inputScope.isPlan() ||
                inputScope.isCloud()) {
            final Set<Long> cloudEntityOids;
            // not expand scope for resource groups, because can be a situation when
            // connected entities exist in different resource groups (vm and attached volume)
            // but scope for resource group should contain only members of this resource group
            if (shouldQueryUsingFilter(inputScope) || isResourceGroup(inputScope)) {
                cloudEntityOids = context.getQueryScope().getScopeOids();
            } else {
                Set<Long> expandedOids = context.getQueryScope().getExpandedOids();
                if (!globalScope.isPresent() && expandedOids.isEmpty()) {
                    // Do not fetch costs without entityFilter if it is not global scoped and
                    // there are no expanded Oids as this leads to querying for all entities;
                    // which is incorrect if user is scoped.
                    logger.debug("Query not for global scope and no oids found in expanded scope." +
                            "Can not fetch cost stats.");
                    cloudEntityOids = null;
                } else {
                    cloudEntityOids = expandedOids;
                }
            }
            Set<StatApiInputDTO> requestedCostPriceStats =
                filterStatInputs(requestedStats, StringConstants.COST_PRICE, null);

            if (isResourceGroup(inputScope)) {
                requestedCostPriceStats =
                        filterRequestedCostStatsForResourceGroups(requestedCostPriceStats,
                                inputScope);
            }

            final Set<StatApiInputDTO> groupByAttachmentInputs =
                filterStatInputs(requestedStats, null, StringConstants.ATTACHMENT);
            final Set<StatApiInputDTO> groupByTierInputs =
                filterStatInputs(requestedStats, null, ApiEntityType.STORAGE_TIER.apiStr());

            final boolean isGenericGroupBy =
                groupByAttachmentInputs.isEmpty() && groupByTierInputs.isEmpty();
            if (!isGenericGroupBy) {
                requestedCostPriceStats = mergeCostRequests(requestedCostPriceStats);
            }
            final List<CloudCostStatRecord> cloudCostStatRecords =
                getCloudStatRecordList(requestedCostPriceStats, cloudEntityOids, context,
                    isGenericGroupBy);

            // For Virtual Volume, when it is grouped by either attachment or storage tier,
            //   aggregation need to be handled explicitly
            final List<StatSnapshotApiDTO> statSnapshots = new ArrayList<>();
            final List<StatApiDTO> numWorkloadStats = new ArrayList<>();
            if (!isGenericGroupBy) {
                Set<TopologyEntityDTO> volumeEntities = new HashSet<>();
                if (!CollectionUtils.isEmpty(cloudEntityOids)) {
                    // Including all cloud volumes, with cost and without cost(AWS ephemeral disks have no cost)
                    volumeEntities = repositoryApi.entitiesRequest(cloudEntityOids)
                            .getFullEntities().filter(t -> t.getEntityType() == EntityType.VIRTUAL_VOLUME_VALUE)
                            .collect(toSet());
                } else if (globalScope.isPresent() && globalScope.get().entityTypes().contains(ApiEntityType.VIRTUAL_VOLUME)
                        && globalScope.get().environmentType().orElse(null) == EnvironmentType.CLOUD) {
                    // For Cloud tab, global scope of all cloud volumes.
                    final SearchParameters searchParameters = SearchProtoUtil
                            .makeSearchParameters(SearchProtoUtil.entityTypeFilter(ApiEntityType.VIRTUAL_VOLUME))
                            .addSearchFilter(SearchFilter.newBuilder()
                                    .setPropertyFilter(SearchProtoUtil.environmentTypeFilter(EnvironmentType.CLOUD))
                                    .build())
                            .build();
                    volumeEntities = repositoryApi.newSearchRequest(searchParameters).getFullEntities().collect(toSet());
                }
                if (!groupByAttachmentInputs.isEmpty()) {
                    final Set<Long> attachedOids = volumeEntities.stream()
                        .filter(topologyEntityDTO -> topologyEntityDTO.hasTypeSpecificInfo() &&
                            topologyEntityDTO.getTypeSpecificInfo().hasVirtualVolume() &&
                            topologyEntityDTO.getTypeSpecificInfo().getVirtualVolume()
                                .hasAttachmentState() &&
                            AttachmentState.ATTACHED == topologyEntityDTO.getTypeSpecificInfo()
                                .getVirtualVolume().getAttachmentState()
                        )
                        .map(TopologyEntityDTO::getOid)
                        .collect(Collectors.toSet());

                    final Set<StatApiInputDTO> countInputs = filterStatInputs(requestedStats, NUM_VOL, StringConstants.ATTACHMENT);
                    if (!countInputs.isEmpty()) {
                        final Map<String, Long> countMap = ImmutableMap.of(
                            StringConstants.ATTACHED, (long)attachedOids.size(),
                            StringConstants.UNATTACHED, (long)volumeEntities.size() - attachedOids.size());
                        countInputs.forEach(inputDTO -> numWorkloadStats.addAll(
                            toCountStatApiDtos(inputDTO, StringConstants.ATTACHMENT, countMap)));
                    }
                    final List<StatSnapshotApiDTO> costStatsByAttachment =
                        getVolumeCostStatsByAttachment(cloudCostStatRecords,
                            filterStatInputs(requestedStats, StringConstants.COST_PRICE, StringConstants.ATTACHMENT), attachedOids);
                    if (CollectionUtils.isNotEmpty(costStatsByAttachment)) {
                        statSnapshots.addAll(costStatsByAttachment);
                    }
                }

                if (!groupByTierInputs.isEmpty()) {
                    final Map<String, Set<Long>> tierNameToVolumeOids =
                        groupVolumeOidsByTierName(volumeEntities);

                    final Set<StatApiInputDTO> countInputs = filterStatInputs(requestedStats, NUM_VOL, ApiEntityType.STORAGE_TIER.apiStr());
                    if (!countInputs.isEmpty()) {
                        final Map<String, Long> counts = tierNameToVolumeOids.entrySet().stream()
                            .collect(
                                Collectors.toMap(e -> e.getKey(), e -> (long)e.getValue().size()));
                        countInputs.forEach(inputDTO -> numWorkloadStats.addAll(
                            toCountStatApiDtos(inputDTO, ApiEntityType.STORAGE_TIER.apiStr(), counts)));
                    }
                    final List<StatSnapshotApiDTO> groupByStorageTierStat =
                        getVolumeCostStatsByTier(cloudCostStatRecords,
                            filterStatInputs(requestedStats, StringConstants.COST_PRICE, ApiEntityType.STORAGE_TIER.apiStr()), tierNameToVolumeOids);
                    if (CollectionUtils.isNotEmpty(groupByStorageTierStat)) {
                        statSnapshots.addAll(groupByStorageTierStat);
                    }
                }
            } else {
                statSnapshots.addAll(cloudCostStatRecords.stream()
                    .map(CloudCostsStatsSubQuery::toCloudStatSnapshotApiDTO)
                    .sorted(Comparator.comparing(StatSnapshotApiDTO::getDate))
                    .collect(toList()));
                numWorkloadStats.addAll(getNumWorkloadStatSnapshot(requestedStats, context));
            }
            statsResponse = mergeStatSnapshotApiDTOWithDate(statSnapshots);
            if (!numWorkloadStats.isEmpty()) {
                // add the numWorkloads to the same timestamp it it exists.
                if (statSnapshots.isEmpty()) {
                    final StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
                    final long statsTime;
                    if (context.getTimeWindow().isPresent()) {
                        TimeWindow window = context.getTimeWindow().get();
                        statsTime = window.endTime();
                    } else {
                        statsTime = context.getCurTime();
                    }
                    statSnapshotApiDTO.setDate(DateTimeUtil.toString(statsTime));
                    statSnapshotApiDTO.setStatistics(numWorkloadStats);
                    statsResponse.add(statSnapshotApiDTO);
                } else {
                    // add numWorkloads to all snapshots
                    statsResponse.forEach(statSnapshotApiDTO -> {
                        List<StatApiDTO> statApiDTOs = new ArrayList<>();
                        statApiDTOs.addAll(statSnapshotApiDTO.getStatistics());
                        statApiDTOs.addAll(numWorkloadStats);
                        statSnapshotApiDTO.setStatistics(statApiDTOs);
                    });
                }
            }
        } else {
            statsResponse = Collections.emptyList();
        }
        return statsResponse;
    }

    @Nonnull
    private Map<String, Set<Long>> groupVolumeOidsByTierName(final Set<TopologyEntityDTO> volumeEntities) {
        final Map<Long, Set<Long>> storageTierOidToVolumeOids = new HashMap<>();
        volumeEntities.forEach(volume -> {
            final long tierOid = volume.getCommoditiesBoughtFromProvidersList().stream()
                .filter(cbfp ->
                    cbfp.getProviderEntityType() == EntityType.STORAGE_TIER_VALUE)
                .mapToLong(CommoditiesBoughtFromProvider::getProviderId)
                .findFirst().orElse(0);

            if (tierOid != 0) {
                storageTierOidToVolumeOids
                    .computeIfAbsent(tierOid, k -> new HashSet<>())
                    .add(volume.getOid());
            } else {
                logger.error("Virtual Volume {} ({}) has no storage tier provider",
                    volume.getDisplayName(), volume.getOid());
            }
        });
        final Function<MinimalEntity, String> getTierDisplayName = tierEntity -> {
            if (Strings.isNullOrEmpty(tierEntity.getDisplayName())) {
                logger.warn("No displayName for storage tier; using oid {}.",
                    tierEntity.getOid());
                return String.valueOf(tierEntity.getOid());
            } else {
                return tierEntity.getDisplayName();
            }
        };

        return repositoryApi
            .newSearchRequest(
                SearchProtoUtil.makeSearchParameters(
                    SearchProtoUtil.idFilter(storageTierOidToVolumeOids.keySet())
                ).build())
            .getMinimalEntities()
            .collect(Collectors.toMap(getTierDisplayName, tier ->
                storageTierOidToVolumeOids.get(tier.getOid())));
    }

    @Nonnull
    private Set<StatApiInputDTO> filterStatInputs(@Nonnull final Set<StatApiInputDTO> statInputs,
            @Nullable final String statName,
            @Nullable final String groupBy) {
        return statInputs.stream()
            .filter(input -> statName == null || statName.equals(input.getName()))
            .filter(input -> groupBy == null ||
                (input.getGroupBy() != null && input.getGroupBy().contains(groupBy)))
            .collect(toSet());
    }

    private static boolean shouldQueryUsingFilter(ApiId inputScope) {
        return inputScope.isRealtimeMarket() ||
                inputScope.getScopeTypes()
                        .map(scopeTypes -> scopeTypes.size() == 1 &&
                                ENTITY_TYPES_TO_GET_COST_BY_FILTER.contains(Iterables.getOnlyElement(scopeTypes)))
                        .orElse(false);
    }

    private boolean isResourceGroup(ApiId inputScope) {
        final Optional<CachedGroupInfo> groupInfo = inputScope.getCachedGroupInfo();
        return groupInfo.isPresent() && groupInfo.get().getGroupType() == GroupType.RESOURCE;
    }

    /**
     * Request CostStats only for entityTypes which exist in resource group.
     *
     * @param requestedCostPriceStats requested cost stats
     * @param inputScope the input scope
     * @return filtered cost stats requests
     */
    @Nonnull
    private Set<StatApiInputDTO> filterRequestedCostStatsForResourceGroups(
            @Nonnull Set<StatApiInputDTO> requestedCostPriceStats, @Nonnull ApiId inputScope) {
        final Optional<Set<ApiEntityType>> scopeTypesOpt = inputScope.getScopeTypes();

        if (scopeTypesOpt.isPresent()) {
            final Set<String> entityTypesInResourceGroup = scopeTypesOpt.get()
                    .stream()
                    .map(ApiEntityType::apiStr)
                    .collect(Collectors.toSet());
            return requestedCostPriceStats.stream()
                    .filter(el -> entityTypesInResourceGroup.contains(el.getRelatedEntityType()))
                    .collect(Collectors.toSet());
        } else {
            return requestedCostPriceStats;
        }
    }

    /**
     * Merge any {@link StatSnapshotApiDTO} objects which have the same date to one {@link StatSnapshotApiDTO} object.
     * truncated times to minutes level to avoid discrepancies. Right now. UI expect only 2 time stamped snapshots.
     * One current stats and one projected.
     *
     * @param original {@link List} list of {@link StatSnapshotApiDTO}
     * @return {@link List}
     */
    @Nonnull
    private static List<StatSnapshotApiDTO> mergeStatSnapshotApiDTOWithDate(
            @Nonnull final List<StatSnapshotApiDTO> original) {
        return new ArrayList<>(Sets.newHashSet(original).stream()
                .collect(Collectors.toMap(snapshot -> ZonedDateTime.parse(snapshot.getDate())
                        .truncatedTo(ChronoUnit.MINUTES).toEpochSecond(), dto -> dto, (currDto, newDto) -> {
                    currDto.getStatistics().addAll(newDto.getStatistics());
                    return currDto;
                })).values());
    }

    /**
     * Group {@link CloudCostStatRecord}s by Storage Tier and map to {@link StatSnapshotApiDTO}s.
     *
     * @param cloudCostStatRecords {@link CloudCostStatRecord}s with cost stats
     * @param requestedStats {@link StatApiInputDTO}s with stat requests
     * @param storageTierDisplayNameToVolumeOids volume oids by storage tier
     * @return converted {@link StatSnapshotApiDTO}s
     */
    @Nonnull
    private List<StatSnapshotApiDTO> getVolumeCostStatsByTier(
            @Nonnull final List<CloudCostStatRecord> cloudCostStatRecords,
            @Nonnull final Set<StatApiInputDTO> requestedStats,
            @Nonnull final Map<String, Set<Long>> storageTierDisplayNameToVolumeOids) {

        final Map<Long, String> tierByVolume = new HashMap<>();
        storageTierDisplayNameToVolumeOids.forEach((tier, oidSet) ->
            oidSet.forEach(oid -> tierByVolume.put(oid, tier)));
        final Map<Long, Map<String, Set<StatRecord>>> recordsByTierByTime = new HashMap<>();
        for (final CloudCostStatRecord cloudCostStatRecord : cloudCostStatRecords) {
            final Map<String, Set<StatRecord>> statsForTier = new HashMap<>();
            for (final StatRecord statRecord : cloudCostStatRecord.getStatRecordsList()) {
                final String tier = tierByVolume.get(statRecord.getAssociatedEntityId());
                statsForTier.computeIfAbsent(tier, (x) -> new HashSet<>()).add(statRecord);
            }
            if (recordsByTierByTime.containsKey(cloudCostStatRecord.getSnapshotDate())) {
                final Map<String, Set<StatRecord>> combined =
                    new HashMap<>(recordsByTierByTime.get(cloudCostStatRecord.getSnapshotDate()));
                statsForTier.forEach((tier, stats) -> combined.merge(tier, stats, Sets::union));
                recordsByTierByTime.put(cloudCostStatRecord.getSnapshotDate(), combined);
            } else {
                recordsByTierByTime.put(cloudCostStatRecord.getSnapshotDate(), statsForTier);
            }
        }

        final List<StatSnapshotApiDTO> statSnapshotApiDTOs = new ArrayList<>();
        requestedStats.forEach(requestedStat -> {
            final ApiEntityType requestEntityType =
                ApiEntityType.fromString(requestedStat.getRelatedEntityType());
            if (ApiEntityType.UNKNOWN.equals(requestEntityType)) {
                logger.error("Unknown request entityType {}",
                    requestedStat.getRelatedEntityType());
            } else {
                recordsByTierByTime.forEach((timestamp, recordsByTier) -> {
                    StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
                    statSnapshotApiDTO.setDisplayName(requestedStat.getName());
                    statSnapshotApiDTO.setDate(DateTimeUtil.toString(timestamp));
                    final List<StatApiDTO> statApiDTOs = new ArrayList<>();
                    recordsByTier.forEach((tier, recordSet) -> {
                        final StatRecord statRecord = recordAggregator.aggregate(Lists.newArrayList(recordSet),
                                Optional.of(requestEntityType.typeNumber()), false);
                        statApiDTOs.add(toStatApiDTO(StringConstants.COST_PRICE, statRecord,
                            createStatFilterApiDTO(ApiEntityType.STORAGE_TIER.apiStr(), tier)));
                    });
                    statSnapshotApiDTO.setStatistics(statApiDTOs);
                    statSnapshotApiDTOs.add(statSnapshotApiDTO);
                });
            }
        });
        return statSnapshotApiDTOs;
    }

    private Set<StatApiInputDTO> mergeCostRequests(@Nonnull final Set<StatApiInputDTO> original) {
        //we already know it's not generic group by (so groupbys aren't necessary) and that they're all costprice
        final Set<StatApiInputDTO> merged = new HashSet<>();
        final Set<String> foundRelated = new HashSet<>();
        final Set<Set<StatFilterApiDTO>> foundFilters = new HashSet<>();
        original.forEach(stat -> {
            if (!foundRelated.contains(stat.getRelatedEntityType())) {
                foundRelated.add(stat.getRelatedEntityType());
                merged.add(stat);
            } else if (stat.getFilters() != null && !foundFilters.contains(ImmutableSet.copyOf(stat.getFilters()))) {
                foundFilters.add(ImmutableSet.copyOf(stat.getFilters()));
                merged.add(stat);
            }
        });
        return merged;
    }

    /**
     * Group {@link CloudCostStatRecord}s by attachment status and map to {@link StatSnapshotApiDTO}s.
     *
     * @param cloudCostStatRecords {@link CloudCostStatRecord}s with cost stats
     * @param requestedStats {@link StatApiInputDTO}s with stat requests
     * @param attachedVolumeOids oids of all attached volumes
     * @return converted {@link StatSnapshotApiDTO}s
     */
    @Nonnull
    private List<StatSnapshotApiDTO> getVolumeCostStatsByAttachment(
            @Nonnull final List<CloudCostStatRecord> cloudCostStatRecords,
            @Nonnull final Set<StatApiInputDTO> requestedStats,
            @Nonnull final Set<Long> attachedVolumeOids) {
        final List<StatSnapshotApiDTO> statSnapshotApiDTOs = new ArrayList<>();
        final Map<Long, Map<String, List<StatRecord>>> recordsByAttachmentByTime = new HashMap<>();
        for (final CloudCostStatRecord ccsr : cloudCostStatRecords) {
            final List<StatRecord> attachedRecords = new ArrayList<>();
            final List<StatRecord> unattachedRecords = new ArrayList<>();
            for (final StatRecord sr : ccsr.getStatRecordsList()) {
                if (attachedVolumeOids.contains(sr.getAssociatedEntityId())) {
                    attachedRecords.add(sr);
                } else {
                    unattachedRecords.add(sr);
                }
            }
            recordsByAttachmentByTime.putIfAbsent(ccsr.getSnapshotDate(), new HashMap<>());
            recordsByAttachmentByTime.get(ccsr.getSnapshotDate())
                .putIfAbsent(StringConstants.ATTACHED, new ArrayList<>());
            recordsByAttachmentByTime.get(ccsr.getSnapshotDate())
                .putIfAbsent(StringConstants.UNATTACHED, new ArrayList<>());
            recordsByAttachmentByTime.get(ccsr.getSnapshotDate()).get(StringConstants.ATTACHED)
                .addAll(attachedRecords);
            recordsByAttachmentByTime.get(ccsr.getSnapshotDate()).get(StringConstants.UNATTACHED)
                .addAll(unattachedRecords);
        }

        requestedStats.forEach(requestedStat -> {
            final ApiEntityType relatedType =
                ApiEntityType.fromString(requestedStat.getRelatedEntityType());
            if (ApiEntityType.UNKNOWN.equals(relatedType)) {
                logger.error("Unknown related entity type {} from request.",
                    requestedStat.getRelatedEntityType());
            } else {
                final Optional<Integer> typeOpt = Optional.of(relatedType.typeNumber());
                recordsByAttachmentByTime.forEach((timestamp, records) -> {
                    final StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
                    statSnapshotApiDTO.setDisplayName(requestedStat.getName());
                    statSnapshotApiDTO.setDate(DateTimeUtil.toString(timestamp));
                    final CloudCostStatRecord.StatRecord attachedRecord = recordAggregator
                        .aggregate(records.get(StringConstants.ATTACHED), typeOpt, false);
                    final CloudCostStatRecord.StatRecord unattachedRecord = recordAggregator
                        .aggregate(records.get(StringConstants.UNATTACHED), typeOpt, false);

                    final StatApiDTO attachedDTO = toStatApiDTO(StringConstants.COST_PRICE,
                        attachedRecord, createStatFilterApiDTO(StringConstants.ATTACHMENT,
                            StringConstants.ATTACHED));
                    final StatApiDTO unattachedDTO = toStatApiDTO(StringConstants.COST_PRICE,
                        unattachedRecord, createStatFilterApiDTO(StringConstants.ATTACHMENT,
                            StringConstants.UNATTACHED));

                    statSnapshotApiDTO.setStatistics(Lists.newArrayList(attachedDTO, unattachedDTO));
                    statSnapshotApiDTOs.add(statSnapshotApiDTO);
                });
            }
        });

        return statSnapshotApiDTOs;
    }

    @Nonnull
    private static StatFilterApiDTO createStatFilterApiDTO(@Nonnull String type,
                                                           @Nonnull String value) {
        StatFilterApiDTO statFilterDTO = new StatFilterApiDTO();
        statFilterDTO.setType(type);
        statFilterDTO.setValue(value);
        return statFilterDTO;
    }

    @Nonnull
    private static StatApiDTO toStatApiDTO(@Nonnull String name,
                                           @Nonnull CloudCostStatRecord.StatRecord statRecord,
                                           @Nonnull StatFilterApiDTO additionalFilter) {
        final StatApiDTO statApiDTO = toStatApiDTO(name, statRecord);
        statApiDTO.setFilters(Collections.singletonList(additionalFilter));
        return statApiDTO;
    }

    @Nonnull
    private static StatApiDTO toStatApiDTO(@Nonnull final String name,
                                    @Nonnull final StatRecord statRecord) {
        final StatApiDTO statApiDTO = new StatApiDTO();
        statApiDTO.setName(name);
        statApiDTO.setUnits(statRecord.getUnits());
        statApiDTO.setValue(statRecord.getValues().getAvg());

        final StatValueApiDTO statValueApiDTO = new StatValueApiDTO();
        statValueApiDTO.setAvg(statRecord.getValues().getAvg());
        statValueApiDTO.setMax(statRecord.getValues().getMax());
        statValueApiDTO.setMin(statRecord.getValues().getMin());
        statValueApiDTO.setTotal(statRecord.getValues().getTotal());
        statApiDTO.setValues(statValueApiDTO);

        return statApiDTO;
    }

    /**
     * Return the count of VMs + Database + DatabaseServers in the cloud.
     */
    private List<StatApiDTO> getNumWorkloadStatSnapshot(@Nonnull final Set<StatApiInputDTO> statsFilters,
                                                        @Nonnull final StatsQueryContext context) throws OperationFailedException {
        List<StatApiDTO> stats = Lists.newArrayList();
        for (StatApiInputDTO statApiInputDTO : statsFilters) {
            if (!filterStatInputs(Collections.singleton(statApiInputDTO), NUM_VOL, null).isEmpty()) {
                storageStatsSubQuery.getAggregateStats(Collections.singleton(statApiInputDTO), context)
                        .forEach(statSnapshotApiDTO -> {
                    stats.addAll(statSnapshotApiDTO.getStatistics());
                });
            } else {
                List<String> entityTypes = WORKLOAD_NAME_TO_ENTITY_TYPES.get(statApiInputDTO.getName());
                if (entityTypes != null) {
                    final Optional<Set<ApiEntityType>> scopeTypesOpt =
                            context.getInputScope().getScopeTypes();
                    // filter out entityTypes which don't exist in scope
                    if (scopeTypesOpt.isPresent() && isResourceGroup(context.getInputScope())) {
                        final Set<String> entityTypesFromScope = scopeTypesOpt.get()
                                .stream()
                                .map(ApiEntityType::apiStr)
                                .collect(Collectors.toSet());
                        entityTypes = entityTypes.stream()
                                        .filter(entityTypesFromScope::contains)
                                        .collect(Collectors.toList());
                    }
                }
                if (entityTypes != null && !entityTypes.isEmpty()) {
                    final Set<Long> scopeIds = context.getQueryScope().getScopeOids();
                    final Set<Long> fetchedScope =
                            context.getInputScope().isResourceGroupOrGroupOfResourceGroups() ?
                                    Collections.singleton(context.getInputScope().oid()) : scopeIds;
                    final float numWorkloads = supplyChainFetcherFactory.newNodeFetcher()
                            .addSeedOids(fetchedScope)
                            .entityTypes(entityTypes)
                            .environmentType(EnvironmentType.CLOUD)
                            .fetchEntityIds()
                            .size();
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
        }
        return stats;
    }

    private List<CloudCostStatRecord> getCloudStatRecordList(@Nonnull final Set<StatApiInputDTO> stats,
                                                             @Nullable final Set<Long> entityStatOids,
                                                             @Nonnull final StatsQueryContext context,
                                                             final boolean isGenericGroupBy) {
        if (stats.isEmpty() || entityStatOids == null) {
            return Collections.emptyList();
        }
        Set<CloudCostStatsQuery> cloudCostStatsQueries = Sets.newHashSet();
        final AtomicLong count = new AtomicLong(0);
        final List<String> groupByValues = Arrays.stream(GroupBy.values())
                .map(GroupBy::getValueDescriptor)
                .map(EnumValueDescriptor::getName)
                .collect(toList());

        List<String> costCategoryValues = Arrays.stream(CostCategory.values()).map(Enum::name).collect(toList());
        stats.forEach(statApiInputDTO -> {
            Builder cloudCostStatsQuery = CloudCostStatsQuery.newBuilder();
            if (ApiEntityType.fromString(statApiInputDTO.getRelatedEntityType()) != ApiEntityType.UNKNOWN) {
                EntityTypeFilter.Builder entityTypeFilter = EntityTypeFilter.newBuilder();
                entityTypeFilter.addEntityTypeId(ApiEntityType
                        .fromStringToSdkType(statApiInputDTO.getRelatedEntityType()));
                cloudCostStatsQuery.setEntityTypeFilter(entityTypeFilter);
            }
            addCloudCostStatsQueryFilter(cloudCostStatsQuery, entityStatOids, context);
            if (statApiInputDTO.getFilters() != null) {
                cloudCostStatsQuery.setCostCategoryFilter(CostCategoryFilter.newBuilder()
                        .addAllCostCategory(statApiInputDTO.getFilters().stream()
                                .filter(filterApiDTO -> costCategoryValues.contains(filterApiDTO.getValue()))
                                .map(filterApiDTO -> CostCategory.valueOf(filterApiDTO.getValue()))
                                .collect(toList())).build());
            }
            //Query id
            cloudCostStatsQuery.setQueryId(count.getAndIncrement());
            //Projected Request..
            cloudCostStatsQuery.setRequestProjected(context.requestProjected());
            if (isGenericGroupBy && statApiInputDTO.getGroupBy() != null && !statApiInputDTO.getGroupBy().isEmpty()) {
                cloudCostStatsQuery.addAllGroupBy(statApiInputDTO.getGroupBy().stream()
                        //todo: Shud do something better to handle costComponent to COST_CATEGORY conversion.
                        .map(groupByString -> {
                            if (groupByString.equals(COST_COMPONENT)) {
                                return GroupBy.COST_CATEGORY;
                            }
                            return groupByValues.contains(groupByString) ?
                                    GroupBy.valueOf(groupByString) : GroupBy.COST_CATEGORY;
                        })
                        .collect(Collectors.toList()));
            }
            //time frames.
            context.getTimeWindow().ifPresent(timeWindow -> {
                cloudCostStatsQuery.setStartDate(timeWindow.startTime());
                cloudCostStatsQuery.setEndDate(timeWindow.endTime());
            });

            if (!buyRiScopeHandler.shouldIncludeBuyRiDiscount(context.getInputScope())) {
                cloudCostStatsQuery.setCostSourceFilter(CostSourceFilter.newBuilder()
                        .addCostSources(Cost.CostSource.BUY_RI_DISCOUNT)
                        .setExclusionFilter(true)
                        .build());
            }
            context.getPlanInstance().ifPresent(plan -> cloudCostStatsQuery.setTopologyContextId(plan.getPlanId()));
            Optional<Builder> cloudCostStatsQueryOptional =
                    updateQueriesByUserScope(context, cloudCostStatsQuery);
            cloudCostStatsQueryOptional.ifPresent(updatedCloudCostQuery ->
                    cloudCostStatsQueries.add(updatedCloudCostQuery.build()));
        });
        if (cloudCostStatsQueries.isEmpty()) {
            return Collections.emptyList();
        } else {
            GetCloudCostStatsRequest getCloudExpenseStatsRequest = GetCloudCostStatsRequest.newBuilder()
                    .addAllCloudCostStatsQuery(cloudCostStatsQueries).build();
            final Iterator<GetCloudCostStatsResponse> response =
                costServiceRpc.getCloudCostStats(getCloudExpenseStatsRequest);
            final List<CloudCostStatRecord> records = new ArrayList<>();
            while (response.hasNext()) {
                records.addAll(response.next().getCloudStatRecordList());
            }
            return records;
        }
    }

    private Optional<Builder> updateQueriesByUserScope(
            @Nonnull final StatsQueryContext context,
            @Nonnull final Builder cloudCostStatsQuery) {
        Set<Long> expandedOids = context.getQueryScope().getExpandedOids();
        if (context.getInputScope().getScopeTypes().isPresent() &&
                shouldQueryUsingFilter(context.getInputScope()) &&
                userSessionContext.isUserObserver()
                && userSessionContext.isUserScoped()) {
            if (expandedOids.isEmpty()) {
                return Optional.empty();
            } else {
                cloudCostStatsQuery.getEntityFilterBuilder().addAllEntityId(expandedOids);
            }
        }
        return Optional.of(cloudCostStatsQuery);
    }

    private void addCloudCostStatsQueryFilter(@Nonnull final Builder cloudCostStatsQuery,
                                              @Nonnull final Set<Long> entityStatOids,
                                              @Nonnull final StatsQueryContext context) {
        //TODO consider create default Cloud group (probably in Group component). Currently the only group
        if (!entityStatOids.isEmpty()) {
            if (context.getInputScope().getScopeTypes().isPresent() && shouldQueryUsingFilter(context.getInputScope())) {
                ApiEntityType scopeType =
                        context.getInputScope().getScopeTypes().get().iterator().next();
                switch (scopeType) {
                    case BUSINESS_ACCOUNT:
                        cloudCostStatsQuery.setAccountFilter(Cost.AccountFilter.newBuilder()
                                .addAllAccountId(entityStatOids));
                        break;
                    case REGION:
                        cloudCostStatsQuery.setRegionFilter(Cost.RegionFilter.newBuilder()
                                .addAllRegionId(entityStatOids));
                        break;
                    case AVAILABILITY_ZONE:
                        cloudCostStatsQuery.setAvailabilityZoneFilter(Cost.AvailabilityZoneFilter.newBuilder()
                                .addAllAvailabilityZoneId(entityStatOids));
                }
            } else {
                cloudCostStatsQuery.getEntityFilterBuilder().addAllEntityId(entityStatOids);
            }
        }
    }

    @Nonnull
    private List<CloudCostStatRecord> getCloudExpensesRecordList(@Nonnull final Set<String> requestGroupBySet,
                                                                 @Nonnull final Set<Long> entities,
                                                                 @Nonnull final StatsQueryContext context) {
        final GetCloudExpenseStatsRequest.Builder builder = GetCloudExpenseStatsRequest.newBuilder();
        context.getTimeWindow().ifPresent(timeWindow -> {
            builder.setStartDate(timeWindow.startTime());
            builder.setEndDate(timeWindow.endTime());
        });

        if (!entities.isEmpty()) {
            builder.getEntityFilterBuilder().addAllEntityId(entities);
        }
        if (requestGroupBySet.contains(StringConstants.BUSINESS_UNIT)) {
            builder.setGroupBy(GroupByType.BUSINESS_UNIT);
        } else if (requestGroupBySet.contains(StringConstants.CSP)) {
            builder.setGroupBy(GroupByType.CSP);
        } else if (requestGroupBySet.contains(StatsService.CLOUD_SERVICE)) {
            builder.setGroupBy(GroupByType.CLOUD_SERVICE);
        }

        // get relevant business account(s) from input scope.
        builder.setScope(getAccountScopeBuilder(context));

        return costServiceRpc.getAccountExpenseStats(
            builder.build()).getCloudStatRecordList();
    }

    /**
     * This method calculates the requested business account scope, if exists, and creates an
     * AccountExpenseQueryScope builder accordingly.
     * In case that the scope has no business account / billing family / group of account,
     * the scope will be "all accounts".
     *
     * @param context The query context, which contains the input scope.
     * @return The relevant business accounts scope.
     */
    @VisibleForTesting
    AccountExpenseQueryScope.Builder getAccountScopeBuilder(@Nonnull final StatsQueryContext context) {
        final AccountExpenseQueryScope.Builder scopeBuilder = AccountExpenseQueryScope.newBuilder();
        if (userSessionContext.isUserObserver() && userSessionContext.isUserScoped()) {
            return scopeBuilder;
        }
        // If the scoped entity/entities are business accounts - use the as scope
        final Set<Long> businessAccounts =
                context.getInputScope().getScopeEntitiesByType().getOrDefault(
                        ApiEntityType.BUSINESS_ACCOUNT, Collections.emptySet());
        if (businessAccounts.isEmpty()) {
            scopeBuilder.setAllAccounts(true);
        } else {
            final IdList.Builder accountsIdList = IdList.newBuilder();
            repositoryApi.entitiesRequest(businessAccounts)
                    .getFullEntities()
                    .filter(e -> e.hasTypeSpecificInfo() && e.getTypeSpecificInfo()
                            .hasBusinessAccount() && e.getTypeSpecificInfo()
                            .getBusinessAccount()
                            .hasAssociatedTargetId())
                    .forEach(e -> accountsIdList.addAccountIds(e.getOid()));
            scopeBuilder.setSpecificAccounts(accountsIdList);
        }
        return scopeBuilder;
    }

    // Get discovered entities.
    private Map<Long, MinimalEntity> getDiscoveredEntityDTOs(ApiEntityType entityType) {
        // find all cloud services
        return repositoryApi.newSearchRequest(SearchProtoUtil.makeSearchParameters(
            SearchProtoUtil.entityTypeFilter(entityType)).build())
                .getMinimalEntities()
            .collect(Collectors.toMap(MinimalEntity::getOid, Function.identity()));
    }

    private static boolean isTopDownRequest(final Set<String> requestGroupBySet) {
        return requestGroupBySet.contains(StringConstants.BUSINESS_UNIT)
            || requestGroupBySet.contains(StringConstants.CSP)
            || requestGroupBySet.contains(StatsService.CLOUD_SERVICE);
    }

    /**
     * Return a type function which returns the filter type according to the "groupBy" part of the query.
     *
     * @param requestGroupBySet the "groupBy" value (businessUnit/CSP/CloudService)
     * @return a type function which returns the relevant filter type
     */
    private Supplier<String> getTypeFunction(@Nonnull final Set<String> requestGroupBySet) {
        if (requestGroupBySet.contains(StringConstants.BUSINESS_UNIT)) {
            return () -> StringConstants.BUSINESS_UNIT;
        }
        if (requestGroupBySet.contains(StringConstants.CSP)) {
            return () -> StringConstants.CSP;
        }
        if (requestGroupBySet.contains(StatsService.CLOUD_SERVICE)) {
            return () -> StatsService.CLOUD_SERVICE;
        }
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * The returned value function receives an associatedEntityID (the associated entity type is
     * different in each case) and a set of cloudServicesDTO (used only when grouping by
     * CSP/CloudService) and returns the relevant filter value.
     *
     * e.g:
     *      When grouping by cloud type, the filter should look like: {cloudService, "AWS_EC2"}
     *      When grouping by CSP, the filter should look like: {CSP, "Azure"}
     *
     * @param requestGroupBySet the "groupBy" value (Target/CSP/CloudService)
     * @return a value function which calculates the filter value according to the filter type and
     * the relevant entityID
     */
    private BiFunction<Long, Map<Long, MinimalEntity>, Optional<String>> getValueFunction(
            @Nonnull final Set<String> requestGroupBySet) {
        // When grouping by cloud provider, the filter's value should be the cloud provider name.
        // In this case entityDTOs is empty.
        if (requestGroupBySet.contains(StringConstants.CSP)) {
            return (targetId, entityDTOs) ->
                    thinTargetCache.getTargetInfo(targetId)
                            .flatMap(thinTargetInfo ->
                                cloudTypeMapper.fromTargetType(thinTargetInfo.probeInfo().type())
                                    .map(Enum::name));
        }



        // When grouping by cloud service, the filter's value should be the cloud service name.
        // In this case, the associated entity ID is a cloud service ID, and the DTOs are
        // cloud service DTOs.
        // When grouping by business account, the filter's value should be the account name.
        // In this case, the associated entity ID is a business account ID, and the DTOs are
        // business account DTOs.
        if (requestGroupBySet.contains(StatsService.CLOUD_SERVICE) || requestGroupBySet.contains(StringConstants.BUSINESS_UNIT)) {
            return (associatedEntityId, entityDTOs) ->
                    Optional.ofNullable(entityDTOs.get(associatedEntityId))
                            .flatMap(entityDTO -> Optional.of(entityDTO.getDisplayName()));
        }

        throw ApiUtils.notImplementedInXL();
    }

    @Nonnull
    public StatSnapshotApiDTO toStatSnapshotApiDTO(@Nonnull final CloudCostStatRecord statSnapshot,
           @Nonnull final Supplier<String> typeFunction,
           @Nonnull final BiFunction<Long, Map<Long, MinimalEntity>, Optional<String>> valueFunction,
           @Nonnull final Map<Long, MinimalEntity> entityDTOs) {
        final StatSnapshotApiDTO dto = new StatSnapshotApiDTO();
        // TODO: Store Epoch information in the CloudCostStatRecord, and map it here
        if (statSnapshot.hasSnapshotDate()) {
            dto.setDate(DateTimeUtil.toString(statSnapshot.getSnapshotDate()));
        }
        dto.setStatistics(statSnapshot.getStatRecordsList().stream()
                .map(statApiDTO -> toStatApiDto(statApiDTO,
                        typeFunction,
                        valueFunction,
                        entityDTOs))
                .collect(toList()));
        return dto;
    }

    @Nonnull
    private StatApiDTO toStatApiDto(@Nonnull final CloudCostStatRecord.StatRecord statRecord,
           @Nonnull final Supplier<String> typeFunction,
           @Nonnull final BiFunction<Long, Map<Long, MinimalEntity>, Optional<String>> valueFunction,
           @Nonnull final Map<Long, MinimalEntity> entityDTOs) {
        final StatApiDTO statApiDTO = toStatApiDTO(statRecord.getName(), statRecord);
        final long associatedEntityId = statRecord.getAssociatedEntityId();

        final BaseApiDTO provider = new BaseApiDTO();
        provider.setUuid(String.valueOf(associatedEntityId));
        statApiDTO.setRelatedEntity(provider);

        // Build filters
        final List<StatFilterApiDTO> filters = new ArrayList<>();
        Optional<String> filterValue = valueFunction.apply(associatedEntityId, entityDTOs);
        if (filterValue.isPresent()) {
            final StatFilterApiDTO resultsTypeFilter = new StatFilterApiDTO();
            resultsTypeFilter.setType(typeFunction.get());
            resultsTypeFilter.setValue(filterValue.get());
            filters.add(resultsTypeFilter);
        }
        statApiDTO.setFilters(filters);
        return statApiDTO;
    }

    /**
     * Convert Cloud related stat snap shot to StatSnapshotApiDTO.
     *
     * @param statSnapshot stat snap shot
     * @return StatSnapshotApiDTO
     */
    private static StatSnapshotApiDTO toCloudStatSnapshotApiDTO(final CloudCostStatRecord statSnapshot) {
        final StatSnapshotApiDTO dto = new StatSnapshotApiDTO();
        if (statSnapshot.hasSnapshotDate()) {
            dto.setDate(DateTimeUtil.toString(statSnapshot.getSnapshotDate()));
        }
        dto.setStatistics(statSnapshot.getStatRecordsList().stream()
                .map(CloudCostsStatsSubQuery::mapStatRecordToStatApiDTO)
                .collect(toList()));
        return dto;
    }

    /**
     * Converts {@link StatRecord} to {@link StatApiDTO}
     *
     * @param statRecord to convert to {@link StatApiDTO}
     * @return {@link StatApiDTO} mapped from statRecord
     */
    public static StatApiDTO mapStatRecordToStatApiDTO(StatRecord statRecord) {
        final StatApiDTO statApiDTO = toStatApiDTO(statRecord.getName(), statRecord);

        if (statRecord.hasCategory()) {
            // Build filters
            final List<StatFilterApiDTO> filters = new ArrayList<>();
            final StatFilterApiDTO resultsTypeFilter = new StatFilterApiDTO();
            resultsTypeFilter.setType(COST_COMPONENT);
            resultsTypeFilter.setValue(getCostCategoryString(statRecord));
            filters.add(resultsTypeFilter);

            if (filters.size() > 0) {
                statApiDTO.setFilters(filters);
            }
        }
        // set related entity type
        if (statRecord.hasAssociatedEntityType()) {
            statApiDTO.setRelatedEntityType(ApiEntityType.fromType(
                    statRecord.getAssociatedEntityType()).apiStr());
        }
        return statApiDTO;
    }

    @Nullable
    @VisibleForTesting
    static String getCostCategoryString(
            @Nonnull final CloudCostStatRecord.StatRecord statRecord) {
        if (!statRecord.hasCategory()) {
            return null;
        }
        final Cost.CostCategory costCategory = statRecord.getCategory();
        try {
            return CostCategory.valueOf(costCategory.name()).name();
        } catch (IllegalArgumentException ex) {
            logger.error("Unknown cost category: " + costCategory, ex);
            return null;
        }
    }

    static class CloudStatRecordAggregator {
        /**
         * Aggregate a list of StatRecord into one StatRecord. Set relatedEntityType if provided.
         */
        CloudCostStatRecord.StatRecord aggregate(@Nonnull List<CloudCostStatRecord.StatRecord> statRecordsList,
                                                         @Nonnull Optional<Integer> relatedEntityType,
                                                         final boolean isRiCost) {
            final CloudCostStatRecord.StatRecord.Builder statRecordBuilder = CloudCostStatRecord.StatRecord.newBuilder();
            final CloudCostStatRecord.StatRecord.StatValue.Builder statValueBuilder = CloudCostStatRecord.StatRecord.StatValue.newBuilder();
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
    }
}

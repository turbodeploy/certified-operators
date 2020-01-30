package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static com.vmturbo.api.component.external.api.util.stats.query.impl.StorageStatsSubQuery.NUM_VOL;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

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
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryExecutor;
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
import com.vmturbo.common.protobuf.GroupProtoUtil;
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
import com.vmturbo.common.protobuf.cost.Cost.GetCloudExpenseStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudExpenseStatsRequest.GroupByType;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO;
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
        StringConstants.NUM_DBSS, "currentNumDBSs");

    private static final Map<String, List<String>> WORKLOAD_NAME_TO_ENTITY_TYPES = ImmutableMap.of(
        StringConstants.NUM_VMS, Collections.singletonList(UIEntityType.VIRTUAL_MACHINE.apiStr()),
        StringConstants.NUM_DBS, Collections.singletonList(UIEntityType.DATABASE.apiStr()),
        StringConstants.NUM_DBSS, Collections.singletonList(UIEntityType.DATABASE_SERVER.apiStr()),
        StringConstants.NUM_WORKLOADS, new ArrayList<>(WORKLOAD_ENTITY_TYPES_API_STR)
    );

    private static final Set<UIEntityType> ENTITY_TYPES_TO_GET_COST_BY_FILTER = ImmutableSet.of(
        UIEntityType.BUSINESS_ACCOUNT, UIEntityType.REGION, UIEntityType.AVAILABILITY_ZONE
    );

    /**
     * Cloud target constant to match UI request, also used in test case.
     * This is used for the "Cost Breakdown by Cloud Account" widget.
     */
    public static final String BUSINESS_UNIT = "businessUnit";

    /**
     * Cloud service constant to match UI request, also used in test cases.
     * This is used for the "Cost Breakdown by Cloud Service" widget.
     */
    public static final String CLOUD_SERVICE = "cloudService";

    /**
     * Cloud service provider constant to match UI request, also used in test cases.
     * This is used for the "Cost Breakdown by Cloud Provider" widget.
     */
    public static final String CSP = "CSP";

    // Internally generated stat name when stats period are not set.
    private static final String CURRENT_COST_PRICE = "currentCostPrice";

    //only queries tagged costPrice should be used to retrieved cost.
    protected static final String COST_PRICE_QUERY_KEY = "costPrice";

    protected static final String COST_COMPONENT = "costComponent";

    public static final String CURRENT_NUM_VOLUMES = "currentNumVolumes";

    private static final Set<String> COST_STATS_SET = ImmutableSet.of(StringConstants.COST_PRICE,
        CURRENT_COST_PRICE);

    private final RepositoryApi repositoryApi;

    private final CostServiceBlockingStub costServiceRpc;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final CloudStatRecordAggregator recordAggregator = new CloudStatRecordAggregator();

    private final ThinTargetCache thinTargetCache;

    private final BuyRiScopeHandler buyRiScopeHandler;

    private final CloudTypeMapper cloudTypeMapper;

    private final StorageStatsSubQuery storageStatsSubQuery;

    public CloudCostsStatsSubQuery(@Nonnull final RepositoryApi repositoryApi,
                                   @Nonnull final CostServiceBlockingStub costServiceRpc,
                                   @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                                   @Nonnull final ThinTargetCache thinTargetCache,
                                   @Nonnull final BuyRiScopeHandler buyRiScopeHandler,
                                   @Nonnull final StorageStatsSubQuery storageStatsSubQuery) {
        this.repositoryApi = repositoryApi;
        this.costServiceRpc = costServiceRpc;
        this.supplyChainFetcherFactory = supplyChainFetcherFactory;
        this.thinTargetCache = thinTargetCache;
        this.buyRiScopeHandler = buyRiScopeHandler;
        this.storageStatsSubQuery = storageStatsSubQuery;
        this.cloudTypeMapper = new CloudTypeMapper();
    }

    @Override
    public boolean applicableInContext(@Nonnull final StatsQueryContext context) {
        // If the query scope is going to be a non-CLOUD global group, we don't need to run
        // this sub-query.
        final Optional<EnvironmentType> globalScopeEnvType = context.getQueryScope().getGlobalScope()
            .flatMap(GlobalScope::environmentType);
        if (globalScopeEnvType.isPresent() && globalScopeEnvType.get() != EnvironmentType.CLOUD) {
            return false;
        }

        return true;
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

        //This was done to handle numVolumes in CloudCostsStatsSubQuery instead of in StorageStatsSubQuery.
        countStats.addAll(context.findStats(ImmutableSet.of(StringConstants.NUM_VIRTUAL_VOLUMES, CURRENT_NUM_VOLUMES)));

        return SubQuerySupportedStats.some(Sets.union(costStats, countStats));
    }

    private static boolean isGroupByAttachment(Set<StatApiInputDTO> requestedStats) {
        return requestedStats.stream().anyMatch(requestedStat -> requestedStat.getGroupBy() != null && requestedStat.getGroupBy().contains(StringConstants.ATTACHMENT) &&
                UIEntityType.VIRTUAL_VOLUME.apiStr().equals(requestedStat.getRelatedEntityType()));
    }

    private static boolean isGroupByStorageTier(Set<StatApiInputDTO> requestedStats) {
        return requestedStats.stream().anyMatch(requestedStat -> requestedStat.getGroupBy() != null && requestedStat.getGroupBy().contains(UIEntityType.STORAGE_TIER.apiStr()) &&
                UIEntityType.VIRTUAL_VOLUME.apiStr().equals(requestedStat.getRelatedEntityType()));
    }

    @Nonnull
    @Override
    public List<StatSnapshotApiDTO> getAggregateStats(@Nonnull final Set<StatApiInputDTO> requestedStats,
                                                      @Nonnull final StatsQueryContext context) throws OperationFailedException {
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
            if (requestGroupBySet.contains(CLOUD_SERVICE)) {
                // for group by Cloud services, we need to find all the services and
                // stitch with expenses in Cost component
                entityDTOs = getDiscoveredEntityDTOs(UIEntityType.CLOUD_SERVICE);
            } else if (requestGroupBySet.contains(BUSINESS_UNIT)) {
                // get business accounts
                entityDTOs = getDiscoveredEntityDTOs(UIEntityType.BUSINESS_ACCOUNT);
            } else {
                entityDTOs = Collections.emptyMap();
            }

            final List<CloudCostStatRecord> cloudStatRecords =
                    getCloudExpensesRecordList(requestGroupBySet,
                            // if groupBy = cloudService, get only the service costs from the DB.
                            requestGroupBySet.contains(CLOUD_SERVICE) ?
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
                cloudEntityOids = context.getQueryScope().getExpandedOids();
            }
            /*
             * Queries with name {@link #COST_PRICE_QUERY_KEY is used for querying to cost component.
             */
            Set<StatApiInputDTO> requestedCostPriceStats = requestedStats.stream()
                    .filter(requestedStat -> requestedStat.getName().equals(COST_PRICE_QUERY_KEY)).collect(toSet());

            if (isResourceGroup(inputScope)) {
                requestedCostPriceStats =
                        filterRequestedCostStatsForResourceGroups(requestedCostPriceStats,
                                inputScope);
            }

            final boolean isGroupByAttachment = isGroupByAttachment(requestedStats);
            final boolean isGroupByStorageTier = isGroupByStorageTier(requestedStats);

            List<CloudCostStatRecord> cloudCostStatRecords = getCloudStatRecordList(requestedCostPriceStats,
                    cloudEntityOids, context, !isGroupByAttachment && !isGroupByStorageTier);

            // For Virtual Volume, when it is grouped by either attachment or storage tier,
            //   aggregation need to be handled explicitly
            List<StatSnapshotApiDTO> statSnapshots = new ArrayList<>();
            if (isGroupByAttachment || isGroupByStorageTier) {
                // add List<StatSnapshotApiDTO> if group by attachment
                if (isGroupByAttachment) {
                    // Get all the Virtual Volume Oid in the CloudCostStatRecords
                    Set<StatApiInputDTO> groupByAttachmentStatApiInputDtos = requestedStats.stream().filter(rs -> UIEntityType.VIRTUAL_VOLUME.apiStr().equals(rs.getRelatedEntityType()) && rs.getGroupBy().contains(StringConstants.ATTACHMENT))
                            .collect(Collectors.toSet());
                    List<StatSnapshotApiDTO> groupByAttachmentStat = getGroupByVVAttachmentStat(cloudCostStatRecords, groupByAttachmentStatApiInputDtos);
                    if (CollectionUtils.isNotEmpty(groupByAttachmentStat)) {
                        statSnapshots.addAll(groupByAttachmentStat);
                    }
                }

                // add List<StatSnapshotApiDTO> if group by storage tier
                if (isGroupByStorageTier) {
                    Set<StatApiInputDTO> groupByStorageTierStatApiInputDtos = requestedStats.stream().filter(rs -> UIEntityType.VIRTUAL_VOLUME.apiStr().equals(rs.getRelatedEntityType()) && rs.getGroupBy().contains(UIEntityType.STORAGE_TIER.apiStr()))
                            .collect(Collectors.toSet());
                    List<StatSnapshotApiDTO> groupByStorageTierStat = getGroupByStorageTierStat(cloudCostStatRecords, groupByStorageTierStatApiInputDtos);
                    if (CollectionUtils.isNotEmpty(groupByStorageTierStat)) {
                        statSnapshots.addAll(groupByStorageTierStat);
                    }
                }
            } else {
                statSnapshots = cloudCostStatRecords.stream()
                    .map(this::toCloudStatSnapshotApiDTO)
                    .sorted(Comparator.comparing(StatSnapshotApiDTO::getDate))
                    .collect(toList());
            }
            statsResponse = mergeStatSnapshotApiDTOWithDate(statSnapshots);
            // add numWorkloads, numVMs, numDBs, numDBSs if requested
            final List<StatApiDTO> numWorkloadStats =
                    getNumWorkloadStatSnapshot(requestedStats, context);
            if (!numWorkloadStats.isEmpty()) {
                // add the numWorkloads to the same timestamp it it exists.
                if (statSnapshots.isEmpty()) {
                    StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
                    context.getTimeWindow()
                            .ifPresent(window -> statSnapshotApiDTO.setDate(DateTimeUtil.toString(window.endTime())));
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
        final Optional<Set<UIEntityType>> scopeTypesOpt = inputScope.getScopeTypes();

        if (scopeTypesOpt.isPresent()) {
            final Set<String> entityTypesInResourceGroup = scopeTypesOpt.get()
                    .stream()
                    .map(UIEntityType::apiStr)
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
     * Group {@link List} of {@link CloudCostStatRecord} objects by Storage Tiers and map to
     *   {@link StatSnapshotApiDTO} objects.
     *
     * @param cloudCostStatRecords {@link List} of {@link CloudCostStatRecord}
     * @param requestedStats {@link Set} of {@link StatSnapshotApiDTO}
     * @return {@link List} of {@link StatSnapshotApiDTO}
     */
    @Nonnull
    @VisibleForTesting
    private List<StatSnapshotApiDTO> getGroupByStorageTierStat(@Nonnull List<CloudCostStatRecord> cloudCostStatRecords,
                                                       @Nonnull Set<StatApiInputDTO> requestedStats) {
        Set<Long> vvOids = cloudCostStatRecords.stream().flatMapToLong(cloudCostStatRecord ->
            cloudCostStatRecord.getStatRecordsList().stream()
                .filter(sr -> sr.getAssociatedEntityType() == CommonDTO.EntityDTO.EntityType.VIRTUAL_VOLUME.getNumber())
                .mapToLong(sr -> sr.getAssociatedEntityId())
        ).boxed().collect(Collectors.toSet());

        // get list of attached VVs
        final Set<TopologyDTO.PartialEntity.ApiPartialEntity> vvEntities = repositoryApi
            .newSearchRequest(
                SearchProtoUtil
                    .makeSearchParameters(SearchProtoUtil.idFilter(vvOids))
                    .build())
            .getEntities()
            .collect(Collectors.toSet());

        Map<Long, List<Long>> storageTierOidToConnectedVVOids = new HashMap<>();
        vvEntities.forEach(vvEntity -> {
            Optional<TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity> storageTierOpt = vvEntity.getConnectedToList().stream()
                .filter(relatedEntity -> relatedEntity.getEntityType() == CommonDTO.EntityDTO.EntityType.STORAGE_TIER.getNumber())
                .findFirst();

            if (storageTierOpt.isPresent()) {
                storageTierOidToConnectedVVOids
                    .computeIfAbsent(storageTierOpt.get().getOid(), k -> new ArrayList<>())
                    .add(vvEntity.getOid());
            } else {
                logger.error("Virtual Volume {} with uuid {} has NO storage tier connected to", vvEntity.getDisplayName(), vvEntity.getOid());
            }
        });

        final Function<MinimalEntity, String> getStorageTierDisplayName = storageTierEntity -> {
            if (Strings.isNullOrEmpty(storageTierEntity.getDisplayName())) {
                logger.warn("No displayName for storage tier entity {}.  Use oid as displayName instead.", storageTierEntity.getOid());
                return String.valueOf(storageTierEntity.getOid());
            } else {
                return storageTierEntity.getDisplayName();
            }
        };

        final Map<String, List<Long>> storageTierDisplayNameToConnectedVVOid = repositoryApi
            .newSearchRequest(
                SearchProtoUtil
                    .makeSearchParameters(SearchProtoUtil.idFilter(storageTierOidToConnectedVVOids.keySet()))
                    .build())
            .getMinimalEntities()
            .collect(Collectors.toMap(getStorageTierDisplayName,
                storageEntity -> storageTierOidToConnectedVVOids.get(storageEntity.getOid())));

        List<StatSnapshotApiDTO> statSnapshotApiDTOs = new ArrayList<>();

        cloudCostStatRecords.forEach(cloudCostStatRecord -> {
            Map<String, List<CloudCostStatRecord.StatRecord>> cloudCostStatRecordGroupBySt = new HashMap<>();
            for (String storageTierName : storageTierDisplayNameToConnectedVVOid.keySet()) {
                List<CloudCostStatRecord.StatRecord> records = cloudCostStatRecord.getStatRecordsList().stream()
                    .filter(rec -> storageTierDisplayNameToConnectedVVOid.get(storageTierName).contains(rec.getAssociatedEntityId()))
                    .collect(Collectors.toList());

                if (CollectionUtils.isNotEmpty(records)) {
                    cloudCostStatRecordGroupBySt.put(storageTierName, records);
                }
            }

            requestedStats.stream().forEach(requestedStat -> {
                UIEntityType requestEntityType = UIEntityType.fromString(requestedStat.getRelatedEntityType());

                if (UIEntityType.UNKNOWN.equals(requestEntityType)) {
                    logger.error("Unknown request entityType {}", requestedStat.getRelatedEntityType());
                } else {
                    StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
                    statSnapshotApiDTO.setDisplayName(requestedStat.getName());
                    statSnapshotApiDTO.setDate(DateTimeUtil.toString(cloudCostStatRecord.getSnapshotDate()));


                    List<StatApiDTO> statApiDTOs = cloudCostStatRecordGroupBySt.entrySet().stream()
                        .map(entry -> {
                            CloudCostStatRecord.StatRecord statRecord = recordAggregator.aggregate(entry.getValue(),
                                Optional.of(Integer.valueOf(requestEntityType.typeNumber())), false);
                            return toStatApiDTO(StringConstants.COST_PRICE, statRecord, createStatFilterApiDTO(UIEntityType.STORAGE_TIER.apiStr(), entry.getKey()));
                        })
                        .collect(Collectors.toList());

                    statSnapshotApiDTO.setStatistics(statApiDTOs);

                    statSnapshotApiDTOs.add(statSnapshotApiDTO);
                }
            });
        });

        return statSnapshotApiDTOs;
    }

    /**
     * Group {@link List} of {@link CloudCostStatRecord} objects by attachment status and map to
     *   {@link StatSnapshotApiDTO} objects.
     *
     * @param cloudCostStatRecords {@link List} of {@link CloudCostStatRecord}
     * @param requestedStats {@link Set} of {@link StatSnapshotApiDTO}
     * @return {@link List} of {@link StatSnapshotApiDTO}
     */
    @Nonnull
    @VisibleForTesting
    List<StatSnapshotApiDTO> getGroupByVVAttachmentStat(@Nonnull List<CloudCostStatRecord> cloudCostStatRecords,
                                                        @Nonnull Set<StatApiInputDTO> requestedStats) {
        Set<Long> vvOids = cloudCostStatRecords.stream().flatMapToLong(cloudCostStatRecord ->
            cloudCostStatRecord.getStatRecordsList().stream()
                .filter(sr -> sr.getAssociatedEntityType() == CommonDTO.EntityDTO.EntityType.VIRTUAL_VOLUME.getNumber())
                .mapToLong(sr -> sr.getAssociatedEntityId())
        ).boxed().collect(Collectors.toSet());

        // Get list of attached VVs
        // Note that there may be extra vvOid returned as one VM may have multiple VVs,
        //  but the VV list will still be the one associated with the cloudCostStatRecords which
        //  the scope originally retrieved.
        final Set<Long> vvOidsAttachedToVM = repositoryApi.entitiesRequest(vvOids)
            .getFullEntities()
            .filter(topologyEntityDTO -> {
                if (!topologyEntityDTO.hasTypeSpecificInfo()) {
                    return false;
                }
                TopologyDTO.TypeSpecificInfo typeSpecificInfo = topologyEntityDTO.getTypeSpecificInfo();
                if (!typeSpecificInfo.hasVirtualVolume()) {
                    return false;
                }
                TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo virtualVolumeInfo = typeSpecificInfo.getVirtualVolume();
                return virtualVolumeInfo.hasAttachmentState() && virtualVolumeInfo.getAttachmentState().equals(
                        CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState.ATTACHED);})
            .map(topologyEntityDTO -> topologyEntityDTO.getOid())
            .collect(Collectors.toSet());

        Predicate<StatRecord> attachedVVFilter =
            testStatRecord -> vvOidsAttachedToVM.contains(testStatRecord.getAssociatedEntityId());

        List<StatSnapshotApiDTO> statSnapshotApiDTOs = new ArrayList<>();

        cloudCostStatRecords.forEach(cloudCostStatRecord -> {
            List<CloudCostStatRecord.StatRecord> vvAttachedStatRecords = new ArrayList<>();
            List<CloudCostStatRecord.StatRecord> vvUnattachedStatRecords = new ArrayList<>();

            cloudCostStatRecord.getStatRecordsList().stream().forEach(statRecord -> {
                if (attachedVVFilter.test(statRecord)) {
                    vvAttachedStatRecords.add(statRecord);
                } else {
                    vvUnattachedStatRecords.add(statRecord);
                }
            });

            requestedStats.forEach(requestedStat -> {
                StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
                statSnapshotApiDTO.setDisplayName(requestedStat.getName());
                statSnapshotApiDTO.setDate(DateTimeUtil.toString(cloudCostStatRecord.getSnapshotDate()));

                UIEntityType requestRelatedEntityType = UIEntityType.fromString(requestedStat.getRelatedEntityType());
                if (UIEntityType.UNKNOWN.equals(requestRelatedEntityType)) {
                    logger.error("Unknown related entity type {} from request.", requestedStat.getRelatedEntityType());
                } else {
                    final Optional<Integer> relatedEntityTypeIdOpt = Optional.of(Integer.valueOf(requestRelatedEntityType.typeNumber()));
                    CloudCostStatRecord.StatRecord attachedRecord = recordAggregator
                        .aggregate(vvAttachedStatRecords, relatedEntityTypeIdOpt, false);
                    CloudCostStatRecord.StatRecord unattachedRecord = recordAggregator
                        .aggregate(vvUnattachedStatRecords, relatedEntityTypeIdOpt, false);

                    StatApiDTO attachedStatApiDto = toStatApiDTO(StringConstants.COST_PRICE, attachedRecord, createStatFilterApiDTO(StringConstants.ATTACHMENT, StringConstants.ATTACHED));
                    StatApiDTO unattachedStatApiDto = toStatApiDTO(StringConstants.COST_PRICE, unattachedRecord, createStatFilterApiDTO(StringConstants.ATTACHMENT, StringConstants.UNATTACHED));

                    statSnapshotApiDTO.setStatistics(Lists.newArrayList(attachedStatApiDto, unattachedStatApiDto));

                    statSnapshotApiDTOs.add(statSnapshotApiDTO);
                }
            });
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
        final Set<Long> scopeIds = context.getQueryScope().getScopeOids();
        List<StatApiDTO> stats = Lists.newArrayList();
        for (StatApiInputDTO statApiInputDTO : statsFilters) {
            if (isVirtualVolumeCount(statApiInputDTO)) {
                storageStatsSubQuery.getAggregateStats(Collections.singleton(statApiInputDTO), context)
                        .forEach(statSnapshotApiDTO -> {
                    stats.addAll(statSnapshotApiDTO.getStatistics());
                });
            } else {
                List<String> entityTypes = WORKLOAD_NAME_TO_ENTITY_TYPES.get(statApiInputDTO.getName());
                if (entityTypes != null) {
                    final Optional<Set<UIEntityType>> scopeTypesOpt =
                            context.getInputScope().getScopeTypes();
                    // filter out entityTypes which don't exist in scope
                    if (scopeTypesOpt.isPresent()) {
                        final Set<String> entityTypesFromScope = scopeTypesOpt.get()
                                .stream()
                                .map(UIEntityType::apiStr)
                                .collect(Collectors.toSet());
                        entityTypes = entityTypes.stream()
                                .filter(entityTypesFromScope::contains)
                                .collect(Collectors.toList());
                    }
                }
                if (entityTypes != null && !entityTypes.isEmpty()) {
                    final float numWorkloads = supplyChainFetcherFactory.newNodeFetcher()
                            .addSeedOids(scopeIds)
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
                                                             @Nonnull final Set<Long> entityStatOids,
                                                             @Nonnull final StatsQueryContext context,
                                                             @Nonnull final boolean isGenericGroupBy) {
        if (stats.isEmpty()) {
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
            if (UIEntityType.fromString(statApiInputDTO.getRelatedEntityType()) != UIEntityType.UNKNOWN) {
                EntityTypeFilter.Builder entityTypeFilter = EntityTypeFilter.newBuilder();
                entityTypeFilter.addEntityTypeId(UIEntityType
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
            cloudCostStatsQueries.add(cloudCostStatsQuery.build());
        });
        GetCloudCostStatsRequest getCloudExpenseStatsRequest = GetCloudCostStatsRequest.newBuilder()
                .addAllCloudCostStatsQuery(cloudCostStatsQueries).build();
        return costServiceRpc.getCloudCostStats(getCloudExpenseStatsRequest).getCloudStatRecordList();
    }

    private void addCloudCostStatsQueryFilter(@Nonnull final Builder cloudCostStatsQuery,
                                              @Nonnull final Set<Long> entityStatOids,
                                              @Nonnull final StatsQueryContext context) {
        //TODO consider create default Cloud group (probably in Group component). Currently the only group
        if (!entityStatOids.isEmpty()) {
            if (context.getInputScope().getScopeTypes().isPresent() && shouldQueryUsingFilter(context.getInputScope())) {
                UIEntityType scopeType =
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
        if (requestGroupBySet.contains(BUSINESS_UNIT)) {
            builder.setGroupBy(GroupByType.BUSINESS_UNIT);
        } else if (requestGroupBySet.contains(CSP)) {
            builder.setGroupBy(GroupByType.CSP);
        } else if (requestGroupBySet.contains(CLOUD_SERVICE)) {
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
        Set<Long> entitiesInScope = new HashSet<>();

        // If the scoped entity/entities are business accounts - use the as scope
        if (StatsQueryExecutor.scopeHasBusinessAccounts(context.getInputScope())) {
            // a scope can be a specific business account / billing family / group of accounts,
            // or global scope (cloud page)
            if (context.getInputScope().isGroup()) {
                // The input scope is a group, i.e a billing family or a temporary group of accounts
                if (context.getInputScope().getCachedGroupInfo().isPresent()) {
                    entitiesInScope.addAll(context.getInputScope().getCachedGroupInfo().get().getEntityIds());
                }
            } else {
                // The input scope is a business account
                entitiesInScope.add(context.getInputScope().oid());
            }
        }

        final AccountExpenseQueryScope.Builder scopeBuilder = AccountExpenseQueryScope.newBuilder();
        if (!entitiesInScope.isEmpty()) {
            scopeBuilder.setSpecificAccounts(IdList.newBuilder()
                    .addAllAccountIds(entitiesInScope));
        } else {
            scopeBuilder.setAllAccounts(true);
        }
        return scopeBuilder;
    }

    // Get discovered entities.
    private Map<Long, MinimalEntity> getDiscoveredEntityDTOs(UIEntityType entityType) {
        // find all cloud services
        return repositoryApi.newSearchRequest(SearchProtoUtil.makeSearchParameters(
            SearchProtoUtil.entityTypeFilter(entityType)).build())
                .getMinimalEntities()
            .collect(Collectors.toMap(MinimalEntity::getOid, Function.identity()));
    }

    private static boolean isTopDownRequest(final Set<String> requestGroupBySet) {
        return requestGroupBySet.contains(BUSINESS_UNIT)
            || requestGroupBySet.contains(CSP)
            || requestGroupBySet.contains(CLOUD_SERVICE);
    }

    /**
     * Return a type function which returns the filter type according to the "groupBy" part of the query.
     *
     * @param requestGroupBySet the "groupBy" value (businessUnit/CSP/CloudService)
     * @return a type function which returns the relevant filter type
     */
    private Supplier<String> getTypeFunction(@Nonnull final Set<String> requestGroupBySet) {
        if (requestGroupBySet.contains(BUSINESS_UNIT)) {
            return () -> BUSINESS_UNIT;
        }
        if (requestGroupBySet.contains(CSP)) {
            return () -> CSP;
        }
        if (requestGroupBySet.contains(CLOUD_SERVICE)) {
            return () -> CLOUD_SERVICE;
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
        if (requestGroupBySet.contains(CSP)) {
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
        if (requestGroupBySet.contains(CLOUD_SERVICE) || requestGroupBySet.contains(BUSINESS_UNIT)) {
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
     * Convert Cloud related stat snap shot to StatSnapshotApiDTO
     * @param statSnapshot stat snap shot
     * @return StatSnapshotApiDTO
     */
    public StatSnapshotApiDTO toCloudStatSnapshotApiDTO(final CloudCostStatRecord statSnapshot) {
        final StatSnapshotApiDTO dto = new StatSnapshotApiDTO();
        if (statSnapshot.hasSnapshotDate()) {
            dto.setDate(DateTimeUtil.toString(statSnapshot.getSnapshotDate()));
        }
        dto.setStatistics(statSnapshot.getStatRecordsList().stream()
            .map(statRecord -> {
                final StatApiDTO statApiDTO = toStatApiDTO(statRecord.getName(), statRecord);

                if (statRecord.hasCategory()) {
                    // Build filters
                    final List<StatFilterApiDTO> filters = new ArrayList<>();
                    final StatFilterApiDTO resultsTypeFilter = new StatFilterApiDTO();
                    resultsTypeFilter.setType(COST_COMPONENT);
                    switch (statRecord.getCategory()) {
                        case ON_DEMAND_COMPUTE:
                            resultsTypeFilter.setValue(CostCategory.ON_DEMAND_COMPUTE.name());
                            break;
                        case IP:
                            resultsTypeFilter.setValue(CostCategory.IP.name());
                            break;
                        case ON_DEMAND_LICENSE:
                            resultsTypeFilter.setValue(CostCategory.ON_DEMAND_LICENSE.name());
                            break;
                        case STORAGE:
                            resultsTypeFilter.setValue(CostCategory.STORAGE.name());
                            break;
                        case RI_COMPUTE:
                            resultsTypeFilter.setValue(CostCategory.RI_COMPUTE.name());
                            break;
                        case RESERVED_LICENSE:
                            resultsTypeFilter.setValue(CostCategory.RESERVED_LICENSE.name());
                            break;
                    }
                    filters.add(resultsTypeFilter);

                    if (filters.size() > 0) {
                        statApiDTO.setFilters(filters);
                    }
                }
                // set related entity type
                if (statRecord.hasAssociatedEntityType()) {
                    statApiDTO.setRelatedEntityType(UIEntityType.fromType(
                        statRecord.getAssociatedEntityType()).apiStr());
                }
                return statApiDTO;
            })
            .collect(toList()));
        return dto;
    }

    private boolean isVirtualVolumeCount(final StatApiInputDTO statApiInputDTO) {
        boolean isSupportedStatCountQuery = NUM_VOL.equals(statApiInputDTO.getName());

        // Only support attachment OR storage tier or no groupBy in request.
        final List<String> listOfGroupBy = statApiInputDTO.getGroupBy();
        boolean isGroupBy = (listOfGroupBy != null && listOfGroupBy.size() == 1 &&
                (listOfGroupBy.contains(StringConstants.ATTACHMENT) ||
                        listOfGroupBy.contains(UIEntityType.STORAGE_TIER.apiStr()))) ||
                listOfGroupBy == null || listOfGroupBy.isEmpty();
        return isSupportedStatCountQuery && isGroupBy;
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

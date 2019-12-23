package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static com.vmturbo.common.protobuf.GroupProtoUtil.WORKLOAD_ENTITY_TYPES;
import static com.vmturbo.common.protobuf.GroupProtoUtil.WORKLOAD_ENTITY_TYPES_API_STR;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.StatsUtils;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
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
import com.vmturbo.api.utils.CompositeEntityTypesSpec;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.EntityTypeFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudExpenseStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudExpenseStatsRequest.GroupByType;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
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

    // Internally generated stat name when stats period are not set.
    private static final String CURRENT_COST_PRICE = "currentCostPrice";

    private static final String COSTCOMPONENT = "costComponent";

    private static final Set<String> COST_STATS_SET = ImmutableSet.of(StringConstants.COST_PRICE,
        CURRENT_COST_PRICE, StringConstants.RI_COST);

    private final RepositoryApi repositoryApi;

    private final CostServiceBlockingStub costServiceRpc;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final CloudStatRecordAggregator recordAggregator;

    private final ThinTargetCache thinTargetCache;

    public CloudCostsStatsSubQuery(@Nonnull final RepositoryApi repositoryApi,
                                   @Nonnull final CostServiceBlockingStub costServiceRpc,
                                   @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                                   @Nonnull final ThinTargetCache thinTargetCache) {
        this(repositoryApi, costServiceRpc, supplyChainFetcherFactory, thinTargetCache, new CloudStatRecordAggregator());
    }

    public CloudCostsStatsSubQuery(@Nonnull final RepositoryApi repositoryApi,
                                   @Nonnull final CostServiceBlockingStub costServiceRpc,
                                   @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                                   @Nonnull final ThinTargetCache thinTargetCache,
                                   @Nonnull final CloudStatRecordAggregator aggregator) {
        this.repositoryApi = repositoryApi;
        this.costServiceRpc = costServiceRpc;
        this.supplyChainFetcherFactory = supplyChainFetcherFactory;
        this.thinTargetCache = thinTargetCache;
        this.recordAggregator = aggregator;
    }

    public boolean applicableInContext(@Nonnull final StatsQueryContext context) {
        if (context.getInputScope().isPlan()) {
            return false;
        }

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

        return SubQuerySupportedStats.some(Sets.union(costStats, countStats));
    }

    @Nonnull
    @Override
    public List<StatSnapshotApiDTO> getAggregateStats(@Nonnull final Set<StatApiInputDTO> requestedStats,
                                                      @Nonnull final StatsQueryContext context) throws OperationFailedException {
        final Set<String> requestGroupBySet = requestedStats.stream()
            .filter(dto -> dto.getGroupBy() != null)
            .flatMap(apiInputDTO -> apiInputDTO.getGroupBy().stream())
            .collect(Collectors.toSet());

        final List<StatSnapshotApiDTO> statsResponse;
        if (isTopDownRequest(requestGroupBySet)) {
            // get the type function and value function according to the "groupBy" part of the query.
            // they will be used to populate the filters in StatApiDTO
            final Supplier<String> typeFunction = getTypeFunction(requestGroupBySet);
            final BiFunction<Long, Map<Long, MinimalEntity>, Optional<String>> valueFunction =
                    getValueFunction(requestGroupBySet);

            // for group by Cloud services, we need to find all the services and
            // stitch with expenses in Cost component
            final Map<Long, MinimalEntity> cloudServiceDTOs = requestGroupBySet.contains(CLOUD_SERVICE) ?
                getDiscoveredServiceDTO() : Collections.emptyMap();

            final List<CloudCostStatRecord> cloudStatRecords =
                getCloudExpensesRecordList(requestGroupBySet,
                    cloudServiceDTOs.keySet(),
                    context);
            statsResponse = cloudStatRecords.stream()
                .map(snapshot -> toStatSnapshotApiDTO(snapshot,
                    typeFunction,
                    valueFunction,
                    cloudServiceDTOs))
                .collect(Collectors.toList());
        } else if (context.getInputScope().isRealtimeMarket() || context.getInputScope().isPlan() ||
                context.getInputScope().isCloud()) {
            final Set<Long> cloudEntityOids;
            if (context.getInputScope().isPlan()) {
                cloudEntityOids = supplyChainFetcherFactory.expandScope(context.getQueryScope().getEntities(),
                    new ArrayList<>(WORKLOAD_ENTITY_TYPES_API_STR));
            } else {
                // Do we need to get the related workload entities here too?
                cloudEntityOids = context.getQueryScope().getEntities();
            }
            List<CloudCostStatRecord> cloudCostStatRecords = getCloudStatRecordList(requestedStats,
                cloudEntityOids, requestGroupBySet, context);

            final boolean isGroupByAttachment = isGroupByAttachment(requestedStats);
            final boolean isGroupByStorageTier = isGroupByStorageTier(requestedStats);

            // Only aggregate the data when it is
            // 1. Not grouped by cost component
            // 2. Not grouped by virtual volume status
            // 3. Not grouped by virtual volume's storage tier
            if (!isGroupByComponentRequest(requestGroupBySet) &&
                !isGroupByAttachment &&
                !isGroupByStorageTier) {
                // have to aggregate as needed.
                cloudCostStatRecords = recordAggregator.aggregate(cloudCostStatRecords,
                    requestedStats, context);
            }


            // For Virtual Volume, when it is grouped by either attachment or storage tier,
            //   aggregation need to be handled explicitly
            if (isGroupByAttachment || isGroupByStorageTier) {
                List<StatSnapshotApiDTO> statSnapshots = new ArrayList<>();
                // add List<StatSnapshotApiDTO> if group by attachment
                if (isGroupByAttachment) {
                    // Get all the Virtual Volume Oid in the CloudCostStatRecords
                    Set<StatApiInputDTO> groupByAttachmentStatApiInputDtos = requestedStats.stream().filter(rs -> UIEntityType.VIRTUAL_VOLUME.apiStr().equals(rs.getRelatedEntityType()) && rs.getGroupBy().contains(StringConstants.ATTACHMENT))
                        .collect(Collectors.toSet());
                    List<StatSnapshotApiDTO> groupByAttachmentStat = getGroupByVVAttachmentStat(cloudCostStatRecords, groupByAttachmentStatApiInputDtos);
                    if (groupByAttachmentStat != null && groupByAttachmentStat.size() > 0) {
                        statSnapshots.addAll(groupByAttachmentStat);
                    }
                }

                // add List<StatSnapshotApiDTO> if group by storage tier
                if (isGroupByStorageTier) {
                    Set<StatApiInputDTO> groupByStorageTierStatApiInputDtos = requestedStats.stream().filter(rs -> UIEntityType.VIRTUAL_VOLUME.apiStr().equals(rs.getRelatedEntityType()) && rs.getGroupBy().contains(UIEntityType.STORAGE_TIER.apiStr()))
                        .collect(Collectors.toSet());
                    List<StatSnapshotApiDTO> groupByStorageTierStat = getGroupByStorageTierStat(cloudCostStatRecords, groupByStorageTierStatApiInputDtos);
                    if (groupByStorageTierStat != null && groupByStorageTierStat.size() > 0) {
                        statSnapshots.addAll(groupByStorageTierStat);
                    }
                }

                // merge any same date record together in results from group by attachment and group by storage tier
                statsResponse = mergeStatSnapshotApiDTOWithDate(statSnapshots);
            } else {
                List<StatSnapshotApiDTO> statSnapshots = cloudCostStatRecords.stream()
                    .map(this::toCloudStatSnapshotApiDTO)
                    .collect(Collectors.toList());

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
                }
                statsResponse = statSnapshots;
            }
        } else {
            statsResponse = Collections.emptyList();
        }
        return statsResponse;
    }

    /**
     * Merge any {@link StatSnapshotApiDTO} objects which have the same date to one {@link StatSnapshotApiDTO} object.
     *
     * @param original {@link List} list of {@link StatSnapshotApiDTO}
     * @return {@link List}
     */
    @Nonnull
    private static List<StatSnapshotApiDTO> mergeStatSnapshotApiDTOWithDate(@Nonnull final List<StatSnapshotApiDTO> original) {
        Map<String, StatSnapshotApiDTO> dateToSnapshot = new HashMap<>();
        for (StatSnapshotApiDTO dto : original) {
            if (dateToSnapshot.containsKey(dto.getDate())) {
                dateToSnapshot.get(dto.getDate()).getStatistics().addAll(dto.getStatistics());
            } else {
                dateToSnapshot.put(dto.getDate(), dto);
            }
        }

        return dateToSnapshot.values().stream().collect(Collectors.toList());
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
    List<StatSnapshotApiDTO> getGroupByStorageTierStat(@Nonnull List<CloudCostStatRecord> cloudCostStatRecords,
                                                       @Nonnull Set<StatApiInputDTO> requestedStats) {
        Set<Long> vvOids = cloudCostStatRecords.stream().flatMapToLong(cloudCostStatRecord ->
            cloudCostStatRecord.getStatRecordsList().stream()
                .filter(sr -> sr.getAssociatedEntityType() == EntityType.VIRTUAL_VOLUME.getNumber())
                .mapToLong(sr -> sr.getAssociatedEntityId())
        ).boxed().collect(Collectors.toSet());

        // get list of attached VVs
        final Set<PartialEntity.ApiPartialEntity> vvEntities = repositoryApi
            .newSearchRequest(
                SearchProtoUtil
                    .makeSearchParameters(SearchProtoUtil.idFilter(vvOids))
                    .build()
            )
            .getEntities()
            .collect(Collectors.toSet());

        Map<Long, List<Long>> storageTierOidToConnectedVVOids = new HashMap<>();
        vvEntities.forEach(vvEntity -> {
            Optional<RelatedEntity> storageTierOpt = vvEntity.getConnectedToList().stream()
                .filter(relatedEntity -> relatedEntity.getEntityType() == EntityType.STORAGE_TIER.getNumber())
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
                    .build()
            )
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
                .filter(sr -> sr.getAssociatedEntityType() == EntityType.VIRTUAL_VOLUME.getNumber())
                .mapToLong(sr -> sr.getAssociatedEntityId())
        ).boxed().collect(Collectors.toSet());

        // Get list of attached VVs
        // Note that there may be extra vvOid returned as one VM may have multiple VVs,
        //  but the VV list will still be the one associated with the cloudCostStatRecords which
        //  the scope originally retrieved.
        final Set<Long> vvOidsAttachedToVM = repositoryApi
            .newSearchRequest(
                SearchProtoUtil
                    .makeSearchParameters(SearchProtoUtil.idFilter(vvOids))
                    .addSearchFilter(SearchFilter.newBuilder()
                        .setTraversalFilter(
                            SearchProtoUtil.traverseToType(TraversalDirection.CONNECTED_FROM,
                                UIEntityType.VIRTUAL_MACHINE.apiStr()))
                    )
                    .addSearchFilter(SearchFilter.newBuilder()
                        .setTraversalFilter(
                            SearchProtoUtil.traverseToType(TraversalDirection.CONNECTED_TO,
                                UIEntityType.VIRTUAL_VOLUME.apiStr())))
                    .build()
            )
            .getOids();

        Predicate<CloudCostStatRecord.StatRecord> attachedVVFilter =
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

            requestedStats.stream().forEach(requestedStat -> {
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
        final Set<Long> scopeIds = context.getQueryScope().getEntities();
        List<StatApiDTO> stats = Lists.newArrayList();
        for (StatApiInputDTO statApiInputDTO : statsFilters) {
            List<String> entityTypes = WORKLOAD_NAME_TO_ENTITY_TYPES.get(statApiInputDTO.getName());
            if (entityTypes != null) {
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
        return stats;
    }

    private List<CloudCostStatRecord> getCloudStatRecordList(@Nonnull final Set<StatApiInputDTO> stats,
                                                             @Nonnull final Set<Long> entityStatOids,
                                                             @Nonnull final Set<String> requestGroupBySet,
                                                             @Nonnull final StatsQueryContext context) {
        final GetCloudCostStatsRequest.Builder builder = GetCloudCostStatsRequest.newBuilder();
        // set entity types filter
        final Set<Integer> relatedEntityTypes = getRelatedEntityTypes(stats);
        if (!relatedEntityTypes.isEmpty()) {
            builder.setEntityTypeFilter(EntityTypeFilter.newBuilder()
                .addAllEntityTypeId(relatedEntityTypes)
                .build());
        }

        if (!StatsUtils.shouldIncludeBuyRiDiscount(context.getInputScope())) {
            builder.setCostSourceFilter(GetCloudCostStatsRequest.CostSourceFilter.newBuilder()
                .addCostSources(Cost.CostSource.BUY_RI_DISCOUNT)
                .setExclusionFilter(true)
                .build());
        }

        //TODO consider create default Cloud group (probably in Group component). Currently the only group
        if (!entityStatOids.isEmpty()) {
            builder.getEntityFilterBuilder().addAllEntityId(entityStatOids);
        }
        if (isGroupByComponentRequest(requestGroupBySet)) {
            builder.setGroupBy(GetCloudCostStatsRequest.GroupByType.COSTCOMPONENT);
        }

        context.getTimeWindow().ifPresent(timeWindow -> {
            builder.setStartDate(timeWindow.startTime());
            builder.setEndDate(timeWindow.endTime());
        });

        if (context.requestProjected()) {
            builder.setRequestProjected(true);
        }

        return costServiceRpc.getCloudCostStats(builder.build()).getCloudStatRecordList();
    }

    private static boolean isGroupByComponentRequest(Set<String> requestGroupBySet) {
        return requestGroupBySet.contains(COSTCOMPONENT);
    }

    private static boolean isGroupByAttachment(Set<StatApiInputDTO> requestedStats) {
        return requestedStats.stream().anyMatch(requestedStat -> requestedStat.getGroupBy() != null && requestedStat.getGroupBy().contains(StringConstants.ATTACHMENT) &&
            UIEntityType.VIRTUAL_VOLUME.apiStr().equals(requestedStat.getRelatedEntityType()));
    }

    private static boolean isGroupByStorageTier(Set<StatApiInputDTO> requestedStats) {
        return requestedStats.stream().anyMatch(requestedStat -> requestedStat.getGroupBy() != null && requestedStat.getGroupBy().contains(UIEntityType.STORAGE_TIER.apiStr()) &&
            UIEntityType.VIRTUAL_VOLUME.apiStr().equals(requestedStat.getRelatedEntityType()));
    }

    private static Set<Integer> getRelatedEntityTypes(@Nonnull Set<StatApiInputDTO> statApiInputDTOs) {
        if (CollectionUtils.isEmpty(statApiInputDTOs)) {
            return Collections.emptySet();
        }

        final Set<Integer> relatedEntityTypes = new HashSet<>();
        statApiInputDTOs.stream()
            .filter(statApiInputDTO -> statApiInputDTO.getRelatedEntityType() != null)
            .forEach(statApiInputDTO -> {
                String entityType = statApiInputDTO.getRelatedEntityType();
                if (CompositeEntityTypesSpec.WORKLOAD_ENTITYTYPE.equals(entityType)) {
                    relatedEntityTypes.addAll(WORKLOAD_ENTITY_TYPES.stream()
                        .map(UIEntityType::typeNumber)
                        .collect(Collectors.toSet())
                    );
                } else {
                    relatedEntityTypes.add(UIEntityType.fromString(entityType).typeNumber());
                }
            });
        return relatedEntityTypes;
    }

    @Nonnull
    @VisibleForTesting
    List<CloudCostStatRecord> getCloudExpensesRecordList(@Nonnull final Set<String> requestGroupBySet,
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

    // Search discovered Cloud services.
    @VisibleForTesting
    Map<Long, MinimalEntity> getDiscoveredServiceDTO() {
        // find all cloud services
        return repositoryApi.newSearchRequest(SearchProtoUtil.makeSearchParameters(
            SearchProtoUtil.entityTypeFilter(UIEntityType.CLOUD_SERVICE)).build())
                .getMinimalEntities()
            .collect(Collectors.toMap(MinimalEntity::getOid, Function.identity()));
    }

    private static boolean isTopDownRequest(final Set<String> requestGroupBySet) {
        return requestGroupBySet.contains(TARGET)
            || requestGroupBySet.contains(CSP)
            || requestGroupBySet.contains(CLOUD_SERVICE);
    }

    /**
     * Return a type function which returns the filter type according to the "groupBy" part of the query.
     *
     * @param requestGroupBySet the "groupBy" value (Target/CSP/CloudService)
     * @return a type function which returns the relevant filter type
     */
    private Supplier<String> getTypeFunction(@Nonnull final Set<String> requestGroupBySet) {
        if (requestGroupBySet.contains(TARGET)) return () -> TARGET;
        if (requestGroupBySet.contains(CSP)) return () -> CSP;
        if (requestGroupBySet.contains(CLOUD_SERVICE)) return () -> CLOUD_SERVICE;
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

        // When grouping by target, the filter's value should be the target's display name.
        // in this case the associated entity is a target, so just get its display name from the
        // thin targets cache.
        if (requestGroupBySet.contains(TARGET))
            return (associatedEntityId, cloudServiceDTOs) ->
                    thinTargetCache.getTargetInfo(associatedEntityId)
                            .flatMap(thinTargetInfo -> Optional.of(thinTargetInfo.displayName()));

        // When grouping by cloud provider, the filter's value should be the cloud provider name.
        // TODO: implement
        if (requestGroupBySet.contains(CSP))
            return (associatedEntityId, cloudServiceDTOs) -> Optional.empty();

        // When grouping by cloud service, the filter's value should be the cloud service name.
        // Since the associated entity is a cloud service, just get the entity's display name from
        // the cloud service entities that we got from the repository before.
        if (requestGroupBySet.contains(CLOUD_SERVICE))
            return (associatedEntityId, cloudServiceDTOs) ->
                    Optional.ofNullable(cloudServiceDTOs.get(associatedEntityId))
                            .flatMap(entityDTO -> Optional.of(entityDTO.getDisplayName()));

        throw ApiUtils.notImplementedInXL();
    }

    @Nonnull
    public StatSnapshotApiDTO toStatSnapshotApiDTO(@Nonnull final CloudCostStatRecord statSnapshot,
           @Nonnull final Supplier<String> typeFunction,
           @Nonnull final BiFunction<Long, Map<Long, MinimalEntity>, Optional<String>> valueFunction,
           @Nonnull final Map<Long, MinimalEntity> cloudServiceDTOs) {
        final StatSnapshotApiDTO dto = new StatSnapshotApiDTO();
        // TODO: Store Epoch information in the CloudCostStatRecord, and map it here
        if (statSnapshot.hasSnapshotDate()) {
            dto.setDate(DateTimeUtil.toString(statSnapshot.getSnapshotDate()));
        }
        dto.setStatistics(statSnapshot.getStatRecordsList().stream()
                .map(statApiDTO -> toStatApiDto(statApiDTO,
                        typeFunction,
                        valueFunction,
                        cloudServiceDTOs))
                .collect(Collectors.toList()));
        return dto;
    }

    private StatApiDTO toStatApiDto(@Nonnull final CloudCostStatRecord.StatRecord statRecord,
           @Nonnull final Supplier<String> typeFunction,
           @Nonnull final BiFunction<Long, Map<Long, MinimalEntity>, Optional<String>> valueFunction,
           @Nonnull final Map<Long, MinimalEntity> cloudServiceDTOs) {
        final StatApiDTO statApiDTO = toStatApiDTO(statRecord.getName(), statRecord);
        final long associatedEntityId = statRecord.getAssociatedEntityId();

        final BaseApiDTO provider = new BaseApiDTO();
        provider.setUuid(String.valueOf(associatedEntityId));
        statApiDTO.setRelatedEntity(provider);

        // Build filters
        final List<StatFilterApiDTO> filters = new ArrayList<>();
        Optional<String> filterValue = valueFunction.apply(associatedEntityId, cloudServiceDTOs);
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
                    resultsTypeFilter.setType(COSTCOMPONENT);
                    switch(statRecord.getCategory()) {
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
            .collect(Collectors.toList()));
        return dto;
    }

    static class CloudStatRecordAggregator {

        // aggregate to one StatRecord per related entity type per CloudCostStatRecord
        List<CloudCostStatRecord> aggregate(@Nonnull final List<CloudCostStatRecord> cloudStatRecords,
                                                    @Nonnull final Set<StatApiInputDTO> requestedStats,
                                                    @Nonnull final StatsQueryContext context) {
            boolean hasRiCostRequest = requestedStats.stream()
                .anyMatch(stat -> StringConstants.RI_COST.equalsIgnoreCase(stat.getName()));
            return cloudStatRecords.stream().map(cloudCostStatRecord -> {
                final CloudCostStatRecord.Builder builder = CloudCostStatRecord.newBuilder();
                builder.setSnapshotDate(cloudCostStatRecord.getSnapshotDate());

                final Map<CostCategory, Map<Integer, List<CloudCostStatRecord.StatRecord>>> statRecordsMap = Maps.newHashMap();
                cloudCostStatRecord.getStatRecordsList().forEach(statRecord ->
                    statRecordsMap.computeIfAbsent(statRecord.getCategory(), k -> new HashMap<>())
                        .computeIfAbsent(statRecord.getAssociatedEntityType(), a -> Lists.newArrayList())
                        .add(statRecord));

                // add ri stat record
                final List<CloudCostStatRecord.StatRecord> riRecords = statRecordsMap.getOrDefault(CostCategory.RI_COMPUTE,
                    Collections.emptyMap()).get(EntityType.VIRTUAL_MACHINE_VALUE);
                if (hasRiCostRequest && riRecords != null) {
                    builder.addStatRecords(aggregate(riRecords, Optional.empty(), true));
                }

                final Set<StatApiInputDTO> statToProcess;
                if (!requestedStats.isEmpty()) {
                    statToProcess = requestedStats;
                } else {
                    // If we the requested stats is empty we still return the cost
                    statToProcess =
                        Collections.singleton(new StatApiInputDTO(StringConstants.COST_PRICE, null,
                            null, null));
                }

                statToProcess.stream()
                    .filter(statApiInputDTO -> StringConstants.COST_PRICE.equals(statApiInputDTO.getName()))
                    .forEach(statApiInputDTO -> {
                        final String relatedEntityType = statApiInputDTO.getRelatedEntityType();
                        final Set<String> filters = CollectionUtils.emptyIfNull(statApiInputDTO.getFilters()).stream()
                            .map(StatFilterApiDTO::getValue).collect(Collectors.toSet());

                        if (relatedEntityType == null) {
                            final List<CloudCostStatRecord.StatRecord> recordsToAggregate;
                            // If the scope is business, we don't want to
                            // count the attached volume cost twice since we considered  it
                            // both in account vms and volumes cost
                            if (context.getInputScope().getScopeTypes().isPresent()
                                && context.getInputScope().getScopeTypes().get()
                                    .equals(Collections.singleton(UIEntityType.BUSINESS_ACCOUNT))) {
                                recordsToAggregate = cloudCostStatRecord.getStatRecordsList().stream()
                                    .filter(this::isStorageCostAssociatedtoVm)
                                    .collect(Collectors.toList());
                            } else {
                                recordsToAggregate = cloudCostStatRecord.getStatRecordsList();
                            }

                            builder.addStatRecords(aggregate(recordsToAggregate,
                                Optional.empty(), false));
                            // two types of request for storage:
                            //   {"name":"costPrice","relatedEntityType":"Storage"}
                            //   {"filters":[{"type":"costComponent","value":"STORAGE"}]
                        } else if ((relatedEntityType.equals(UIEntityType.VIRTUAL_MACHINE.apiStr()) && filters.contains("STORAGE"))
                            || relatedEntityType.equals(UIEntityType.STORAGE.apiStr())) {
                            // for the category storage, only get the record of entity type VM, since
                            // the cost of entity type volume is included in the vm record
                            final List<CloudCostStatRecord.StatRecord> statRecordsList = statRecordsMap.getOrDefault(
                                CostCategory.STORAGE, Collections.emptyMap()).get(EntityType.VIRTUAL_MACHINE_VALUE);
                            if (!CollectionUtils.isEmpty(statRecordsList)) {
                                builder.addStatRecords(aggregate(statRecordsList, Optional.of(EntityType.STORAGE_VALUE), false));
                            }
                        } else if (relatedEntityType.equals(CompositeEntityTypesSpec.WORKLOAD_ENTITYTYPE)) {
                            final List<CloudCostStatRecord.StatRecord> statRecordsList;
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
                                        CostCategory.ON_DEMAND_LICENSE, Collections.emptyMap()).values().stream()
                                        .flatMap(List::stream).collect(Collectors.toList()));
                                }
                            }
                            if (!statRecordsList.isEmpty()) {
                                builder.addStatRecords(aggregate(statRecordsList, Optional.empty(), false));
                            }
                        } else {
                            int entityType = UIEntityType.fromString(relatedEntityType).typeNumber();
                            final List<CloudCostStatRecord.StatRecord> statRecordsList;
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
                                        CostCategory.ON_DEMAND_LICENSE, Collections.emptyMap()).get(entityType)));
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

        private boolean isStorageCostAssociatedtoVm(StatRecord statRecord) {
                return !statRecord.hasAssociatedEntityType()
                    || !statRecord.hasCategory()
                    || statRecord.getAssociatedEntityType() != UIEntityType.VIRTUAL_MACHINE.typeNumber()
                    || statRecord.getCategory() != CostCategory.STORAGE;
        }

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

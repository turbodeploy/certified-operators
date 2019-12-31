package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static com.vmturbo.common.protobuf.GroupProtoUtil.WORKLOAD_ENTITY_TYPES_API_STR;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors.EnumValueDescriptor;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.CloudTypeMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
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
     * Cloud target constant to match UI request, also used in test case
     */
    private static final String TARGET = "target";

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

    //only queries tagged costPrice should be used to retrieved cost.
    protected static final String COST_PRICE_QUERY_KEY = "costPrice";

    protected static final String COST_COMPONENT = "costComponent";

    //only requestedStats with Numworkload shud be used to calculate numWorkloads.
    private static final String NUM_WORKLOAD_REQUEST_KEY = "numWorkloads";

    private static final Set<String> COST_STATS_SET = ImmutableSet.of(StringConstants.COST_PRICE,
        CURRENT_COST_PRICE, StringConstants.RI_COST);

    private final RepositoryApi repositoryApi;

    private final CostServiceBlockingStub costServiceRpc;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final CloudStatRecordAggregator recordAggregator = new CloudStatRecordAggregator();

    private final ThinTargetCache thinTargetCache;

    private final BuyRiScopeHandler buyRiScopeHandler;

    private final CloudTypeMapper cloudTypeMapper;

    public CloudCostsStatsSubQuery(@Nonnull final RepositoryApi repositoryApi,
                                   @Nonnull final CostServiceBlockingStub costServiceRpc,
                                   @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                                   @Nonnull final ThinTargetCache thinTargetCache,
                                   @Nonnull final BuyRiScopeHandler buyRiScopeHandler) {
        this.repositoryApi = repositoryApi;
        this.costServiceRpc = costServiceRpc;
        this.supplyChainFetcherFactory = supplyChainFetcherFactory;
        this.thinTargetCache = thinTargetCache;
        this.buyRiScopeHandler = buyRiScopeHandler;
        this.cloudTypeMapper = new CloudTypeMapper();
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
            .collect(toSet());

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
                .collect(toList());
        } else if (context.getInputScope().isRealtimeMarket() || context.getInputScope().isPlan() ||
                context.getInputScope().isCloud()) {
            final Set<Long> cloudEntityOids;
            if (context.getInputScope().isPlan()) {
                cloudEntityOids =
                    supplyChainFetcherFactory.expandScope(context.getQueryScope().getExpandedOids(),
                    new ArrayList<>(WORKLOAD_ENTITY_TYPES_API_STR));
            } else {
                if (shouldQueryUsingFilter(context.getInputScope())) {
                    cloudEntityOids = context.getQueryScope().getScopeOids();
                } else {
                    cloudEntityOids = context.getQueryScope().getExpandedOids();
                }
            }
            /*
             * Queries with name {@link #COST_PRICE_QUERY_KEY is used for querying to cost component.
             */
            Set<StatApiInputDTO> requestedCostPriceStats = requestedStats.stream()
                    .filter(requestedStat -> requestedStat.getName().equals(COST_PRICE_QUERY_KEY)).collect(toSet());

            List<CloudCostStatRecord> cloudCostStatRecords = getCloudStatRecordList(requestedCostPriceStats,
                    cloudEntityOids, context);
            List<StatSnapshotApiDTO> statSnapshots = cloudCostStatRecords.stream()
                    .map(this::toCloudStatSnapshotApiDTO)
                    .sorted(Comparator.comparing(StatSnapshotApiDTO::getDate))
                    .collect(toList());
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

    private boolean shouldQueryUsingFilter(ApiId inputScope) {
        return inputScope.getScopeTypes().isPresent()
            && inputScope.getScopeTypes().get().size() == 1
            && ENTITY_TYPES_TO_GET_COST_BY_FILTER
            .contains(inputScope.getScopeTypes().get().iterator().next());
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
                                                             @Nonnull final StatsQueryContext context) {
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
            EntityTypeFilter.Builder entityTypeFilter = EntityTypeFilter.newBuilder();
            if (UIEntityType.fromString(statApiInputDTO.getRelatedEntityType()) != UIEntityType.UNKNOWN) {
                entityTypeFilter.addEntityTypeId(UIEntityType
                        .fromStringToSdkType(statApiInputDTO.getRelatedEntityType()));
            }
            cloudCostStatsQuery.setEntityTypeFilter(entityTypeFilter);
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
            if (statApiInputDTO.getGroupBy() != null && !statApiInputDTO.getGroupBy().isEmpty()) {
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
        if (!entityStatOids.isEmpty() && context.getInputScope().getScopeTypes().isPresent()) {
            if (shouldQueryUsingFilter(context.getInputScope())) {
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
        if (requestGroupBySet.contains(TARGET)) { return () -> TARGET; }
        if (requestGroupBySet.contains(CSP)) { return () -> CSP; }
        if (requestGroupBySet.contains(CLOUD_SERVICE)) { return () -> CLOUD_SERVICE; }
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
        if (requestGroupBySet.contains(TARGET)) {
            return (associatedEntityId, cloudServiceDTOs) ->
                    thinTargetCache.getTargetInfo(associatedEntityId)
                            .flatMap(thinTargetInfo -> Optional.of(thinTargetInfo.displayName()));
        }

        // When grouping by cloud provider, the filter's value should be the cloud provider name.
        // The associated entity is a target, therefore we need to get the CPS name by the
        // target ID.
        if (requestGroupBySet.contains(CSP)) {
            return (associatedEntityId, cloudServiceDTOs) ->
                    thinTargetCache.getTargetInfo(associatedEntityId)
                            .flatMap(thinTargetInfo ->
                                    Optional.of(cloudTypeMapper.fromTargetType(
                                            thinTargetInfo.probeInfo().type()).name()));
        }

        // When grouping by cloud service, the filter's value should be the cloud service name.
        // Since the associated entity is a cloud service, just get the entity's display name from
        // the cloud service entities that we got from the repository before.
        if (requestGroupBySet.contains(CLOUD_SERVICE)) {
            return (associatedEntityId, cloudServiceDTOs) ->
                    Optional.ofNullable(cloudServiceDTOs.get(associatedEntityId))
                            .flatMap(entityDTO -> Optional.of(entityDTO.getDisplayName()));
        }

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
                .collect(toList()));
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
                    resultsTypeFilter.setType(COST_COMPONENT);
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
            .collect(toList()));
        return dto;
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

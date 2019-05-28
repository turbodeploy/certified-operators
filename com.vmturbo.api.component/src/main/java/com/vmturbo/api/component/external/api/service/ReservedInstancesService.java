package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.util.ApiUtils.isGlobalScope;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.component.external.api.mapper.MarketMapper;
import com.vmturbo.api.component.external.api.mapper.ReservedInstanceMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.reservedinstance.ReservedInstanceApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IReservedInstancesService;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.EntityFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntityTypeFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceSpecByIdsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceSpecServiceGrpc.ReservedInstanceSpecServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class ReservedInstancesService implements IReservedInstancesService {

    private final Logger logger = LogManager.getLogger();

    private final ReservedInstanceBoughtServiceBlockingStub reservedInstanceService;

    private final ReservedInstanceSpecServiceBlockingStub reservedInstanceSpecService;

    private final ReservedInstanceUtilizationCoverageServiceBlockingStub riUtilizationCoverageService;

    private final ReservedInstanceMapper reservedInstanceMapper;

    private final RepositoryApi repositoryApi;

    private final GroupExpander groupExpander;

    private final PlanServiceBlockingStub planRpcService;

    private final CostServiceBlockingStub costServiceRpc;

    private final SupplyChainFetcherFactory supplyChainFetcher;

    private long realtimeTopologyContextId;

    public ReservedInstancesService(
        @Nonnull final ReservedInstanceBoughtServiceBlockingStub reservedInstanceService,
        @Nonnull final ReservedInstanceSpecServiceBlockingStub reservedInstanceSpecService,
        @Nonnull final ReservedInstanceUtilizationCoverageServiceBlockingStub riUtilizationCoverageService,
        @Nonnull final ReservedInstanceMapper reservedInstanceMapper,
        @Nonnull final RepositoryApi repositoryApi,
        @Nonnull final GroupExpander groupExpander,
        @Nonnull final PlanServiceBlockingStub planRpcService,
        @Nonnull final CostServiceBlockingStub costServiceRpc,
        @Nonnull final SupplyChainFetcherFactory supplyChainFetcher,
        final long realtimeTopologyContextId) {
        this.reservedInstanceService = Objects.requireNonNull(reservedInstanceService);
        this.reservedInstanceSpecService = Objects.requireNonNull(reservedInstanceSpecService);
        this.riUtilizationCoverageService = Objects.requireNonNull(riUtilizationCoverageService);
        this.reservedInstanceMapper = Objects.requireNonNull(reservedInstanceMapper);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.groupExpander = Objects.requireNonNull(groupExpander);
        this.planRpcService = Objects.requireNonNull(planRpcService);
        this.costServiceRpc = Objects.requireNonNull(costServiceRpc);
        this.supplyChainFetcher = Objects.requireNonNull(supplyChainFetcher);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    @Override
    public List<ReservedInstanceApiDTO> getReservedInstances(@Nullable String scope) throws Exception {
        final List<ReservedInstanceBought> reservedInstancesBought = getReservedInstancesBought(scope);
        final Set<Long> reservedInstanceSpecIds = reservedInstancesBought.stream()
                .map(ReservedInstanceBought::getReservedInstanceBoughtInfo)
                .map(ReservedInstanceBoughtInfo::getReservedInstanceSpec)
                .collect(Collectors.toSet());

        final List<ReservedInstanceSpec> reservedInstanceSpecs =
                reservedInstanceSpecService.getReservedInstanceSpecByIds(
                        GetReservedInstanceSpecByIdsRequest.newBuilder()
                                .addAllReservedInstanceSpecIds(reservedInstanceSpecIds)
                                .build())
                .getReservedInstanceSpecList();
        final Map<Long, ReservedInstanceSpec> reservedInstanceSpecMap = reservedInstanceSpecs.stream()
                .collect(Collectors.toMap(ReservedInstanceSpec::getId, Function.identity()));
        final Set<Long> relatedEntityIds = getRelatedEntityIds(reservedInstancesBought, reservedInstanceSpecMap);
        // Get full service entity information for RI related entity(such as account, region,
        // availability zones, tier...).
        final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap = repositoryApi.getServiceEntitiesById(
                ServiceEntitiesRequest.newBuilder(relatedEntityIds).build()).entrySet().stream()
                .filter(entry -> entry.getValue().isPresent())
                .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().get()));
        final List<ReservedInstanceApiDTO> results = new ArrayList<>();
        for (ReservedInstanceBought reservedInstanceBought : reservedInstancesBought) {
            results.add(reservedInstanceMapper.mapToReservedInstanceApiDTO(reservedInstanceBought,
                    reservedInstanceSpecMap.get(reservedInstanceBought.getReservedInstanceBoughtInfo()
                            .getReservedInstanceSpec()), serviceEntityApiDTOMap));
        }
        return results;
    }

    @Override
    public ReservedInstanceApiDTO getReservedInstanceByUuid(@Nonnull String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public EntityStatsPaginationResponse getReservedInstancesStats(
                           @Nonnull StatScopesApiInputDTO inputDto,
                           final EntityStatsPaginationRequest paginationRequest) throws Exception {
        if (!isValidInputArgument(inputDto)) {
            throw new InvalidOperationException("Input dto data is not supported: " + inputDto);
        }
        //TODO: support multiple scopes.
        final String scope = inputDto.getScopes().get(0);
        final Optional<Group> groupOptional = groupExpander.getGroup(scope);
        final Optional<PlanInstance> optPlan = fetchPlanInstance(scope);
        // it has already checked statistics can only has one element.
        final String riStatsName = inputDto.getPeriod().getStatistics().get(0).getName();
        if (riStatsName.equals(StringConstants.NUM_RI)) {
            final Map<Long, Long> reservedInstanceCountMap = getReservedInstanceCountMap(scope, groupOptional, optPlan);
            final Map<Long, ServiceEntityApiDTO> riServiceEntityApiDtoMap = repositoryApi.getServiceEntitiesById(
                    ServiceEntitiesRequest.newBuilder(reservedInstanceCountMap.keySet()).build())
                    .entrySet().stream()
                    .filter(entry -> entry.getValue().isPresent())
                    .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().get()));
            final EntityStatsApiDTO result =
                    reservedInstanceMapper.riCountMapToEntityStatsApiDTO(reservedInstanceCountMap,
                            riServiceEntityApiDtoMap);
            return paginationRequest.allResultsResponse(Lists.newArrayList(result));
        }

        final long startTime = getStartTime(inputDto.getPeriod(), optPlan);
        final long endTime = getEndTime(inputDto.getPeriod());
        if (riStatsName.equals(StringConstants.RI_COUPON_UTILIZATION)) {
            final List<ReservedInstanceStatsRecord> riStatsRecords =
                    getRiUtilizationStats(startTime, endTime, scope, groupOptional, optPlan);
            final EntityStatsApiDTO result =
                reservedInstanceMapper.convertRIStatsRecordsToEntityStatsApiDTO(riStatsRecords,
                            scope, groupOptional, optPlan, false);
            return paginationRequest.allResultsResponse(Lists.newArrayList(result));
        } else if (riStatsName.equals(StringConstants.RI_COUPON_COVERAGE)) {
            final List<ReservedInstanceStatsRecord> riStatsRecords =
                getRiCoverageStats(startTime, endTime, scope, groupOptional, optPlan);
            final EntityStatsApiDTO result =
                reservedInstanceMapper.convertRIStatsRecordsToEntityStatsApiDTO(riStatsRecords,
                    scope, groupOptional, optPlan, true);
            return paginationRequest.allResultsResponse(Lists.newArrayList(result));
        } else if (riStatsName.equals(StringConstants.RI_COST)) {
            final EntityStatsApiDTO result = getRiCostStats(startTime, endTime, scope, groupOptional, optPlan);
            return paginationRequest.allResultsResponse(Lists.newArrayList(result));
        } else {
            logger.error("Not support reserved instance stats type: " + riStatsName);
            throw new UnknownObjectException(riStatsName + " is not supported query type.");
        }
    }

    /**
     * Get a list of {@link ReservedInstanceBought} which belong to the input scope.
     *
     * @param scope The scope could be global market, a group, a region, a availability zone or a account.
     * @return a list of {@link ReservedInstanceBought}.
     * @throws UnknownObjectException if the input scope type is not supported.
     */
    private List<ReservedInstanceBought> getReservedInstancesBought(@Nullable final String scope)
            throws UnknownObjectException {
        final Optional<Group> groupOptional = groupExpander.getGroup(scope);
        if (isGlobalScope(scope, groupOptional)) {
            return reservedInstanceService.getReservedInstanceBoughtByFilter(
                    GetReservedInstanceBoughtByFilterRequest.newBuilder().build())
                    .getReservedInstanceBoughtsList();
        } else if (groupOptional.isPresent()) {
            final int groupEntityType = GroupProtoUtil.getEntityType(groupOptional.get());
            final Set<Long> expandedOidsList = groupExpander.expandUuid(scope);
            final GetReservedInstanceBoughtByFilterRequest request =
                    createGetReservedInstanceBoughtByFilterRequest(expandedOidsList, groupEntityType);
            return reservedInstanceService.getReservedInstanceBoughtByFilter(request)
                    .getReservedInstanceBoughtsList();
        } else {
            // if cloud plan, use plan scope ids for the RI request
            final Optional<PlanInstance> optPlan = fetchPlanInstance(scope);
            final Set<Long> scopeIds = optPlan.map(MarketMapper::getPlanScopeIds)
                .orElse(Sets.newHashSet(Long.valueOf(scope)));
            final ServiceEntityApiDTO scopeEntity = repositoryApi.getServiceEntityForUuid(scopeIds.iterator().next());
            final int scopeEntityType = UIEntityType.fromString(scopeEntity.getClassName()).typeNumber();
            final GetReservedInstanceBoughtByFilterRequest request =
                    createGetReservedInstanceBoughtByFilterRequest(scopeIds, scopeEntityType);
            return reservedInstanceService.getReservedInstanceBoughtByFilter(request)
                    .getReservedInstanceBoughtsList();
        }
    }

    /**
     * Get a list of entity id which needs to get full entity information from repository component,
     * such as region, availability zone, account, compute tier entities.
     *
     * @param reservedInstanceBoughts a list of {@link ReservedInstanceBought}.
     * @param reservedInstanceSpecMap a Map which key is spec id, value is {@link ReservedInstanceSpec}.
     * @return a list of entity ids.
     */
    private Set<Long> getRelatedEntityIds(@Nonnull final List<ReservedInstanceBought> reservedInstanceBoughts,
                                          @Nonnull final Map<Long, ReservedInstanceSpec> reservedInstanceSpecMap) {
        final Set<Long> relateEntityIds = new HashSet<>();
        for (ReservedInstanceBought reservedInstanceBought : reservedInstanceBoughts) {
            final ReservedInstanceBoughtInfo reservedInstanceBoughtInfo =
                    reservedInstanceBought.getReservedInstanceBoughtInfo();
            final long riSpecId = reservedInstanceBoughtInfo.getReservedInstanceSpec();
            relateEntityIds.add(reservedInstanceBoughtInfo.getBusinessAccountId());
            if (reservedInstanceBoughtInfo.hasAvailabilityZoneId()) {
                relateEntityIds.add(reservedInstanceBoughtInfo.getAvailabilityZoneId());
            }
            // The reserved instance spec must be exist.
            final ReservedInstanceSpecInfo reservedInstanceSpecInfo =
                    reservedInstanceSpecMap.get(riSpecId).getReservedInstanceSpecInfo();
            relateEntityIds.add(reservedInstanceSpecInfo.getRegionId());
            relateEntityIds.add(reservedInstanceSpecInfo.getTierId());
        }
        return relateEntityIds;
    }

    /**
     * Get the reserved instance count map which belongs to the input scope.
     *
     * @param scope The scope could be global market, a group, a region, a availability zone or a account.
     * @param groupOptional The optional of {@link Group}.
     * @return a Map which key is computer tier id, value is the count of reserved instance bought.
     * @throws UnknownObjectException if scope type is not supported.
     */
    private Map<Long, Long> getReservedInstanceCountMap(@Nonnull final String scope,
                                                        @Nonnull final Optional<Group> groupOptional,
                                                        @Nonnull final Optional<PlanInstance> optPlan)
            throws UnknownObjectException {
        if (isGlobalScope(scope, groupOptional)) {
            return reservedInstanceService.getReservedInstanceBoughtCount(
                    GetReservedInstanceBoughtCountRequest.newBuilder().build())
                    .getReservedInstanceCountMapMap();
        } else if (groupOptional.isPresent()) {
            final int groupEntityType = GroupProtoUtil.getEntityType(groupOptional.get());
            final Set<Long> expandedOidsList = groupExpander.expandUuid(scope);
            final GetReservedInstanceBoughtCountRequest request =
                    createGetReservedInstanceBoughtCountRequest(expandedOidsList, groupEntityType);
            return reservedInstanceService.getReservedInstanceBoughtCount(request)
                    .getReservedInstanceCountMapMap();
        } else {
            // if cloud plan, use plan scope ids for the RI request
            final Set<Long> scopeIds = optPlan.map(MarketMapper::getPlanScopeIds)
                .orElse(Sets.newHashSet(Long.valueOf(scope)));
            final ServiceEntityApiDTO scopeEntity = repositoryApi.getServiceEntityForUuid(scopeIds.iterator().next());
            final int scopeEntityType = UIEntityType.fromString(scopeEntity.getClassName()).typeNumber();
            final GetReservedInstanceBoughtCountRequest request =
                    createGetReservedInstanceBoughtCountRequest(scopeIds, scopeEntityType);
            return reservedInstanceService.getReservedInstanceBoughtCount(request)
                .getReservedInstanceCountMapMap();
        }
    }

    /**
     * Create a {@link GetReservedInstanceBoughtCountRequest} based on input filter type and filter ids.
     *
     * @param filterIds a list of ids need to filter by.
     * @param filterType the filter entity type.
     * @return {@link GetReservedInstanceBoughtCountRequest}.
     * @throws UnknownObjectException
     */
    private GetReservedInstanceBoughtCountRequest createGetReservedInstanceBoughtCountRequest(
            @Nonnull final Set<Long> filterIds,
            final int filterType) throws UnknownObjectException {
        final GetReservedInstanceBoughtCountRequest.Builder request =
                GetReservedInstanceBoughtCountRequest.newBuilder();
        if (filterType == EntityDTO.EntityType.REGION_VALUE) {
            request.setRegionFilter(RegionFilter.newBuilder()
                    .addAllRegionId(filterIds));
        } else if (filterType == EntityDTO.EntityType.AVAILABILITY_ZONE_VALUE) {
            request.setAvailabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                    .addAllAvailabilityZoneId(filterIds));
        } else if (filterType == EntityDTO.EntityType.BUSINESS_ACCOUNT_VALUE) {
            request.setAccountFilter(AccountFilter.newBuilder()
                    .addAllAccountId(filterIds));
        } else {
            throw new UnknownObjectException("filter type: "  + filterType + " is not supported.");
        }
        return request.build();
    }

    /**
     * Create a {@link GetReservedInstanceBoughtByFilterRequest} based on input filter type and filter ids.
     *
     * @param filterIds a list of ids need to filter by.
     * @param filterType the filter entity type.
     * @return {@link GetReservedInstanceBoughtByFilterRequest}
     * @throws UnknownObjectException
     */
    private GetReservedInstanceBoughtByFilterRequest createGetReservedInstanceBoughtByFilterRequest(
            @Nonnull final Set<Long> filterIds,
            final int filterType) throws UnknownObjectException {
        final GetReservedInstanceBoughtByFilterRequest.Builder request =
                GetReservedInstanceBoughtByFilterRequest.newBuilder();
        if (filterType == EntityDTO.EntityType.REGION_VALUE) {
            request.setRegionFilter(RegionFilter.newBuilder()
                    .addAllRegionId(filterIds));
        } else if (filterType == EntityDTO.EntityType.AVAILABILITY_ZONE_VALUE) {
            request.setAvailabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                    .addAllAvailabilityZoneId(filterIds));
        } else if (filterType == EntityDTO.EntityType.BUSINESS_ACCOUNT_VALUE) {
            request.setAccountFilter(AccountFilter.newBuilder()
                    .addAllAccountId(filterIds));
        } else {
            throw new UnknownObjectException("filter type: "  + filterType + " is not supported.");
        }
        return request.build();
    }

    /**
     * Get a list of {@link ReservedInstanceStatsRecord} for reserved instance utilization stats request.
     *
     * @param startDateMillis the request start date millis.
     * @param endDateMillis the request end date millis.
     * @param scope the scope of the request.
     * @param groupOptional a optional of {@link Group}.
     * @return a list of {@link ReservedInstanceStatsRecord}.
     * @throws UnknownObjectException if the scope entity is unknown.
     */
    private List<ReservedInstanceStatsRecord> getRiUtilizationStats(
            final long startDateMillis,
            final long endDateMillis,
            @Nonnull final String scope,
            @Nonnull final Optional<Group> groupOptional,
            @Nonnull final Optional<PlanInstance> optPlan) throws UnknownObjectException {
        if (isGlobalScope(scope, groupOptional)) {
            return riUtilizationCoverageService.getReservedInstanceUtilizationStats(
                    GetReservedInstanceUtilizationStatsRequest.newBuilder()
                            .setStartDate(startDateMillis)
                            .setEndDate(endDateMillis)
                            .build())
                    .getReservedInstanceStatsRecordsList();
        } else if (groupOptional.isPresent()) {
            final int groupEntityType = GroupProtoUtil.getEntityType(groupOptional.get());
            final Set<Long> expandedOidsList = groupExpander.expandUuid(scope);
            final GetReservedInstanceUtilizationStatsRequest request =
                    createGetReservedInstanceUtilizationStatsRequest(startDateMillis, endDateMillis,
                            expandedOidsList, groupEntityType);
            return riUtilizationCoverageService.getReservedInstanceUtilizationStats(request)
                    .getReservedInstanceStatsRecordsList();
        } else {
            // if cloud plan, use plan scope ids for the RI request
            final Set<Long> scopeIds = optPlan.map(MarketMapper::getPlanScopeIds)
                .orElse(Sets.newHashSet(Long.valueOf(scope)));
            final ServiceEntityApiDTO scopeEntity = repositoryApi.getServiceEntityForUuid(scopeIds.iterator().next());
            final int scopeEntityType = UIEntityType.fromString(scopeEntity.getClassName()).typeNumber();
            final GetReservedInstanceUtilizationStatsRequest request =
                    createGetReservedInstanceUtilizationStatsRequest(startDateMillis, endDateMillis,
                        scopeIds, scopeEntityType);
            return riUtilizationCoverageService.getReservedInstanceUtilizationStats(request)
                    .getReservedInstanceStatsRecordsList();
        }
    }

    /**
     * Get a list of {@link ReservedInstanceStatsRecord} for reserved instance coverage stats request.
     *
     * @param startDateMillis the request start date millis.
     * @param endDateMillis the request end date millis.
     * @param scope the scope of the request.
     * @param groupOptional a optional of {@link Group}.
     * @param optPlan an optional of {@link PlanInstance}
     * @return a list of {@link ReservedInstanceStatsRecord}.
     * @throws UnknownObjectException if the scope entity is unknown.
     */
    public List<ReservedInstanceStatsRecord> getRiCoverageStats(
            final long startDateMillis,
            final long endDateMillis,
            final String scope,
            final Optional<Group> groupOptional,
            @Nonnull Optional<PlanInstance> optPlan) throws UnknownObjectException {
        if (isGlobalScope(scope, groupOptional)) {
            return riUtilizationCoverageService.getReservedInstanceCoverageStats(
                GetReservedInstanceCoverageStatsRequest.newBuilder()
                    .setStartDate(startDateMillis)
                    .setEndDate(endDateMillis)
                    .build())
                .getReservedInstanceStatsRecordsList();
        } else if (groupOptional.isPresent()) {
            final int groupEntityType = GroupProtoUtil.getEntityType(groupOptional.get());
            final Set<Long> expandedOidsList = groupExpander.expandUuid(scope);
            final GetReservedInstanceCoverageStatsRequest request =
                createGetReservedInstanceCoverageStatsRequest(startDateMillis, endDateMillis,
                    expandedOidsList, groupEntityType);
            return riUtilizationCoverageService.getReservedInstanceCoverageStats(request)
                .getReservedInstanceStatsRecordsList();
        } else {
            // if cloud plan, use plan scope ids for the RI request
            final Set<Long> scopeIds = optPlan.map(MarketMapper::getPlanScopeIds)
                .orElse(Sets.newHashSet(Long.valueOf(scope)));
            final ServiceEntityApiDTO scopeEntity = repositoryApi.getServiceEntityForUuid(scopeIds.iterator().next());
            final int scopeEntityType = UIEntityType.fromString(scopeEntity.getClassName()).typeNumber();
            final GetReservedInstanceCoverageStatsRequest request =
                createGetReservedInstanceCoverageStatsRequest(startDateMillis, endDateMillis,
                    scopeIds, scopeEntityType);
            return riUtilizationCoverageService.getReservedInstanceCoverageStats(request)
                .getReservedInstanceStatsRecordsList();
        }
    }

    /**
     * Get a list of {@link StatSnapshotApiDTO} for reserved instance coverage stats from cost component.
     *
     * @param scope the scope of the request.
     * @param inputDto a {@link StatPeriodApiInputDTO}.
     * @return a list of {@link StatSnapshotApiDTO}.
     * @throws UnknownObjectException if scope entity is unknown.
     */
    public List<StatSnapshotApiDTO> getRICoverageOrUtilizationStats(
            @Nonnull final String scope,
            @Nonnull final StatPeriodApiInputDTO inputDto,
            @Nonnull final Optional<PlanInstance> optPlan) throws UnknownObjectException {
        final Optional<Group> groupOptional = groupExpander.getGroup(scope);
        long startTime = getStartTime(inputDto, optPlan);
        long endTime = getEndTime(inputDto);
        // TODO: add the projected reserved instance utilization stats.
        String statName = inputDto.getStatistics().get(0).getName();
        if (StringConstants.RI_COUPON_COVERAGE.equals(statName)) {
            final List<ReservedInstanceStatsRecord> riStatsRecords = getRiCoverageStats(
                startTime, endTime, scope, groupOptional, optPlan);
            return reservedInstanceMapper.convertRIStatsRecordsToStatSnapshotApiDTO(riStatsRecords, true);
        } else if (StringConstants.RI_COUPON_UTILIZATION.equals(statName)) {
            final List<ReservedInstanceStatsRecord> riStatsRecords = getRiUtilizationStats(
                startTime, endTime, scope, groupOptional, optPlan);
            return reservedInstanceMapper.convertRIStatsRecordsToStatSnapshotApiDTO(riStatsRecords, false);
        } else {
            // not supported ri stats
            return Collections.emptyList();
        }
    }

    /**
     * Get the latest RI coverage stats for the given scope.
     *
     * @param scope the scope to fetch latest RI coverage stats for
     * @return an optional of StatApiDTO
     */
    public Optional<StatApiDTO> getLatestRICoverageStats(@Nonnull final String scope) {
        final Optional<Group> groupOptional = groupExpander.getGroup(scope);
        // use last 1 hour as start time and current time as end time
        Instant now = Instant.now();
        long startTime = now.minus(Duration.ofHours(1)).toEpochMilli();
        long endTime = now.toEpochMilli();

        try {
            final List<ReservedInstanceStatsRecord> riStatsRecords = getRiCoverageStats(
                startTime, endTime, scope, groupOptional, Optional.empty());
            if (riStatsRecords.isEmpty()) {
                return Optional.empty();
            }
            // use first one from response as latest stat
            return Optional.of(reservedInstanceMapper.createRIUtilizationStatApiDTO(
                riStatsRecords.get(0), true));
        } catch (UnknownObjectException e) {
            return Optional.empty();
        }
    }

    public EntityStatsApiDTO getRiCostStats(long startDateMillis,
                                            long endDateMillis,
                                            @Nonnull String scope,
                                            @Nonnull Optional<Group> groupOptional,
                                            @Nonnull Optional<PlanInstance> optPlan)
                throws OperationFailedException, InterruptedException, UnknownObjectException {
        // get all VMs related to scope
        Set<Long> relatedVMs = getScopesForRiCostStatsRequest(scope, groupOptional, optPlan);

        // build request and fetch ri cost stats
        final GetCloudCostStatsRequest.Builder builder = GetCloudCostStatsRequest.newBuilder();
        builder.setEntityTypeFilter(EntityTypeFilter.newBuilder()
            .addEntityTypeId(EntityType.VIRTUAL_MACHINE_VALUE)
            .build());
        builder.setEntityFilter(EntityFilter.newBuilder().addAllEntityId(relatedVMs).build());
        builder.setStartDate(startDateMillis);
        builder.setEndDate(endDateMillis);
        final List<CloudCostStatRecord> riCostStatRecords = costServiceRpc.getCloudCostStats(
            builder.build()).getCloudStatRecordList();

        return reservedInstanceMapper.convertRiCostStatsRecordsToEntityStatsApiDTO(
            riCostStatRecords, scope, groupOptional, optPlan);
    }

    private Set<Long> getScopesForRiCostStatsRequest(@Nonnull String scope,
                                                     @Nonnull Optional<Group> groupOptional,
                                                     @Nonnull Optional<PlanInstance> optPlan)
                throws OperationFailedException, InterruptedException {
        if (isGlobalScope(scope, groupOptional)) {
            return Collections.emptySet();
        }
        final Set<Long> sourceScopeIds;
        if (groupOptional.isPresent()) {
            sourceScopeIds = groupExpander.expandUuid(scope);
        } else if (optPlan.isPresent()) {
            // if cloud plan, use plan scope ids for the RI request
            sourceScopeIds = MarketMapper.getPlanScopeIds(optPlan.get());
        } else {
            sourceScopeIds = Sets.newHashSet(Long.valueOf(scope));
        }

        // get all VMs ids related to source scopes, using supply chain fetcher
        SupplychainApiDTO supplychain = supplyChainFetcher.newApiDtoFetcher()
            .topologyContextId(realtimeTopologyContextId)
            .addSeedUuids(sourceScopeIds.stream().map(String::valueOf).collect(Collectors.toList()))
            .entityTypes(Lists.newArrayList(UIEntityType.VIRTUAL_MACHINE.apiStr()))
            .entityDetailType(EntityDetailType.entity)
            .fetch();
        return supplychain.getSeMap().get(UIEntityType.VIRTUAL_MACHINE.apiStr())
            .getInstances().keySet().stream()
            .map(Long::valueOf)
            .collect(Collectors.toSet());
    }

    /**
     * Create a {@link GetReservedInstanceUtilizationStatsRequest} based on input parameters.
     *
     * @param startDateMillis the request start date millis.
     * @param endDateMillis the request end date millis.
     * @param filterIds a list of filter ids.
     * @param filterType the filter type.
     * @return a {@link GetReservedInstanceUtilizationStatsRequest}.
     * @throws UnknownObjectException if the filter type is unknown.
     */
    private GetReservedInstanceUtilizationStatsRequest createGetReservedInstanceUtilizationStatsRequest(
            final long startDateMillis,
            final long endDateMillis,
            @Nonnull final Set<Long> filterIds,
            final int filterType)  throws UnknownObjectException {
        final GetReservedInstanceUtilizationStatsRequest.Builder request =
                GetReservedInstanceUtilizationStatsRequest.newBuilder()
                        .setStartDate(startDateMillis)
                        .setEndDate(endDateMillis);
        if (filterType == EntityDTO.EntityType.REGION_VALUE) {
            request.setRegionFilter(RegionFilter.newBuilder()
                    .addAllRegionId(filterIds));
        } else if (filterType == EntityDTO.EntityType.AVAILABILITY_ZONE_VALUE) {
            request.setAvailabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                    .addAllAvailabilityZoneId(filterIds));
        } else if (filterType == EntityDTO.EntityType.BUSINESS_ACCOUNT_VALUE) {
            request.setAccountFilter(AccountFilter.newBuilder()
                    .addAllAccountId(filterIds));
        } else {
            throw new UnknownObjectException("filter type: "  + filterType + " is not supported.");
        }
        return request.build();
    }

    /**
     * Create a {@link GetReservedInstanceCoverageStatsRequest} based on input parameters.
     *
     * @param startDateMillis the request start date milliseconds.
     * @param endDateMillis the request end date milliseconds.
     * @param filterIds a list of filter ids.
     * @param filterType the filter type.
     * @return a {@link GetReservedInstanceCoverageStatsRequest}.
     * @throws UnknownObjectException if the filter type is unknown.
     */
    private GetReservedInstanceCoverageStatsRequest createGetReservedInstanceCoverageStatsRequest(
        final long startDateMillis,
        final long endDateMillis,
        @Nonnull final Set<Long> filterIds,
        final int filterType) throws UnknownObjectException {
        final GetReservedInstanceCoverageStatsRequest.Builder request =
            GetReservedInstanceCoverageStatsRequest.newBuilder()
                .setStartDate(startDateMillis)
                .setEndDate(endDateMillis);
        if (filterType == EntityDTO.EntityType.REGION_VALUE) {
            request.setRegionFilter(RegionFilter.newBuilder()
                .addAllRegionId(filterIds));
        } else if (filterType == EntityDTO.EntityType.AVAILABILITY_ZONE_VALUE) {
            request.setAvailabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                .addAllAvailabilityZoneId(filterIds));
        } else if (filterType == EntityDTO.EntityType.BUSINESS_ACCOUNT_VALUE) {
            request.setAccountFilter(AccountFilter.newBuilder()
                .addAllAccountId(filterIds));
        } else if (filterType == EntityType.VIRTUAL_MACHINE_VALUE) {
            request.setEntityFilter(EntityFilter.newBuilder()
                .addAllEntityId(filterIds));
        }
        else {
            throw new UnknownObjectException("filter type: "  + filterType + " is not supported.");
        }
        return request.build();
    }

    /**
     * Check if input dto is invalid or not.
     *
     * @param inputDto {@link StatScopesApiInputDTO}.
     * @return a true if it is valid, otherwise return false;
     */
    private boolean isValidInputArgument( @Nonnull StatScopesApiInputDTO inputDto) {
        if (inputDto.getScopes() != null && inputDto.getScopes().size() > 0
                && inputDto.getPeriod() != null && inputDto.getPeriod().getStatistics() != null) {
            final List<StatApiInputDTO> statApiInputDTOS = inputDto.getPeriod().getStatistics();
            if (statApiInputDTOS.size() == 1
                    && statApiInputDTOS.get(0).getName().equals(StringConstants.NUM_RI)
                    && statApiInputDTOS.get(0).getGroupBy() != null
                    && statApiInputDTOS.get(0).getGroupBy().size() == 1
                    && statApiInputDTOS.get(0).getGroupBy().get(0).equals(StringConstants.TEMPLATE)) {
                return true;
            }
            if (statApiInputDTOS.size() == 1 &&
                (statApiInputDTOS.get(0).getName().equals(StringConstants.RI_COUPON_UTILIZATION) ||
                    statApiInputDTOS.get(0).getName().equals(StringConstants.RI_COUPON_COVERAGE) ||
                    statApiInputDTOS.get(0).getName().equals(StringConstants.RI_COST))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Fetch the PlanInstance for the given uuid. It will return empty if the uuid is not valid, or
     * plan doesn't exist.
     */
    public Optional<PlanInstance> fetchPlanInstance(@Nonnull String uuid) {
        try {
            // the uuid may be magic string from UI
            final long planId = Long.parseLong(uuid);
            // fetch plan from plan orchestrator
            final PlanDTO.OptionalPlanInstance planInstanceOptional = planRpcService.getPlan(
                PlanDTO.PlanId.newBuilder()
                    .setPlanId(planId)
                    .build());
            return planInstanceOptional.hasPlanInstance()
                ? Optional.of(planInstanceOptional.getPlanInstance())
                : Optional.empty();
        } catch (IllegalArgumentException | StatusRuntimeException e) {
            return Optional.empty();
        }
    }

    /**
     * Get the start time which will be used in the RI stats request. startDate may not be set
     * only in case of plan. If it's plan, use plan startTime as the start time.
     */
    private long getStartTime(@Nonnull final StatPeriodApiInputDTO inputDto,
                              @Nonnull final Optional<PlanInstance> optPlan) {
        if (inputDto.getStartDate() != null) {
            return DateTimeUtil.parseTime(inputDto.getStartDate());
        } else if (optPlan.isPresent()) {
            // startDate may not be set only in case of plan
            return optPlan.get().getStartTime();
        } else {
            // by default use 10 min from now
            return Instant.now().minus(Duration.ofMinutes(10)).toEpochMilli();
        }
    }

    /**
     * Get the end time which will be used in the RI stats request. endDate may not be set or
     * UI may pass "1M" as endDate, in this case, we use one day from now as end time.
     */
    private long getEndTime(@Nonnull final StatPeriodApiInputDTO inputDto) {
        final String endDate = inputDto.getEndDate();
        if ("1M".equals(endDate) || endDate == null) {//todo: check if 1M can be removed
            // use 1 day from now as end time
            return Instant.now().plus(Duration.ofDays(1)).toEpochMilli();
        }
        return DateTimeUtil.parseTime(inputDto.getEndDate());
    }
}
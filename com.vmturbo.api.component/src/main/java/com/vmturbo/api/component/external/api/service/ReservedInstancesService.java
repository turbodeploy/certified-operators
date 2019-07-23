package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.util.ApiUtils.isGlobalScope;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import com.vmturbo.api.component.external.api.mapper.MarketMapper;
import com.vmturbo.api.component.external.api.mapper.ReservedInstanceMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryExecutor;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
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

    private final StatsQueryExecutor statsQueryExecutor;

    private final UuidMapper uuidMapper;

    public ReservedInstancesService(
            @Nonnull final ReservedInstanceBoughtServiceBlockingStub reservedInstanceService,
            @Nonnull final ReservedInstanceSpecServiceBlockingStub reservedInstanceSpecService,
            @Nonnull final ReservedInstanceUtilizationCoverageServiceBlockingStub riUtilizationCoverageService,
            @Nonnull final ReservedInstanceMapper reservedInstanceMapper,
            @Nonnull final RepositoryApi repositoryApi,
            @Nonnull final GroupExpander groupExpander,
            @Nonnull final PlanServiceBlockingStub planRpcService,
            @Nonnull final StatsQueryExecutor statsQueryExecutor,
            @Nonnull final UuidMapper uuidMapper) {
        this.reservedInstanceService = Objects.requireNonNull(reservedInstanceService);
        this.reservedInstanceSpecService = Objects.requireNonNull(reservedInstanceSpecService);
        this.riUtilizationCoverageService = Objects.requireNonNull(riUtilizationCoverageService);
        this.reservedInstanceMapper = Objects.requireNonNull(reservedInstanceMapper);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.groupExpander = Objects.requireNonNull(groupExpander);
        this.planRpcService = Objects.requireNonNull(planRpcService);
        this.statsQueryExecutor = Objects.requireNonNull(statsQueryExecutor);
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
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
        final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap =
            repositoryApi.entitiesRequest(relatedEntityIds).getSEMap();
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
        final ApiId scope = uuidMapper.fromUuid(inputDto.getScopes().get(0));

        final EntityStatsApiDTO entityStatsApiDTO = new EntityStatsApiDTO();
        entityStatsApiDTO.setUuid(scope.uuid());
        entityStatsApiDTO.setDisplayName(scope.getDisplayName());
        entityStatsApiDTO.setStats(statsQueryExecutor.getAggregateStats(scope, inputDto.getPeriod()));
        return paginationRequest.allResultsResponse(Lists.newArrayList(entityStatsApiDTO));
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
            final long scopeId = scopeIds.iterator().next();
            final int scopeEntityType = repositoryApi.entityRequest(scopeId)
                .getMinimalEntity()
                .orElseThrow(() -> new UnknownObjectException("Unknown scope id: " + scopeId))
                .getEntityType();
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
}
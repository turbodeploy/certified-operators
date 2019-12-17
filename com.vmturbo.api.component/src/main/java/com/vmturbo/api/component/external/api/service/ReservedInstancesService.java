package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.util.ApiUtils.isGlobalScope;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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

import com.google.common.collect.ImmutableSet;
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
import com.vmturbo.api.component.external.api.util.stats.StatsQueryExecutor;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.reservedinstance.ReservedInstanceApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IReservedInstancesService;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceSpecByIdsRequest;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceSpecServiceGrpc.ReservedInstanceSpecServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

public class ReservedInstancesService implements IReservedInstancesService {

    private final ReservedInstanceBoughtServiceBlockingStub reservedInstanceService;

    private final ReservedInstanceSpecServiceBlockingStub reservedInstanceSpecService;

    private final ReservedInstanceUtilizationCoverageServiceBlockingStub riUtilizationCoverageService;

    private final ReservedInstanceMapper reservedInstanceMapper;

    private final RepositoryApi repositoryApi;

    private final GroupExpander groupExpander;

    private final PlanServiceBlockingStub planRpcService;

    private final StatsQueryExecutor statsQueryExecutor;

    private final UuidMapper uuidMapper;

    private static final Set<UIEntityType> SUPPORTED_RI_FILTER_TYPES =
                    ImmutableSet.of(UIEntityType.REGION, UIEntityType.AVAILABILITY_ZONE,
                                    UIEntityType.BUSINESS_ACCOUNT);

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
        final Collection<ReservedInstanceBought> reservedInstancesBought = getReservedInstancesBought(scope);
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
        validateArgument(inputDto);

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
    private Collection<ReservedInstanceBought> getReservedInstancesBought(@Nullable final String scope)
            throws UnknownObjectException {
        final Optional<Grouping> groupOptional = groupExpander.getGroup(scope);
        if (isGlobalScope(scope, groupOptional)) {
            return reservedInstanceService.getReservedInstanceBoughtByFilter(
                    GetReservedInstanceBoughtByFilterRequest.newBuilder().build())
                    .getReservedInstanceBoughtsList();
        } else if (groupOptional.isPresent()) {
            final Set<UIEntityType> groupEntityType =
                    GroupProtoUtil.getEntityTypes(groupOptional.get());
            final Set<Long> expandedOidsList = groupExpander.expandUuid(scope);
            Map<Long, ReservedInstanceBought> result = new HashMap<>();
            groupEntityType.retainAll(SUPPORTED_RI_FILTER_TYPES);

            for (UIEntityType entityType : groupEntityType) {
                final GetReservedInstanceBoughtByFilterRequest request =
                                createGetReservedInstanceBoughtByFilterRequest(
                                                expandedOidsList, entityType.typeNumber());
                reservedInstanceService.getReservedInstanceBoughtByFilter(request)
                    .getReservedInstanceBoughtsList()
                    .forEach(ri -> result.put(ri.getId(), ri));
            }

            return result.values();
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
    private Set<Long> getRelatedEntityIds(@Nonnull final Collection<ReservedInstanceBought> reservedInstanceBoughts,
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
            relateEntityIds.addAll(reservedInstanceBoughtInfo.getReservedInstanceScopeInfo()
                    .getApplicableBusinessAccountIdList());
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
        if (!SUPPORTED_RI_FILTER_TYPES.contains(filterType)) {
           throw new UnknownObjectException("filter type: "  + filterType + " is not supported.");
        }
        request.addAllScopeSeedOids(filterIds).setScopeEntityType(filterType);
        return request.build();
    }

    /**
     * Checks if input dto is invalid or not. In case the input is invalid, throws an exception.
     *
     * @param inputDto {@link StatScopesApiInputDTO}.
     * @throws InvalidOperationException if the input has no scope or {@code "period"} field
     */
    private void validateArgument(@Nonnull StatScopesApiInputDTO inputDto)
            throws InvalidOperationException {
        // scope of the query should be defined
        if (inputDto.getScopes() == null || inputDto.getScopes().isEmpty()) {
            throw new InvalidOperationException("Input query does not specify a scope");
        }

        // the query must inquire about exactly one statistic
        if (inputDto.getPeriod() == null
                || inputDto.getPeriod().getStatistics() == null
                || inputDto.getPeriod().getStatistics().isEmpty()) {
            throw new InvalidOperationException("Input query does not specify statistics");
        }

        for (StatApiInputDTO statApiInputDTO : inputDto.getPeriod().getStatistics()) {
            // there should be a valid statistic requested
            if (statApiInputDTO.getName() == null) {
                throw new InvalidOperationException("Missing requested statistic name");
            }

            // go through all statistics that can be requested and validate each case
            switch (statApiInputDTO.getName()) {
                case StringConstants.NUM_RI:
                    // this statistic should be grouped by template
                    if (statApiInputDTO.getGroupBy() == null || statApiInputDTO.getGroupBy().isEmpty()) {
                        // add default grouping by template
                        statApiInputDTO.setGroupBy(Collections.singletonList(StringConstants.TEMPLATE));
                    } else if (statApiInputDTO.getGroupBy().size() > 1
                        || !statApiInputDTO.getGroupBy().get(0).equals(StringConstants.TEMPLATE)) {
                        throw new InvalidOperationException("This query should be grouped by template");
                    }
                    break;

                // these statistics are valid
                case StringConstants.RI_COUPON_UTILIZATION:
                case StringConstants.RI_COUPON_COVERAGE:
                case StringConstants.RI_COST:
                    break;

                default:
                    // this statistic is invalid / unknown
                    throw new InvalidOperationException(
                        "Invalid statistic " + statApiInputDTO.getName() + " requested");
            }
        }
    }

    /**
     * Fetch the PlanInstance for the given uuid. It will return empty if the uuid is not valid, or
     * plan doesn't exist.
     */
    private Optional<PlanInstance> fetchPlanInstance(@Nonnull String uuid) {
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

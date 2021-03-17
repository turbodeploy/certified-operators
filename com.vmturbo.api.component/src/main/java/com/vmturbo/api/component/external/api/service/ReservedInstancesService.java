package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Collection;
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


import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ReservedInstanceMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.StatsUtils;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryExecutor;
import com.vmturbo.api.dto.BaseApiDTO;
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
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.AccountFilter.AccountFilterType;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtForScopeRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoveredEntitiesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoveredEntitiesResponse.EntitiesCoveredByReservedInstance;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceSpecByIdsRequest;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceSpecServiceGrpc.ReservedInstanceSpecServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.utils.StringConstants;

public class ReservedInstancesService implements IReservedInstancesService {

    private final ReservedInstanceBoughtServiceBlockingStub reservedInstanceService;

    private final PlanReservedInstanceServiceBlockingStub planReservedInstanceService;

    private final ReservedInstanceSpecServiceBlockingStub reservedInstanceSpecService;

    private final ReservedInstanceUtilizationCoverageServiceBlockingStub
            reservedInstanceUtilizationCoverageService;

    private final ReservedInstanceMapper reservedInstanceMapper;

    private final RepositoryApi repositoryApi;

    private final GroupExpander groupExpander;

    private final StatsQueryExecutor statsQueryExecutor;

    private final UuidMapper uuidMapper;

    public ReservedInstancesService(
            @Nonnull final ReservedInstanceBoughtServiceBlockingStub reservedInstanceService,
            @Nonnull final PlanReservedInstanceServiceBlockingStub planReservedInstanceService,
            @Nonnull final ReservedInstanceSpecServiceBlockingStub reservedInstanceSpecService,
            @Nonnull final ReservedInstanceUtilizationCoverageServiceBlockingStub reservedInstanceUtilizationCoverageService,
            @Nonnull final ReservedInstanceMapper reservedInstanceMapper,
            @Nonnull final RepositoryApi repositoryApi,
            @Nonnull final GroupExpander groupExpander,
            @Nonnull final StatsQueryExecutor statsQueryExecutor,
            @Nonnull final UuidMapper uuidMapper) {
        this.reservedInstanceService = Objects.requireNonNull(reservedInstanceService);
        this.planReservedInstanceService = Objects.requireNonNull(planReservedInstanceService);
        this.reservedInstanceSpecService = Objects.requireNonNull(reservedInstanceSpecService);
        this.reservedInstanceMapper = Objects.requireNonNull(reservedInstanceMapper);
        this.reservedInstanceUtilizationCoverageService = Objects.requireNonNull(
                reservedInstanceUtilizationCoverageService);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.groupExpander = Objects.requireNonNull(groupExpander);
        this.statsQueryExecutor = Objects.requireNonNull(statsQueryExecutor);
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
    }

    @Override
    public List<ReservedInstanceApiDTO> getReservedInstances(
            @Nullable String scopeUuid,
            @Nullable Boolean includeAllUsable,
            @Nullable com.vmturbo.api.enums.AccountFilterType filterType) throws Exception {
        if (UserScopeUtils.isUserScoped()) {
            return Collections.emptyList();
        }
        // default to the real time market as the scope
        scopeUuid = Optional.ofNullable(scopeUuid)
            .orElse(UuidMapper.UI_REAL_TIME_MARKET_STR);

        final ApiId scope = uuidMapper.fromUuid(scopeUuid);
        final AccountFilterType accountFilterType = filterType == null ? AccountFilterType.PURCHASED_BY
                : ReservedInstanceMapper.mapApiAccountFilterTypeToXl(filterType.name());
        final Collection<ReservedInstanceBought> reservedInstancesBought = getReservedInstancesBought(
                scope, Objects.isNull(includeAllUsable) ? false : includeAllUsable, accountFilterType);
        if (reservedInstancesBought.isEmpty()) {
            return Collections.emptyList();
        }
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

        final Set<Long> businessAccountIds = serviceEntityApiDTOMap.values()
                .stream()
                .filter(dto -> ApiEntityType.BUSINESS_ACCOUNT.apiStr().equals(dto.getClassName()))
                .map(dto -> Long.parseLong(dto.getUuid()))
                .collect(Collectors.toSet());

        final List<TopologyDTO.TopologyEntityDTO> relatedBusinessAccountsList = repositoryApi
                .entitiesRequest(businessAccountIds).getFullEntities().collect(Collectors.toList());

        final Map<Long, EntitiesCoveredByReservedInstance> entitiesCoveredByReservedInstancesMap =
                reservedInstanceUtilizationCoverageService.getReservedInstanceCoveredEntities(
                        GetReservedInstanceCoveredEntitiesRequest.newBuilder()
                                .addAllReservedInstanceId(reservedInstancesBought.stream()
                                        .map(ReservedInstanceBought::getId)
                                        .collect(Collectors.toSet()))
                                .build()).getEntitiesCoveredByReservedInstancesMap();

        final List<ReservedInstanceApiDTO> results = new ArrayList<>();
        for (final ReservedInstanceBought reservedInstanceBought : reservedInstancesBought) {
            final EntitiesCoveredByReservedInstance entitiesCoveredByReservedInstance =
                    entitiesCoveredByReservedInstancesMap.getOrDefault(
                            reservedInstanceBought.getId(),
                            EntitiesCoveredByReservedInstance.getDefaultInstance());
            results.add(reservedInstanceMapper.mapToReservedInstanceApiDTO(reservedInstanceBought,
                    reservedInstanceSpecMap.get(
                            reservedInstanceBought.getReservedInstanceBoughtInfo()
                                    .getReservedInstanceSpec()), serviceEntityApiDTOMap,
                    entitiesCoveredByReservedInstance.getCoveredEntityIdCount(),
                    entitiesCoveredByReservedInstance.getCoveredUndiscoveredAccountIdCount(),
                    relatedBusinessAccountsList));
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
        // Populate basic entity data in the output dto based on the scope
        StatsMapper.populateEntityDataEntityStatsApiDTO(scope, entityStatsApiDTO);
        entityStatsApiDTO.setStats(statsQueryExecutor.getAggregateStats(scope, inputDto.getPeriod()));
        return paginationRequest.allResultsResponse(Lists.newArrayList(entityStatsApiDTO));
    }

    /**
     * Get a list of {@link ReservedInstanceBought} which belong to the input scope.
     *
     * @param scope The scope could be global market, a group, a region, a availability zone or a account.
     * @param includeAllUsable Whether to include all potentially usable RIs given {@param scope}
     * @param accountFilterType relevant to account scopes, indicates if RIs to be retrieved are used
     *                          by or purchased by the passed account ids.
     * @return a list of {@link ReservedInstanceBought}.
     * @throws UnknownObjectException if the input scope type is not supported.
     */
    private Collection<ReservedInstanceBought> getReservedInstancesBought(
            @Nonnull ApiId scope, @Nonnull Boolean includeAllUsable,
            final AccountFilterType accountFilterType)
            throws UnknownObjectException {
        String scopeUuid = String.valueOf(scope.oid());
        final Optional<Grouping> groupOptional = groupExpander.getGroup(scopeUuid);

        if (StatsUtils.isValidScopeForRIBoughtQuery(scope)) {
            // Currently, the RIs within scope of a plan are not indexed within the cost component
            // by the plan ID/topology context ID. Instead, when the scope seed OIDs are sent to the
            // cost component, they are expanded. As part of this expansion, the billing families
            // linked to any accounts in scope are pulled in. This differs from realtime behavior in
            // that there is no scope expansion. This logic will be collapsed to a single
            // branch for both RT and plans
            if (scope.isPlan()) { // This is a plan and the call passes in plan id
                UuidMapper.CachedPlanInfo planInfo = scope.getCachedPlanInfo().get();
                final GetPlanReservedInstanceBoughtRequest request =
                            GetPlanReservedInstanceBoughtRequest.newBuilder()
                                    .setPlanId(planInfo.getPlanInstance().getPlanId())
                                .build();
                return planReservedInstanceService.getPlanReservedInstanceBought(request).getReservedInstanceBoughtsList();
            } else { // this is real-time or plans for which the call passes in the entity/group scope uuid rather than the plan id
                // get all RIs that are usable within the given region/zone, and billing family
                if (includeAllUsable) {
                    return reservedInstanceService.getReservedInstanceBoughtForScope(
                            GetReservedInstanceBoughtForScopeRequest.newBuilder()
                                    .addAllScopeSeedOids(scope.getScopeOids())
                                    .build()).getReservedInstanceBoughtList();
                }

                final GetReservedInstanceBoughtByFilterRequest.Builder requestBuilder =
                        GetReservedInstanceBoughtByFilterRequest.newBuilder();

                /* At present USED_AND_PURCHASED_BY will only return RIs that are used
                 by discovered accounts.
                 The additional flag at the request filter level is required for
                 non account scopes like regions, zones.
                 This should be cleaned by by a follow up story where
                 the account filter type sent in by the UI should be moved outside
                 as a generic RI filter type. */
                if (accountFilterType == AccountFilterType.USED_AND_PURCHASED_BY) {
                    requestBuilder.setExcludeUndiscoveredUnused(true);
                }

                // add any scope filters
                scope.getScopeEntitiesByType().forEach((entityType, entityOids) -> {
                    switch (entityType) {
                        case REGION:
                            requestBuilder.setRegionFilter(
                                    RegionFilter.newBuilder()
                                            .addAllRegionId(entityOids)
                                            .build());
                            break;
                        case AVAILABILITY_ZONE:
                            requestBuilder.setZoneFilter(AvailabilityZoneFilter.newBuilder()
                                .addAllAvailabilityZoneId(entityOids));
                            final Set<Long> parentRegions =
                                repositoryApi.getRegion(entityOids).getOids();
                            requestBuilder.setRegionFilter(RegionFilter.newBuilder()
                                .addAllRegionId(parentRegions));
                            break;
                        case BUSINESS_ACCOUNT:
                            requestBuilder.setAccountFilter(
                                    AccountFilter.newBuilder()
                                            .addAllAccountId(entityOids)
                                            .setAccountFilterType(accountFilterType)
                                            .build());
                            break;
                        default:
                            // This is an unsupported scope type, therefore we'll ignore it
                            break;
                    }
                });

                return reservedInstanceService
                        .getReservedInstanceBoughtByFilter(requestBuilder.build())
                        .getReservedInstanceBoughtsList();
            }
        } else { // The call for groups is only made from plans.
            if (groupOptional.isPresent()) {
                return reservedInstanceService.getReservedInstanceBoughtForScope(
                        GetReservedInstanceBoughtForScopeRequest.newBuilder()
                                .addAllScopeSeedOids(scope.getScopeOids())
                                .build()).getReservedInstanceBoughtList();
            } else if (!StringUtils.isNumeric(scopeUuid)) {
                throw new IllegalArgumentException(String.format("%s is illegal argument. "
                        + "Should be a valid numeric id.", scope.oid()));
            } else {
                throw new IllegalArgumentException(String.format("%s is illegal argument. "
                        + "Should be a valid scope id. A valid scope is an id for a "
                                + "zone/region/Account or scope =\"Market\" for Market.",
                        scope.oid()));
            }
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
            // TODO (OM-57608): Remove this assertion. 'name' is an optional field on the request!
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

    @Override
    public List<BaseApiDTO> getEntitiesCoveredByReservedInstance(
            @Nonnull final String reservedInstanceUuid) {
        final long reservedInstanceOid = Long.parseLong(reservedInstanceUuid);
        final EntitiesCoveredByReservedInstance entitiesCoveredByReservedInstances =
                reservedInstanceUtilizationCoverageService.getReservedInstanceCoveredEntities(
                        GetReservedInstanceCoveredEntitiesRequest.newBuilder()
                                .addReservedInstanceId(reservedInstanceOid)
                                .build()).getEntitiesCoveredByReservedInstancesOrDefault(
                        reservedInstanceOid,
                        EntitiesCoveredByReservedInstance.getDefaultInstance());
        if (entitiesCoveredByReservedInstances.getCoveredEntityIdList().isEmpty()) {
            return Collections.emptyList();
        } else {
            return repositoryApi.entitiesRequest(
                    new HashSet<>(entitiesCoveredByReservedInstances.getCoveredEntityIdList()))
                    .getMinimalEntities()
                    .map(ServiceEntityMapper::toBaseApiDTO)
                    .collect(Collectors.toList());
        }
    }
}

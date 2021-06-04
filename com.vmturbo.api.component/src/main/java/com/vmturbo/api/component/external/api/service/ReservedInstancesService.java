package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import com.vmturbo.api.dto.group.FilterApiDTO;
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
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.AccountReferenceFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.AccountReferenceType;
import com.vmturbo.common.protobuf.cloud.CloudCommon.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtForScopeRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoveredEntitiesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoveredEntitiesResponse.EntitiesCoveredByReservedInstance;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceSpecByIdsRequest;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceSpecServiceGrpc.ReservedInstanceSpecServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO;

public class ReservedInstancesService implements IReservedInstancesService {

    /**
     * Logger.
     */
    private static final Logger logger = LogManager.getLogger();

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

    private final GroupServiceGrpc.GroupServiceBlockingStub groupServiceBlockingStub;

    public ReservedInstancesService(
            @Nonnull final ReservedInstanceBoughtServiceBlockingStub reservedInstanceService,
            @Nonnull final PlanReservedInstanceServiceBlockingStub planReservedInstanceService,
            @Nonnull final ReservedInstanceSpecServiceBlockingStub reservedInstanceSpecService,
            @Nonnull final ReservedInstanceUtilizationCoverageServiceBlockingStub reservedInstanceUtilizationCoverageService,
            @Nonnull final ReservedInstanceMapper reservedInstanceMapper,
            @Nonnull final RepositoryApi repositoryApi,
            @Nonnull final GroupExpander groupExpander,
            @Nonnull final StatsQueryExecutor statsQueryExecutor,
            @Nonnull final UuidMapper uuidMapper,
            @Nonnull final GroupServiceGrpc.GroupServiceBlockingStub groupServiceBlockingStub) {
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
        this.groupServiceBlockingStub = Objects.requireNonNull(groupServiceBlockingStub);
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
        final AccountReferenceType accountFilterType = filterType == null
                ? AccountReferenceType.PURCHASED_BY
                : ReservedInstanceMapper.mapApiAccountFilterTypeToXl(filterType.name());
        final Collection<ReservedInstanceBought> reservedInstancesBought = getReservedInstancesBought(
                scope, Objects.isNull(includeAllUsable) ? false : includeAllUsable, accountFilterType);
        return buildReservedInstanceApiDTO(reservedInstancesBought);
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
            final AccountReferenceType accountFilterType)
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
                if (accountFilterType == AccountReferenceType.USED_AND_PURCHASED_BY) {
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
                                    AccountReferenceFilter.newBuilder()
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

    /**
     * Get a list of reserved instances in the scope based on filter type and scope oid. The scope is set
     * as part of the filterAPIDTO. expVal holds the oids of the required scope. Multiple scope oids
     * of the same type can be added and separated by the pipe character (|). The filterType is also set
     * indicating if the scope id is for Regions, Accounts, Billing Families, Cloud Service providers,
     * Groups or Resource Groups. For Groups, we support only VM Groups and Resource Groups containing VMs.
     * For all other group types, we log an error and return an empty list.
     *
     * @param filterApiDTOList list of {@link FilterApiDTO}
     * @return list of {@link ReservedInstanceApiDTO}
     * @throws Exception Generic Exception thrown.
     */
    @Override
    public List<ReservedInstanceApiDTO> getReservedInstances(@Nonnull List<FilterApiDTO> filterApiDTOList) throws Exception {

        final GetReservedInstanceBoughtByFilterRequest.Builder requestBuilder =
                        GetReservedInstanceBoughtByFilterRequest.newBuilder();
        boolean requestBuilderPopulated = false;
        for (FilterApiDTO filterApiDTO : filterApiDTOList) {
            final String filterType = filterApiDTO.getFilterType();
            final String expVal = filterApiDTO.getExpVal();

            //split the string oids by | as a delimiter.
            final String[] splitOids = expVal.split("\\|");
            final List<Long> oidList = Arrays.asList(splitOids).stream().map(a -> Long.parseLong(a.trim()))
                            .collect(Collectors.toList());
            if (oidList.isEmpty()) {
                return Collections.emptyList();
            }

            List<Long> accountOids = new ArrayList<>();
            final Optional<RIFilter> riFilterOptional = RIFilter.valueOfRIFilter(filterType);
            if (!riFilterOptional.isPresent()) {
                final StringBuilder errorMessageBuilder = new StringBuilder("Unsupported filter type. Please pick a valid filter type among the list");
                Arrays.stream(RIFilter.values()).forEach(a -> errorMessageBuilder.append(" " + a.getRiFilterValue() + ","));
                errorMessageBuilder.replace(errorMessageBuilder.length() - 1,
                                errorMessageBuilder.length(), ".");
                throw new UnsupportedOperationException(errorMessageBuilder.toString());
            }
            final RIFilter riFilter = RIFilter.valueOfRIFilter(filterType).get();
            switch(riFilter) {
                case RI_BY_REGION:
                    requestBuilder.setRegionFilter(RegionFilter.newBuilder().addAllRegionId(oidList));
                    requestBuilderPopulated = true;
                    break;

                case RI_BY_ACCOUNT_USED:
                    requestBuilder.setAccountFilter(createAccountFilter(oidList, AccountReferenceType.USED_BY));
                    requestBuilderPopulated = true;
                    break;

                case RI_BY_ACCOUNT_PURCHASED:
                    requestBuilder.setAccountFilter(createAccountFilter(oidList, AccountReferenceType.PURCHASED_BY));
                    requestBuilderPopulated = true;
                    break;

                case RI_BY_ACCOUNT_ALL:
                    requestBuilder.setAccountFilter(createAccountFilter(oidList, AccountReferenceType.USED_AND_PURCHASED_BY));
                    requestBuilderPopulated = true;
                    break;

                case RI_BY_BILLING_FAMILY_USED:
                    //Aggregate list of Account oids within a Billing Family.
                    accountOids = aggrgegateAccountOidsForBillingFamily(oidList);
                    requestBuilder.setAccountFilter(createAccountFilter(accountOids, AccountReferenceType.USED_BY));
                    requestBuilderPopulated = true;
                    break;

                case RI_BY_BILLING_FAMILY_PURCHASED:
                    //Aggregate list of Account oids within a Billing Family.
                    accountOids = aggrgegateAccountOidsForBillingFamily(oidList);
                    requestBuilder.setAccountFilter(createAccountFilter(accountOids, AccountReferenceType.PURCHASED_BY));
                    requestBuilderPopulated = true;
                    break;

                case RI_BY_BILLING_FAMILY_ALL:
                    accountOids = aggrgegateAccountOidsForBillingFamily(oidList);
                    requestBuilder.setAccountFilter(createAccountFilter(accountOids, AccountReferenceType.USED_AND_PURCHASED_BY));
                    requestBuilderPopulated = true;
                    break;

                case RI_BY_CLOUD_PROVIDER:
                    // For every CSP, get all it's underlying regions.
                    final Set<Long> regionOids = repositoryApi.expandServiceProvidersToRegions(
                                    new HashSet<>(oidList));
                    requestBuilder.setRegionFilter(RegionFilter.newBuilder().addAllRegionId(regionOids));
                    requestBuilderPopulated = true;
                    break;

                case RI_BY_GROUP:
                case RI_BY_RESOURCE_GROUP:
                    //Get virtual machines associated with the group.
                    final Set<Long> virtualMachineOids = getVmOids(oidList);
                    if (virtualMachineOids.size() == 0) {
                        logger.error("Group / Resource Group does not contain any virtual machines.");
                        continue;
                    }

                    //Get business accounts associated with the VMs.
                    List<Long> allAccountOids = getAllBusinessAccountsForVMs(virtualMachineOids);
                    if (allAccountOids.size() == 0) {
                        logger.error("Group / Resource Group does not contain any business accounts.");
                        continue;
                    }

                    //Get regions associated with the VMs.
                    final Set<Long> regionOidSet =
                                    repositoryApi.getRegion(virtualMachineOids).getEntities()
                                                    .map(a -> a.getOid())
                                                    .collect(Collectors.toSet());
                    if (regionOidSet.size() == 0) {
                        logger.error("Group / Resource Group does not contain any regions.");
                        continue;
                    }

                    requestBuilder.setAccountFilter(createAccountFilter(allAccountOids,
                            AccountReferenceType.USED_AND_PURCHASED_BY));
                    requestBuilder.setRegionFilter(RegionFilter.newBuilder().addAllRegionId(regionOidSet));
                    requestBuilderPopulated = true;
                    break;
                default:
                    final StringBuilder errorMessageBuilder = new StringBuilder("Unsupported filter type. Please pick a valid filter type among the list");
                    Arrays.stream(RIFilter.values()).forEach(a -> errorMessageBuilder.append(" " + a.getRiFilterValue() + ","));
                    errorMessageBuilder.replace(errorMessageBuilder.length() - 1,
                                    errorMessageBuilder.length(), ".");
                    throw new UnsupportedOperationException(errorMessageBuilder.toString());
            };
        }

        if (requestBuilderPopulated == false) {
            return Collections.emptyList();
        }

        final List<ReservedInstanceBought> reservedInstanceBoughtList =
                        reservedInstanceService.getReservedInstanceBoughtByFilter(
                                        requestBuilder.build())
                                        .getReservedInstanceBoughtsList();

        return buildReservedInstanceApiDTO(reservedInstanceBoughtList);
    }

    private List<Long> aggrgegateAccountOidsForBillingFamily(List<Long> oidList) {
        List<Long> aggregatedAccountOids = new ArrayList<>();
        // First check if the oids we have is that of billing family or account oids
        if (isBillingFamily(oidList)) {
            //dealing with a billing family
            //For every billing family, get all it's account OIDs and aggregate them.
            for (Long billingFamilyOid : oidList) {
                final ApiId scope = uuidMapper.fromOid(billingFamilyOid);
                aggregatedAccountOids.addAll(scope.getScopeOids());
            }
        } else {
            // dealing with an account
            aggregatedAccountOids.addAll(getAllBusinessAccounts(oidList.stream().collect(Collectors.toSet())));
        }
        return aggregatedAccountOids;
    }

    /**
     * Given a list of oids, we check the first oid to determine if this list contains a list of billing family oids or
     * business account oids. Please note that the list is expected to contain a homogenous list of oids. i.e they should
     * all be either billing family oids or business account oids. A mix of billing family oids or business account oids
     * is not handled.
     *
     * @param oidList a homogenous list of billing family oids or business account oids.
     * @return boolean indicating if oid list contains billingFamily oids or not. A false indicates it conatins business account oids.
     */
    private boolean isBillingFamily(List<Long> oidList) {
        Long oid = oidList.get(0);
        logger.info("Using the first oid in the list {} to determine if oid is a billing family or a business account", oid);
        Optional<Grouping> group = groupExpander.getGroup(Long.toString(oid));
        if (group.isPresent() && CommonDTO.GroupDTO.GroupType.BILLING_FAMILY  == group.get().getDefinition().getType()) {
            logger.info("Oid {} is a Billing Family", oid);
            return true;
        } else {
            logger.info("Oid {} is a Business Account", oid);
            return false;
        }
    }

    private Set<Long> getVmOids(List<Long> oidList) {
        final Set<Long> virtualMachineOids = new HashSet<>();
        for (Long groupOid : oidList) {
            //First check the Group Type.
            final Optional<Grouping> group = groupExpander.getGroup(Long.toString(groupOid));
            if (!group.isPresent()) {
                continue;
            }
            final Grouping grouping = group.get();
            final GroupDTO.GroupDefinition groupingDefinition = grouping.getDefinition();
            final CommonDTO.GroupDTO.GroupType groupType = groupingDefinition.getType();
            final GroupDTO.StaticMembers staticGroupMembers = groupingDefinition.getStaticGroupMembers();

            final Optional<GroupDTO.StaticMembers.StaticMembersByType> vmMembersOptional =
                            staticGroupMembers.getMembersByTypeList().stream()
                                            .filter(a -> a.getType().getEntity()
                                                            == CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                                            .findFirst();
            if ((CommonDTO.GroupDTO.GroupType.RESOURCE == groupType || CommonDTO.GroupDTO.GroupType.REGULAR == groupType)
                            && vmMembersOptional.isPresent()) {
                final GroupDTO.StaticMembers.StaticMembersByType staticMembersByType =
                                vmMembersOptional.get();
                virtualMachineOids.addAll(staticMembersByType.getMembersList());
            } else {
                logger.error("No VMs found in group {}. Currently we support this call only for Resource Groups which contain VMs and Groups of VMs", groupOid);
            }
        }
        return virtualMachineOids;
    }

    private List<Long> getAllBusinessAccountsForVMs (Set<Long> virtualMachineOids) {
        // Given a Set of vm oids, get the billing family that they belong to and return all business accounts within the BF
        // For every VM, get a list of the Business Accounts associated with them
        final Set<Long> businessAccountOids = repositoryApi.getOwningBusinessAccount(virtualMachineOids)
                .getEntities().map(a -> a.getOid()).collect(Collectors.toSet());
        return getAllBusinessAccounts(businessAccountOids);
    }

    private List<Long> getAllBusinessAccounts(Set<Long> businessAccountOids) {
        List<Long> accountOids = new ArrayList<>();

        // For every account, get the billing family that it belongs to
        final GroupDTO.GetGroupsForEntitiesResponse groupsForEntitiesResponse = groupServiceBlockingStub.getGroupsForEntities(
                        GroupDTO.GetGroupsForEntitiesRequest.newBuilder()
                                        .addAllEntityId(businessAccountOids)
                                        .addGroupType(CommonDTO.GroupDTO.GroupType.BILLING_FAMILY)
                                        .setLoadGroupObjects(true)
                                        .build());

        //Get account oids part of the billing families
        for (Grouping billingFamilyGroup: groupsForEntitiesResponse.getGroupsList()) {
            final List<GroupDTO.StaticMembers.StaticMembersByType> membersByTypeList =
                            billingFamilyGroup.getDefinition().getStaticGroupMembers()
                                            .getMembersByTypeList();
            final Optional<GroupDTO.StaticMembers.StaticMembersByType> staticMemberByTypeOptional =
                            membersByTypeList.stream()
                                            .filter(a -> a.getType().getEntity()
                                                            == CommonDTO.EntityDTO.EntityType.BUSINESS_ACCOUNT_VALUE)
                                            .findFirst();
            if (staticMemberByTypeOptional.isPresent()) {
                final List<Long> membersList =
                                staticMemberByTypeOptional.get().getMembersList();
                accountOids.addAll(membersList);
            }
        }
        return accountOids;
    }

    private List<ReservedInstanceApiDTO> buildReservedInstanceApiDTO(@Nonnull Collection<ReservedInstanceBought> reservedInstanceBoughtCollection)
                    throws Exception {
        if (reservedInstanceBoughtCollection.isEmpty()) {
            return Collections.emptyList();
        }

        final Set<Long> reservedInstanceSpecIds = reservedInstanceBoughtCollection.stream()
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
        final Set<Long> relatedEntityIds = getRelatedEntityIds(reservedInstanceBoughtCollection, reservedInstanceSpecMap);
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
                                                        .addAllReservedInstanceId(reservedInstanceBoughtCollection.stream()
                                                                        .map(ReservedInstanceBought::getId)
                                                                        .collect(Collectors.toSet()))
                                                        .build()).getEntitiesCoveredByReservedInstancesMap();

        final List<ReservedInstanceApiDTO> results = new ArrayList<>();
        for (final ReservedInstanceBought reservedInstanceBought : reservedInstanceBoughtCollection) {
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

    private AccountReferenceFilter.Builder createAccountFilter(List<Long> accountOidList, AccountReferenceType accountFilterType) {
        final AccountReferenceFilter.Builder accountFilterBuilder = AccountReferenceFilter.newBuilder();
        accountFilterBuilder.setAccountFilterType(accountFilterType).addAllAccountId(accountOidList);
        return accountFilterBuilder;
    }

    /**
     * Enums for RIFilter Types
     */
    public enum RIFilter {
        /**
         * Filter Type for all RIs in Region.
         */
        RI_BY_REGION ("RIByRegion"),

        /**
         * Filter Type for all RIs Purchased by an Account.
         */
        RI_BY_ACCOUNT_PURCHASED ("RIByAccountPurchased"),

        /**
         * Filter Type for all RIs Used by an Account.
         */
        RI_BY_ACCOUNT_USED ("RIByAccountUsed"),

        /**
         * Filter Type for all RIs Purchased and Used by an Account.
         */
        RI_BY_ACCOUNT_ALL ("RIByAccountAll"),

        /**
         * Filter Type for all RIs Purchased by a Billing Family.
         */
        RI_BY_BILLING_FAMILY_PURCHASED ("RIByBillingFamilyPurchased"),

        /**
         * Filter Type for all RIs Used by a Billing Family.
         */
        RI_BY_BILLING_FAMILY_USED ("RIByBillingFamilyUsed"),

        /**
         * Filter Type for all RIs Purchased and Used by a Billing Family.
         */
        RI_BY_BILLING_FAMILY_ALL ("RIByBillingFamilyAll"),

        /**
         * Filter Type for all RIs in a Cloud Service Provider.
         */
        RI_BY_CLOUD_PROVIDER ("RIByCloudProvider"),

        /**
         * Filter Type for all RIs in a VM Group.
         */
        RI_BY_GROUP ("RIByGroup"),

        /**
         * Filter Type for all RIs in a Resource Group.
         */
        RI_BY_RESOURCE_GROUP ("RIByResourceGroup");

        private String riFilterValue;

        RIFilter(String riFilterValue) {
            this.riFilterValue = riFilterValue;
        }

        /**
         * Get the value associated with an enum.
         *
         * @return String value indicating the value of an enum
         */
        public String getRiFilterValue() {
            return riFilterValue;
        }

        /**
         * Given a string value, return an enum with a matching value.
         *
         * @param riFilterValue String value refering to an enum.
         * @return Optional value with a matching Enum if one exists.
         */
        public static Optional<RIFilter> valueOfRIFilter(String riFilterValue) {
            return Arrays.stream(RIFilter.values())
                            .filter(a -> a.getRiFilterValue().equalsIgnoreCase(riFilterValue)).findFirst();
        }
    }
}



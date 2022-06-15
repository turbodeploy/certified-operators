package com.vmturbo.api.component.external.api.util.stats.query.impl;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext.TimeWindow;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cloud.CloudCommon.EntityFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsRecord;
import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsRecord.SavingsRecord;
import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.common.protobuf.cost.Cost.EntityTypeFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetEntitySavingsStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.ResourceGroupFilter;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.repository.api.RepositoryClient;

/**
 * Sub-query for handling requests for entity savings/investments.
 */
public class EntitySavingsSubQuery implements StatsSubQuery {
    private final CostServiceBlockingStub costServiceRpc;

    private final GroupExpander groupExpander;

    private final RepositoryClient repositoryClient;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private static final Set<String> SUPPORTED_STATS = Arrays.stream(EntitySavingsStatsType.values())
            .map(EntitySavingsStatsType::name)
            .collect(Collectors.toSet());

    private final UuidMapper uuidMapper;

    /**
     * Constructor for EntitySavingsSubQuery.
     *
     * @param costServiceRpc cost RPC service
     * @param groupExpander group expander
     * @param repositoryClient repository client
     */
    public EntitySavingsSubQuery(@Nonnull final CostServiceBlockingStub costServiceRpc,
                                 @Nonnull final GroupExpander groupExpander,
                                 @Nonnull final RepositoryClient repositoryClient,
                                 @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                                 @Nonnull final UuidMapper uuidMapper) {
        this.costServiceRpc = costServiceRpc;
        this.groupExpander = groupExpander;
        this.repositoryClient = repositoryClient;
        this.uuidMapper = uuidMapper;
        this.supplyChainFetcherFactory = supplyChainFetcherFactory;
    }

    @Override
    public boolean applicableInContext(@Nonnull final StatsQueryContext context) {
        // This sub query is designed for real-time market and only for cloud entities or groups.
        // Hybrid groups are also applicable. Only the cloud entities in the group will have savings
        // stats. There is no need to process on-prem entities or groups since they don't have
        // savings data.
        ApiId inputScope = context.getInputScope();
        return !inputScope.isPlan()
                && (inputScope.isCloud() || inputScope.isHybridGroup() || inputScope.isRealtimeMarket());
    }

    @Override
    public SubQuerySupportedStats getHandledStats(@Nonnull final StatsQueryContext context) {
        return SubQuerySupportedStats.some(context.findStats(SUPPORTED_STATS));
    }

    @Nonnull
    @Override
    public List<StatSnapshotApiDTO> getAggregateStats(@Nonnull final Set<StatApiInputDTO> stats,
                                                      @Nonnull final StatsQueryContext context)
            throws OperationFailedException, InterruptedException, ConversionException {

        GetEntitySavingsStatsRequest.Builder request = GetEntitySavingsStatsRequest.newBuilder();

        // Set time range.
        boolean includeHistorical = context.getTimeWindow().map(TimeWindow::includeHistorical).orElse(false);
        if (context.getTimeWindow().isPresent() && includeHistorical) {
            TimeWindow timeWindow = context.getTimeWindow().get();
            request.setStartDate(timeWindow.startTime());
            request.setEndDate(timeWindow.endTime());
        } else {
            // This sub-query will only process historical requests.
            // Return empty list if request is not historical or time range is not present.
            return Collections.emptyList();
        }

        // Set requested stat names.
        Set<EntitySavingsStatsType> requestedStatsTypes = stats.stream()
                .map(StatApiInputDTO::getName)
                .filter(SUPPORTED_STATS::contains)
                .map(EntitySavingsStatsType::valueOf)
                .collect(Collectors.toSet());
        if (requestedStatsTypes.isEmpty()) {
            return Collections.emptyList();
        }
        request.addAllStatsTypes(requestedStatsTypes);

        if (context.getSessionContext().isUserScoped()) {
            Set<ApiId> userScopeIds = context.getSessionContext().getUserAccessScope().getScopeGroupIds().stream()
                    .map(uuidMapper::fromOid).collect(Collectors.toSet());
            setRequestforScopedUser(userScopeIds, request, context);
        } else {
            // Set Request for Non Scoped Users
            setRequest(context.getInputScope(), request);
        }

        // Call cost component api to get the list of stats
        Iterator<EntitySavingsStatsRecord> savingsStatsRecords = costServiceRpc.getEntitySavingsStats(request.build());

        // convert response to list of StatSnapshotApiDTO
        final List<StatSnapshotApiDTO> statsResponse = new ArrayList<>();
        while (savingsStatsRecords.hasNext()) {
            EntitySavingsStatsRecord record = savingsStatsRecords.next();
            statsResponse.add(toStatSnapshotApiDTO(record));
        }

        return statsResponse;
    }

    private Set<Integer> getScopeTypes(ApiId apiId) {
        Set<Integer> scopeTypes = new HashSet<>();

        if (apiId.isGroup()) {
            if (apiId.getCachedGroupInfo().isPresent()) {
                apiId.getCachedGroupInfo().get().getEntityTypes().stream()
                        .map(ApiEntityType::typeNumber).forEach(scopeTypes::add);
            }
        } else {
            apiId.getScopeTypes().ifPresent(entityTypes -> entityTypes.stream()
                    .map(ApiEntityType::typeNumber)
                    .forEach(scopeTypes::add));
        }
        return scopeTypes;
    }

    /**
     * Convert the response from cost component api to StatSnapshotApiDTO.
     *
     * @param record stats record
     * @return stats converted to StatSnapshotApiDTO
     */
    private StatSnapshotApiDTO toStatSnapshotApiDTO(EntitySavingsStatsRecord record) {
        final StatSnapshotApiDTO dto = new StatSnapshotApiDTO();
        // set values
        dto.setDate(DateTimeUtil.toString(record.getSnapshotDate()));
        dto.setStatistics(record.getStatRecordsList().stream()
                .map(EntitySavingsSubQuery::toStatApiDTO)
                .collect(toList()));
        dto.setEpoch(Epoch.HISTORICAL);
        return dto;
    }

    private static StatApiDTO toStatApiDTO(@Nonnull final SavingsRecord savingsRecord) {
        final StatApiDTO statApiDTO = new StatApiDTO();
        statApiDTO.setName(savingsRecord.getName());
        float value = savingsRecord.getValue();
        statApiDTO.setValue(value);

        final StatValueApiDTO statValueApiDTO = new StatValueApiDTO();
        statValueApiDTO.setAvg(value);
        statValueApiDTO.setMax(value);
        statValueApiDTO.setMin(value);
        statValueApiDTO.setTotal(value);
        statApiDTO.setValues(statValueApiDTO);
        statApiDTO.setUnits("$");

        return statApiDTO;
    }

    /**
     * Set the request for non-scoped User.
     *
     * @param apiId inputscope oid
     * @param request request builder
     */
    private void setRequest(ApiId apiId, GetEntitySavingsStatsRequest.Builder request) {
        if (apiId.isResourceGroupOrGroupOfResourceGroups()) {
            // Resource groups are handled differently because they are a kind of group and members
            // are already determined. However, we want to send the resource OID(s) in the
            // request for entity savings and use the entity_cloud_scope table to look up its members.
            // Doing the scope expansion by using the entity_cloud_scope table will include entities
            // that have been deleted, and allow savings to be calculated more correctly.
            ResourceGroupFilter.Builder resourceGroupFilterBuilder = ResourceGroupFilter.newBuilder();
            long scopeOid = apiId.oid();
            Optional<GroupType> groupType = apiId.getGroupType();
            if (groupType.isPresent() && groupType.get() == GroupType.REGULAR) {
                // Scope is a group of resource groups.
                Optional<GroupAndMembers> groupAndMembers =
                        groupExpander.getGroupWithImmediateMembersOnly(Long.toString(scopeOid));
                groupAndMembers.ifPresent(g -> resourceGroupFilterBuilder.addAllResourceGroupOid(g.members()));
            } else {
                // Scope is a single resource group.
                resourceGroupFilterBuilder.addResourceGroupOid(scopeOid);
            }
            request.setResourceGroupFilter(resourceGroupFilterBuilder);
        } else if (apiId.isBillingFamilyOrGroupOfBillingFamilies()) {
            // Add all related accounts to the filter builder.
            EntityFilter.Builder entityFilterBuilder = EntityFilter.newBuilder();
            long scopeOid = apiId.oid();
            Optional<GroupAndMembers> groupAndMembers = groupExpander.getGroupWithMembersAndEntities(Long.toString(scopeOid));
            if (groupAndMembers.isPresent()) {
                entityFilterBuilder.addAllEntityId(groupAndMembers.get().entities());
            }
            EntityTypeFilter entityTypeFilter = EntityTypeFilter.newBuilder()
            .addEntityTypeId(EntityType.BUSINESS_ACCOUNT_VALUE).build();
            request.setEntityFilter(entityFilterBuilder);
            request.setEntityTypeFilter(entityTypeFilter);
        } else if (apiId.isRealtimeMarket()) {
            // If the scope is "Market" but not user scoped, use all cloud service providers and use them as the scope.
            EntityFilter.Builder entityFilterBuilder = EntityFilter.newBuilder();
            repositoryClient.getEntitiesByType(ImmutableList.of(EntityType.SERVICE_PROVIDER))
            .forEach(sp -> entityFilterBuilder.addEntityId(sp.getOid()));
            request.setEntityFilter(entityFilterBuilder);
            EntityTypeFilter entityTypeFilter = EntityTypeFilter.newBuilder().addEntityTypeId(
                    EntityType.SERVICE_PROVIDER_VALUE).build();
            request.setEntityTypeFilter(entityTypeFilter);
        } else {
            // Set entity OIDs.
            EntityFilter entityFilter = EntityFilter.newBuilder().addAllEntityId(apiId.getScopeOids()).build();

            request.setEntityFilter(entityFilter);
            // Set entity types.
            Set<Integer> scopeTypes = getScopeTypes(apiId);
            EntityTypeFilter entityTypeFilter = EntityTypeFilter.newBuilder().addAllEntityTypeId(
                    scopeTypes).build();
            request.setEntityTypeFilter(entityTypeFilter);
        }
    }

    /**
     * Set the request for scoped User.
     *
     * @param userScopeIds groups the user is scoped to
     * @param request request builder
     * @param context user context
     */
    @VisibleForTesting
    void setRequestforScopedUser(Set<ApiId> userScopeIds, GetEntitySavingsStatsRequest.Builder request,
            @Nonnull final StatsQueryContext context) throws OperationFailedException {
        //the User must be scoped to a resource group or an account to use the cloud scope table
        boolean scopedToRG = userScopeIds.stream().allMatch(apiId -> apiId.isResourceGroupOrGroupOfResourceGroups());
        boolean scopedToAccount = userScopeIds.stream().allMatch(apiId -> isBillingFamilyOrAccount(apiId));
        // if User is scoped and searching for a specific account/entity/resource group in that scope
        if (context.getInputScope().isGroup() || context.getInputScope().isEntity()) {
            if (!scopedToRG && !scopedToAccount) {
                //Everything else(like group of VMs, one RG and one Account etc ) , get the stats from the supply chain
                setScopedRequestFromSupplyChain(context, request);
            } else {
                if (context.getInputScope().isResourceGroupOrGroupOfResourceGroups() || (scopedToRG
                        && isBillingFamilyOrAccount(context.getInputScope()))) {
                    //AccessScope:Account inputScope:RG or AccessScope:RG InputScope: Account/RG
                    ResourceGroupFilter.Builder resourceGroupFilterBuilder = ResourceGroupFilter.newBuilder();
                    resourceGroupFilterBuilder.addAllResourceGroupOid(Sets.intersection(getScopedResourceGroupIds(userScopeIds),
                            getScopedResourceGroupIds(Collections.singleton(context.getInputScope()))));
                    request.setResourceGroupFilter(resourceGroupFilterBuilder);
                } else if (isBillingFamilyOrAccount(context.getInputScope())) {
                    //accessScope & inputScope : Account
                    EntityFilter.Builder entityFilterBuilder = EntityFilter.newBuilder();
                    entityFilterBuilder.addAllEntityId(Sets.intersection(getScopedAccountIds(userScopeIds),
                            getScopedAccountIds(Collections.singleton(context.getInputScope()))));
                    EntityTypeFilter entityTypeFilter = EntityTypeFilter.newBuilder().addEntityTypeId(
                            EntityType.BUSINESS_ACCOUNT_VALUE).build();
                    request.setEntityFilter(entityFilterBuilder);
                    request.setEntityTypeFilter(entityTypeFilter);
                } else {
                    //accessScope : RGs/Account inputScope : groupofVMs
                    setScopedRequestFromSupplyChain(context, request);
                }
            }
        } else {
            // Global view of a scoped user
            if (scopedToRG) {
                ResourceGroupFilter.Builder resourceGroupFilterBuilder = ResourceGroupFilter.newBuilder();
                resourceGroupFilterBuilder.addAllResourceGroupOid(getScopedResourceGroupIds(userScopeIds));
                request.setResourceGroupFilter(resourceGroupFilterBuilder);
            } else if (scopedToAccount) {
                EntityFilter.Builder entityFilterBuilder = EntityFilter.newBuilder();
                entityFilterBuilder.addAllEntityId(getScopedAccountIds(userScopeIds));
                EntityTypeFilter entityTypeFilter = EntityTypeFilter.newBuilder().addEntityTypeId(
                        EntityType.BUSINESS_ACCOUNT_VALUE).build();
                request.setEntityFilter(entityFilterBuilder);
                request.setEntityTypeFilter(entityTypeFilter);
            } else {
                //multiple groups with different entityTypes have to use the supply chain
                setScopedRequestFromSupplyChain(context, request);
            }
        }
    }

    private Set<Long> getScopedResourceGroupIds(Set<ApiId> scopeIds) {
        Set<Long> resGroups = new HashSet<>();
        for (ApiId scopeId : scopeIds) {
            if (scopeId.isResourceGroupOrGroupOfResourceGroups()) {
                Optional<GroupType> groupType = scopeId.getGroupType();
                if (groupType.isPresent() && groupType.get() == GroupType.REGULAR) {
                    // Scope is a group of resource groups.
                    Optional<GroupAndMembers> groupAndMembers =
                            groupExpander.getGroupWithImmediateMembersOnly(Long.toString(scopeId.oid()));
                    resGroups.addAll(groupAndMembers.get().members());
                } else {
                    // Scope is a single resource group.
                    resGroups.add(scopeId.oid());
                }
            } else if (isBillingFamilyOrAccount(scopeId)) {
                Optional<GroupAndMembers> groupAndMembers = groupExpander.getGroupWithMembersAndEntities(Long.toString(scopeId.oid()));
                if (groupAndMembers.isPresent()) {
                    // Scope is a Billing Family , find the associated members and get all the resource groups associated with it
                    resGroups.addAll(groupExpander.getResourceGroupsForAccounts(
                                    groupAndMembers.get().entities()).stream()
                            .map(res -> res.getId()).collect(Collectors.toSet()));
                } else {
                    // Scope is an account, get all the resource groups associated with it
                    resGroups.addAll(groupExpander.getResourceGroupsForAccounts(scopeId.getScopeOids())
                            .stream()
                            .map(res -> res.getId())
                            .collect(Collectors.toSet()));
                }
            }
        }
        return resGroups;
    }

    private Set<Long> getScopedAccountIds(Set<ApiId> scopeIds) {
        Set<Long> accountId = new HashSet<>();
        for (ApiId scopeId : scopeIds) {
            if (isBillingFamilyOrAccount(scopeId)) {
                // Get the account Ids
                Optional<GroupAndMembers> groupAndMembers = groupExpander.getGroupWithMembersAndEntities(Long.toString(scopeId.oid()));
                if (groupAndMembers.isPresent()) {
                    accountId.addAll(groupAndMembers.get().entities());
                } else {
                    accountId.addAll(scopeId.getScopeOids());
                }
            }
        }
        return accountId;
    }

    /**
     * Get the Saving stats from supplychain if a user.
     * is scoped to multiple groups of different entity types
     *
     * @param context stats record
     * @param request request builder
     */
    private void setScopedRequestFromSupplyChain(@Nonnull final StatsQueryContext context,
            GetEntitySavingsStatsRequest.Builder request) throws OperationFailedException {
        final SupplyChainNodeFetcherBuilder builder =
                supplyChainFetcherFactory.newNodeFetcher().entityTypes(
                        ApiEntityType.ENTITY_TYPES_WITH_COST.stream().collect(Collectors.toList()));
        builder.addSeedOids(context.getInputScope().getScopeOids());
        Set<Long> allEntitiesInScope = builder.fetch().values().stream()
                .flatMap(node -> node.getMembersByStateMap().values().stream())
        .flatMap(memberList -> memberList.getMemberOidsList().stream())
                .collect(Collectors.toSet());
        EntityFilter entityFilter = EntityFilter.newBuilder()
                .addAllEntityId(allEntitiesInScope).build();
        request.setEntityFilter(entityFilter);
    }

    private boolean isBillingFamilyOrAccount(ApiId scopeId) {
        return (scopeId.isBillingFamilyOrGroupOfBillingFamilies() || scopeId.getScopeTypes().get().stream()
                .allMatch(eT -> eT.typeNumber() == EntityType.BUSINESS_ACCOUNT_VALUE));
    }

}



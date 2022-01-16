package com.vmturbo.api.component.external.api.util.action;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor.ActionStatsQuery;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.ActionGroupFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.MgmtUnitSubgroupFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.MgmtUnitSubgroupFilter.ManagementUnits;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.TimeRange;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Responsible for mapping an {@link ActionApiInputDTO} to the matching queries to use in XL.
 */
class HistoricalQueryMapper {

    private static final Logger logger = LogManager.getLogger();

    private final ActionSpecMapper actionSpecMapper;
    private final BuyRiScopeHandler buyRiScopeHandler;
    private final GroupExpander groupExpander;
    private final Clock clock;

    HistoricalQueryMapper(@Nonnull final ActionSpecMapper actionSpecMapper,
                          @Nonnull final BuyRiScopeHandler buyRiScopeHandler,
                          @Nonnull GroupExpander groupExpander,
                          @Nonnull final Clock clock) {
        this.actionSpecMapper = actionSpecMapper;
        this.buyRiScopeHandler = buyRiScopeHandler;
        this.groupExpander = groupExpander;
        this.clock = clock;
    }

    @Nonnull
    Map<ApiId, HistoricalActionStatsQuery> mapToHistoricalQueries(@Nonnull final ActionStatsQuery query) {
        // A historical query must have a start time.
        Preconditions.checkArgument(query.actionInput().getStartTime() != null);

        final long startTime = DateTimeUtil.parseTime(query.actionInput().getStartTime());
        final long endTime;
        if (query.actionInput().getEndTime() != null) {
            endTime = DateTimeUtil.parseTime(query.actionInput().getEndTime());
        } else {
            // If the end time is unset, we will check for stats from the start time until now.
            endTime = clock.millis();
        }

        final TimeRange timeRange = TimeRange.newBuilder()
            .setStartTime(startTime)
            .setEndTime(endTime)
            .build();
        final Optional<GroupBy> groupByOps = extractGroupByCriteria(query);
        final Map<ApiId, MgmtUnitSubgroupFilter> filtersByScope =
            extractMgmtUnitSubgroupFilter(query, groupByOps);

        return filtersByScope.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, entry -> {
                final HistoricalActionStatsQuery.Builder grpcQueryBuilder =
                    HistoricalActionStatsQuery.newBuilder()
                        .setActionGroupFilter(extractActionGroupFilter(query, entry.getKey()))
                        .setMgmtUnitSubgroupFilter(entry.getValue())
                        .setTimeRange(timeRange);

                groupByOps.ifPresent(grpcQueryBuilder::setGroupBy);
                return grpcQueryBuilder.build();
            }));
    }

    @Nonnull
    private Optional<GroupBy> extractGroupByCriteria(@Nonnull final ActionStatsQuery query) {
        final List<String> groupByFields = query.actionInput().getGroupBy();
        if (!CollectionUtils.isEmpty(groupByFields)) {
            if (groupByFields.size() > 1) {
                // TODO (roman, Feb 26 2019): OM-43462 - support multiple group-by criteria.
                // At this time there are no UI widgets that need it.
                logger.warn("Action stats query contains multiple group-by criteria: {}." +
                    "We will only use the first.", groupByFields);
            }
            final String groupBy = groupByFields.get(0);
            switch (groupBy) {
                case StringConstants.RISK_SUB_CATEGORY:
                    return Optional.of(GroupBy.ACTION_CATEGORY);
                case StringConstants.RISK:
                    return Optional.of(GroupBy.ACTION_RELATED_RISK);
                case StringConstants.ACTION_STATES:
                    return Optional.of(GroupBy.ACTION_STATE);
                case StringConstants.BUSINESS_UNIT:
                    return Optional.of(GroupBy.BUSINESS_ACCOUNT_ID);
                case StringConstants.RESOURCE_GROUP:
                    return Optional.of(GroupBy.RESOURCE_GROUP_ID);
                default:
                    logger.error("Unhandled action stats group-by criteria: {}", groupBy);
                    return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    @Nonnull
    private ActionGroupFilter extractActionGroupFilter(
            @Nonnull final ActionStatsQuery query,
            @Nonnull final ApiId scope) {
        final ActionGroupFilter.Builder agFilterBldr = ActionGroupFilter.newBuilder();

        CollectionUtils.emptyIfNull(query.actionInput().getActionModeList()).stream()
            .map(ActionSpecMapper::mapApiModeToXl)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .forEach(agFilterBldr::addActionMode);

        CollectionUtils.emptyIfNull(query.actionInput().getActionStateList()).stream()
            .map(ActionSpecMapper::mapApiStateToXl)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .forEach(agFilterBldr::addActionState);

        CollectionUtils.emptyIfNull(query.actionInput().getRiskSubCategoryList()).stream()
            .map(ActionSpecMapper::mapApiActionCategoryToXl)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .forEach(agFilterBldr::addActionCategory);

        agFilterBldr.addAllActionType(buyRiScopeHandler.extractActionTypes(
                query.actionInput(), scope));

        return agFilterBldr.build();
    }

    @Nonnull
    Map<ApiId, MgmtUnitSubgroupFilter> extractMgmtUnitSubgroupFilter(@Nonnull final ActionStatsQuery query, Optional<GroupBy> groupByOps) {
        return query.scopes().stream()
            .distinct()
            .collect(Collectors.toMap(Function.identity(), scope -> {
                final MgmtUnitSubgroupFilter.Builder mgmtSubgroupFilterBldr =
                    MgmtUnitSubgroupFilter.newBuilder();
                if (scope.isRealtimeMarket()) {
                    mgmtSubgroupFilterBldr.setMarket(true);
                } else if (scope.isGlobalTempGroup()) {
                    // If it's a global-scope temporary group, we treat it as a case of the market.
                    mgmtSubgroupFilterBldr.setMarket(true);
                    // If the query doesn't specify explicit related entity types, use the type
                    // of the group as the entity type.
                    //
                    // If the query DOES specify explicit related entity types, ignore the group
                    // entity types. i.e. saying "get me stats for all PMs related to all VMs in
                    // the system" is pretty much the same - or close enough - to "get me stats for
                    // all PMs in the system".
                    if (query.getRelatedEntityTypes().isEmpty()) {
                        // The .get() is safe because we know it's a group (or else we wouldn't be
                        // in this block.
                        scope.getScopeTypes().get().stream()
                            .map(ApiEntityType::typeNumber)
                            .forEach(mgmtSubgroupFilterBldr::addEntityType);
                    }
                } else if (needExpansion(scope, groupByOps)) {
                    ManagementUnits mgunits = getExpandedMgmtUnits(scope, groupByOps);
                    mgmtSubgroupFilterBldr.setMgmtUnits(mgunits);
                } else {
                    mgmtSubgroupFilterBldr.setMgmtUnitId(scope.oid());
                }

                // Don't set entity types for single BA and RG scopes, as the only entityType stored in db is -1 (UNSET)
                // Groups of these scopes already pass empty related entity type lists.
                // TODO: This clause would need to be re-evaluated along with any changes made in OM-79415.
                if (!(scope.isEntity() && scope.getClassName().equals(ApiEntityType.BUSINESS_ACCOUNT.apiStr()))
                        && !(scope.isResourceGroupOrGroupOfResourceGroups() && scope.getGroupType().isPresent()
                        && scope.getGroupType().get() == GroupType.RESOURCE)) {
                    mgmtSubgroupFilterBldr.addAllEntityType(query.getRelatedEntityTypes());
                }
                query.getEnvironmentType().ifPresent(mgmtSubgroupFilterBldr::setEnvironmentType);

                return mgmtSubgroupFilterBldr.build();
            }));
    }

    private boolean needExpansion(@Nonnull final ApiId scope, Optional<GroupBy> groupByOps) {
        if (scope.isEntity()) {
            return scope.getClassName().equals(ApiEntityType.BUSINESS_ACCOUNT.apiStr())
                && groupByOps.isPresent()
                && groupByOps.get() == GroupBy.RESOURCE_GROUP_ID;
        } else if (scope.isGroup()) {
            Optional<UuidMapper.CachedGroupInfo> groupInfoOptional = scope.getCachedGroupInfo();
            if (groupInfoOptional.isPresent()) {
                UuidMapper.CachedGroupInfo groupInfo = groupInfoOptional.get();
                if (Collections.singleton(ApiEntityType.BUSINESS_ACCOUNT)
                    .equals(groupInfo.getEntityTypes())) {
                    return true;
                } else if (groupInfo.getNestedGroupTypes().size() == 1) {
                    final GroupType nestedGroupType =
                        groupInfo.getNestedGroupTypes().iterator().next();
                    return nestedGroupType == GroupType.RESOURCE ||
                        nestedGroupType == GroupType.COMPUTE_HOST_CLUSTER;
                }
            }
        }
        return false;
    }

    private ManagementUnits getExpandedMgmtUnits(ApiId scope, Optional<GroupBy> groupByOps) {
        ManagementUnits.Builder builder = ManagementUnits.newBuilder();
        if (scope.isEntity()) {
            // If we are in the scope of account but we are grouping by resource group,
            // we should replace the scope with list of resource groups for that account
            groupExpander.getResourceGroupsForAccounts(Collections.singleton(scope.oid()))
                .forEach(grp -> builder.addMgmtUnitIds(grp.getId()));
        } else {
            if (Collections.singleton(ApiEntityType.BUSINESS_ACCOUNT)
                    .equals(scope.getCachedGroupInfo().get().getEntityTypes())
                && groupByOps.isPresent()
                && groupByOps.get() == GroupBy.RESOURCE_GROUP_ID) {
                groupExpander.getResourceGroupsForAccounts(scope.getCachedGroupInfo().get().getEntityIds())
                    .forEach(grp -> builder.addMgmtUnitIds(grp.getId()));
            } else {
                groupExpander.getGroupWithImmediateMembersOnly(scope.uuid())
                    .map(GroupAndMembers::members)
                    .orElse(Collections.emptyList())
                    .forEach(builder::addMgmtUnitIds);
            }
        }
        return builder.build();
    }

}

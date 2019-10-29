package com.vmturbo.api.component.external.api.util.action;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.ActionTypeMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor.ActionStatsQuery;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.ActionGroupFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.MgmtUnitSubgroupFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.TimeRange;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * Responsible for mapping an {@link ActionApiInputDTO} to the matching queries to use in XL.
 */
class HistoricalQueryMapper {

    private static final Logger logger = LogManager.getLogger();

    private final ActionSpecMapper actionSpecMapper;

    HistoricalQueryMapper(@Nonnull final ActionSpecMapper actionSpecMapper) {
        this.actionSpecMapper = actionSpecMapper;
    }

    @Nonnull
    Map<ApiId, HistoricalActionStatsQuery> mapToHistoricalQueries(@Nonnull final ActionStatsQuery query) {
        // A historical query must have a start and end time.
        // If a start and end time is not set, we should be looking at live stats.
        Preconditions.checkArgument(query.actionInput().getStartTime() != null);
        Preconditions.checkArgument(query.actionInput().getEndTime() != null);

        final TimeRange timeRange = TimeRange.newBuilder()
            .setStartTime(DateTimeUtil.parseTime(query.actionInput().getStartTime()))
            .setEndTime(DateTimeUtil.parseTime(query.actionInput().getEndTime()))
            .build();
        final ActionGroupFilter actionGroupFilter = extractActionGroupFilter(query);
        final Optional<GroupBy> groupByOps =
            extractGroupByCriteria(query);
        final Map<ApiId, MgmtUnitSubgroupFilter> filtersByScope =
            extractMgmtUnitSubgroupFilter(query);

        return filtersByScope.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, entry -> {
                final HistoricalActionStatsQuery.Builder grpcQueryBuilder =
                    HistoricalActionStatsQuery.newBuilder()
                        .setActionGroupFilter(actionGroupFilter)
                        .setMgmtUnitSubgroupFilter(entry.getValue())
                        .setTimeRange(timeRange);

                groupByOps.ifPresent(grpcQueryBuilder::setGroupBy);
                return grpcQueryBuilder.build();
            }));
    }

    @Nonnull
    Optional<GroupBy> extractGroupByCriteria(@Nonnull final ActionStatsQuery query) {
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
                case StringConstants.ACTION_STATES:
                    return Optional.of(GroupBy.ACTION_STATE);
                default:
                    logger.error("Unhandled action stats group-by criteria: {}", groupBy);
                    return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    @Nonnull
    ActionGroupFilter extractActionGroupFilter(@Nonnull final ActionStatsQuery query) {
        final ActionGroupFilter.Builder agFilterBldr = ActionGroupFilter.newBuilder();

        CollectionUtils.emptyIfNull(query.actionInput().getActionModeList()).stream()
            .map(actionSpecMapper::mapApiModeToXl)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .forEach(agFilterBldr::addActionMode);

        CollectionUtils.emptyIfNull(query.actionInput().getActionStateList()).stream()
            .map(actionSpecMapper::mapApiStateToXl)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .forEach(agFilterBldr::addActionState);

        CollectionUtils.emptyIfNull(query.actionInput().getRiskSubCategoryList()).stream()
            .map(actionSpecMapper::mapApiActionCategoryToXl)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .forEach(agFilterBldr::addActionCategory);

        CollectionUtils.emptyIfNull(query.actionInput().getActionTypeList()).stream()
            .map(ActionTypeMapper::fromApi)
            .flatMap(Collection::stream)
            .forEach(agFilterBldr::addActionType);

        return agFilterBldr.build();
    }

    @Nonnull
    Map<ApiId, MgmtUnitSubgroupFilter> extractMgmtUnitSubgroupFilter(@Nonnull final ActionStatsQuery query) {
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
                            .map(UIEntityType::typeNumber)
                            .forEach(mgmtSubgroupFilterBldr::addEntityType);
                    }
                } else {
                    mgmtSubgroupFilterBldr.setMgmtUnitId(scope.oid());
                }

                mgmtSubgroupFilterBldr.addAllEntityType(query.getRelatedEntityTypes());
                query.getEnvironmentType().ifPresent(mgmtSubgroupFilterBldr::setEnvironmentType);

                return mgmtSubgroupFilterBldr.build();
            }));
    }
}

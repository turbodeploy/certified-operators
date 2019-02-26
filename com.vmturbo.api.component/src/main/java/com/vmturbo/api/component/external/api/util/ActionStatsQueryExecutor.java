package com.vmturbo.api.component.external.api.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.ActionTypeMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStat;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStats;
import com.vmturbo.common.protobuf.action.ActionDTO.GetHistoricalActionStatsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionCountsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionCountsQuery.ActionGroupFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionCountsQuery.GroupBy;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionCountsQuery.MgmtUnitSubgroupFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionCountsQuery.TimeRange;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * A shared utility class to execute action stats queries, meant to be used by whichever
 * service needs to do so.
 */
public class ActionStatsQueryExecutor {

    private static final Logger logger = LogManager.getLogger();

    private final ActionsServiceBlockingStub actionsServiceBlockingStub;

    private final QueryMapper queryMapper;

    private final ActionStatsMapper actionStatsMapper;

    public ActionStatsQueryExecutor(@Nonnull final ActionsServiceBlockingStub actionsServiceBlockingStub,
                                    @Nonnull final ActionSpecMapper actionSpecMapper) {
        this(actionsServiceBlockingStub, new QueryMapper(actionSpecMapper), new ActionStatsMapper(actionSpecMapper));
    }

    @VisibleForTesting
    ActionStatsQueryExecutor(@Nonnull final ActionsServiceBlockingStub actionsServiceBlockingStub,
                             @Nonnull final QueryMapper queryMapper,
                             @Nonnull final ActionStatsMapper actionStatsMapper) {
        this.actionsServiceBlockingStub = Objects.requireNonNull(actionsServiceBlockingStub);
        this.queryMapper = Objects.requireNonNull(queryMapper);
        this.actionStatsMapper = Objects.requireNonNull(actionStatsMapper);
    }

    /**
     * Retrieve the action stats targetted by an {@link ActionStatsQuery}.
     *
     * @param query The query (build from {@link ImmutableActionStatsQuery}).
     * @return The {@link ActionStats} protobuf returned by the action orchestrator.
     */
    @Nonnull
    public List<StatSnapshotApiDTO> retrieveActionStats(@Nonnull final ActionStatsQuery query) {
        final HistoricalActionCountsQuery grpcQuery = queryMapper.mapToHistoricalQuery(query);
        final ActionStats actionStats = actionsServiceBlockingStub.getHistoricalActionStats(
            GetHistoricalActionStatsRequest.newBuilder()
                .setQuery(grpcQuery)
                .build()).getActionStats();
        return actionStatsMapper.actionStatsToApiSnapshot(actionStats);
    }

    /**
     * A query for actions.
     */
    @Value.Immutable
    public interface ActionStatsQuery {
        /**
         * The scope for the query - either an OID or the entire market. Note that historical
         * action stats are only available for certain types of objects (e.g. global environment,
         * clusters). Querying for "invalid" types of objects (e.g. individual entities) will
         * return no results.
         *
         * Note (roman, Jan 15 2019): Right now the API supports querying multiple scopes, but in
         * practice we usually query for one scope. To query multiple scopes, run multiple queries.
         * In the future if querying multiple scopes becomes important we can add it.
         */
        ApiId scope();

        /**
         * The entity type to restrict the query to. Note: {@link ActionStatsQuery#actionInput()}
         * also contains a field to restrict entity types
         * ({@link ActionApiInputDTO#getRelatedEntityTypes()}) but some REST API calls accept
         * a separate entity type, so we add it here instead of forcing them to change the
         * input DTO.
         */
        Optional<Integer> entityType();

        /**
         * The {@link ActionApiInputDTO} that specifies the kinds of stats to retrieve.
         * Note that we don't support all the possible options and groupings.
         */
        ActionApiInputDTO actionInput();
    }

    static class ActionStatsMapper {

        private final ActionSpecMapper actionSpecMapper;

        ActionStatsMapper(@Nonnull final ActionSpecMapper actionSpecMapper) {
            this.actionSpecMapper = actionSpecMapper;
        }

        @Nonnull
        public List<StatSnapshotApiDTO> actionStatsToApiSnapshot(@Nonnull final ActionStats actionStats) {
            return actionStats.getStatSnapshotsList().stream()
                .map(actionStatSnapshot -> {
                    final StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
                    statSnapshotApiDTO.setDate(DateTimeUtil.toString(actionStatSnapshot.getTime()));
                    statSnapshotApiDTO.setStatistics(actionStatSnapshot.getStatsList().stream()
                        .flatMap(actionStat -> actionStatXLtoAPI(actionStat).stream())
                        .collect(Collectors.toList()));
                    return statSnapshotApiDTO;
                })
                .collect(Collectors.toList());
        }

        /**
         *
         * @param actionStat
         * @return A list of {@link StatApiDTO}s that the {@link ActionStat} maps to. The return type
         *         is a list because a single action stat can map to multiple stats - one for action
         *         count, one for entity count, one for savings/investment.
         */
        @Nonnull
        public List<StatApiDTO> actionStatXLtoAPI(@Nonnull final ActionStat actionStat) {

            // The filters that applied to get this action stat
            final List<StatFilterApiDTO> commonFilters = new ArrayList<>();
            // (roman, Jan 7 2019): In the case of multiple filters we don't currently return
            // the filters in the same order as the request. This may become a problem.
            if (actionStat.hasActionCategory()) {
                final StatFilterApiDTO categoryFilter = new StatFilterApiDTO();
                categoryFilter.setType(StringConstants.RISK_SUB_CATEGORY);
                categoryFilter.setValue(ActionSpecMapper.mapXlActionCategoryToApi(
                    actionStat.getActionCategory()));
                commonFilters.add(categoryFilter);
            }

            if (actionStat.hasActionState()) {
                final StatFilterApiDTO stateFilter = new StatFilterApiDTO();
                stateFilter.setType(StringConstants.ACTION_STATES);
                stateFilter.setValue(
                    actionSpecMapper.mapXlActionStateToApi(actionStat.getActionState()).name());
                commonFilters.add(stateFilter);
            }

            final List<StatApiDTO> retStats = new ArrayList<>();
            if (actionStat.hasActionCount()) {
                final StatApiDTO actionCountStat = new StatApiDTO();
                actionCountStat.setName(StringConstants.NUM_ACTIONS);

                actionCountStat.setFilters(commonFilters);

                final StatValueApiDTO statValue = actionStatValueXLtoAPI(actionStat.getActionCount());
                actionCountStat.setValues(statValue);
                actionCountStat.setValue(statValue.getAvg());

                retStats.add(actionCountStat);
            }

            if (actionStat.hasEntityCount()) {
                final StatApiDTO entityCountStat = new StatApiDTO();
                entityCountStat.setName(StringConstants.NUM_ENTITIES);

                entityCountStat.setFilters(commonFilters);

                final StatValueApiDTO statValue = actionStatValueXLtoAPI(actionStat.getEntityCount());
                entityCountStat.setValues(statValue);
                entityCountStat.setValue(statValue.getAvg());

                retStats.add(entityCountStat);
            }

            // TODO (roman, Jan 7 2019): Handle investments/savings.

            return retStats;
        }

        @Nonnull
        public StatValueApiDTO actionStatValueXLtoAPI(@Nonnull final ActionStat.Value actionStatValue) {
            final StatValueApiDTO apiValueDTO = new StatValueApiDTO();
            apiValueDTO.setAvg((float)actionStatValue.getAvg());
            apiValueDTO.setMax((float)actionStatValue.getMax());
            apiValueDTO.setMin((float)actionStatValue.getMin());
            apiValueDTO.setTotal((float)actionStatValue.getTotal());
            return apiValueDTO;
        }
    }

    /**
     * Responsible for mapping an {@link ActionApiInputDTO} to the matching queries to use in XL.
     */
    @VisibleForTesting
    static class QueryMapper {
        private final ActionSpecMapper actionSpecMapper;

        QueryMapper(@Nonnull final ActionSpecMapper actionSpecMapper) {
            this.actionSpecMapper = actionSpecMapper;
        }

        @Nonnull
        HistoricalActionCountsQuery mapToHistoricalQuery(@Nonnull final ActionStatsQuery query) {
            // A historical query must have a start and end time.
            // If a start and end time is not set, we should be looking at live stats.
            Preconditions.checkArgument(query.actionInput().getStartTime() != null);
            Preconditions.checkArgument(query.actionInput().getEndTime() != null);

            final HistoricalActionCountsQuery.Builder grpcQueryBuilder =
                HistoricalActionCountsQuery.newBuilder()
                    .setActionGroupFilter(extractActionGroupFilter(query))
                    .setMgmtUnitSubgroupFilter(extractMgmtUnitSubgroupFilter(query))
                    .setTimeRange(TimeRange.newBuilder()
                        .setStartTime(DateTimeUtil.parseTime(query.actionInput().getStartTime()))
                        .setEndTime(DateTimeUtil.parseTime(query.actionInput().getEndTime())));

            extractGroupByCriteria(query).ifPresent(grpcQueryBuilder::setGroupBy);

            return grpcQueryBuilder.build();
        }

        @Nonnull
        Optional<GroupBy> extractGroupByCriteria(@Nonnull final ActionStatsQuery query) {
            final List<String> groupByFields = query.actionInput().getGroupBy();
            if (!CollectionUtils.isEmpty(groupByFields)) {
                if (groupByFields.size() > 1) {
                    logger.error("Action stats query contains multiple group-by criteria: {}." +
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
        MgmtUnitSubgroupFilter extractMgmtUnitSubgroupFilter(@Nonnull final ActionStatsQuery query) {
            final MgmtUnitSubgroupFilter.Builder mgmtSubgroupFilterBldr =
                MgmtUnitSubgroupFilter.newBuilder();
            if (query.scope().isRealtimeMarket()) {
                mgmtSubgroupFilterBldr.setMarket(true);
            } else {
                mgmtSubgroupFilterBldr.setMgmtUnitId(query.scope().oid());
            }

            query.entityType().ifPresent(mgmtSubgroupFilterBldr::addEntityType);
            CollectionUtils.emptyIfNull(query.actionInput().getRelatedEntityTypes()).stream()
                .map(ServiceEntityMapper::fromUIEntityType)
                .forEach(mgmtSubgroupFilterBldr::addEntityType);

            if (query.actionInput().getEnvironmentType() == EnvironmentType.CLOUD) {
                mgmtSubgroupFilterBldr.setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD);
            } else if (query.actionInput().getEnvironmentType() == EnvironmentType.ONPREM) {
                mgmtSubgroupFilterBldr.setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM);
            }
            return mgmtSubgroupFilterBldr.build();
        }
    }
}

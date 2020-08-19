package com.vmturbo.api.component.external.api.util.action;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor.ActionStatsQuery;
import com.vmturbo.api.component.external.api.util.action.GroupByFilters.GroupByFiltersFactory;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.ActionCostType;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStat;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStats;
import com.vmturbo.common.protobuf.action.ActionDTO.CurrentActionStat;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Maps action count stats from XL format to API {@link StatSnapshotApiDTO}s.
 */
class ActionStatsMapper {

    private static final Map<ActionCostType, String> COST_TYPE_STR =
        ImmutableMap.<ActionCostType, String>builder()
            .put(ActionCostType.SAVING, StringConstants.SAVINGS)
            .put(ActionCostType.INVESTMENT, StringConstants.INVESTMENT)
            .put(ActionCostType.SUPER_SAVING, StringConstants.SUPER_SAVINGS)
            .build();

    private static final Logger logger = LogManager.getLogger();

    private final Clock clock;

    private final GroupByFiltersFactory groupByFiltersFactory;

    ActionStatsMapper(@Nonnull final Clock clock,
                      @Nonnull final ActionSpecMapper actionSpecMapper) {
        this(clock, new GroupByFiltersFactory(actionSpecMapper));
    }

    /**
     * Constructor that allows injection of a mock {@link GroupByFiltersFactory}, for testing
     * in isolation.
     */
    @VisibleForTesting
    ActionStatsMapper(@Nonnull final Clock clock,
                      @Nonnull final GroupByFiltersFactory groupByFiltersFactory) {
        this.clock = clock;
        this.groupByFiltersFactory = groupByFiltersFactory;
    }

    /**
     * Map a list of {@link CurrentActionStat}s to a {@link StatSnapshotApiDTO}.
     *
     * @param currentActionStats The current stats retrieved from the action orchestrator.
     * @param query The {@link ActionStatsQuery} the stats are for.
     * @param entityLookup A map of entity oid to {@link MinimalEntity}. In the case of group by template
     *                     requests, we need to have the name of the entity in the response.
     *                     The names are looked up using this map.
     * @param cspLookup A map of entity oid to {@link MinimalEntity} to cloud type.
     * @return The {@link StatSnapshotApiDTO} to return to the caller.
     */
    @Nonnull
    public StatSnapshotApiDTO currentActionStatsToApiSnapshot(
            @Nonnull final List<CurrentActionStat> currentActionStats,
            @Nonnull final ActionStatsQuery query,
            @Nonnull Map<Long, MinimalEntity> entityLookup,
            @Nonnull Map<Long, String> cspLookup) {
        final StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
        statSnapshotApiDTO.setDate(query.currentTimeStamp().isPresent() ? query.currentTimeStamp().get()
                        : DateTimeUtil.toString(clock.millis()));
        statSnapshotApiDTO.setStatistics(currentActionStats.stream()
            .flatMap(stat -> currentActionStatXlToApi(stat, query, entityLookup, cspLookup).stream())
            .collect(Collectors.toList()));
        return statSnapshotApiDTO;
    }

    /**
     * Map a {@link ActionStats} object for a historical query to a list of
     * {@link StatSnapshotApiDTO}s.
     *
     * @param actionStats The historical stats retrieved from the action orchestrator.
     * @param query The {@link ActionStatsQuery} the stats are for.
     * @return The {@link StatSnapshotApiDTO}s to return to the caller.
     */
    @Nonnull
    public List<StatSnapshotApiDTO> historicalActionStatsToApiSnapshots(
                @Nonnull final ActionStats actionStats,
                @Nonnull final ActionStatsQuery query) {
        return actionStats.getStatSnapshotsList().stream()
            .map(actionStatSnapshot -> {
                final StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
                statSnapshotApiDTO.setDate(DateTimeUtil.toString(actionStatSnapshot.getTime()));
                statSnapshotApiDTO.setStatistics(actionStatSnapshot.getStatsList().stream()
                    .flatMap(actionStat -> actionStatXLtoAPI(actionStat, query).stream())
                    .collect(Collectors.toList()));
                return statSnapshotApiDTO;
            })
            .collect(Collectors.toList());
    }

    @Nonnull
    private List<StatApiDTO> currentActionStatXlToApi(@Nonnull final CurrentActionStat actionStat,
                                                      @Nonnull final ActionStatsQuery query,
                                                      @Nonnull final Map<Long, MinimalEntity> entityLookup,
                                                      @Nonnull final Map<Long, String> cspLookup) {
        // The filters that applied to get this action stat
        final GroupByFilters groupByFilters = groupByFiltersFactory.filtersForQuery(query);
        if (actionStat.getStatGroup().hasActionCategory()) {
            groupByFilters.setCategory(actionStat.getStatGroup().getActionCategory());
        }

        if (actionStat.getStatGroup().hasActionRelatedRisk()) {
            groupByFilters.setRelatedRisk(actionStat.getStatGroup().getActionRelatedRisk());
        }

        if (actionStat.getStatGroup().hasActionState()) {
            groupByFilters.setState(actionStat.getStatGroup().getActionState());
        }

        if (actionStat.getStatGroup().hasActionType()) {
            groupByFilters.setType(actionStat.getStatGroup().getActionType());
        }

        if (actionStat.getStatGroup().hasCostType()) {
            groupByFilters.setActionCostType(actionStat.getStatGroup().getCostType());
        }

        if (actionStat.getStatGroup().hasCsp()) {
            final String cspType = cspLookup.get(Long.parseLong(actionStat.getStatGroup().getCsp()));
            if (!StringUtils.isEmpty(cspType)) {
                groupByFilters.setCSP(cspType);
            }
        }

        if (actionStat.getStatGroup().hasSeverity()) {
            groupByFilters.setActionRiskSeverity(actionStat.getStatGroup().getSeverity());
        }

        if (actionStat.getStatGroup().hasTargetEntityType()) {
            groupByFilters.setTargetEntityType(actionStat.getStatGroup().getTargetEntityType());
        }

        if (actionStat.getStatGroup().hasBusinessAccountId()) {
            groupByFilters.setBusinessAccountId(actionStat.getStatGroup().getBusinessAccountId());
        }

        if (actionStat.getStatGroup().hasResourceGroupId()) {
            groupByFilters.setResourceGroupId(actionStat.getStatGroup().getResourceGroupId());
        }

        if (actionStat.getStatGroup().hasTargetEntityId()) {
            if (query.actionInput().getGroupBy().contains(StringConstants.TEMPLATE)) {
                MinimalEntity template = entityLookup.get(actionStat.getStatGroup().getTargetEntityId());
                if (template != null) {
                    groupByFilters.setTemplate(template.getDisplayName());
                } else {
                    // We do this as a fallback instead of throwing an exception
                    groupByFilters.setTemplate(String.valueOf(actionStat.getStatGroup().getTargetEntityId()));
                }
            } else {
                groupByFilters.setTargetEntityId(actionStat.getStatGroup().getTargetEntityId());
            }
        }

        if (actionStat.getStatGroup().hasReasonCommodityBaseType()) {
            groupByFilters.setReasonCommodity(actionStat.getStatGroup().getReasonCommodityBaseType());
        }

        final List<StatApiDTO> retStats = new ArrayList<>();
        if (actionStat.hasActionCount()) {
            retStats.add(newApiStat(
                StringConstants.NUM_ACTIONS,
                groupByFilters,
                numberToAPIStatValue(actionStat.getActionCount())));
        }

        if (actionStat.hasEntityCount()) {
            retStats.add(newApiStat(
                StringConstants.NUM_ENTITIES,
                groupByFilters,
                numberToAPIStatValue(actionStat.getEntityCount())));
        }

        // The "costType" specified in the query is a filter that restricts the types of action
        // cost stats we return. If it's not set, we return all non-zero ones.
        final Stream<ActionCostType> costTypes = query.getCostType()
            .map(Stream::of)
            .orElseGet(() -> Stream.of(ActionCostType.values()));
        costTypes.forEach(actionCostType -> {
            // We only want to return cost stats when investments/savings are non-zero, even
            // if they are explicitly set to zero.
            if (actionCostType == ActionCostType.ACTION_COST_TYPE_NONE) {
                // There is no reason to inject costPrice stat, when it is not a SAVING or INVESTMENT action.
                // But it is also not an error so we simply do nothing here.
            } else if (actionCostType == ActionCostType.INVESTMENT) {
                if (actionStat.getInvestments() > 0) {
                    retStats.add(newCostApiStat(groupByFilters,
                        numberToAPIStatValue((float) actionStat.getInvestments()),
                        ActionCostType.INVESTMENT));
                }
            } else if (actionCostType == ActionCostType.SAVING) {
                if (actionStat.getSavings() > 0) {
                    retStats.add(newCostApiStat(groupByFilters,
                        numberToAPIStatValue((float) actionStat.getSavings()),
                        ActionCostType.SAVING));
                }
            } else if (actionCostType == ActionCostType.SUPER_SAVING) {
                // We don't currently support super-savings, but this is not an error.
                logger.debug("Skipping costPrice stat injection for action cost type: {}", actionCostType);
            } else {
                logger.error("Action cost type: {} not supported for action stats queries.",
                    actionCostType);
            }
        });
        return retStats;
    }

    /**
     *
     * @param actionStat
     * @return A list of {@link StatApiDTO}s that the {@link ActionStat} maps to. The return type
     *         is a list because a single action stat can map to multiple stats - one for action
     *         count, one for entity count, one for savings/investment.
     */
    @Nonnull
    private List<StatApiDTO> actionStatXLtoAPI(@Nonnull final ActionStat actionStat,
                                              @Nonnull final ActionStatsQuery query) {
        final GroupByFilters groupByFilters = groupByFiltersFactory.filtersForQuery(query);
        if (actionStat.getStatGroup().hasActionCategory()) {
            groupByFilters.setCategory(actionStat.getStatGroup().getActionCategory());
        }

        if (actionStat.getStatGroup().hasActionState()) {
            groupByFilters.setState(actionStat.getStatGroup().getActionState());
        }

        if (actionStat.getStatGroup().hasBusinessAccountId()) {
            groupByFilters.setBusinessAccountId(actionStat.getStatGroup().getBusinessAccountId());
        }

        if (actionStat.getStatGroup().hasResourceGroupId()) {
            groupByFilters.setResourceGroupId(actionStat.getStatGroup().getResourceGroupId());
        }

        if (actionStat.getStatGroup().hasActionRelatedRisk()) {
            groupByFilters.setRelatedRisk(actionStat.getStatGroup().getActionRelatedRisk());
        }

        final List<StatApiDTO> retStats = new ArrayList<>();
        if (actionStat.hasActionCount()) {
            retStats.add(newApiStat(StringConstants.NUM_ACTIONS,
                groupByFilters,
                actionStatValueXLtoAPI(actionStat.getActionCount())));
        }

        if (actionStat.hasEntityCount()) {
            retStats.add(newApiStat(StringConstants.NUM_ENTITIES,
                groupByFilters,
                actionStatValueXLtoAPI(actionStat.getEntityCount())));
        }

        final Set<ActionCostType> requestedCostTypes = query.getCostType()
            .map(Collections::singleton)
            // By default, we return both savings and investments.
            .orElseGet(() -> Sets.newHashSet(ActionCostType.INVESTMENT, ActionCostType.SAVING));
        requestedCostTypes.forEach(costType -> {
            // Note - Feb 26 2019 - the UI doesn't expect zero values for investments/savings.
            // We only want to return those stats when there were some investments/savings.
            // If any investments or savings were provided, we would expect the "max" to be greater than 0.
            if (costType.equals(ActionCostType.INVESTMENT) && actionStat.getInvestments().getMax() > 0) {
                retStats.add(newCostApiStat(groupByFilters,
                    actionStatValueXLtoAPI(actionStat.getInvestments()),
                    ActionCostType.INVESTMENT));
            } else if (costType.equals(ActionCostType.SAVING) && actionStat.getSavings().getMax() > 0) {
                retStats.add(newCostApiStat(groupByFilters,
                    actionStatValueXLtoAPI(actionStat.getSavings()),
                    ActionCostType.SAVING));
            }
        });

        return retStats;
    }

    @Nonnull
    private StatValueApiDTO numberToAPIStatValue(final float number) {
        final StatValueApiDTO apiValueDTO = new StatValueApiDTO();
        apiValueDTO.setAvg(number);
        apiValueDTO.setMax(number);
        apiValueDTO.setMin(number);
        apiValueDTO.setTotal(number);
        return apiValueDTO;
    }

    @Nonnull
    private StatValueApiDTO actionStatValueXLtoAPI(@Nonnull final ActionStat.Value actionStatValue) {
        final StatValueApiDTO apiValueDTO = new StatValueApiDTO();
        apiValueDTO.setAvg((float)actionStatValue.getAvg());
        apiValueDTO.setMax((float)actionStatValue.getMax());
        apiValueDTO.setMin((float)actionStatValue.getMin());
        apiValueDTO.setTotal((float)actionStatValue.getTotal());
        return apiValueDTO;
    }

    @Nonnull
    private StatApiDTO newCostApiStat(@Nonnull final GroupByFilters groupByFilters,
                                      @Nonnull final StatValueApiDTO statValue,
                                      ActionCostType actionCostType) {
        // Super-savings not supported right now.
        Preconditions.checkArgument(actionCostType == ActionCostType.INVESTMENT ||
            actionCostType == ActionCostType.SAVING);
        final StatApiDTO value = newApiStat(StringConstants.COST_PRICE, groupByFilters, statValue);
        final String costFilterValue = COST_TYPE_STR.get(actionCostType);
        if (costFilterValue != null) {
            value.addFilter(StringConstants.PROPERTY, costFilterValue);
        }
        value.setUnits(StringConstants.DOLLARS_PER_HOUR);
        return value;
    }

    @Nonnull
    private StatApiDTO newApiStat(@Nonnull final String name,
                                  @Nonnull final GroupByFilters commonFilters,
                                  @Nonnull final StatValueApiDTO statValue) {
        final StatApiDTO statApiDTO = new StatApiDTO();
        statApiDTO.setName(name);

        statApiDTO.setFilters(commonFilters.getFilters());
        statApiDTO.setValues(statValue);
        statApiDTO.setValue(statValue.getAvg());
        return statApiDTO;
    }

}

package com.vmturbo.api.component.external.api.util.action;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

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
import com.vmturbo.components.common.utils.StringConstants;

/**
 * Maps action count stats from XL format to API {@link StatSnapshotApiDTO}s.
 */
class ActionStatsMapper {

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
     * @return The {@link StatSnapshotApiDTO} to return to the caller.
     */
    @Nonnull
    public StatSnapshotApiDTO currentActionStatsToApiSnapshot(
            @Nonnull final List<CurrentActionStat> currentActionStats,
            @Nonnull final ActionStatsQuery query) {
        final StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
        statSnapshotApiDTO.setDate(query.currentTimeStamp().isPresent() ? query.currentTimeStamp().get()
                        : DateTimeUtil.toString(clock.millis()));
        statSnapshotApiDTO.setStatistics(currentActionStats.stream()
            .flatMap(stat -> currentActionStatXlToApi(stat, query).stream())
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
                                                      @Nonnull final ActionStatsQuery query) {
        // The filters that applied to get this action stat
        final GroupByFilters groupByFilters = groupByFiltersFactory.filtersForQuery(query);
        if (actionStat.getStatGroup().hasActionCategory()) {
            groupByFilters.setCategory(actionStat.getStatGroup().getActionCategory());
        }

        if (actionStat.getStatGroup().hasActionState()) {
            groupByFilters.setState(actionStat.getStatGroup().getActionState());
        }

        if (actionStat.getStatGroup().hasActionType()) {
            groupByFilters.setType(actionStat.getStatGroup().getActionType());
        }

        if (actionStat.getStatGroup().hasTargetEntityType()) {
            groupByFilters.setTargetEntityType(actionStat.getStatGroup().getTargetEntityType());
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

        query.getCostType().ifPresent(actionCostType -> {
            // We only want to return cost stats when investments/savings are non-zero, even
            // if they are explicitly set to zero.
            if (actionCostType == ActionCostType.INVESTMENT && actionStat.getInvestments() > 0) {
                retStats.add(newCostApiStat(groupByFilters,
                    numberToAPIStatValue((float)actionStat.getInvestments()),
                    ActionCostType.INVESTMENT));
            } else if (actionCostType == ActionCostType.SAVING && actionStat.getSavings() > 0) {
                retStats.add(newCostApiStat(groupByFilters,
                    numberToAPIStatValue((float)actionStat.getSavings()),
                    ActionCostType.SAVING));
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
        if (actionStat.hasActionCategory()) {
            groupByFilters.setCategory(actionStat.getActionCategory());
        }

        if (actionStat.hasActionState()) {
            groupByFilters.setState(actionStat.getActionState());
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

        query.getCostType().ifPresent(costType -> {
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

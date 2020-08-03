package com.vmturbo.api.component.external.api.mapper;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategoryStats;
import com.vmturbo.common.protobuf.action.ActionDTO.StateAndModeCount;
import com.vmturbo.common.protobuf.action.ActionDTO.TypeCount;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Static utility responsible for mapping {@link TypeCount}s to the appropriate
 * {@link StatSnapshotApiDTO} for use by the UI.
 */
public class ActionCountsMapper {

    private final static String PROPERTY_NAME = "property";

    @Nonnull
    public static List<StatSnapshotApiDTO> countsByTypeToApi(@Nonnull final List<TypeCount> typeCounts) {
        List<StatSnapshotApiDTO> retList = Collections.emptyList();
        if (!typeCounts.isEmpty()) {
            final StatSnapshotApiDTO retDto = new StatSnapshotApiDTO();
            retDto.setDate(DateTimeUtil.getNow());
            retDto.setStatistics(typeCounts.stream()
                    .map(typeCount -> {
                        final float floatVal = new Long(typeCount.getCount()).floatValue();
                        final StatApiDTO statDto = new StatApiDTO();
                        statDto.setName(StringConstants.NUM_ACTIONS);

                        // (roman, May 30 2017) There is currently only one type of filtering
                        // supported here. In the future we may have to support
                        // some kind of "real" group-by functionality, but that will
                        // also require a redesign of the TypeCount API in the Action Orchestrator.
                        final StatFilterApiDTO typeFilter = new StatFilterApiDTO();
                        typeFilter.setType(StringConstants.ACTION_TYPES);
                        typeFilter.setValue(ActionTypeMapper.toApiApproximate(typeCount.getType()).name());

                        statDto.setFilters(Collections.singletonList(typeFilter));

                        statDto.setValue(floatVal);

                        final StatValueApiDTO valueDto = new StatValueApiDTO();
                        valueDto.setAvg(floatVal);
                        // need to include max, min and total value, even though they are same.
                        valueDto.setMax(floatVal);
                        valueDto.setMin(floatVal);
                        valueDto.setTotal(floatVal);
                        statDto.setValues(valueDto);
                        return statDto;
                    })
                    .collect(Collectors.toList()));
            retList = Collections.singletonList(retDto);
        }
        return retList;
    }

    /**
     * Transform the result returned from Action Orchestrator to UI accepted format.
     *
     * @param stateAndModeCountsByDateMap the map has date as key and List of StateAndModeCount as value.
     * @return List of StatSnapshotApiDTO.
     */
    @Nonnull
    public static List<StatSnapshotApiDTO> countsByStateAndModeGroupByDateToApi(@Nonnull final Map<Long, List<StateAndModeCount>> stateAndModeCountsByDateMap) {
        List<StatSnapshotApiDTO> retList = Lists.newArrayList();
        for (Map.Entry<Long, List<StateAndModeCount>> entry : stateAndModeCountsByDateMap.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                final StatSnapshotApiDTO retDto = new StatSnapshotApiDTO();
                retDto.setDate(DateTimeUtil.toString(entry.getKey()));
                retDto.setStatistics(entry.getValue().stream()
                        .map(stateAndModeCount -> {
                            final float floatVal = new Long(stateAndModeCount.getCount()).floatValue();
                            final StatApiDTO statDto = new StatApiDTO();
                            statDto.setName(StringConstants.NUM_ACTIONS);

                            final StatFilterApiDTO stateFilter = new StatFilterApiDTO();
                            stateFilter.setType(StringConstants.ACTION_STATES);
                            stateFilter.setValue(stateAndModeCount.getState().toString());

                            final StatFilterApiDTO modeFilter = new StatFilterApiDTO();
                            modeFilter.setType(StringConstants.ACTION_MODES);
                            modeFilter.setValue(stateAndModeCount.getMode().name());


                            statDto.setFilters(Lists.newArrayList(stateFilter, modeFilter));

                            statDto.setValue(floatVal);

                            final StatValueApiDTO valueDto = new StatValueApiDTO();
                            valueDto.setAvg(floatVal);
                            // need to include max, min and total value, even though they are same.
                            valueDto.setMax(floatVal);
                            valueDto.setMin(floatVal);
                            valueDto.setTotal(floatVal);
                            statDto.setValues(valueDto);
                            return statDto;
                        })
                        .collect(Collectors.toList()));
                retList.add(retDto);
            }
        }
        return retList;
    }

    @Nonnull
    public static List<StatSnapshotApiDTO> convertActionCategoryStatsToApiStatSnapshot(
            @Nonnull final List<ActionCategoryStats> actionCategoryStats) {

        List<StatSnapshotApiDTO> statSnapshotApiDTOList = Lists.newArrayList();
        final StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
        statSnapshotApiDTO.setDate(DateTimeUtil.getNow());
        List<StatApiDTO> statApiDtos = Lists.newArrayList();

        for (ActionCategoryStats stat : actionCategoryStats) {
            statApiDtos.addAll(createStatApiDTOs(stat));
        }
        statSnapshotApiDTO.setStatistics(statApiDtos);
        statSnapshotApiDTOList.add(statSnapshotApiDTO);
        return statSnapshotApiDTOList;
    }

    private static List<StatApiDTO> createStatApiDTOs(@Nonnull final ActionCategoryStats stat) {

        List<StatApiDTO> statApiDTOS = Lists.newArrayList();

        final StatFilterApiDTO riskCategoryFilter = new StatFilterApiDTO();
        riskCategoryFilter.setType(StringConstants.RISK_SUB_CATEGORY);
        riskCategoryFilter.setValue(ActionSpecMapper.mapXlActionCategoryToApi(stat.getActionCategory()));

        final StatFilterApiDTO savingsFilter = new StatFilterApiDTO();
        savingsFilter.setType(PROPERTY_NAME);
        savingsFilter.setValue(StringConstants.SAVINGS);

        final StatFilterApiDTO investmentFilter = new StatFilterApiDTO();
        investmentFilter.setType(PROPERTY_NAME);
        investmentFilter.setValue(StringConstants.INVESTMENT);

        statApiDTOS.add(createStatApiDTO(StringConstants.NUM_ACTIONS,
                    Collections.singletonList(riskCategoryFilter),
                    stat.getActionsCount()));

        statApiDTOS.add(createStatApiDTO(StringConstants.NUM_ENTITIES,
                Collections.singletonList(riskCategoryFilter),
                stat.getEntitiesCount()));

        statApiDTOS.add(createStatApiDTO(StringConstants.COST_PRICE,
                Arrays.asList(riskCategoryFilter, savingsFilter),
                stat.getEntitiesCount()));

        statApiDTOS.add(createStatApiDTO(StringConstants.COST_PRICE,
                Arrays.asList(riskCategoryFilter, investmentFilter),
                stat.getEntitiesCount()));

        return statApiDTOS;
    }

    private static StatApiDTO createStatApiDTO(@Nonnull String statName,
                                               @Nonnull List<StatFilterApiDTO> filters,
                                               float statValue) {

        final StatApiDTO statDto = new StatApiDTO();
        statDto.setName(statName);
        statDto.setFilters(filters);

        statDto.setValue(statValue);
        final StatValueApiDTO valueDto = new StatValueApiDTO();
        valueDto.setAvg(statValue);
        valueDto.setMax(statValue);
        valueDto.setMin(statValue);
        valueDto.setTotal(statValue);
        statDto.setValues(valueDto);

        return  statDto;
    }
}

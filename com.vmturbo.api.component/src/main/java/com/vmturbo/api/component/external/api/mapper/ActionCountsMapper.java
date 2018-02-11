package com.vmturbo.api.component.external.api.mapper;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.StateAndModeCount;
import com.vmturbo.common.protobuf.action.ActionDTO.TypeCount;

/**
 * Static utility responsible for mapping {@link TypeCount}s to the appropriate
 * {@link StatSnapshotApiDTO} for use by the UI.
 */
public class ActionCountsMapper {

    private static final Logger logger = LogManager.getLogger();

    /**
     * This is the name the API/UI expects for counts filtered by type.
     */
    @VisibleForTesting
    public static final String ACTION_TYPES_NAME = "actionTypes";

    /**
     * This is the name the API/UI expects for counts filtered by state.
     */
    @VisibleForTesting
    public static final String ACTION_STATE_NAME = "actionStates";

    /**
     * This is the name the API/UI expects for counts filtered by mode.
     */
    @VisibleForTesting
    public static final String ACTION_MODE_NAME = "actionModes";

    /**
     * This is the name the API/UI expect when returning action stats values.
     *
     */
    @VisibleForTesting
    public static final String ACTION_COUNT_STAT_NAME = "numActions";

    private ActionCountsMapper() {}

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
                        statDto.setName(ACTION_COUNT_STAT_NAME);

                        // (roman, May 30 2017) There is currently only one type of filtering
                        // supported here. In the future we may have to support
                        // some kind of "real" group-by functionality, but that will
                        // also require a redesign of the TypeCount API in the Action Orchestrator.
                        final StatFilterApiDTO typeFilter = new StatFilterApiDTO();
                        typeFilter.setType(ACTION_TYPES_NAME);
                        typeFilter.setValue(ActionTypeMapper.toApi(typeCount.getType()));

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
                            statDto.setName(ACTION_COUNT_STAT_NAME);

                            final StatFilterApiDTO stateFilter = new StatFilterApiDTO();
                            stateFilter.setType(ACTION_STATE_NAME);
                            stateFilter.setValue(stateAndModeCount.getState().toString());

                            final StatFilterApiDTO modeFilter = new StatFilterApiDTO();
                            modeFilter.setType(ACTION_MODE_NAME);
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

}

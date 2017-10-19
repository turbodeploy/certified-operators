package com.vmturbo.api.component.external.api.mapper;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.utils.DateTimeUtil;
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

}

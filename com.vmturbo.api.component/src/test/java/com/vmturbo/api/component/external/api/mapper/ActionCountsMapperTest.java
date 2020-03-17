package com.vmturbo.api.component.external.api.mapper;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.StateAndModeCount;
import com.vmturbo.common.protobuf.action.ActionDTO.TypeCount;
import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Unit tests for the {@link ActionSpecMapper}.
 */
public class ActionCountsMapperTest {

    @Test
    public void testEmpty() {
        final List<StatSnapshotApiDTO> retList =
                ActionCountsMapper.countsByTypeToApi(Collections.emptyList());
        Assert.assertTrue(retList.isEmpty());
    }

    @Test
    public void testOneType() {
        final List<StatSnapshotApiDTO> retList = ActionCountsMapper.countsByTypeToApi(Collections.singletonList(
                TypeCount.newBuilder()
                    .setType(ActionType.MOVE)
                    .setCount(10)
                        .build()));
        Assert.assertEquals(1, retList.size());
        final StatSnapshotApiDTO dto = retList.get(0);
        Assert.assertEquals(1, dto.getStatistics().size());

        Assert.assertNotNull(dto.getDate());
        // We won't get an exact match on the time, so just check that the
        // date is reverse-parseable by DateTimeUtil.
        DateTimeUtil.parseTime(dto.getDate());

        final StatApiDTO stat = dto.getStatistics().get(0);
        checkStat(stat, 10, ActionType.MOVE);
    }

    @Test
    public void testTwoTypes() {
        final List<StatSnapshotApiDTO> retList = ActionCountsMapper.countsByTypeToApi(Arrays.asList(
                TypeCount.newBuilder()
                        .setType(ActionType.MOVE)
                        .setCount(10)
                        .build(),
                TypeCount.newBuilder()
                        .setType(ActionType.RESIZE)
                        .setCount(10)
                        .build()));

        Assert.assertEquals(1, retList.size());

        final StatSnapshotApiDTO dto = retList.get(0);
        Assert.assertEquals(2, dto.getStatistics().size());

        Assert.assertNotNull(dto.getDate());
        // We won't get an exact match on the time, so just check that the
        // date is reverse-parseable by DateTimeUtil.
        DateTimeUtil.parseTime(dto.getDate());

        // Because StatApiDTOs don't override equals, just assume that the
        // order will be the same as in the input. If this test ever fails because the
        // order is different, rewrite the tests, because the order shouldn't matter!
        final StatApiDTO moveStat = dto.getStatistics().get(0);
        checkStat(moveStat, 10, ActionType.MOVE);
        final StatApiDTO resizeStat = dto.getStatistics().get(1);
        checkStat(resizeStat, 10, ActionType.RESIZE);
    }

    @Test
    public void testTwoStateAndModeWithGroupByDateToApi() {
        Map<Long, List<StateAndModeCount>> map = Maps.newHashMap();
        List list = Arrays.asList(
                StateAndModeCount.newBuilder()
                        .setState(ActionState.FAILED)
                        .setMode(ActionMode.MANUAL)
                        .setCount(10)
                        .build(),
                StateAndModeCount.newBuilder()
                        .setState(ActionState.FAILED)
                        .setMode(ActionMode.MANUAL)
                        .setCount(10)
                        .build());
        map.put(1517428240l, list);
        final List<StatSnapshotApiDTO> retList = ActionCountsMapper.countsByStateAndModeGroupByDateToApi(map);

        Assert.assertEquals(1, retList.size());

        final StatSnapshotApiDTO dto = retList.get(0);
        Assert.assertEquals(2, dto.getStatistics().size());

        Assert.assertNotNull(dto.getDate());
        Assert.assertEquals(2, dto.getStatistics().get(0).getFilters().size());
        Assert.assertTrue(dto.getStatistics().get(0).getValue().equals(10.0f));
        Assert.assertEquals(2, dto.getStatistics().get(1).getFilters().size());
        Assert.assertTrue(dto.getStatistics().get(1).getValue().equals(10.0f));
    }

    @Test
    public void testTwoStateModeAndTwoDateWithGroupByDateToApi() {
        Map<Long, List<StateAndModeCount>> map = Maps.newHashMap();
        List list = Arrays.asList(
                StateAndModeCount.newBuilder()
                        .setState(ActionState.FAILED)
                        .setMode(ActionMode.MANUAL)
                        .setCount(5)
                        .build(),
                StateAndModeCount.newBuilder()
                        .setState(ActionState.FAILED)
                        .setMode(ActionMode.MANUAL)
                        .setCount(5)
                        .build());
        LocalDateTime yesterday = LocalDateTime.now().minusDays(1);
        LocalDateTime now = LocalDateTime.now();

        map.put(TimeUtil.localDateTimeToMilli(yesterday, Clock.systemUTC()), list);
        map.put(TimeUtil.localDateTimeToMilli(now, Clock.systemUTC()), list); // different date
        final List<StatSnapshotApiDTO> retList = ActionCountsMapper.countsByStateAndModeGroupByDateToApi(map);

        Assert.assertEquals(2, retList.size());
        for (int i = 0; i <= 1; i++) {
            final StatSnapshotApiDTO dto = retList.get(i);
            Assert.assertEquals(2, dto.getStatistics().size());

            Assert.assertNotNull(dto.getDate());
            Assert.assertEquals(2, dto.getStatistics().get(0).getFilters().size());
            Assert.assertTrue(dto.getStatistics().get(0).getValue().equals(5.0f));
            Assert.assertEquals(2, dto.getStatistics().get(1).getFilters().size());
            Assert.assertTrue(dto.getStatistics().get(1).getValue().equals(5.0f));
        }
    }

    private void checkStat(@Nonnull final StatApiDTO stat, float val, ActionType actionType) {
        Assert.assertEquals(StringConstants.NUM_ACTIONS, stat.getName());
        Assert.assertEquals(1, stat.getFilters().size());
        Assert.assertEquals(StringConstants.ACTION_TYPES, stat.getFilters().get(0).getType());
        Assert.assertEquals(ActionTypeMapper.toApiApproximate(actionType).name(), stat.getFilters().get(0).getValue());
        Assert.assertEquals(Float.valueOf(val), stat.getValue());
        Assert.assertEquals(Float.valueOf(val), stat.getValues().getAvg());
        Assert.assertEquals(Float.valueOf(val), stat.getValues().getMax());
        Assert.assertEquals(Float.valueOf(val), stat.getValues().getMin());
        Assert.assertEquals(Float.valueOf(val), stat.getValues().getTotal());
    }
}

package com.vmturbo.topology.processor.history.timeslot;

import java.time.Clock;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.forecasting.TimeInMillisConstants;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.history.BaseGraphRelatedTest;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.CommodityFieldAccessor;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.ICommodityFieldAccessor;
import com.vmturbo.topology.processor.history.timeslot.TimeSlotCommodityData.SlotStatistics;

/**
 * Unit tests for TimeSlotCommodityData.
 */
public class TimeSlotCommodityDataTest extends BaseGraphRelatedTest {
    private static final double DELTA = 0.001;
    private static final int SLOTS = 2;
    private static final CommodityType COMM_TYPE = CommodityType.newBuilder().setType(1).build();
    private static final EntityCommodityFieldReference FIELD =
                    new EntityCommodityFieldReference(1, COMM_TYPE, CommodityField.USED);
    private TimeslotHistoricalEditorConfig config;
    private Clock clock;

    /**
     * Initializes all resources required by tests.
     */
    @Before
    public void before() {
        config = Mockito.mock(TimeslotHistoricalEditorConfig.class);
        Mockito.when(config.isPlan()).thenReturn(false);
        Mockito.when(config.getSlots(Mockito.anyLong())).thenReturn(SLOTS);
        clock = Mockito.mock(Clock.class);
        Mockito.when(config.getClock()).thenReturn(clock);
    }

    /**
     * Test the conversion of db values to internal cache structure.
     */
    @Test
    public void testRecordToSlots() {
        List<Pair<Long, StatRecord>> dbValue = new LinkedList<>();
        SlotStatistics[] slots = new SlotStatistics[2];
        slots[0] = new SlotStatistics();
        slots[1] = new SlotStatistics();

        float capacity = 100;
        float used1 = 40;
        float used2 = 60;
        float used3 = 70;
        float used4 = 10;
        float used5 = 40;
        // in slot1
        dbValue.add(Pair.create(0L, createStatRecord(capacity, used1)));
        dbValue.add(Pair.create(1L, createStatRecord(capacity, used2)));
        // in slot2
        dbValue.add(Pair.create(TimeInMillisConstants.HOUR_LENGTH_IN_MILLIS * 12 + 1,
                                createStatRecord(capacity, used3)));
        dbValue.add(Pair.create(TimeInMillisConstants.HOUR_LENGTH_IN_MILLIS * 12 + 100,
                                createStatRecord(capacity, used4)));
        dbValue.add(Pair.create(TimeInMillisConstants.HOUR_LENGTH_IN_MILLIS * 36 + 34,
                                createStatRecord(capacity, used5)));

        TimeSlotCommodityData.recordsToSlots(dbValue, slots);

        Assert.assertEquals(2, slots[0].getCount());
        Assert.assertEquals((used1 + used2) / capacity, slots[0].getTotal(), DELTA);
        Assert.assertEquals(3, slots[1].getCount());
        Assert.assertEquals((used3 + used4 + used5) / capacity, slots[1].getTotal(), DELTA);
    }

    /**
     * Test that aggregate method accounts for passed running values from DTOs.
     */
    @Test
    public void testAggregate() {
        final float cap = 100F;
        final double used1 = 76F;
        final double used2 = 99F;
        final double used3 = 12F;
        final double used4 = 54F;
        final double used5 = 66F;
        final TopologyEntity entity =
                        mockEntity(1, 1, COMM_TYPE, cap, used1, null, null, null, null, true);
        final ICommodityFieldAccessor accessor = createAccessor(cap, entity, 1000);
        final CommoditySoldDTO.Builder commSold =
                        entity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList()
                                        .get(0);

        TimeSlotCommodityData tcd = new TimeSlotCommodityData();
        tcd.init(FIELD, null, config, accessor);

        // 2 points in 1st hour
        Mockito.when(clock.millis()).thenReturn(1L);
        Mockito.doReturn(used1).when(accessor).getRealTimeValue(FIELD);
        tcd.aggregate(FIELD, config, accessor);
        commSold.getHistoricalUsedBuilder().clear();
        Mockito.doReturn(used2).when(accessor).getRealTimeValue(FIELD);
        tcd.aggregate(FIELD, config, accessor);

        // should still be 0 values
        Assert.assertTrue(commSold.hasHistoricalUsed());
        Assert.assertEquals(SLOTS, commSold.getHistoricalUsed().getTimeSlotCount());
        Assert.assertEquals(0, commSold.getHistoricalUsed().getTimeSlot(0), DELTA);
        Assert.assertEquals(0, commSold.getHistoricalUsed().getTimeSlot(1), DELTA);
        commSold.getHistoricalUsedBuilder().clear();

        // advance time by an hour and add a point - previous hour should be accounted for
        final float expectedFirstHourAvg = (float)((used1 + used2) / 2 / cap);
        Mockito.doReturn(TimeInMillisConstants.HOUR_LENGTH_IN_MILLIS + 1).when(clock).millis();
        Mockito.doReturn(used3).when(accessor).getRealTimeValue(FIELD);
        tcd.aggregate(FIELD, config, accessor);
        Assert.assertEquals(expectedFirstHourAvg, commSold.getHistoricalUsed().getTimeSlot(0), DELTA);
        Assert.assertEquals(0, commSold.getHistoricalUsed().getTimeSlot(1), DELTA);
        commSold.getHistoricalUsedBuilder().clear();

        // advance time by 12 hours and add a point
        final float expectedFirstSlotAvg = (float)((expectedFirstHourAvg + used3 / cap) / 2);
        Mockito.doReturn(TimeInMillisConstants.HOUR_LENGTH_IN_MILLIS * 12 + 1).when(clock).millis();
        Mockito.doReturn(used4).when(accessor).getRealTimeValue(FIELD);
        tcd.aggregate(FIELD, config, accessor);
        Assert.assertEquals(expectedFirstSlotAvg, commSold.getHistoricalUsed().getTimeSlot(0), DELTA);
        Assert.assertEquals(0, commSold.getHistoricalUsed().getTimeSlot(1), DELTA);
        commSold.getHistoricalUsedBuilder().clear();

        // and another hour and a point
        Mockito.doReturn(TimeInMillisConstants.HOUR_LENGTH_IN_MILLIS * 13 + 1).when(clock).millis();
        Mockito.doReturn(used5).when(accessor).getRealTimeValue(FIELD);
        tcd.aggregate(FIELD, config, accessor);
        Assert.assertEquals(expectedFirstSlotAvg, commSold.getHistoricalUsed().getTimeSlot(0), DELTA);
        Assert.assertEquals(used4 / cap, commSold.getHistoricalUsed().getTimeSlot(1), DELTA);

    }

    private static StatRecord createStatRecord(float capacity, float used) {
        return StatRecord.newBuilder().setCapacity(StatValue.newBuilder().setAvg(capacity).build())
                        .setUsed(StatValue.newBuilder().setAvg(used).build()).build();
    }

    private static ICommodityFieldAccessor createAccessor(float cap, TopologyEntity entity,
                                                          long lastPointTimestamp) {
        ICommodityFieldAccessor accessor = Mockito.spy(new CommodityFieldAccessor(
            mockGraph(Collections.singleton(entity))));
        Mockito.doReturn((double)cap).when(accessor).getCapacity(FIELD);
        return accessor;
    }
}

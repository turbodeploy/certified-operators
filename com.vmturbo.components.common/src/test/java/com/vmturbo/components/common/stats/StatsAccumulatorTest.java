package com.vmturbo.components.common.stats;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;

public class StatsAccumulatorTest {
    @Test
    public void testAccumulationOneValue() {
        final StatsAccumulator value = new StatsAccumulator();
        value.record(1);
        assertEquals(1, value.getMin(), 0);
        assertEquals(1, value.getMax(), 0);
        assertEquals(1, value.getAvg(), 0);
        assertEquals(1, value.getTotal(), 0);

        final StatValue statValue = value.toStatValue();
        assertEquals(1, statValue.getMin(), 0);
        assertEquals(1, statValue.getMax(), 0);
        assertEquals(1, statValue.getAvg(), 0);
        assertEquals(1, statValue.getTotal(), 0);
    }

    @Test
    public void testAccumulationTwoValues() {
        final StatsAccumulator value = new StatsAccumulator();
        value.record(1);
        value.record(3);
        assertEquals(1, value.getMin(), 0);
        assertEquals(3, value.getMax(), 0);
        assertEquals(2, value.getAvg(), 0);
        assertEquals(4, value.getTotal(), 0);

        final StatValue statValue = value.toStatValue();
        assertEquals(1, statValue.getMin(), 0);
        assertEquals(3, statValue.getMax(), 0);
        assertEquals(2, statValue.getAvg(), 0);
        assertEquals(4, statValue.getTotal(), 0);
    }

    @Test
    public void testAccumulationFixedValue() {
        final StatValue value = StatsAccumulator.singleStatValue(10.0f);
        assertEquals(10.0f, value.getMin(), 0);
        assertEquals(10.0f, value.getMax(), 0);
        assertEquals(10.0f, value.getAvg(), 0);
        assertEquals(10.0f, value.getTotal(), 0);
    }

    /**
     * The intention of this test is to ensure that the "max" value can never be less than the "min"
     * or "average" values.
     */
    @Test
    public void testAccumulationTwoValuesMissingPeak() {
        final StatsAccumulator value = new StatsAccumulator();
        value.record(1, 0);
        value.record(3, 0);
        assertEquals(0, value.getMin(), 0);
        assertEquals(3, value.getMax(), 0);
        assertEquals(2, value.getAvg(), 0);
        assertEquals(4, value.getTotal(), 0);

        final StatValue statValue = value.toStatValue();
        assertEquals(0, statValue.getMin(), 0);
        assertEquals(3, statValue.getMax(), 0);
        assertEquals(2, statValue.getAvg(), 0);
        assertEquals(4, statValue.getTotal(), 0);
    }
}

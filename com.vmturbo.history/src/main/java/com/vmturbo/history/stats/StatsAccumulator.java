package com.vmturbo.history.stats;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;

/**
 * A utility class to accumulate values and keep track of the min, max, total and average.
 */
public class StatsAccumulator {
    private double min = Double.MAX_VALUE;
    private double max = Double.MIN_VALUE;
    private double total = 0;
    private int count = 0;

    /**
     * Record the value to the accumulation.
     *
     * @param value The value to record.
     * @return A reference to {@link this} for method chaining.
     */
    @Nonnull
    public StatsAccumulator record(double value) {
        min = Math.min(value, min);
        max = Math.max(value, max);
        total += value;
        ++count;

        return this;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getTotal() {
        return total;
    }

    public double getAvg() {
        return count == 0 ? 0 : total / count;
    }

    public int getCount() {
        return count;
    }

    public static StatValue singleStatValue(final float value) {
        return new StatsAccumulator()
            .record(value)
            .toStatValue();
    }

    @Nonnull
    public StatValue toStatValue() {
        return StatValue.newBuilder()
            .setAvg((float)getAvg())
            .setTotal((float)getTotal())
            .setMax((float)getMax())
            .setMin((float)getMin())
            .build();
    }
}

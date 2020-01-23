package com.vmturbo.components.common.stats;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;

/**
 * A utility class to accumulate values and keep track of the min, max, total and average.
 */
public class StatsAccumulator {
    private double min = Double.MAX_VALUE;
    private double max = Double.MIN_VALUE;
    private double total = 0;
    private double totalMax = 0;
    private double totalMin = 0;
    private int count = 0;

    /**
     * Record the value to the accumulation.
     *
     * <p>When we only have 1 data point, the values for min, average and max are equal.
     *
     * @param value The value to record.
     * @return A reference to {@link this} for method chaining.
     */
    @Nonnull
    public StatsAccumulator record(double value) {
        return record(value, value, value);
    }

    /**
     * Record the value to the accumulation.
     * Some commodities have an average and peak value. The peak value is used to calculate the max
     * and totalMax. Assume min value = average value.
     *
     * @param avgValue average value
     * @param peakValue peak value
     * @return A reference to {@link this} for method chaining.
     */
    @Nonnull
    public StatsAccumulator record(double avgValue, double peakValue) {
        return record(avgValue, avgValue, peakValue);
    }

    /**
     * Record the value to the accumulation.
     *
     * @param minValue minimum value
     * @param avgValue average value
     * @param peakValue peak value
     * @return A reference to {@link this} for method chaining.
     */
    @Nonnull
    public StatsAccumulator record(double minValue, double avgValue, double peakValue) {
        min = Math.min(minValue, min);
        max = Math.max(peakValue, max);
        total += avgValue;
        totalMax += peakValue;
        totalMin += minValue;
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

    public double getTotalMax() {
        return totalMax;
    }

    public double getTotalMin() {
        return totalMin;
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
            .setMax((float)getMax())
            .setMin((float)getMin())
            .setTotal((float)getTotal())
            .setTotalMax((float)getTotalMax())
            .setTotalMin((float)getTotalMin())
            .build();
    }
}

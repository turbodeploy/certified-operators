package com.vmturbo.repository.topology.util;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;

/**
 * Utility class to support organizing and creating data for PlanEntityStatsExtractor.
 */
public class PlanEntityStatsExtractorUtil {

    /**
     * Builds a {@link StatRecord} with commodityName and float value.
     *
     * @param commodityName the name describing stat
     * @param vmDensity the value to set to stat
     * @return StatRecord object using commodityName and vmDensity values.
     */
    public static StatRecord buildVmDensityStatRecord(@Nonnull final String commodityName,
            final float vmDensity) {
        final StatValue statValue = buildStatValue(vmDensity);

        return StatRecord.newBuilder()
                .setName(commodityName)
                .setCurrentValue(vmDensity)
                .setUsed(statValue)
                .setPeak(statValue)
                .setCapacity(statValue)
                .build();
    }

    /**
     * Create a {@link StatValue} initialized from a single value. All the fields
     * are set to the same value.
     *
     * @param value the value to initialize the StatValue with
     * @return a {@link StatValue} initialized with all fields set from the given value
     */
    public static StatValue buildStatValue(float value) {
        return StatValue.newBuilder()
                .setAvg(value)
                .setMin(value)
                .setMax(value)
                .setTotal(value)
                .build();
    }

}

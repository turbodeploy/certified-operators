package com.vmturbo.cost.component.savings;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;

/**
 * Keeps stats (like REALIZED_SAVINGS) of a particular type, aggregated at a given timestamp.
 */
public class AggregatedSavingsStats extends BaseSavingsStats {
    /**
     * Creates a new one.
     *
     * @param timestamp Stats timestamp, e.g 14:00:00 for hourly stats.
     * @param statsType Type of stats, e.g REALIZED_INVESTMENTS.
     * @param statsValue Value of stats field.
     */
    public AggregatedSavingsStats(long timestamp, EntitySavingsStatsType statsType,
            @Nonnull Double statsValue) {
        super(timestamp, statsType, statsValue);
    }
}

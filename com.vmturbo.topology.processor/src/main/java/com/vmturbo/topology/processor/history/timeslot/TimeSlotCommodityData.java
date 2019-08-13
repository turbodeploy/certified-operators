package com.vmturbo.topology.processor.history.timeslot;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.topology.processor.history.CachingHistoricalEditorConfig;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.IHistoryCommodityData;

/**
 * Per-commodity cache for storing utilizations for time-slot calculations.
 * TODO dmitry provide configuration and db value (collection of floats)
 */
public class TimeSlotCommodityData
                implements IHistoryCommodityData<CachingHistoricalEditorConfig, Void> {
    private float[] utilizations;
    private long firstTimestamp;
    private long count;

    @Override
    public void aggregate(@Nonnull EntityCommodityFieldReference field,
                         @Nullable CachingHistoricalEditorConfig config) {
        // TODO dmitry update the array and calculate the averages
        // TODO dmitry trace log
    }

    @Override
    public void init(@Nullable Void dbValue, @Nullable CachingHistoricalEditorConfig config) {
        // TODO dmitry reinitialize the store according to config
    }

}

package com.vmturbo.topology.processor.history.timeslot;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.ICommodityFieldAccessor;
import com.vmturbo.topology.processor.history.IHistoryCommodityData;

/**
 * Per-commodity cache for storing utilizations for time-slot calculations.
 * TODO dmitry provide configuration and db value (collection of floats)
 */
public class TimeSlotCommodityData
                implements IHistoryCommodityData<TimeslotHistoricalEditorConfig, Void> {
    private float[] utilizations;
    private long firstTimestamp;
    private long count;

    @Override
    public void aggregate(@Nonnull EntityCommodityFieldReference field,
                          @Nonnull TimeslotHistoricalEditorConfig config,
                          @Nonnull ICommodityFieldAccessor commodityFieldsAccessor) {
        // TODO dmitry update the array and calculate the averages
        // TODO dmitry trace log
    }

    @Override
    public void init(@Nonnull EntityCommodityFieldReference field,
                     @Nullable Void dbValue, @Nonnull TimeslotHistoricalEditorConfig config,
                     @Nonnull ICommodityFieldAccessor commodityFieldsAccessor) {
        // TODO dmitry reinitialize the store according to config
    }

}

package com.vmturbo.topology.processor.history.histutil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.topology.processor.history.CachingHistoricalEditorConfig;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.ICommodityFieldAccessor;
import com.vmturbo.topology.processor.history.IHistoryCommodityData;

/**
 * Per-commodity data for "historical utilization" - one previous point.
 * TODO dmitry provide weights from config
 */
public class HistUtilizationCommodityData
                implements IHistoryCommodityData<CachingHistoricalEditorConfig, Float, Void> {
    private Float lastUsed;

    @Override
    public void init(@Nonnull EntityCommodityFieldReference field,
                     @Nullable Float dbValue, @Nonnull CachingHistoricalEditorConfig config,
                     @Nonnull ICommodityFieldAccessor commodityFieldsAccessor) {
        if (lastUsed == null) {
            lastUsed = dbValue;
        }
    }

    @Override
    public void aggregate(@Nonnull EntityCommodityFieldReference field,
                          @Nonnull CachingHistoricalEditorConfig config,
                          @Nonnull ICommodityFieldAccessor commodityFieldsAccessor) {
        // TODO dmitry weighted average with previous value and update the latter, trace log
    }

}

package com.vmturbo.topology.processor.history.histutil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.topology.processor.history.CachingHistoricalEditorConfig;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.IHistoryCommodityData;

/**
 * Per-commodity data for "historical utilization" - one previous point.
 * TODO dmitry provide weights from config
 */
public class HistUtilizationCommodityData
                implements IHistoryCommodityData<CachingHistoricalEditorConfig, Float> {
    private Float lastUsed;

    @Override
    public void init(@Nullable Float dbValue, @Nullable CachingHistoricalEditorConfig config) {
        if (lastUsed == null) {
            lastUsed = dbValue;
        }
    }

    @Override
    public void aggregate(@Nonnull EntityCommodityFieldReference field,
                                         @Nullable CachingHistoricalEditorConfig config) {
        // TODO dmitry weighted average with previous value and update the latter, trace log
    }

}

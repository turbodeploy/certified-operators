package com.vmturbo.topology.processor.history.maxvalue;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.topology.processor.history.CachingHistoricalEditorConfig;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.IHistoryCommodityData;

/**
 * Pre-calculated cache for maximum value over retention period.
 */
public class MaxValueCommodityData implements IHistoryCommodityData<CachingHistoricalEditorConfig, Float, Void> {
    // TODO dmitry handle dbMax expiration
    private Float topologyMax;
    private Float dbMax;

    @Override
    public void aggregate(@Nonnull EntityCommodityFieldReference field,
                          @Nonnull CachingHistoricalEditorConfig config,
                          @Nonnull HistoryAggregationContext context) {
        // TODO dmitry trace log
        float newUsed = Optional.ofNullable(context.getAccessor().getRealTimeValue(field))
                        .orElse(0d).floatValue();
        if (topologyMax == null) {
            topologyMax = newUsed;
        } else {
            topologyMax = Math.max(newUsed, topologyMax);
        }
        float max = Math.max(topologyMax, dbMax == null ? 0f : dbMax);
        context.getAccessor().updateHistoryValue(field, hv -> hv.setMaxQuantity(max),
                                                   MaxValueEditor.class.getSimpleName());
    }

    @Override
    public void init(@Nonnull EntityCommodityFieldReference field,
                     @Nullable Float dbValue,
                     @Nonnull CachingHistoricalEditorConfig config,
                     @Nonnull HistoryAggregationContext context) {
        if (dbValue != null) {
            dbMax = dbValue;
        }
    }

    @Override
    public boolean needsReinitialization(@Nonnull EntityCommodityReference ref,
                    @Nonnull HistoryAggregationContext context,
                    @Nonnull CachingHistoricalEditorConfig cachingHistoricalEditorConfig) {
        // TODO avasin handle dbMax values
        return false;
    }

}

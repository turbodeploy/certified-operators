package com.vmturbo.topology.processor.history.maxvalue;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.topology.processor.history.CachingHistoricalEditorConfig;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.ICommodityFieldAccessor;
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
                          @Nonnull ICommodityFieldAccessor commodityFieldsAccessor) {
        // TODO dmitry trace log
        float newUsed = Optional.ofNullable(commodityFieldsAccessor.getRealTimeValue(field))
                        .orElse(0d).floatValue();
        if (topologyMax == null) {
            topologyMax = newUsed;
        } else {
            topologyMax = Math.max(newUsed, topologyMax);
        }
        float max = Math.max(topologyMax, dbMax == null ? 0f : dbMax);
        commodityFieldsAccessor.updateHistoryValue(field, hv -> hv.setMaxQuantity(max),
                                                   MaxValueEditor.class.getSimpleName());
    }

    @Override
    public void init(@Nonnull EntityCommodityFieldReference field,
                     @Nullable Float dbValue,
                     @Nonnull CachingHistoricalEditorConfig config,
                     @Nonnull ICommodityFieldAccessor commodityFieldsAccessor) {
        if (dbValue != null) {
            dbMax = dbValue;
        }
    }

}

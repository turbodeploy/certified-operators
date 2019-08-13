package com.vmturbo.topology.processor.history.maxvalue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.topology.processor.history.CachingHistoricalEditorConfig;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.IHistoryCommodityData;

/**
 * Pre-calculated cache for maximum value over retention period.
 */
public class MaxValueCommodityData implements IHistoryCommodityData<CachingHistoricalEditorConfig, Float> {
    // TODO dmitry handle dbMax expiration
    private Float topologyMax;
    private Float dbMax;

    @Override
    public void aggregate(@Nonnull EntityCommodityFieldReference field,
                          @Nullable CachingHistoricalEditorConfig config) {
        // TODO dmitry trace log
        float newUsed = (float)field.getSoldBuilder().getUsed();
        if (topologyMax == null) {
            topologyMax = newUsed;
        } else {
            topologyMax = Math.max(newUsed, topologyMax);
        }
        float max = Math.max(topologyMax, dbMax == null ? 0f : dbMax);
        // this is parallelized and builder gets updated
        // TODO dmitry consider per-commodity-instance locking mechanics exposed from AbstractHistoricalEditor
        // (or consider moving all the history editing into stitching and using TopologicalChangelog?)
        synchronized (field.getSoldBuilder()) {
            field.getSoldBuilder().getHistoricalUsedBuilder().setMaxQuantity(max);
        }
    }

    @Override
    public void init(@Nullable Float dbValue,
                     @Nullable CachingHistoricalEditorConfig config) {
        if (dbValue != null) {
            dbMax = dbValue;
        }
    }

}

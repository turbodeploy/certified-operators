package com.vmturbo.topology.processor.history.percentile;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.topology.processor.history.CachingHistoricalEditorConfig;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.IHistoryCommodityData;

/**
 * Pre-calculated per-commodity field cache for percentile data.
 * TODO dmitry provide configuration (percentile params) and db value (array of counts)
 */
public class PercentileCommodityData
                implements IHistoryCommodityData<CachingHistoricalEditorConfig, Void> {
    // TODO dmitry implement
    // private UtilizationCountStore utilizationCounts;

    @Override
    public void aggregate(@Nonnull EntityCommodityFieldReference field,
                                         @Nullable CachingHistoricalEditorConfig config) {
        // TODO dmitry update store and set the percentile score
        // TODO dmitry trace log
    }

    @Override
    public void init(@Nullable Void dbValue, @Nullable CachingHistoricalEditorConfig config) {
        // TODO dmitry update store when db value is read or parameters change
    }

}

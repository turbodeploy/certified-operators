package com.vmturbo.topology.processor.history;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Per-commodity pre-calculated or cached historical data.
 *
 * @param <Config> per-editor type configuration values holder
 * @param <DbValue> the pre-calculated data as retrieved from the persistent store
 */
public interface IHistoryCommodityData<Config, DbValue> {
    /**
     * Aggregate new running usage value from topology with commodity history and update the cache.
     * Update the relevant historical value in the commodity builder.
     *
     * @param field what field is being aggregated and updated
     * @param config configuration (e.g. observation window or averaging weights)
     */
    void aggregate(@Nonnull EntityCommodityFieldReference field, @Nullable Config config);

    /**
     * (Re)initialize the underlying cache according to configuration values and
     * pre-calculated persisted data.
     *
     * @param dbValue previous values loaded from persistent store, if applicable
     * @param config configuration (e.g. observation window)
     */
    void init(@Nullable DbValue dbValue, @Nullable Config config);
}

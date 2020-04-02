package com.vmturbo.topology.processor.history;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Per-commodity pre-calculated or cached historical data.
 *
 * @param <Config> per-editor type configuration values holder
 * @param <DbValue> the pre-calculated data as retrieved from the persistent store
 * @param <CheckpointResult> the result of checkpoint, if applicable
 */
public interface IHistoryCommodityData<Config, DbValue, CheckpointResult> {
    /**
     * Aggregate new running usage value from topology with commodity history and update the cache.
     * Update the relevant historical value in the commodity builder.
     *
     * @param field what field is being aggregated and updated
     * @param config configuration (e.g. observation window or averaging weights)
     * @param context pipeline context, including access to commodity builders
     */
    void aggregate(@Nonnull EntityCommodityFieldReference field, @Nonnull Config config,
                   @Nonnull HistoryAggregationContext context);

    /**
     * (Re)initialize the underlying cache according to configuration values and
     * pre-calculated persisted data.
     *
     * @param field what field is being initialized
     * @param dbValue previous values loaded from persistent store, if applicable
     * @param config configuration (e.g. observation window)
     * @param context pipeline context, including access to commodity builders
     */
    void init(@Nonnull EntityCommodityFieldReference field,
              @Nullable DbValue dbValue, @Nonnull Config config,
              @Nonnull HistoryAggregationContext context);

    /**
     * Perform the periodic maintenance upon checkpoint of the data to the persistent store.
     *
     * @param outdated the values that are going out of observation window
     * @return the result to be persisted, null if n/a
     * @throws HistoryCalculationException when failed to update the backend data
     */
    @Nullable
    default CheckpointResult checkpoint(@Nonnull List<DbValue> outdated) throws HistoryCalculationException {
        return null;
    }
}

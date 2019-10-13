package com.vmturbo.topology.processor.history;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.EntityCommodityReference;

/**
 * Load the history commodity data from the persistent store for given commodity fields references.
 *
 * @param <Config> per-editor type configuration values holder
 * @param <DbValue> the historical data subtype specific value as returned from historydb
 */
public interface IHistoryLoadingTask<Config, DbValue> {
    /**
     * Load the history values for given set of commodity fields from the persistent store (history db).
     *
     * @param commodities collection of commodities to process
     * @param config configuration (e.g. observation window or averaging weights)
     * @return per-entity field values
     * @throws HistoryCalculationException when failed
     * @throws InterruptedException when interrupted
     */
    @Nonnull
    Map<EntityCommodityFieldReference, DbValue>
       load(@Nonnull Collection<EntityCommodityReference> commodities, @Nonnull Config config)
                       throws HistoryCalculationException, InterruptedException;
}

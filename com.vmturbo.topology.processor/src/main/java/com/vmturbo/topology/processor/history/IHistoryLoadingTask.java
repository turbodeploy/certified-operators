package com.vmturbo.topology.processor.history;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.longs.LongSet;

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
     * It is expected that all entities are of the same type.
     * @param commodities collection of commodities to process
     * @param config configuration (e.g. observation window or averaging weights)
     * @param oidsToUse non expired oids that should be used
     * @return per-entity field values
     * @throws HistoryCalculationException when failed
     * @throws InterruptedException when interrupted
     */
    @Nonnull
    Map<EntityCommodityFieldReference, DbValue>
       load(@Nonnull Collection<EntityCommodityReference> commodities, @Nonnull Config config, final LongSet oidsToUse)
                       throws HistoryCalculationException, InterruptedException;
}

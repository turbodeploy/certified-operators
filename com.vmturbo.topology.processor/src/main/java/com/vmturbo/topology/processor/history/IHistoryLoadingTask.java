package com.vmturbo.topology.processor.history;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;

/**
 * Load the history commodity data from the persistent store for given commodity fields references.
 *
 * @param <DbValue> the historical data subtype specific value as returned from historydb
 */
public interface IHistoryLoadingTask<DbValue> {
    /**
     * Load the history values for given set of commodity fields from the persistent store (history db).
     *
     * @param commodities collection of commodities to process
     * @return per-entity field values
     * @throws HistoryCalculationException when failed
     */
    @Nonnull
    Map<EntityCommodityFieldReference, DbValue>
       load(@Nonnull Collection<EntityCommodityReferenceWithBuilder> commodities) throws HistoryCalculationException;
}

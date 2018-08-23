package com.vmturbo.cost.component.pricing;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.cost.component.pricing.PriceTableMerge.PriceTableMergeFactory;

/**
 * A temporary in-memory implementation of the {@link PriceTableStore}.
 */
@ThreadSafe
public class InMemoryPriceTableStore implements PriceTableStore {

    private final PriceTableMergeFactory mergeFactory;

    @GuardedBy("priceTablesLock")
    private final Map<String, PriceTable> priceTablesByProbeCategory = new HashMap<>();

    private final Object priceTablesLock = new Object();

    public InMemoryPriceTableStore(@Nonnull final PriceTableMergeFactory mergeFactory) {
        this.mergeFactory = Objects.requireNonNull(mergeFactory);
    }

    @Nonnull
    @Override
    public PriceTable getMergedPriceTable() {
        final PriceTableMerge mergeOp = mergeFactory.newMerge();
        synchronized (priceTablesLock) {
            // Merging is not a very expensive operation, so we can afford to do it every time
            // instead of caching the global table. In a SQL-backed implementation it may be
            // worth keeping the global table in-memory, since the per-probe price tables only
            // get updated once a day, and it would be good to avoid going to the database.
            return mergeOp.merge(priceTablesByProbeCategory.values());
        }
    }

    @Override
    @Nonnull
    public Optional<PriceTable> putPriceTable(@Nonnull final String probeType, @Nonnull final PriceTable priceTable) {
        synchronized (priceTablesLock) {
            return Optional.ofNullable(priceTablesByProbeCategory.put(probeType, priceTable));
        }
    }
}

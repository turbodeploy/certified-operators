package com.vmturbo.cost.component.stores;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * In-memory single field data store.
 *
 * @param <DataTypeT> type of data.
 * @param <FilterTypeT> The filter type.
 */
@ThreadSafe
public class InMemorySingleFieldDataStore<DataTypeT, FilterTypeT> implements SingleFieldDataStore<DataTypeT, FilterTypeT> {

    private final AtomicReference<DataTypeT> data = new AtomicReference<>();

    private final DataFilterApplicator<DataTypeT, FilterTypeT> filterApplicator;

    /**
     * Constructs a new {@link InMemorySingleFieldDataStore} instance.
     * @param filterApplicator THe filter applicator.
     */
    public InMemorySingleFieldDataStore(@Nonnull DataFilterApplicator<DataTypeT, FilterTypeT> filterApplicator) {
        this.filterApplicator = Objects.requireNonNull(filterApplicator);
    }

    @Nonnull
    @Override
    public Optional<DataTypeT> getData() {
        return Optional.ofNullable(this.data.get());
    }

    @Override
    public Optional<DataTypeT> filterData(FilterTypeT filter) {
        return getData()
                .map(data -> filterApplicator.filterData(data, filter));
    }

    @Override
    public void setData(@Nullable final DataTypeT data) {
        this.data.set(data);
    }
}

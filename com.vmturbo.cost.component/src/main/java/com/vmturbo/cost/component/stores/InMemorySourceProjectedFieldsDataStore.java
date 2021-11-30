package com.vmturbo.cost.component.stores;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * In-memory source and projected fields data store.
 *
 * @param <T> type of data.
 * @param <S> type of store.
 */
@ThreadSafe
public class InMemorySourceProjectedFieldsDataStore<T, S extends SingleFieldDataStore<T>>
        implements SourceProjectedFieldsDataStore<T> {

    private final S sourceSingleFieldDataStore;
    private final S projectedSingleFieldDataStore;

    /**
     * Constructor.
     *
     * @param sourceSingleFieldDataStore the source store.
     * @param projectedSingleFieldDataStore the projected store.
     */
    public InMemorySourceProjectedFieldsDataStore(@Nonnull final S sourceSingleFieldDataStore,
            @Nonnull final S projectedSingleFieldDataStore) {
        this.sourceSingleFieldDataStore = sourceSingleFieldDataStore;
        this.projectedSingleFieldDataStore = projectedSingleFieldDataStore;
    }

    @Nonnull
    @Override
    public Optional<T> getProjectedData() {
        return this.projectedSingleFieldDataStore.getData();
    }

    @Override
    public void setProjectedData(@Nullable final T data) {
        this.projectedSingleFieldDataStore.setData(data);
    }

    @Nonnull
    @Override
    public Optional<T> getSourceData() {
        return this.sourceSingleFieldDataStore.getData();
    }

    @Override
    public void setSourceData(@Nullable final T data) {
        this.sourceSingleFieldDataStore.setData(data);
    }
}

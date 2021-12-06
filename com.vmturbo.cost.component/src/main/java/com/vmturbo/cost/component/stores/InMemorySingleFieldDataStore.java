package com.vmturbo.cost.component.stores;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * In-memory single field data store.
 *
 * @param <T> type of data.
 */
@ThreadSafe
public class InMemorySingleFieldDataStore<T> implements SingleFieldDataStore<T> {

    private final AtomicReference<T> data = new AtomicReference<>();

    @Nonnull
    @Override
    public Optional<T> getData() {
        return Optional.ofNullable(this.data.get());
    }

    @Override
    public void setData(@Nullable final T data) {
        this.data.set(data);
    }
}

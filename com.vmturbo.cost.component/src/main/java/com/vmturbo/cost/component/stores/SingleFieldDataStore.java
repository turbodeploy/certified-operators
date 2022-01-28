package com.vmturbo.cost.component.stores;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Single field data store.
 *
 * @param <T> param of data.
 * @param <FilterTypeT> The type of a filter on {@link T}.
 */
public interface SingleFieldDataStore<T, FilterTypeT> {

    /**
     * Sets the {@link T}.
     *
     * @param data the {@link T}.
     */
    void setData(@Nullable T data);

    /**
     * Gets the {@link T}.
     *
     * @return the {@link T}.
     */
    @Nonnull
    Optional<T> getData();

    /**
     * Applies {@code filter} to {@link #getData()}.
     * @param filter The filter.
     * @return The filtered data.
     */
    @Nonnull
    Optional<T> filterData(FilterTypeT filter);
}
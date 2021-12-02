package com.vmturbo.cost.component.stores;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Single field data store.
 *
 * @param <T> param of data.
 */
public interface SingleFieldDataStore<T> {

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
}
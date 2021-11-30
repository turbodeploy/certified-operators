package com.vmturbo.cost.component.stores;

import java.util.Optional;

import javax.annotation.Nonnull;

/**
 * Source and projected fields data store.
 *
 * @param <T> type of data.
 */
public interface SourceProjectedFieldsDataStore<T> {

    /**
     * Sets the source {@link T}.
     *
     * @param data the {@link T}.
     */
    void setSourceData(@Nonnull T data);

    /**
     * Sets the projected {@link T}.
     *
     * @param data the {@link T}.
     */
    void setProjectedData(@Nonnull T data);

    /**
     * Gets the source {@link T}.
     *
     * @return the {@link T}.
     */
    @Nonnull
    Optional<T> getSourceData();

    /**
     * Gets the projected {@link T}.
     *
     * @return the {@link T}.
     */
    @Nonnull
    Optional<T> getProjectedData();
}

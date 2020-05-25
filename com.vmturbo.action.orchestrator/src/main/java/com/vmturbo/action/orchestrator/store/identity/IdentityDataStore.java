package com.vmturbo.action.orchestrator.store.identity;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;

/**
 * Store for identity metadata.
 *
 * @param <T> identity model data type.
 */
public interface IdentityDataStore<T> {
    /**
     * Fetches OIDs for the specified models if they present in the store.
     *
     * @param models models to retrieve OIDs for
     * @return map of model -> OID for all the models that are present in the store.
     */
    @Nonnull
    Map<T, Long> fetchOids(@Nonnull Collection<T> models);

    /**
     * Persists models with associated OIDs in the store.
     *
     * @param models models and OIDs to store
     */
    void persistModels(@Nonnull Map<T, Long> models);
}

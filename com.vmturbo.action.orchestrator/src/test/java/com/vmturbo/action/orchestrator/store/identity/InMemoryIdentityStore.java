package com.vmturbo.action.orchestrator.store.identity;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;

/**
 * In-memory implementation for identity store. Useful for testing
 *
 * @param <T> identity model type
 */
public class InMemoryIdentityStore<T> implements IdentityDataStore<T> {

    private final Map<T, Long> identityData = new ConcurrentHashMap<>();

    @Nonnull
    @Override
    public Map<T, Long> fetchOids(@Nonnull Collection<T> models) {
        final Map<T, Long> result = new HashMap<>();
        for (T model : models) {
            final Long oid = identityData.get(model);
            if (oid != null) {
                result.put(model, oid);
            }
        }
        return result;
    }

    @Override
    public void persistModels(@Nonnull Map<T, Long> models) {
        identityData.putAll(models);
    }
}

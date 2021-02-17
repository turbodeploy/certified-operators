package com.vmturbo.cost.component.savings;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Implementation for state cache.
 */
class InMemoryEntityStateCache implements EntityStateCache {
    private final Map<Long, EntityState> stateMap = new ConcurrentHashMap<>();

    public boolean containsEntityState(long entityOid) {
        return stateMap.containsKey(entityOid);
    }

    public void setEntityState(@Nonnull final EntityState state) {
        stateMap.put(state.getEntityId(), state);
    }

    @Nullable
    public EntityState getEntityState(long entityOid) {
        return stateMap.get(entityOid);
    }

    @Nullable
    public EntityState removeEntityState(long entityOid) {
        return stateMap.remove(entityOid);
    }

    public int size() {
        return stateMap.size();
    }

    @Nonnull
    public Stream<EntityState> getAll() {
        return stateMap.values().stream();
    }
}

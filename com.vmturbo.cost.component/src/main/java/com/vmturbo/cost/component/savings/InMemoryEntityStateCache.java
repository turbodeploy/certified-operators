package com.vmturbo.cost.component.savings;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Implementation for state cache.
 * TODO Remove this class.
 */
class InMemoryEntityStateCache implements EntityStateCache {
    private final Map<Long, EntityState> stateMap = new ConcurrentHashMap<>();

    @Override
    public void removeInactiveState() {
        stateMap.entrySet().removeIf(entry -> entry.getValue().isDeletePending());
    }

    public int size() {
        return stateMap.size();
    }

    @Nonnull
    public Stream<EntityState> getAll() {
        return stateMap.values().stream();
    }

    // TODO Temporary until entity state persistence is implemented
    @Nullable
    public Map<Long, EntityState> getStateMap() {
        return this.stateMap;
    }
}

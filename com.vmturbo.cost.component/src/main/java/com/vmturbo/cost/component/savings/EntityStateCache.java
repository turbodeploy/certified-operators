package com.vmturbo.cost.component.savings;

import java.util.Map;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Cache interface for keeping entity savings state.
 * TODO Remove inner classes EntityState and SavingsInvestments.
 * TODO Update methods for the SqlEntityStore implementation.
 */
interface EntityStateCache {
    /**
     * Remove all inactive entity states.
     */
    void removeInactiveState();

    // TODO Remove this after we support reading entity state from peristent storage.
    @Nullable
    Map<Long, EntityState> getStateMap();

    /**
     * Current size of state map.
     *
     * @return Size.
     */
    int size();

    /**
     * Gets all entities.
     *
     * @return Stream of all existing entities.
     */
    @Nonnull
    Stream<EntityState> getAll();

}

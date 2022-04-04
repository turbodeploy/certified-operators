package com.vmturbo.cost.component.savings;

import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Interface for reading and writing entity state.
 */
public interface StateStore {
    /**
     * Gets entity state for the given entity from DB if present.
     *
     * @param entityId Entity to get state for.
     * @return EntityState, or null if not found in DB.
     * @throws EntitySavingsException Thrown on DB error.
     */
    @Nullable
    EntityState getEntityState(long entityId) throws EntitySavingsException;

    /**
     * Get stream of all entity states.
     *
     * @param consumer Consumer of states.
     * @throws EntitySavingsException Thrown on DB error.
     */
    void getEntityStates(Consumer<EntityState> consumer) throws EntitySavingsException;

    /**
     * Updates the given state in store.
     *
     * @param state State to update.
     * @throws EntitySavingsException Thrown on DB error.
     */
    void updateEntityState(@Nonnull EntityState state) throws EntitySavingsException;
}

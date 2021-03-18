package com.vmturbo.cost.component.savings;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

/**
 * Interface for read/write of entity states.
 */
public interface EntityStateStore {

    /**
     * Get a map of entity states given a set of entity IDs.
     *
     * @param entityIds a set of entity IDs
     * @return a map of entity Id to entity states
     * @throws EntitySavingsException error during operation
     */
    @Nonnull
    Map<Long, EntityState> getEntityStates(@Nonnull Set<Long> entityIds) throws EntitySavingsException;

    /**
     * Get a map of entities that had state changes in the last period as a result of events.
     *
     * @return a map of entity Id to entity states
     * @throws EntitySavingsException error during operation
     */
    Map<Long, EntityState> getUpdatedEntityStates() throws EntitySavingsException;

    /**
     * Set the updated_by_event values to zeroes.
     *
     * @throws EntitySavingsException error during operation
     */
    void clearUpdatedFlags() throws EntitySavingsException;

    /**
     * Delete entity states given a set of entity IDs.
     *
     * @param entityIds a set of entity IDs
     * @throws EntitySavingsException error during operation
     */
    void deleteEntityStates(@Nonnull Set<Long> entityIds) throws EntitySavingsException;

    /**
     * Update entity states. If the state of the entity is not already in the store, create it.
     *
     * @param entityStateMap entity ID mapped to entity state
     * @throws EntitySavingsException error during operation
     */
    void updateEntityStates(@Nonnull Map<Long, EntityState> entityStateMap) throws EntitySavingsException;

    /**
     * Get all entity states.
     *
     * @return all entity states
     * @throws EntitySavingsException error durring operation
     */
    Stream<EntityState> getAllEntityStates() throws EntitySavingsException;
}

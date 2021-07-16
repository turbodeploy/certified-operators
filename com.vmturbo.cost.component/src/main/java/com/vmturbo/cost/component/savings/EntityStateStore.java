package com.vmturbo.cost.component.savings;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;

/**
 * Interface for read/write of entity states.
 *
 * @param <T> object used for transaction management (e.g. jooq DSLContext)
 */
public interface EntityStateStore<T> {

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
     * Get a map of entity states that need to be processed even without driving events.  This
     * includes all entity states that were updated in the previous calculation pass and entity
     * states that contain an expired action.
     *
     * @param timestamp timestamp of the end of the period being processed
     * @return a map of entity_oid -> EntityState that must be processed.
     * @throws EntitySavingsException when an error occurs
     */
    Map<Long, EntityState> getForcedUpdateEntityStates(LocalDateTime timestamp)
            throws EntitySavingsException;

    /**
     * Set the updated_by_event values to zeroes.
     *
     * @param transaction object used for transaction management
     * @throws EntitySavingsException error during operation
     */
    void clearUpdatedFlags(T transaction) throws EntitySavingsException;

    /**
     * Delete entity states given a set of entity IDs.
     *
     * @param entityIds a set of entity IDs
     * @param transaction object used for transaction management
     * @throws EntitySavingsException error during operation
     */
    void deleteEntityStates(@Nonnull Set<Long> entityIds, T transaction) throws EntitySavingsException;

    /**
     * Update entity states. If the state of the entity is not already in the store, create it.
     *
     * @param entityStateMap entity ID mapped to entity state
     * @param cloudTopology cloud topology
     * @param transaction object used for transaction management
     * @throws EntitySavingsException error during operation
     */
    void updateEntityStates(@Nonnull Map<Long, EntityState> entityStateMap,
                            @Nonnull TopologyEntityCloudTopology cloudTopology,
                            T transaction)
            throws EntitySavingsException;

    /**
     * Get all entity states.
     *
     * @return all entity states
     * @throws EntitySavingsException error durring operation
     */
    Stream<EntityState> getAllEntityStates() throws EntitySavingsException;
}

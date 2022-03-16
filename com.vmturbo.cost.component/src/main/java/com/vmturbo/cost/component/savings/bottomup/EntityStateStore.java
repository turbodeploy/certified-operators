package com.vmturbo.cost.component.savings.bottomup;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopology;

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
     * @param uuids if non-empty, return only the forced entity states that are in the uuid list
     * @return a map of entity_oid -> EntityState that must be processed.
     * @throws EntitySavingsException when an error occurs
     */
    Map<Long, EntityState> getForcedUpdateEntityStates(LocalDateTime timestamp, Set<Long> uuids)
            throws EntitySavingsException;

    /**
     * Set the updated_by_event values to zeroes.
     *
     * @param transaction object used for transaction management
     * @param uuids if non-empty, only clear the entity states that are in the uuid list
     * @throws EntitySavingsException error during operation
     */
    void clearUpdatedFlags(T transaction, Set<Long> uuids) throws EntitySavingsException;

    /**
     * Delete entity states given a set of entity IDs.
     *
     * @param entityIds a set of entity IDs
     * @param transaction object used for transaction management
     * @throws EntitySavingsException error during operation
     */
    void deleteEntityStates(@Nonnull Set<Long> entityIds, T transaction) throws EntitySavingsException;

    /**
     * Update a single existing entity states. This is used by the TEM to updated existing state,
     * and does not affect the cloud topology table.
     *
     * @param entityState entity state to write
     * @throws EntitySavingsException error during operation
     */
    void updateEntityState(@Nonnull EntityState entityState) throws EntitySavingsException;

    /**
     * Update entity states. If the state of the entity is not already in the store, create it.
     *
     * @param entityStateMap entity ID mapped to entity state
     * @param cloudTopology cloud topology
     * @param transaction object used for transaction management
     * @param uuids if non-empty, the list of UUIDs to be updated, else all UUIDs will be updated
     * @throws EntitySavingsException error during operation
     */
    void updateEntityStates(@Nonnull Map<Long, EntityState> entityStateMap,
            @Nonnull TopologyEntityCloudTopology cloudTopology,
            T transaction, @Nonnull Set<Long> uuids)
            throws EntitySavingsException;

    /**
     * Get all entity states.
     *
     * <p>NOTE: This stream must be closed through try-with-resource, as it may represent underlying
     * database connections.
     *
     * @return all entity states
     * @throws EntitySavingsException error during operation
     */
    Stream<EntityState> getAllEntityStates() throws EntitySavingsException;

    /**
     * Get all entity states. Use this method to pass in a transaction object so this method will
     * return states that are updated or added in the transaction even before they are committed.
     *
     * <p>NOTE: This stream must be closed through try-with-resource, as it may represent underlying
     * database connections.
     *
     * @param transaction object used for transaction management
     * @return all entity states
     * @throws EntitySavingsException error during operation
     */
    Stream<EntityState> getAllEntityStates(T transaction) throws EntitySavingsException;
}

package com.vmturbo.topology.processor.targets;

import java.util.List;

import javax.annotation.Nonnull;

/**
 * Object to abstract away the storing of {@link Target} objects.
 * This object is underneath the {@link TargetStore} to take care of simple, dumb persistence of
 * {@link Target}s. It should not contain other business logic.
 */
public interface TargetDao {

    /**
     * Get all {@link Target}s in the DAO.
     *
     * @return The list of {@link Target} objects.
     */
    List<Target> getAll();

    /**
     * Store a {@link Target} in the DAO.
     *
     * @param target The {@link Target} to store.
     */
    void store(@Nonnull Target target);

    /**
     * Remove a target from the DAO, by ID.
     *
     * @param targetId The {@link Target} to remove.
     */
    void remove(long targetId);
}

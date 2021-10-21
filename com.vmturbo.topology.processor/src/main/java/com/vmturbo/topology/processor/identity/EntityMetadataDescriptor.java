package com.vmturbo.topology.processor.identity;

import java.util.Collection;

/**
 * The VMTEntityMetadataDescriptor implements entity metadata descriptor.
 */
public interface EntityMetadataDescriptor {

    /**
     * Returns the metadata version.
     *
     * @return The metadata version.
     */
    int getMetadataVersion();

    /**
     * Returns the entity type.
     *
     * @return The entity type.
     */
    String getEntityType();

    /**
     * Returns the target type.
     *
     * @return The target type.
     */
    String getTargetType();

    /**
     * Returns the ranks for the Identifying property set.
     *
     * @return The Identifying property set.
     */
    Collection<Integer> getIdentifyingPropertyRanks();

    /**
     * Returns the ranks for the Volatile identifying property set.
     *
     * @return The Volatile identifying property set.
     */
    Collection<Integer> getVolatilePropertyRanks();

    /**
     * Returns the ranks for the NonVolatile identifying property set.
     *
     * @return The Volatile identifying property set.
     */
    Collection<Integer> getNonVolatilePropertyRanks();

    /**
     * Returns ranks for the Heuristic property set.
     *
     * @return The Heuristic property set.
     */
    Collection<Integer> getHeuristicPropertyRanks();

    /**
     * Get the threshold for what percentage of heuristic properties must match
     * for an entity to be considered the same when heuristic matching.
     *
     * @return The heuristic threshold. A percentage. Value is in the range [0,100]
     */
    int getHeuristicThreshold();
}

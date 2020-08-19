package com.vmturbo.topology.graph;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A {@link TopologyGraphEntity} which supports searches. Not all entity implementations support
 * searching - some may only be required for supply chain traversal (or other purposes).
 *
 * @param <E> The type of entity.
 */
public interface TopologyGraphSearchableEntity<E extends TopologyGraphSearchableEntity<E>> extends TopologyGraphEntity<E> {

    /**
     * Get additional searchable properties for this entity.
     *
     * @param clazz The expected type of the entity searchable properties (associated with the type
     *              of the entity).
     * @param <S> The type of the entity searchable properties.
     * @return The {@link SearchableProps} for this particular entity type.
     */
    @Nullable
    default <S extends SearchableProps> S getSearchableProps(@Nonnull Class<S> clazz) {
        SearchableProps props = getSearchableProps();
        if (clazz.isInstance(props)) {
            return (S)props;
        } else {
            return null;
        }
    }

    /**
     * Get the searchable properties of this entity.
     *
     * @return The {@link SearchableProps}.
     */
    @Nonnull
    SearchableProps getSearchableProps();
}

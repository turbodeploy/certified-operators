package com.vmturbo.topology.graph;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A {@link TopologyGraphEntity} which supports searches. Not all entity implementations support
 * searching - some may only be required for supply chain traversal (or other purposes).
 *
 * @param <E> The type of entity.
 */
public interface TopologyGraphSearchableEntity<E extends TopologyGraphSearchableEntity> extends TopologyGraphEntity<E> {

    /**
     * Get additional searchable properties for this entity.
     *
     * @param clazz The expected type of the entity searchable properties (associated with the type
     *              of the entity).
     * @param <T> The type of the entity searchable properties.
     * @return The {@link SearchableProps} for this particular entity type.
     */
    @Nullable
    <T extends SearchableProps> T getSearchableProps(@Nonnull Class<T> clazz);

    default SearchableProps getSearchableProps() {
        return getSearchableProps(SearchableProps.class);
    }

}

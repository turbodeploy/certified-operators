package com.vmturbo.topology.graph.search.filter;

import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.topology.graph.SearchableProps;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphSearchableEntity;

/**
 * A property filter selects members of the topology that match
 * some predicate.
 *
 * All TopologyFilter operations must be safe to run on parallel streams.
 * That is, no filter application may maintain any internal state that would
 * make its {@link #apply(Stream, TopologyGraph)} method unsafe to run concurrently.
 *
 * @param <E> {@inheritDoc}
 */
@Immutable
public class PropertyFilter<E extends TopologyGraphSearchableEntity<E>> implements TopologyFilter<E> {
    private final Predicate<E> test;

    /**
     * Create a new property filter from a predicate.
     *
     * @param test The predicate.
     */
    public PropertyFilter(@Nonnull final Predicate<E> test) {
        this.test = Objects.requireNonNull(test);
    }

    /**
     * Create a new property filter that will return false if the input entity is not of the
     * specified type, and will apply the predicate to the entity's searchable properties if
     * it is. This allows the user to specify which {@link SearchableProps} they want to filter on
     * in the predicate.
     *
     * @param propsTest The predicate for the searchable properties.
     * @param clazz The class of {@link SearchableProps} (which is tied to the type of the entity).
     * @param <E> The type of searchable entity.
     * @param <T> The type of {@link SearchableProps} for the target entity type.
     * @return A {@link PropertyFilter} that can be used to filter entities. It will return false
     *         if the type doesn't match. If the type matches it will apply the predicate to
     *         the {@link SearchableProps}.
     */
    public static <E extends TopologyGraphSearchableEntity<E>, T extends SearchableProps> PropertyFilter<E> typeSpecificFilter(Predicate<T> propsTest, Class<T> clazz) {
        return new PropertyFilter<>(e -> {
            T props = e.getSearchableProps(clazz);
            if (props == null) {
                return false;
            } else {
                return propsTest.test(props);
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Stream<E> apply(@Nonnull final Stream<E> vertices,
                           @Nonnull final TopologyGraph<E> graph) {
        return vertices.filter(test);
    }

    /**
     * Test if a particular {@link TopologyGraphSearchableEntity} passes the filter.
     *
     * @param entity The {@link TopologyGraphSearchableEntity} to test against the filter.
     * @return True if the {@link TopologyGraphSearchableEntity} passes the filter, false otherwise.
     */
    @VisibleForTesting
    boolean test(@Nonnull E entity, @Nonnull final TopologyGraph<E> graph) {
        return apply(Stream.of(entity), graph).findFirst().isPresent();
    }
}

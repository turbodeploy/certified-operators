package com.vmturbo.topology.graph.search.filter;

import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphEntity;

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
public class PropertyFilter<E extends TopologyGraphEntity<E>> implements TopologyFilter<E> {
    private final Predicate<E> test;

    public PropertyFilter(@Nonnull final Predicate<E> test) {
        this.test = Objects.requireNonNull(test);
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
     * Test if a particular {@link TopologyGraphEntity} passes the filter.
     *
     * @param entity The {@link TopologyGraphEntity} to test against the filter.
     * @return True if the {@link TopologyGraphEntity} passes the filter, false otherwise.
     */
    boolean test(@Nonnull E entity) {
        return test.test(entity);
    }

    /**
     * Get the predicate that will be used by this filter.
     *
     * @return The predicate that will be used by this filter.
     */
    public Predicate<E> getTest() {
        return test;
    }
}

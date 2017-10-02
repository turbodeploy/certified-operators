package com.vmturbo.topology.processor.group.filter;

import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.topology.processor.topology.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyGraph.Vertex;

/**
 * A property filter selects members of the topology that match
 * some predicate.
 *
 * All TopologyFilter operations must be safe to run on parallel streams.
 * That is, no filter application may maintain any internal state that would
 * make its {@link #apply(Stream, TopologyGraph)} method unsafe to run concurrently.
 */
public class PropertyFilter implements TopologyFilter {
    private final Predicate<Vertex> test;

    public PropertyFilter(@Nonnull final Predicate<Vertex> test) {
        this.test = Objects.requireNonNull(test);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Stream<Vertex> apply(@Nonnull final Stream<Vertex> vertices,
                                @Nonnull final TopologyGraph graph) {
        return vertices.filter(test::test);
    }

    /**
     * Test if a particular vertex passes the filter.
     *
     * @param vertex The vertex to test against the filter.
     * @return True if the vertex passes the filter, false otherwise.
     */
    boolean test(@Nonnull Vertex vertex) {
        return test.test(vertex);
    }

    /**
     * Get the predicate that will be used by this filter.
     *
     * @return The predicate that will be used by this filter.
     */
    public Predicate<Vertex> getTest() {
        return test;
    }
}

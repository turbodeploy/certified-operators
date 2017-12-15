package com.vmturbo.topology.processor.group.filter;

import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.topology.processor.topology.TopologyEntity;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * A property filter selects members of the topology that match
 * some predicate.
 *
 * All TopologyFilter operations must be safe to run on parallel streams.
 * That is, no filter application may maintain any internal state that would
 * make its {@link #apply(Stream, TopologyGraph)} method unsafe to run concurrently.
 */
public class PropertyFilter implements TopologyFilter {
    private final Predicate<TopologyEntity> test;

    public PropertyFilter(@Nonnull final Predicate<TopologyEntity> test) {
        this.test = Objects.requireNonNull(test);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Stream<TopologyEntity> apply(@Nonnull final Stream<TopologyEntity> vertices,
                                @Nonnull final TopologyGraph graph) {
        return vertices.filter(test::test);
    }

    /**
     * Test if a particular {@link TopologyEntity} passes the filter.
     *
     * @param entity The {@link TopologyEntity} to test against the filter.
     * @return True if the {@link TopologyEntity} passes the filter, false otherwise.
     */
    boolean test(@Nonnull TopologyEntity entity) {
        return test.test(entity);
    }

    /**
     * Get the predicate that will be used by this filter.
     *
     * @return The predicate that will be used by this filter.
     */
    public Predicate<TopologyEntity> getTest() {
        return test;
    }
}

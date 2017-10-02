package com.vmturbo.topology.processor.group.filter;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.topology.processor.topology.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyGraph.Vertex;

/**
 * A generic filter that can be applied to a stream of vertices in the graph
 * to produce an output stream of vertices where the output stream members match
 * some criteria associated with the filter.
 *
 * Note that the size of the output stream may be smaller than, the same size as,
 * or larger than the input stream.
 *
 * Filters are designed to be composed together to resolveDynamicGroup vertices (entities)
 * that match specific criteria.
 *
 * See Search.proto for the criteria used to define filters.
 *
 * IMPORTANT NOTE FOR ALL IMPLEMENTING SUBCLASSES:
 * All TopologyFilter operations must be safe to run on parallel streams.
 * That is, no filter application may maintain any internal state that would
 * make its {@link #apply(Stream, TopologyGraph)} method unsafe to run concurrently.
 */
@FunctionalInterface
public interface TopologyFilter {
    /**
     * Apply the filter to the stream input vertices to generate a stream of output vertices
     * that match certain criteria related to the filter instance.
     *
     * This method is safe to run on a parallel stream.
     *
     * @param vertices A stream of vertices in the topology. Note that the output stream members
     *                   are NOT guaranteed to be members of the input stream for all types of filters
     *                   (ie {@link TraversalFilter} and its descendants).
     * @param graph The topology graph that can be used for lookups during filtering.
     *              Note that the {@link TopologyGraph} is immutable.
     * @return A stream of entity OIDs whose members match the criteria associated with this filter
     *         instance.
     */
    @Nonnull
    Stream<Vertex> apply(@Nonnull Stream<Vertex> vertices, @Nonnull final TopologyGraph graph);
}

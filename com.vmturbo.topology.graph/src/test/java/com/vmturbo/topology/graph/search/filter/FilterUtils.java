package com.vmturbo.topology.graph.search.filter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.topology.graph.TestGraphEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * A set of utilities for testing filters.
 */
public class FilterUtils {
    /**
     * Prevent instantiation because contains only static helpers
     */
    private FilterUtils() { }

    /**
     * Filter a list of OIDs over a graph using a given filter to generate collection of other OIDs.
     *
     * @param filter The filter to use.
     * @param graph The graph to filter over.
     * @param startingPoints The starting points of the filter operation.
     *
     * @return A collection of OIDs for the vertices in the graph that matched the filter.
     */
    public static Collection<Long> filterOids(@Nonnull final TopologyFilter<TestGraphEntity> filter,
                                              @Nonnull final TopologyGraph<TestGraphEntity> graph, long... startingPoints) {
        Stream<TestGraphEntity> startingVertices = Arrays.stream(startingPoints)
            .mapToObj(l -> l)
            .map(graph::getEntity)
            .filter(Optional::isPresent)
            .map(Optional::get);

        return filter.apply(startingVertices, graph)
            .map(TestGraphEntity::getOid)
            .collect(Collectors.toList());
    }

}

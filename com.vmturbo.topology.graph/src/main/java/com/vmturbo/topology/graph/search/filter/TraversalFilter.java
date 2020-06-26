package com.vmturbo.topology.graph.search.filter;

import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition.VerticesCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphSearchableEntity;

/**
 * A filter that traverses the topology graph and inserts certain visited nodes
 * into the output stream. The criteria for stopping the traversal and accepting a node
 * as belonging to the output stream are specific to implementing subclasses.
 *
 * The direction of the traversal direction is fixed at construction time.
 *
 * As we have defined the API, traversals may be defined that:
 *
 * 1. Start from a fixed set of nodes in the graph.
 * 2. Follows edges in only a single direction (either produces or consumes)
 * 3. Include nodes in the result set based on either:
 *    a. Being exactly n hops in the chosen direction from a node in the starting set.
 *    b. Having a property that matches some user-defined predicate
 *       (i.e. entityType == VirtualMachine, displayName matches some regex, etc.)
 *
 * @param <E> {@inheritDoc}
 */
public abstract class TraversalFilter<E extends TopologyGraphSearchableEntity<E>> implements TopologyFilter<E> {

    protected final TraversalDirection traversalDirection;

    public TraversalFilter(@Nonnull final TraversalDirection traversalDirection) {
        this.traversalDirection = Objects.requireNonNull(traversalDirection);
    }

    /**
     * Get the traversal direction for this filter.
     *
     * @return The traversal direction for this filter.
     */
    public TraversalDirection getTraversalDirection() {
        return traversalDirection;
    }

    /**
     * Get a method that looks up neighbors in the graph according to the traversal's direction.
     *
     * @param graph The graph on which to perform lookups.
     * @return A method that can be used to lookup neighbors for a particular vertex in the graph.
     */
    protected Function<E, Stream<E>> neighborLookup(@Nonnull final TopologyGraph<E> graph) {
        // Note the difference between noun and verb here.
        // Following the CONSUMES relation is done by fetching producers,
        // and following the PRODUCES relation is done by fetching consumers.
        switch (traversalDirection) {
            case CONSUMES:
                return graph::getProviders;
            case PRODUCES:
                return graph::getConsumers;
            case CONNECTED_FROM:
                return e ->
                    Stream.concat(graph.getOwnersOrAggregators(e),
                                  e.getInboundAssociatedEntities().stream());
            case CONNECTED_TO:
                return e ->
                    Stream.concat(graph.getOwnedOrAggregatedEntities(e),
                                  e.getOutboundAssociatedEntities().stream());
            case AGGREGATED_BY:
                return graph::getOwnersOrAggregators;
            case AGGREGATES:
                return graph::getOwnedOrAggregatedEntities;
            case OWNED_BY:
                return graph::getOwner;
            case OWNS:
                return graph::getOwnedEntities;
            case CONTROLLED_BY:
                return graph::getControllers;
            case CONTROLS:
                return graph::getControlledEntities;
            default:
                throw new UnsupportedOperationException("Unsupported traversal direction: " +
                        traversalDirection);
        }
    }

    /**
     * Apply a filter that traverses the topology graph and inserts certain visited nodes
     * into the output stream. The criteria for stopping the traversal and accepting a node
     * as belonging to the output stream are specific to implementing subclasses.
     *
     * As an optimization, traversals are filtered to distinct values.
     *
     * @param vertices A stream of vertices in the topology. Note that the output stream members
     *                   are NOT guaranteed to be members of the input stream.
     * @param graph The topology graph that can be used for lookups during filtering.
     *              Note that the {@link TopologyGraph} is immutable.
     * @return A stream of entity OIDs whose members match the traversal criteria
     */
    @Nonnull
    @Override
    public Stream<E> apply(@Nonnull final Stream<E> vertices, @Nonnull final TopologyGraph<E> graph) {
        return traverse(vertices, graph).distinct();
    }

    /**
     * Apply a filter that traverses the topology graph and inserts certain visited nodes
     * into the output stream. The criteria for stopping the traversal and accepting a node
     * as belonging to the output stream are specific to implementing subclasses.
     *
     * @param vertices A stream of vertices in the topology. Note that the output stream members
     *                   are NOT guaranteed to be members of the input stream.
     * @param graph The topology graph that can be used for lookups during filtering.
     *              Note that the {@link TopologyGraph} is immutable.
     * @return A stream of entity OIDs whose members match the traversal criteria
     */
    @Nonnull
    protected abstract Stream<E> traverse(@Nonnull final Stream<E> vertices,
                                             @Nonnull final TopologyGraph<E> graph);

    /**
     * Traverse a topology graph in a fixed direction to a fixed depth. Only include entities
     * visited at exactly depth == traversalDepth when performing the traversal. But if
     * VerticesCondition is set, it will check the number of connected vertices of the given
     * entity type at the given depth, if it matches, then the starting vertex is included.
     *
     * @param <E>  The type of entity.
     */
    public static class TraversalToDepthFilter<E extends TopologyGraphSearchableEntity<E>> extends TraversalFilter<E> {
        private final int traversalDepth;

        private final VerticesCondition verticesCondition;

        public TraversalToDepthFilter(@Nonnull final TraversalDirection traversalDirection,
                final int traversalDepth, @Nullable VerticesCondition verticesCondition) {
            super(traversalDirection);
            if (traversalDepth < 0) {
                throw new IllegalArgumentException("Negative traversal depth" + traversalDepth + " not allowed.");
            }

            this.traversalDepth = traversalDepth;
            this.verticesCondition = verticesCondition;
        }

        /**
         * Traverse a graph to a fixed depth. Only include entities encountered at exactly depth == traversalDepth
         * when performing the traversal. Traversal stops when depth == traversalDepth.
         * Consider the following topology (links below E,F,G are not shown):
         *
         *   A
         *   |
         *   B   C  D
         *    \ /   |
         *     E    F   G
         *         ...
         *
         * where the input to {@link #traverse(Stream, TopologyGraph)} traversalDirection==PRODUCES is (E, F, G)
         * At traversalDepth == 0, then the output stream will be (E, F, G)
         * At traversalDepth == 1, then the output stream will be (B, C, D)
         * At traversalDepth == 2, then the output stream will be (A)
         * At traversalDepth >= 3, then the output stream will be empty
         *
         * @param vertices A stream of vertices in the topology. Note that the output stream members
         *                 are NOT guaranteed to be members of the input stream.
         * @param graph The topology graph that can be used for lookups during filtering.
         *              Note that the {@link TopologyGraph} is immutable.
         * @return The stream of vertices at traversalDepth matching this instance's traversalDepth from any
         *         of the vertices in the input stream.
         */
        @Nonnull
        @Override
        protected Stream<E> traverse(@Nonnull Stream<E> vertices,
                                                  @Nonnull final TopologyGraph<E> graph) {
            if (verticesCondition != null) {
                // Filter original input vertices by number of connected vertices in a depth.
                // This is meant to return members of the original stream which satisfied the given
                // verticesCondition (connected to a specific # of entities of a specific entity
                // type at the traversal depth)
                return vertices.filter(vertex -> {
                    Stream<E> nodesForDepth = getNodesForDepth(Stream.of(vertex), graph);
                    // check the number of connected vertices and return true if it matches
                    final NumericFilter numConnectedVerticesFilter = verticesCondition.getNumConnectedVertices();
                    long connectedVerticesCount = nodesForDepth.filter(entity -> entity.getEntityType() ==
                            verticesCondition.getEntityType()).count();
                    return comparisonValueMatch(connectedVerticesCount, numConnectedVerticesFilter.getValue(),
                            numConnectedVerticesFilter.getComparisonOperator());
                });
            }
            return getNodesForDepth(vertices, graph);
        }

        /**
         * Given stream lazy evaluation, performs a DFS to a fixed depth and collects all nodes
         * at exactly depth==traversalDepth.
         *
         * @param vertices the vertices to start with
         * @param graph the graph to traverse
         * @return stream of nodes for a given depth
         */
        private Stream<E> getNodesForDepth(@Nonnull Stream<E> vertices,
                                                        @Nonnull final TopologyGraph<E> graph) {
            for (int i = 0; i < traversalDepth; i++) {
                vertices = vertices.flatMap(neighborLookup(graph));
            }
            return vertices;
        }
    }

    /**
     * Check if the actual value matches the expected value for the given ComparisonOperator.
     */
    private static boolean comparisonValueMatch(final long actualValue, final long expectedValue,
            @Nonnull final ComparisonOperator operator) {
        switch (operator) {
            case EQ:
                return actualValue == expectedValue;
            case NE:
                return actualValue != expectedValue;
            case GT:
                return actualValue > expectedValue;
            case GTE:
                return actualValue >= expectedValue;
            case LT:
                return actualValue < expectedValue;
            case LTE:
                return actualValue <= expectedValue;
            default:
                throw new IllegalArgumentException("Unknown operator type: " + operator);
        }
    }

    /**
     * Traverse a topology graph in a fixed direction until entities of a given type are reached.
     *
     * @param <E> The type of entity.
     */
    public static class TraversalToPropertyFilter<E extends TopologyGraphSearchableEntity<E>> extends TraversalFilter<E> {

        /**
         * The maximum depth permitted to recurse to when traversing for entities of a given type.
         * This is used as a sanity check against supply chains deeper than the max call stack
         * (something that should never happen in practice).
         */
        public final static int MAX_RECURSION_DEPTH = 128;

        private final PropertyFilter stoppingFilter;

        private static final Logger logger = LogManager.getLogger();

        /**
         * Construct a new traversal filter.
         *
         * @param traversalDirection The direction in which to traverse.
         * @param stoppingFilter A filter that can be used to test when traversal should stop.
         */
        public TraversalToPropertyFilter(@Nonnull final TraversalDirection traversalDirection,
                                         @Nonnull final PropertyFilter stoppingFilter) {
            super(traversalDirection);
            this.stoppingFilter = stoppingFilter;
        }

        /**
         * Traverse a topology graph in a fixed direction until entities matching a given property are reached.
         * Traversal will continue until either reaching a leaf or visiting an that passes the stopping filter.
         * Consider the following topology (links below E,F,G are not shown):
         *
         *  VM-Z
         *    |
         * VDC-A
         *    | \
         * VDC-B VDC-C  VDC-D  VM-E
         *     \ /      |        |
         *    PM-E     PM-F    PM-G
         *         ...
         *
         * where the input to {@link #traverse(Stream, TopologyGraph)} for traversalDirection==PRODUCES is (E, F, G)
         *
         * For a propertyFilter that stops when entityType==PM, then the output stream will be (E, F, G)
         * For a propertyFilter that stops when entityType==VDC, then the output stream will be (B, C, D)
         * NOTE: although A is a VDC, it will not be visited because traversal from E stops at B.
         * For a propertyFilter that stops when entityType==VM, then the output stream will be (Z,E)
         *
         * TODO: Note that stopping at the first match makes it impossible to, for example,
         *       reach VDC-A from VDC-B or vice-versa.
         *
         * @param vertices A stream of vertices in the topology. Note that the output stream members
         *                 are NOT guaranteed to be members of the input stream.
         * @return The stream of entities reachable from the input stream in the given direction whose entityType
         *         matches while traversing until finding a matching entityType.
         */
        @Nonnull
        @Override
        protected Stream<E> traverse(@Nonnull Stream<E> vertices, @Nonnull final TopologyGraph<E> graph) {
            return vertices
                .flatMap(vertex -> traverseToProperty(vertex, neighborLookup(graph), 0));
        }

        /**
         * Traverse the topology graph in a specific direction until reaching a vertices that match
         * the stopping filter.
         *
         * Will exit the traversal early if the recursion exceeds a maximum depth. In the real world,
         * supply chains tend to be fairly shallow and should never approach this depth.
         *
         * @param entity The vertex from which to begin/continue the traversal.
         * @param neighborLookup The method to use to lookup neighbors in a given direction.
         * @param traversalNesting The current recursion depth of the traversal.
         * @return A stream of all vertices in the graph that match the stoppingFilter reachable
         *         from the source in the given direction (where traversal stops at vertices that match
         *         the stoppingFilter).
         */
        private Stream<E> traverseToProperty(@Nonnull final E entity,
                                                  @Nonnull final Function<E, Stream<E>> neighborLookup,
                                                  int traversalNesting) {
            if (stoppingFilter.test(entity)) {
                return Stream.of(entity);
            } else {
                if (traversalNesting >= MAX_RECURSION_DEPTH) {
                    logger.error("Maximum chain depth exceeded in traversal. Prematurely ending traversal.");
                    return Stream.empty(); // Do not exceed maximum recursion depth
                } else {
                    return neighborLookup.apply(entity)
                        .flatMap(neighbor -> traverseToProperty(neighbor, neighborLookup, traversalNesting + 1));
                }
            }
        }
    }
}

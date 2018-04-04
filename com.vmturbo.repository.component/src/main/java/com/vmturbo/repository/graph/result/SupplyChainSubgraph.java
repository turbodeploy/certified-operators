package com.vmturbo.repository.graph.result;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode.MemberList;
import com.vmturbo.repository.constant.RepoObjectState;

/**
 * An in-memory graph built from supply chain queries used for traversal in order to compute the actual
 * supply chain for display.
 *
 * The graph is actually a subgraph of the whole topology consisting of the vertices reachable from
 * a particular starting point in the supply chain.
 */
@Immutable
public class SupplyChainSubgraph {
    /**
     * A map permitting lookup from OID to {@link SupplyChainVertex}.
     */
    private final Map<String, SupplyChainVertex> graph;

    /**
     * The starting vertex for the traversal that generated the subgraph.
     */
    private final String startingVertexId;

    /**
     * The entity type of the startingVertex.
     */
    private final String startingVertexEntityType;

    /**
     * A function that, given a vertex, returns the neighbors for that vertex in a particular direction
     * (ie providers or consumers).
     */
    @FunctionalInterface
    private interface NeighborFunction {
        @Nonnull
        List<SupplyChainVertex> neighborsFor(@Nonnull final SupplyChainVertex vertex);
    }

    /**
     * Create a new subgraph instance. The subgraph contains the section of the topology topology
     * reachable from a particular starting point in the supply chain.
     *
     * The origin entity on the input subgraph results must be the same.
     *
     * @param providersResult The edges for providers leading out from the starting vertex.
     * @param consumersResult The edges for consumers leading out from the starting vertex.
     */
    public SupplyChainSubgraph(@Nonnull final SubgraphResult providersResult,
                               @Nonnull final SubgraphResult consumersResult) {
        final ResultVertex providerOrigin = providersResult.getOrigin();
        Preconditions.checkArgument(providerOrigin.equals(consumersResult.getOrigin()));

        startingVertexId = providerOrigin.getId();
        startingVertexEntityType = providerOrigin.getEntityType();

        graph = new HashMap<>();
        graph.put(startingVertexId, new SupplyChainVertex(providerOrigin));

        addNeighbors(providersResult);
        addNeighbors(consumersResult);
    }

    /**
     * Get the id of the starting vertex. The starting vertex is the vertex from which
     * traversal was performed to build the subgraph.
     *
     * @return the id of the starting vertex.
     */
    public String getStartingVertexId() {
        return startingVertexId;
    }

    /**
     * Get the entity type of the starting vertex. The starting vertex is the vertex from which
     * traversal was performed to build the subgraph.
     *
     * @return the entity type of the starting vertex.
     */
    public String getStartingVertexEntityType() {
        return startingVertexEntityType;
    }


    /**
     * Get a collection of {@link SupplyChainNode}s for the supply chain starting from the starting vertex
     * for this subgraph.
     *
     * The single-source supply chain algorithm can essentially be described as follows:
     * starting from a single node in the graph, traverse outwards in both provider and
     * consumer directions in a breadth-first fashion. The first time you encounter an
     * entity of a given type, count the number of entities of that type which are
     * reachable from the start and mark ALL entities of that type as counted. If you
     * again visit an entity of a counted type, ignore it from your total count. At the
     * end of the traversal, return the counts for each entity type that were reached
     * during the traversal.
     *
     * Note that we make an exception for entities that buy from entities of the same type
     * (ie VDC's). In the specific case where an entity buys from its same type, we do
     * not count the depth as increasing and instead include all entities buying from
     * each other of that same type.
     *
     * @return {@link SupplyChainNode}s for the supply chain starting from the starting vertex.
     */
    public List<SupplyChainNode> toSupplyChainNodes() {
        /**
         * A map of entityType -> SupplyChainNode of that entity type.
         * Entities are added to the nodes in the nodeMap as the BFS proceeds.
         */
        final Map<String, SupplyChainNodeBuilder> nodeMap = new HashMap<>();

        /**
         * Perform a breadth-first-search starting from the starting node.
         * The first time we hit an entity of a certain type, add a {@link SupplyChainNode} to our nodeMap
         * for that type. If a node already exists for that type, it indicates we should skip entities of that
         * type.
         */
        final SupplyChainVertex startingVertex = graph.get(startingVertexId);

        final Deque<VertexAndNeighbor> frontier = new ArrayDeque<>();

        // Traverse outward from the starting vertex to collect supply chain providers
        frontier.add(new VertexAndNeighbor(startingVertex, null));
        traverseSupplyChainBFS(nodeMap, frontier, SupplyChainVertex::getProviders, 1);

        // Traverse outward from the starting vertex to collect supply chain consumers
        // Start from the starting vertex consumers because the starting vertex itself
        // was added in the producers traversal.
        frontier.addAll(Lists.transform(startingVertex.getConsumers(),
            consumer -> new VertexAndNeighbor(consumer, startingVertex)));
        traverseSupplyChainBFS(nodeMap, frontier, SupplyChainVertex::getConsumers, 1);

        return nodeMap.values().stream()
            .map(nodeBuilder -> nodeBuilder.buildNode(graph))
            .collect(Collectors.toList());
    }

    /**
     * Perform a breadth-first-search traversal starting from the nodes contained in the frontier {@link Deque}.
     *
     * @param nodeMap          The map of OID->SupplyChainNode builders containing the results being built
     *                         from the BFS traversal. Vertices in the graph of a type that already has an entry in the nodeMap
     *                         are skipped.
     * @param frontier         The traversal frontier for the BFS.
     *                         Frontier contains the entities being traversed at the current depth of the BFS.
     * @param neighborFunction The function that, given a vertex, can retrieve the neighbors for the vertex in
     *                         a particular direction (ie provider or consumer neighbors).
     * @param currentDepth     The current depth of the BFS (ie how many hops we are from the starting vertex).
     */
    private void traverseSupplyChainBFS(@Nonnull final Map<String, SupplyChainNodeBuilder> nodeMap,
                                        @Nonnull final Deque<VertexAndNeighbor> frontier,
                                        @Nonnull final NeighborFunction neighborFunction,
                                        final int currentDepth) {
        // nextFrontier are the entities to be traversed at depth+1.
        final Deque<VertexAndNeighbor> nextFrontier = new ArrayDeque<>();
        final Set<String> visitedEntityTypes = new HashSet<>();
        visitedEntityTypes.addAll(nodeMap.keySet());

        while (!frontier.isEmpty()) {
            final VertexAndNeighbor vertexAndNeighbor = frontier.removeFirst();
            final SupplyChainVertex vertex = vertexAndNeighbor.vertex;

            /** Only add a node when we have not already visited an entity of the same type
             *  or if the connection corresponds to a "self-loop" where an entity buys from
             *  or sells to an entity of the same type (see {@link #toSupplyChainNodes})
             */
            if (!visitedEntityTypes.contains(vertex.getEntityType()) || vertexAndNeighbor.sameEntityTypes()) {
                nextFrontier.addAll(neighborFunction.neighborsFor(vertex).stream()
                        .map(neighbor -> new VertexAndNeighbor(neighbor, vertex))
                        .collect(Collectors.toList()));
                final SupplyChainNodeBuilder nodeBuilder =
                        nodeMap.computeIfAbsent(vertex.getEntityType(), entityType -> new SupplyChainNodeBuilder());

                nodeBuilder.setSupplyChainDepth(currentDepth);
                nodeBuilder.setEntityType(vertex.getEntityType());
                nodeBuilder.addMember(vertex.getOid(), vertex.getState());
            }
        }

        // Recursively add nodes in the next frontier.
        // Supply chains have small enough depth that we don't need to worry about stack overflow.
        if (!nextFrontier.isEmpty()) {
            traverseSupplyChainBFS(nodeMap, nextFrontier, neighborFunction, currentDepth + 1);
        }
    }

    /**
     * Add neighbors from the {@link com.vmturbo.repository.graph.result.SupplyChainSubgraph.SubgraphResult} to
     * {@link this#graph} by traversing the edges.
     *
     * Add connections in both the provider and consumer directions.
     *
     * @param subgraphResult the result whose edges should be traversed to add neighbors to the graph
     *                       internal to this {@link SupplyChainSubgraph}.
     */
    private void addNeighbors(@Nonnull final SubgraphResult subgraphResult) {
        subgraphResult.getEdgeCollection().stream()
            .forEach(edgeCollectionResult ->
                edgeCollectionResult.getEdges().forEach(edge -> {
                    final ResultVertex resultConsumer = edge.getConsumer();
                    final ResultVertex resultProvider = edge.getProvider();

                    final SupplyChainVertex supplyChainConsumer =
                        graph.computeIfAbsent(resultConsumer.getId(), id -> new SupplyChainVertex(resultConsumer));
                    final SupplyChainVertex supplyChainProvider =
                        graph.computeIfAbsent(resultProvider.getId(), id -> new SupplyChainVertex(resultProvider));

                    supplyChainConsumer.providers.add(supplyChainProvider);
                    supplyChainProvider.consumers.add(supplyChainConsumer);
                })
            );
    }

    /**
     * Relationships in the subgraph are reciprocal. That is, if A is a consumer of B, it means that B will be
     * a provider of A.
     */
    public static class SupplyChainVertex {
        /**
         * The set of all entities in the topology that consume commodities from this {@link SupplyChainVertex}.
         * Note that a core assumption in order to be able to use a list instead of a set here is that the
         * ArangoDB query that generates results provides unique edges.
         */
        private final List<SupplyChainVertex> consumers;

        /**
         * The set of all entities in the topology that provide commodities to this {@link SupplyChainVertex}..
         * Note that a core assumption in order to be able to use a list instead of a set here is that the
         * ArangoDB query that generates results provides unique edges.
         */
        private final List<SupplyChainVertex> providers;

        private final String oid;

        private final String entityType;

        private final String state;

        public SupplyChainVertex(@Nonnull ResultVertex resultVertex) {
            this(resultVertex.getId(), resultVertex.getEntityType(), resultVertex.getState());
        }

        public SupplyChainVertex(@Nonnull final String oid,
                                 @Nonnull final String entityType,
                                 @Nonnull final String state) {
            this.oid = Objects.requireNonNull(oid);
            this.entityType = Objects.requireNonNull(entityType);
            this.state = Objects.requireNonNull(state);
            consumers = new ArrayList<>();
            providers = new ArrayList<>();
        }

        @Nonnull
        public List<SupplyChainVertex> getConsumers() {
            return Collections.unmodifiableList(consumers);
        }

        @Nonnull
        public List<SupplyChainVertex> getProviders() {
            return Collections.unmodifiableList(providers);
        }

        @Nonnull
        public String getEntityType() {
            return entityType;
        }

        @Nonnull
        public String getOid() {
            return oid;
        }

        @Nonnull
        public String getState() {
            return state;
        }
    }

    /**
     * A simple class that pairs a vertex and the neighbor from which we reached that vertex during
     * a BFS traversal of the {@link SupplyChainSubgraph}.
     */
    @Immutable
    private static class VertexAndNeighbor {
        public final SupplyChainVertex vertex;
        private final SupplyChainVertex sourceNeighbor;

        /**
         * Create a new {@link VertexAndNeighbor}.
         * @param vertex A vertex in the graph.
         * @param sourceNeighbor The neighbor of the vertex from which we reached the neighbor during a BFS
         *                       supply-chain traversal. For the origin vertex of the BFS, the sourceNeighbor
         *                       will be null, otherwise the sourceNeighbor will be non-null.
         */
        public VertexAndNeighbor(@Nonnull final SupplyChainVertex vertex,
                                 @Nullable final SupplyChainVertex sourceNeighbor) {
            this.vertex = Objects.requireNonNull(vertex);
            this.sourceNeighbor = sourceNeighbor;
        }

        /**
         * Returns true if and only if the vertex and its source neighbor have the same entity type.
         * If the source neighbor is null, returns false.
         *
         * @return If the vertex and its sourceNeighbor have the same entityType.
         */
        public boolean sameEntityTypes() {
            return sourceNeighbor != null && vertex.getEntityType().equals(sourceNeighbor.getEntityType());
        }
    }

    /**
     * A subgraph starting from an origin retrieved from ArangoDB in response to a supply chain query.
     *
     * Contains all the edges reachable from the origin in a single direction (either provider or consumer).
     */
    @Immutable
    public static class SubgraphResult {

        private final ResultVertex origin;

        /**
         *  All the edges reachable from the origin in a single direction (either provider or consumer)
         */
        private final List<EdgeCollectionResult> edgeCollection;

        /**
         * Default constructor required for initialization via ArangoDB's java driver.
         */
        public SubgraphResult() {
            this(new ResultVertex(), Collections.emptyList());
        }

        @VisibleForTesting
        SubgraphResult(@Nonnull final ResultVertex origin,
                       @Nonnull final List<EdgeCollectionResult> edgeCollection) {
            this.origin = Objects.requireNonNull(origin);
            this.edgeCollection = Objects.requireNonNull(edgeCollection);
        }

        public List<EdgeCollectionResult> getEdgeCollection() {
            return edgeCollection;
        }

        public ResultVertex getOrigin() {
            return origin;
        }
    }

    /**
     * A collection of edges reachable from an origin retrieved from ArangoDB in response to a supply chain query.
     */
    @Immutable
    public static class EdgeCollectionResult {
        /**
         * For consumer queries, the provider is of this entity type.
         * For provider queries, the consumer is of this entity type.
         */
        private final String entityType;

        private final List<ResultEdge> edges;

        public EdgeCollectionResult() {
            this("", Collections.emptyList());
        }

        public EdgeCollectionResult(@Nonnull final String entityType,
                                    @Nonnull final List<ResultEdge> edges) {
            this.edges = Objects.requireNonNull(edges);
            this.entityType = Objects.requireNonNull(entityType);
        }

        public List<ResultEdge> getEdges() {
            return edges;
        }
    }

    /**
     * An edge value retrieved from ArangoDB in response to a supply chain query.
     */
    @Immutable
    public static class ResultEdge {
        private final ResultVertex provider;
        private final ResultVertex consumer;

        /**
         * Default constructor required for initialization via ArangoDB's java driver.
         */
        public ResultEdge() {
            this(new ResultVertex(), new ResultVertex());
        }

        @VisibleForTesting
        ResultEdge(@Nonnull final ResultVertex provider, @Nonnull final ResultVertex consumer) {
            this.provider = Objects.requireNonNull(provider);
            this.consumer = Objects.requireNonNull(consumer);
        }

        public ResultVertex getProvider() {
            return provider;
        }

        public ResultVertex getConsumer() {
            return consumer;
        }
    }

    /**
     * A vertex value retrieved from ArangoDB in response to a supply chain query.
     */
    @Immutable
    public static class ResultVertex {
        private final String id;

        private final String entityType;

        private final String state;

        /**
         * Default constructor required for initialization with ArangoDB's java driver.
         */
        public ResultVertex() {
            this("", "", "");
        }

        @VisibleForTesting
        ResultVertex(@Nonnull final String id, @Nonnull final String entityType, @Nonnull final String state) {
            this.id = Objects.requireNonNull(id);
            this.entityType = Objects.requireNonNull(entityType);
            this.state = Objects.requireNonNull(state);
        }

        public String getId() {
            return id;
        }

        public String getEntityType() {
            return entityType;
        }

        public String getState() {
            return state;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, entityType, state);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ResultVertex)) {
                return false;
            }

            ResultVertex v = (ResultVertex)other;
            return Objects.equals(id, v.id) &&
                Objects.equals(entityType, v.entityType) &&
                Objects.equals(state, v.state);
        }
    }

    /**
     * A better version of {@link SupplyChainNode.Builder}, mainly for efficient computation
     * of the per-state members.
     */
    @VisibleForTesting
    public static class SupplyChainNodeBuilder {
        private final Map<Integer, Set<Long>> membersByState = new HashMap<>();

        private int supplyChainDepth;

        private String entityType;

        public void setSupplyChainDepth(final int supplyChainDepth) {
            this.supplyChainDepth = supplyChainDepth;
        }

        public void setEntityType(@Nullable final String entityType) {
            if (entityType != null) {
                this.entityType = entityType;
            }
        }

        public void addMember(@Nonnull final String oid, @Nullable final String state) {
            if (state != null) {
                final Set<Long> membersForState = membersByState.computeIfAbsent(
                        RepoObjectState.toTopologyEntityState(state),
                        k -> new HashSet<>());
                membersForState.add(Long.parseLong(oid));
            }
        }

        @Nonnull
        public SupplyChainNode buildNode(@Nonnull final Map<String, SupplyChainVertex> graph) {
            final SupplyChainNode.Builder protoNodeBuilder = SupplyChainNode.newBuilder()
                    .setSupplyChainDepth(supplyChainDepth);
            if (entityType != null) {
                protoNodeBuilder.setEntityType(entityType);
            }
            final Set<String> connectedProviderTypes = new HashSet<>();
            final Set<String> connectedConsumerTypes = new HashSet<>();
            membersByState.forEach((state, memberSet) -> {
                protoNodeBuilder.putMembersByState(state, MemberList.newBuilder()
                        .addAllMemberOids(memberSet)
                        .build());
                memberSet.forEach(memberOid -> {
                    final SupplyChainVertex vertex = graph.get(memberOid.toString());
                    if (vertex != null) {
                        vertex.getProviders().forEach(provider ->
                                connectedProviderTypes.add(provider.getEntityType()));
                        vertex.getConsumers().forEach(provider ->
                                connectedConsumerTypes.add(provider.getEntityType()));
                    }
                });
            });
            protoNodeBuilder.addAllConnectedProviderTypes(connectedProviderTypes);
            protoNodeBuilder.addAllConnectedConsumerTypes(connectedConsumerTypes);
            return protoNodeBuilder.build();
        }
    }
}

package com.vmturbo.repository.graph.result;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;

/**
 * An in-memory graph built from supply chain queries used for traversal in order to compute the actual
 * supply chain for display.<p/>
 *
 * The graph is actually a subgraph of the whole topology consisting of the vertices reachable from
 * a particular starting point in the supply chain.  It is constructed by computing the transitive closure
 * of the consumers of the starting point and composing it with the transitive closure of the providers
 * of the starting point.<p/>
 *
 * In some cases, an entity type may be consumer (or producer) of another in a direct and indirect way.
 * For example, VMs consume directly from Storage, but they can also be found indirectly in the supply
 * chain: VDCs consume from Storage and VMs consume from VDCs.  It is usually undesirable to include in
 * the supply chain the entities that come from the indirect way.  In our example, if our starting point
 * is an entity of type Storage, we only wish to include the VMs that directly consume from it; not the
 * ones that consume from any VCD that consumes from that storage.<p/>
 *
 * To satisfy this requirement, we keep a record of visited entity types.  As soon as an entity type is
 * seen once, we mark it visited.  No more entities of that type will be added if they are found in the
 * transitive closure again.  In our example, if we find two VMs consuming from our Storage, we will add
 * them to the graph, we will mark VM as a "visited entity type" and we will ignore all other VMs that we
 * will encounter while computing the transitive closure.<p/>
 *
 * Of course, this requirement has its exceptions.  For example, starting from a DiskArray entity, we want
 * to find all Storage entities consuming from it, regardless of whether they consume directly from
 * it or through a LogicalPool entity.  In another example, consider VDCs.  A VDC can consume from another
 * VDC (VDCs form hierarchies with unbounded height).  When we encounter a VDC, we want to compute the full
 * hierarchy of its consumer (or producer) VDCs<p/>
 *
 * To allow for these exceptions, we define the concept of "mandatory edges."  If an edge is mandatory, the
 * algorithm is forced to traverse it, regardless of whether the destination entity type has been visited
 * before.  For the time being, we define two cases of mandatory edges:
 * <ul>
 *     <il>Edges between a VDC and a VDC</il>
 *     <il>Edges between a Storage and a LogicalPool</il>
 * </ul>
 * More cases may be added in the future.<p/>
 *
 * Given the above examples of mandatory edges, it should be clear that a Storage consuming from a
 * LogicalPool will be included in the supply chain of the underlying DiskArray entity, even if there are
 * other Storage entities consuming directly from the disk array.  Similarly, starting from the lowest
 * level of VDCs in a hierarchy and going up, we will include all of them in the supply chain, together
 * with their consumers.
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
     * Supply chain nodes of this graph
     */
    private final List<SupplyChainNode> supplyChainNodes;

    /**
     * This maps entity type names to their corresponding supply chain nodes,
     * during the construction of the supply chain graph.
     */
    private final Map<String, SupplyChainNodeBuilder> nodeMap = new HashMap<>();

    /**
     * Structures to be used during the construction of the supply chain node graph.
     */
    private Deque<VertexAndNeighbor> frontier;
    private final Set<String> visitedEntityTypes = new HashSet<>();
    private final Set<SupplyChainVertex> visitedEntities = new HashSet<>();

    /**
     * This collection defines the pairs of entity types for which the edges are mandatory.
     * Pairs are represented as sets of size 2.
     */
    private static final Set<Set<UIEntityType>> mandatoryEdgeTypes =
            ImmutableSet.of(
                ImmutableSet.of(UIEntityType.VIRTUAL_DATACENTER, UIEntityType.VIRTUAL_DATACENTER),
                ImmutableSet.of(UIEntityType.STORAGE, UIEntityType.LOGICALPOOL)
            );

    /**
     * Create the supply chain graph, given two graphs between entities: one for the "produces" and one
     * for the "consumes" relation.  Each graph specifies an origin entity.  The origin entity on the
     * input graphs must be the same.
     *
     * @param providersResult The providers leading out from the starting vertex.
     * @param consumersResult The consumers leading out from the starting vertex.
     */
    public SupplyChainSubgraph(@Nonnull final List<ResultVertex> providersResult,
                               @Nonnull final List<ResultVertex> consumersResult) {
        final ResultVertex providerOrigin = providersResult.get(0);
        Preconditions.checkArgument(providerOrigin.getId().equals(
                consumersResult.get(0).getId()));

        startingVertexId = providerOrigin.getId();

        graph = new HashMap<>();
        graph.put(startingVertexId, new SupplyChainVertex(providerOrigin));

        addNeighbors(providersResult);
        addNeighbors(consumersResult);
        supplyChainNodes = constructSupplyChainNodeGraph();
    }

    /**
     * Return the collection of supply chain nodes as a list (a fresh list is created to avoid
     * aliasing the internal state of the object).
     *
     * @return the collection of supply chain nodes as a list.
     */
    public List<SupplyChainNode> toSupplyChainNodes() {
        return Collections.unmodifiableList(supplyChainNodes);
    }

    private List<SupplyChainNode> constructSupplyChainNodeGraph() {
        final SupplyChainVertex startingVertex = graph.get(startingVertexId);

        // Traverse outward from the starting vertex to collect supply chain providers
        frontier = new ArrayDeque<>();
        frontier.add(new VertexAndNeighbor(startingVertex, null));
        traverseSupplyChainBFS(SupplyChainVertex::getProviders, 1);

        // Traverse outward from the starting vertex to collect supply chain consumers
        // Start from the starting vertex consumers because the starting vertex itself
        // was added in the producers traversal.
        frontier =
            new ArrayDeque<>(
                Lists.transform(
                    startingVertex.getConsumers(),
                    consumer -> new VertexAndNeighbor(consumer, startingVertex))
            );
        traverseSupplyChainBFS(SupplyChainVertex::getConsumers, 2);

        return nodeMap.values().stream()
            .map(nodeBuilder -> nodeBuilder.buildNode(graph))
            .collect(Collectors.toList());
    }

    /**
     * Perform a breadth-first-search traversal starting from the nodes contained in the frontier.
     *
     * @param neighborFunction The function that, given a vertex, can retrieve the neighbors for the vertex in
     *                         a particular direction (ie provider or consumer neighbors).
     * @param currentDepth     The current depth of the BFS (ie how many hops we are from the starting vertex).
     */
    private void traverseSupplyChainBFS(
            @Nonnull final NeighborFunction neighborFunction,
            final int currentDepth) {
        // nextFrontier are the entities to be traversed at depth+1.
        final Deque<VertexAndNeighbor> nextFrontier = new ArrayDeque<>();
        final Set<String> visitedEntityTypesInThisDepth = new HashSet<>();

        while (!frontier.isEmpty()) {
            final VertexAndNeighbor vertexAndNeighbor = frontier.removeFirst();
            final SupplyChainVertex vertex = vertexAndNeighbor.vertex;
            visitedEntities.add(vertex);

            // Only add a node when we have not already visited an entity of the same type
            // or if the connection corresponds to a mandatory edge.
            if (!visitedEntityTypes.contains(vertex.getEntityType()) || vertexAndNeighbor.mandatoryEdge()) {
                nextFrontier.addAll(
                        neighborFunction.neighborsFor(vertex).stream()
                            .filter(neighbor -> !visitedEntities.contains(neighbor))
                            .map(neighbor -> new VertexAndNeighbor(neighbor, vertex))
                            .collect(Collectors.toList()));
                final SupplyChainNodeBuilder nodeBuilder =
                        nodeMap.computeIfAbsent(vertex.getEntityType(), entityType -> new SupplyChainNodeBuilder());
                visitedEntityTypesInThisDepth.add(vertex.getEntityType());

                nodeBuilder.setSupplyChainDepth(currentDepth);
                nodeBuilder.setEntityType(vertex.getEntityType());
                nodeBuilder.addMember(vertex.getOid(), vertex.getState());
            }
        }
        visitedEntityTypes.addAll(visitedEntityTypesInThisDepth);

        if (!nextFrontier.isEmpty()) {
            frontier = nextFrontier;
            traverseSupplyChainBFS(neighborFunction, currentDepth + 1);
        }
    }

    /**
     * Add neighbors from the {@link List<com.vmturbo.repository.graph.result.SupplyChainSubgraph.ResultVertex>} to
     * {@link this#graph} by traversing the edges.
     *
     * Add connections in both the provider and consumer directions.
     *
     * @param vertices The vertices whose edges should be traversed to add neighbors to the graph
     *                       internal to this {@link SupplyChainSubgraph}.
     */
    private void addNeighbors(@Nonnull final List<ResultVertex> vertices) {
        Map<String, ResultVertex> resultVertexMap = vertices.stream()
                        .collect(Collectors.toMap(ResultVertex::getId,
                                Function.identity(),
                                // Here we don't care about the provider/consumer relations.
                                // We are only interested in the id, state and type.
                                // So we just pick any one if there are duplicates as the
                                // id, state and type will be the same.
                                (vertex1, vertex2) -> vertex1));

        vertices.forEach(resultVertex -> {
            // Origin vertex doesn't have any provider or consumer for itself.
            if (Strings.isNullOrEmpty(resultVertex.getProvider()) &&
                    Strings.isNullOrEmpty(resultVertex.getConsumer())) {
                return;
            }

            if (!Strings.isNullOrEmpty(resultVertex.getProvider())) {
                final ResultVertex resultProvider =
                        resultVertexMap.get(resultVertex.getProvider());
                final SupplyChainVertex supplyChainProvider =
                        graph.computeIfAbsent(resultProvider.getId(),
                                id -> new SupplyChainVertex(resultProvider));
                final SupplyChainVertex supplyChainConsumer =
                        graph.computeIfAbsent(resultVertex.getId(),
                                id -> new SupplyChainVertex(resultVertex));
                supplyChainConsumer.providers.add(supplyChainProvider);
                supplyChainProvider.consumers.add(supplyChainConsumer);

            }

            if (!Strings.isNullOrEmpty(resultVertex.getConsumer())) {
                final ResultVertex resultConsumer =
                        resultVertexMap.get(resultVertex.getConsumer());
                final SupplyChainVertex supplyChainProvider =
                        graph.computeIfAbsent(resultVertex.getId(),
                                id -> new SupplyChainVertex(resultVertex));
                final SupplyChainVertex supplyChainConsumer =
                        graph.computeIfAbsent(resultConsumer.getId(),
                                id -> new SupplyChainVertex(resultConsumer));
                supplyChainConsumer.providers.add(supplyChainProvider);
                supplyChainProvider.consumers.add(supplyChainConsumer);

            }
        });
    }


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
         * Returns true if and only if the edge between the vertex and its source neighbor is mandatory.
         *
         * @return if and only if the edge between the vertex and its source neighbor is mandatory.
         */
        public boolean mandatoryEdge() {
            return mandatoryEdgeTypes.contains(
                ImmutableSet.of(
                    UIEntityType.fromString(vertex.getEntityType()),
                    UIEntityType.fromString(sourceNeighbor.getEntityType())
                )
            );
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
                        UIEntityState.fromString(state).toEntityState().getNumber(),
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

    /**
     * A vertex value retrieved from ArangoDB in response to a supply chain query.
     */
    @Immutable
    public static class ResultVertex {

        private final String oid;

        private final String entityType;

        private final String state;

        private final String provider;

        private final String consumer;

        /**
         * Default constructor required for initialization with ArangoDB's java driver.
         */
        public ResultVertex() {
            this("", "", "", "", "");
        }

        @VisibleForTesting
        ResultVertex(@Nonnull final String oid,
                     @Nonnull final String entityType,
                     @Nonnull final String state,
                     @Nonnull final String provider,
                     @Nonnull final String consumer) {
            this.oid = Objects.requireNonNull(oid);
            this.entityType = Objects.requireNonNull(entityType);
            this.state = Objects.requireNonNull(state);
            this.provider= Objects.requireNonNull(provider);
            this.consumer = Objects.requireNonNull(consumer);
        }

        public String getId() {
            return oid;
        }

        public String getEntityType() {
            return entityType;
        }

        public String getState() {
            return state;
        }

        public String getProvider() {
            return provider;
        }

        public String getConsumer() {
            return consumer;
        }

        @Override
        public int hashCode() {
            return Objects.hash(oid, entityType, state, provider, consumer);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ResultVertex)) {
                return false;
            }

            ResultVertex v = (ResultVertex)other;
            return Objects.equals(oid, v.oid) &&
                    Objects.equals(entityType, v.entityType) &&
                    Objects.equals(state, v.state) &&
                    Objects.equals(provider, v.provider) &&
                    Objects.equals(consumer, v.consumer);
        }

        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append("oid:").append(oid).append("\n")
                    .append("entityType:").append(entityType).append("\n")
                    .append("provider:").append(provider).append("\n")
                    .append("consumer:").append(consumer).append("\n");
            return sb.toString();
        }
    }
}

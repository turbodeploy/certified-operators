package com.vmturbo.topology.graph.supplychain;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphEntity;

/**
 * Responsible for resolving supply chain queries on a {@link TopologyGraph}.
 *
 * @param <E> The type of {@link TopologyGraphEntity} in the graph.
 */
public class SupplyChainResolver<E extends TopologyGraphEntity<E>> {

    private final static Logger logger = LogManager.getLogger();

    /**
     * This collection defines the pairs of entity types for which the edges are mandatory.
     * Pairs are represented as sets of size 2.
     */
    private static final Set<Set<Integer>> MANDATORY_EDGE_TYPES =
        ImmutableSet.of(
            ImmutableSet.of(EntityType.VIRTUAL_DATACENTER_VALUE, EntityType.VIRTUAL_DATACENTER_VALUE),
            ImmutableSet.of(EntityType.STORAGE_VALUE, EntityType.LOGICAL_POOL_VALUE),
            ImmutableSet.of(EntityType.LOGICAL_POOL_VALUE, EntityType.DISK_ARRAY_VALUE)
        );

    private static class Context<E extends TopologyGraphEntity<E>> {

        private final Set<Integer> visitedEntityTypes = new HashSet<>();
        private final Set<E> visitedEntities = new HashSet<>();

        /**
         * This maps entity type names to their corresponding supply chain nodes,
         * during the construction of the supply chain graph.
         */
        private final Map<Integer, SupplyChainNodeBuilder<E>> nodeMap = new HashMap<>();

        /**
         * Structures to be used during the construction of the supply chain node graph.
         */
        private Deque<VertexAndNeighbor<E>> frontier;

        private final Predicate<E> memberPredicate;

        private Context(@Nonnull final Predicate<E> memberPredicate) {
            this.memberPredicate = memberPredicate;
        }
    }

    /**
     * Get the supply chain starting from a set of vertices.
     *
     * @param startingVerticesId The vertices at the "root" of the supply chain. These should all
     *                           be of the same type.
     * @param topologyGraph The topology graph to use for the supply chain query.
     * @param memberPredicate A predicate to restrict the entities that will be included in the
     *                        supply chain. Note - if an entity does not pass this predicate,
     *                        neither it nor any of the entities its connected to will be included
     *                        in the supply chain (unless those entities are connected to by
     *                        other entities that pass the predicate).
     * @return The list of {@link SupplyChainNode}s, one for each type of entity in the final
     *         supply chain.
     */
    @Nonnull
    public Map<UIEntityType, SupplyChainNode> getSupplyChainNodes(final Set<Long> startingVerticesId,
                                                     @Nonnull final TopologyGraph<E> topologyGraph,
                                                     @Nonnull final Predicate<E> memberPredicate) {
        final Context<E> context = new Context<>(memberPredicate);

        final Map<Integer, List<E>> startingEntitiesByType =
            topologyGraph.getEntities(startingVerticesId)
                .filter(memberPredicate)
                .collect(Collectors.groupingBy(E::getEntityType));

        final List<E> initialFrontier;
        if (startingEntitiesByType.isEmpty()) {
            // No matching entities.
            return Collections.emptyMap();
        } else if (startingEntitiesByType.size() > 1) {
            // It is the responsibility of the caller to group starting entities by type, and
            // make individual calls to the supply chain resolver for each type.
            throw new IllegalArgumentException("getSupplyChainNodes should only be called " +
                "with entities of one type. Got: " + startingEntitiesByType.keySet());
        } else {
            // Guaranteed to have exactly one value.
            initialFrontier = startingEntitiesByType.values().iterator().next();
        }

        // Traverse outward from the starting vertex to collect supply chain providers
        context.frontier = new ArrayDeque<>();
        initialFrontier.forEach(entity ->
            context.frontier.add(new VertexAndNeighbor<>(entity, null)));

        traverseSupplyChainBFS(E::getProviders, 1, context);

        // Traverse outward from the starting vertices to collect supply chain consumers
        // Start from the starting vertices consumers because the starting vertices themselves
        // was added in the producers traversal.
        context.frontier = new ArrayDeque<>();
        for (E entity : initialFrontier) {
            entity.getConsumers().stream()
                .filter(memberPredicate)
                .map(consumer -> new VertexAndNeighbor<>(consumer, entity))
                .forEach(context.frontier::add);
        }

        traverseSupplyChainBFS(E::getConsumers, 2, context);

        return context.nodeMap.values().stream()
            .collect(Collectors.toMap(
                bldr -> UIEntityType.fromType(bldr.entityType),
                bldr -> bldr.buildNode(topologyGraph)));
    }

    /**
     * A function that, given a vertex, returns the neighbors for that vertex in a particular direction
     * (ie providers or consumers).
     */
    @FunctionalInterface
    private interface NeighborFunction<E extends TopologyGraphEntity<E>> {
        @Nonnull
        List<E> neighborsFor(@Nonnull final E vertex);
    }


    /**
     * Perform a breadth-first-search traversal starting from the nodes contained in the frontier.
     *  @param neighborFunction The function that, given a vertex, can retrieve the neighbors for the vertex in
     *                         a particular direction (ie provider or consumer neighbors).
     * @param currentDepth     The current depth of the BFS (ie how many hops we are from the starting vertex).
     * @param context The context of the ongoing supply chain query. Contains state global to the
     *                query.
     */
    private void traverseSupplyChainBFS(
            @Nonnull final NeighborFunction<E> neighborFunction,
            final int currentDepth,
            @Nonnull final Context<E> context) {
        // nextFrontier are the entities to be traversed at depth+1.
        final Deque<VertexAndNeighbor<E>> nextFrontier = new ArrayDeque<>();
        final Set<Integer> visitedEntityTypesInThisDepth = new HashSet<>();

        while (!context.frontier.isEmpty()) {
            final VertexAndNeighbor<E> vertexAndNeighbor = context.frontier.removeFirst();
            final E vertex = vertexAndNeighbor.vertex;
            context.visitedEntities.add(vertex);

            // Only add a node when we have not already visited an entity of the same type
            // or if the connection corresponds to a mandatory edge.
            if (!context.visitedEntityTypes.contains(vertex.getEntityType()) || vertexAndNeighbor.mandatoryEdge()) {
                nextFrontier.addAll(
                    neighborFunction.neighborsFor(vertex).stream()
                        .filter(context.memberPredicate)
                        .filter(neighbor -> !context.visitedEntities.contains(neighbor))
                        .map(neighbor -> new VertexAndNeighbor<E>(neighbor, vertex))
                        .collect(Collectors.toList()));
                final SupplyChainNodeBuilder<E> nodeBuilder =
                    context.nodeMap.computeIfAbsent(vertex.getEntityType(),
                        entityType -> new SupplyChainNodeBuilder<>(currentDepth, vertex.getEntityType()));
                visitedEntityTypesInThisDepth.add(vertex.getEntityType());

                nodeBuilder.addMember(vertex);
            }
        }
        context.visitedEntityTypes.addAll(visitedEntityTypesInThisDepth);

        if (!nextFrontier.isEmpty()) {
            context.frontier = nextFrontier;
            traverseSupplyChainBFS(neighborFunction, currentDepth + 1, context);
        }
    }


    /**
     * A simple class that pairs a vertex and the neighbor from which we reached that vertex during
     * a BFS traversal of the {@link TopologyGraph}.
     */
    @Immutable
    private static class VertexAndNeighbor<E extends TopologyGraphEntity<E>> {
        public final E vertex;
        private final E sourceNeighbor;

        /**
         * Create a new {@link VertexAndNeighbor}.
         * @param vertex A vertex in the graph.
         * @param sourceNeighbor The neighbor of the vertex from which we reached the neighbor during a BFS
         *                       supply-chain traversal. For the origin vertex of the BFS, the sourceNeighbor
         *                       will be null, otherwise the sourceNeighbor will be non-null.
         */
        public VertexAndNeighbor(@Nonnull final E vertex,
                                 @Nullable final E sourceNeighbor) {
            this.vertex = Objects.requireNonNull(vertex);
            this.sourceNeighbor = sourceNeighbor;
        }

        /**
         * Returns true if and only if the edge between the vertex and its source neighbor is mandatory.
         *
         * @return if and only if the edge between the vertex and its source neighbor is mandatory.
         */
        public boolean mandatoryEdge() {
            return MANDATORY_EDGE_TYPES.contains(
                ImmutableSet.of(vertex.getEntityType(),
                    sourceNeighbor.getEntityType()));
        }
    }

    /**
     * A better version of {@link SupplyChainNode.Builder}, mainly for efficient computation
     * of the per-state members.
     */
    @VisibleForTesting
    public static class SupplyChainNodeBuilder<E extends TopologyGraphEntity<E>> {
        private final Map<Integer, Set<Long>> membersByState = new HashMap<>();

        private final int supplyChainDepth;

        private final int entityType;

        public SupplyChainNodeBuilder(final int supplyChainDepth,
                                      final int entityType) {
            this.supplyChainDepth = supplyChainDepth;
            this.entityType = entityType;
        }

        public void addMember(@Nonnull final E member) {
            // We don't check the member predicate here because we shouldn't be processing
            // entities that don't match the predicate anyway.
            final Set<Long> membersForState = membersByState.computeIfAbsent(
                member.getEntityState().getNumber(),
                k -> new HashSet<>());
            membersForState.add(member.getOid());
        }

        @Nonnull
        public SupplyChainNode buildNode(@Nonnull final TopologyGraph<? extends TopologyGraphEntity> graph) {
            final SupplyChainNode.Builder protoNodeBuilder = SupplyChainNode.newBuilder()
                .setSupplyChainDepth(supplyChainDepth)
                .setEntityType(UIEntityType.fromType(entityType).apiStr());

            final Set<String> connectedProviderTypes = new HashSet<>();
            final Set<String> connectedConsumerTypes = new HashSet<>();
            membersByState.forEach((state, memberSet) -> {
                protoNodeBuilder.putMembersByState(state, MemberList.newBuilder()
                    .addAllMemberOids(memberSet)
                    .build());
                memberSet.forEach(memberOid -> {
                    graph.getEntity(memberOid).ifPresent(entity -> {
                        entity.getProviders().forEach(provider ->
                            connectedProviderTypes.add(UIEntityType.fromType(provider.getEntityType()).apiStr()));
                        entity.getConsumers().forEach(provider ->
                            connectedConsumerTypes.add(UIEntityType.fromType(provider.getEntityType()).apiStr()));
                    });
                });
            });
            protoNodeBuilder.addAllConnectedProviderTypes(connectedProviderTypes);
            protoNodeBuilder.addAllConnectedConsumerTypes(connectedConsumerTypes);
            return protoNodeBuilder.build();
        }
    }
}

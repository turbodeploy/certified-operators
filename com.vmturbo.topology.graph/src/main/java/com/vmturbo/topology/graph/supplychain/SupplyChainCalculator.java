package com.vmturbo.topology.graph.supplychain;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphEntity;

/**
 * This component calculates scoped supply chains, i.e., supply chains based on a seed.
 */
public class SupplyChainCalculator {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Get the supply chain starting from a set of vertices.
     *
     * @param topology the topology to work on
     * @param seed the collection of entities to start from
     * @param entityFilter a predicate that filters traversed entities
     * @param traversalRulesLibrary traversal rules to be used in this
     *                              supply chain generation
     * @param <E> The type of {@link TopologyGraphEntity} in the graph
     * @return The {@link SupplyChainNode}s of the result,
     *         grouped by entity type
     */
    @Nonnull
    public <E extends TopologyGraphEntity<E>> Map<Integer, SupplyChainNode> getSupplyChainNodes(
            @Nonnull TopologyGraph<E> topology, @Nonnull Collection<Long> seed,
            @Nonnull Predicate<E> entityFilter, @Nonnull TraversalRulesLibrary<E> traversalRulesLibrary) {
        final Queue<TraversalState> frontier =
                new LinkedList<>(seed.stream()
                        .map(id -> new TraversalState(id, TraversalMode.START, 1))
                        .collect(Collectors.toList()));
        final Map<Integer, SupplyChainNodeBuilder<E>> resultBuilder = new HashMap<>();
        final Set<Long> visitedEntities = new HashSet<>();

        while (!frontier.isEmpty()) {
            // remove traversal state from frontier
            final TraversalState traversalState = frontier.remove();
            long entityId = traversalState.getEntityId();
            final TraversalMode traversalMode = traversalState.getTraversalMode();

            // skip already traversed entities
            if (visitedEntities.contains(entityId)) {
                continue;
            }

            // get entity
            final E entity = topology.getEntity(entityId).orElse(null);
            if (entity == null) {
                logger.error(
                        "Error while constructing supply chain: missing entity with id {}", entityId);
                continue;
            }

            // record the entity
            visitedEntities.add(entityId);

            // only keep entities that satisfy the entity filter
            if (!entityFilter.test(entity)) {
                continue;
            }

            final int entityTypeId = entity.getEntityType();

            // the depth of a supply chain node is
            // the smallest number of iterations from the seed
            // that is required to reach an entity of the
            // corresponding entity type in the traversal
            // currently, it is not used by the clients,
            // but it is expected in the output
            final int depth = traversalState.getDepth();

            // add the entity to the result
            final SupplyChainNodeBuilder entityTypeNode =
                resultBuilder.computeIfAbsent(
                        entityTypeId, k -> new SupplyChainNodeBuilder(
                                                ApiEntityType.fromType(entityTypeId).apiStr(), depth))
                    .addEntity(entity);

            // add entity type arrows to the result
            getOutgoingArrowsStream(entity)
                .filter(e -> isEntityInResult(e, resultBuilder))
                .forEach(otherEntity -> {
                    final int otherEntityType = otherEntity.getEntityType();
                    final SupplyChainNodeBuilder otherEntityTypeNode = resultBuilder.get(otherEntityType);

                    // there should be an arrow from entityTypeNode to otherEntityTypeNode
                    entityTypeNode.addProviderType(otherEntityType);
                    otherEntityTypeNode.addConsumerType(entityTypeId);
                });
            getIngoingArrowsStream(entity)
                .filter(e -> isEntityInResult(e, resultBuilder))
                .forEach(otherEntity -> {
                    final int otherEntityType = otherEntity.getEntityType();
                    final SupplyChainNodeBuilder otherEntityTypeNode = resultBuilder.get(otherEntityType);

                    // there should be an arrow from otherEntityTypeNode to entityTypeNode
                    entityTypeNode.addConsumerType(otherEntityType);
                    otherEntityTypeNode.addProviderType(entityTypeId);
                });

            // apply a traversal rule to add traversal states to the frontier
            traversalRulesLibrary.apply(entity, traversalMode, depth, frontier);
        }

        // build the result
        final Map<Integer, SupplyChainNode> result = new HashMap<>();
        for (Entry<Integer, SupplyChainNodeBuilder<E>> entry : resultBuilder.entrySet()) {
            result.put(entry.getKey(), entry.getValue().build());
        }
        return result;
    }

    /**
     * Utility method to get a list of entity type names
     * from a collection of entity type ids.
     *
     * @param entityTypeIds a set of entity type ids
     * @return the translation to a list of entity type names
     */
    private static List<String> getEntityTypeNames(@Nonnull Collection<Integer> entityTypeIds) {
        return entityTypeIds.stream()
                    .map(entityTypeId -> ApiEntityType.fromType(entityTypeId).apiStr())
                    .collect(Collectors.toList());
    }

    @Nonnull
    private static <E extends TopologyGraphEntity<E>> Stream<E> getOutgoingArrowsStream(@Nonnull E entity) {
        return Stream.concat(entity.getProviders().stream(),
                             Stream.concat(entity.getAggregatorsAndOwner().stream(),
                                           entity.getOutboundAssociatedEntities().stream()));
    }

    @Nonnull
    private static <E extends TopologyGraphEntity<E>> Stream<E> getIngoingArrowsStream(@Nonnull E entity) {
        return Stream.concat(entity.getConsumers().stream(),
                             Stream.concat(entity.getAggregatedAndOwnedEntities().stream(),
                                           entity.getInboundAssociatedEntities().stream()));
    }

    private static <E extends TopologyGraphEntity<E>> boolean isEntityInResult(
            @Nonnull E entity, @Nonnull Map<Integer, SupplyChainNodeBuilder<E>> result) {
        final SupplyChainNodeBuilder<E> node = result.get(entity.getEntityType());
        return node != null && node.isEntityIdInNode(entity);
    }

    /**
     * Pairs an entity with a traversal tag and a depth.
     * {@link TraversalState}s will be the elements of the frontier of
     * the supply chain traversal.
     */
    @Immutable
    public static class TraversalState {
        private long entityId;
        private TraversalMode traversalMode;
        private int depth;

        /**
         * Pair an entity and a traversal mode to create a new {@link TraversalState}.
         *
         * @param entityId id of the entity related to the traversal state
         * @param traversalMode traversal mode related to the traversal state
         * @param depth depth of traversal
         */
        public TraversalState(long entityId, @Nonnull TraversalMode traversalMode, int depth) {
            this.entityId = entityId;
            this.traversalMode = traversalMode;
            this.depth = depth;
        }

        public long getEntityId() {
            return entityId;
        }

        @Nonnull
        public TraversalMode getTraversalMode() {
            return traversalMode;
        }

        public int getDepth() {
            return depth;
        }
    }

    /**
     * Mode of traversing in a traversal state.
     */
    public enum TraversalMode {
        /**
         * We are traversing an entity that belongs to the seed.
         */
        START,
        /**
         * We are traversing downwards the supply chain.
         */
        CONSUMES,
        /**
         * We are traversing upwards the supply chain.
         */
        PRODUCES,
        /**
         * We are traversing owners and aggregators.
         */
        AGGREGATED_BY
    }

    /**
     * Auxiliary builder for {@link SupplyChainNode}s.
     *
     * @param <E> the subclass of {@link TopologyGraphEntity}
     *            that represents the entity
     */
    public static class SupplyChainNodeBuilder<E extends TopologyGraphEntity<E>> {
        /**
         * The internal {@link SupplyChainNode.Builder}.
         */
        private final SupplyChainNode.Builder builder;
        /**
         * Map from states to entity ids.
         */
        private final Map<Integer, Set<Long>> membersByState = new HashMap<>();
        /**
         * Outgoing arrows to this supply chain node. This includes
         * all provider types, the type of the owner, and all aggregator
         * types. It also includes outgoing associations.
         *
         * <p>Outgoing associations are connections of type
         *    {@link com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType#NORMAL_CONNECTION}
         *    that point outwards. These are:
         *    <li>
         *        <ul>from Volume to Storage or Storage Tier</ul>
         *        <ul>from Volume to Zone or Region</ul>
         *        <ul>from VM to Volume</ul>
         *        <ul>from Compute Tier to Storage Tier</ul>
         *    </li>
         * </p>
         */
        private final Set<Integer> providerTypes = new HashSet<>();
        /**
         * Ingoing arrows to this supply chain node. This includes
         * all consumer, owned, and aggregated types. It also includes
         * ingoing associations.
         *
         * <p>Ingoing associations are connections of type
         *    {@link com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType#NORMAL_CONNECTION}
         *    that point inwards. These are the exact reverse of the outgoing
         *    associations (see above).
         * </p>
         */
        private final Set<Integer> consumerTypes = new HashSet<>();

        /**
         * Create a {@link SupplyChainNodeBuilder}.
         *
         * @param entityType entity type for {@link SupplyChainNode} to be built
         * @param depth depth of the node
         */
        public SupplyChainNodeBuilder(@Nonnull String entityType, int depth) {
            builder = SupplyChainNode.newBuilder()
                            .setEntityType(entityType)
                            .setSupplyChainDepth(depth);
        }

        /**
         * Add a new entity in the node.
         *
         * @param entity the entity to add
         * @return this builder
         */
        @Nonnull
        public SupplyChainNodeBuilder<E> addEntity(@Nonnull E entity) {
            membersByState.computeIfAbsent(entity.getEntityState().getNumber(), k -> new HashSet<>())
                    .add(entity.getOid());
            return this;
        }

        /**
         * Add a new provider type to the node.
         *
         * @param providerType id of the provider type
         * @return this builder
         */
        @Nonnull
        public SupplyChainNodeBuilder<E> addProviderType(int providerType) {
            providerTypes.add(providerType);
            return this;
        }

        /**
         * Add a new consumer type to the node.
         *
         * @param consumerType id of the consumer type
         * @return this builder
         */
        @Nonnull
        public SupplyChainNodeBuilder<E> addConsumerType(int consumerType) {
            consumerTypes.add(consumerType);
            return this;
        }

        /**
         * Build the {@link SupplyChainNode}.
         *
         * @return the {@link SupplyChainNode} built.
         */
        @Nonnull
        public SupplyChainNode build() {
            membersByState.entrySet().forEach(entry ->
                builder.putMembersByState(
                    entry.getKey(),
                    MemberList.newBuilder()
                        .addAllMemberOids(entry.getValue())
                        .build()));
            builder.addAllConnectedProviderTypes(getEntityTypeNames(providerTypes));
            builder.addAllConnectedConsumerTypes(getEntityTypeNames(consumerTypes));
            return builder.build();
        }

        /**
         * Check if an entity is in the node.
         *
         * @param entity to check
         * @return true iff this entity is in the node
         */
        public boolean isEntityIdInNode(E entity) {
            return membersByState.getOrDefault(entity.getEntityState().getNumber(), Collections.emptySet())
                        .contains(entity.getOid());
        }
    }
}

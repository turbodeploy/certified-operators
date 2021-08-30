package com.vmturbo.topology.graph.supplychain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.repository.SupplyChainProto.LeafEntitiesResponse.LeafEntity;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphEntity;

/**
 * This component calculates scoped supply chains, i.e., supply chains based on a seed.
 */
public class SupplyChainCalculator {
    private static final Logger logger = LogManager.getLogger();

    private static final  Set<TraversalMode> LEAVES_DIRECTION_MODES =
        Sets.newHashSet(TraversalMode.START, TraversalMode.PRODUCES);

    /**
     * Get the supply chain entities starting from a set of vertices.
     *
     * @param topology              - the topology to work on.
     * @param seed                  - the collection of entities to start from.
     * @param filterOutClasses      - entity types that should not be included.
     * @param traversalRulesLibrary -  a set of traversal rules that are used for analysing
     *                              supply chain relationships.
     * @param <E>                   The type of {@link TopologyGraphEntity} in the graph.
     * @return a list of {@link LeafEntity}.
     */
    public <E extends TopologyGraphEntity<E>> List<E>  getLeafEntities(
            @Nonnull TopologyGraph<E> topology,
            @Nonnull Collection<Long> seed,
            @Nonnull List<EntityType> filterOutClasses,
            @Nonnull TraversalRulesLibrary<E> traversalRulesLibrary) {

        final List<Integer> filterOutClassesNumbers = filterOutClasses.stream()
                .map(EntityType::getNumber).collect(Collectors.toList());
        final Set<E> leaves = new HashSet<>();
        final Queue<TraversalState> frontier = new Frontier<>(seed.stream()
                .map(id -> new TraversalState(id, id, TraversalMode.START, 1))
                .collect(Collectors.toList()), topology);
        while (!frontier.isEmpty()) {
            // remove traversal state from frontier
            final TraversalState state = frontier.remove();
            // skip the state if it does not match the desired direction
            if (!LEAVES_DIRECTION_MODES.contains(state.getTraversalMode())) {
                continue;
            }
            long entityId = state.getEntityId();
            // get entity
            final E entity = topology.getEntity(entityId).orElse(null);
            if (entity == null) {
                logger.error("Error while constructing supply chain: "
                        + "missing entity with id {}", entityId);
                continue;
            }
            // add entity to the result
            if (shouldIncludeEntity(entity, filterOutClassesNumbers)) {
                leaves.add(entity);
            }
            // apply a traversal rule to add traversal states to the frontier
            if (!filterOutClassesNumbers.contains(entity.getEntityType())) {
                traversalRulesLibrary.apply(entity, state.getTraversalMode(), state.getDepth(), frontier);
            }
        }
        return new ArrayList<>(leaves);
    }

    /**
     * The method decides if an entity should be included to the response based on filter and the
     * defined highest entity type.
     *
     * @param entity           - entity to analyse.
     * @param filterOutClasses - entity types that should not be included.
     * @param <E>              - The type of {@link TopologyGraphEntity} in the graph.
     * @return TRUE if it should be included.
     */
    private <E extends TopologyGraphEntity<E>> boolean shouldIncludeEntity(@Nonnull E entity,
        @Nonnull List<Integer> filterOutClasses) {
        if (filterOutClasses.contains(entity.getEntityType())) {
            return false;
        }
        List<E> filteredConsumers = entity.getConsumers();
        if (filteredConsumers.isEmpty()) {
            // If no consumers then get controlled entities for CONTROLLED_BY relationship
            filteredConsumers = entity.getControlledEntities();
        }
        if (filteredConsumers.isEmpty()) {
            return true;
        }
        filteredConsumers = new ArrayList<>(filteredConsumers);
        filteredConsumers.removeIf(e -> filterOutClasses.contains(e.getEntityType()));
        return filteredConsumers.isEmpty();
    }

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
        final Queue<TraversalState> frontier = new Frontier<>(seed.stream()
                        .map(id -> new TraversalState(id, id, TraversalMode.START, 1))
                        .collect(Collectors.toList()), topology);
        final Map<Integer, SupplyChainNodeBuilder<E>> resultBuilder = new HashMap<>();
        while (!frontier.isEmpty()) {
            // remove traversal state from frontier
            final TraversalState traversalState = frontier.remove();
            long entityId = traversalState.getEntityId();
            final TraversalMode traversalMode = traversalState.getTraversalMode();

            // get entity
            final E entity = topology.getEntity(entityId).orElse(null);
            if (entity == null) {
                logger.error(
                        "Error while constructing supply chain: missing entity with id {}", entityId);
                continue;
            }

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
                                                ApiEntityType.fromType(entityTypeId).typeNumber(), depth))
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
    private static List<Integer> getEntityTypeNames(@Nonnull Collection<Integer> entityTypeIds) {
        return entityTypeIds.stream()
                    .map(entityTypeId -> ApiEntityType.fromType(entityTypeId).typeNumber())
                    .collect(Collectors.toList());
    }

    @Nonnull
    private static <E extends TopologyGraphEntity<E>> Stream<E> getOutgoingArrowsStream(@Nonnull E entity) {
        return Stream.concat(entity.getProviders().stream(),
                             Stream.concat(entity.getAggregatorsAndOwner().stream(),
                                 Stream.concat(entity.getControllers().stream(), entity.getOutboundAssociatedEntities().stream())));
    }

    @Nonnull
    private static <E extends TopologyGraphEntity<E>> Stream<E> getIngoingArrowsStream(@Nonnull E entity) {
        return Stream.concat(entity.getConsumers().stream(),
                             Stream.concat(entity.getAggregatedAndOwnedEntities().stream(),
                                 Stream.concat(entity.getControlledEntities().stream(), entity.getInboundAssociatedEntities().stream())));
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
        private long whoAdded;
        private long entityId;
        private TraversalMode traversalMode;
        private int depth;

        /**
         * Pair an entity and a traversal mode to create a new {@link TraversalState}.
         *
         * @param whoAdded id of the entity which added entityId which processing caused creation of a new {@link TraversalState}.
         * @param entityId id of the entity related to the traversal state
         * @param traversalMode traversal mode related to the traversal state
         * @param depth depth of traversal
         */
        public TraversalState(long whoAdded, long entityId, @Nonnull TraversalMode traversalMode, int depth) {
            this.whoAdded = whoAdded;
            this.entityId = entityId;
            this.traversalMode = traversalMode;
            this.depth = depth;
        }

        public long getWhoAdded() {
            return whoAdded;
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

        /**
         * This class is used to construct a mutable version of a
         * {@link TraversalState} object.  It is used when the depth of
         * the traversal is not known at the time of the construction
         * of the {@link TraversalState} object.
         */
        public static class Builder {
            private final TraversalState traversalState;

            /**
             * Pair an entity and a traversal mode to create a new
             * mutable {@link TraversalState}.
             *
             * @param whoAdded id of the entity which added entityId which processing caused creation of a new {@link TraversalState}.
             * @param entityId id of the entity related to the traversal state
             * @param traversalMode traversal mode related to the traversal state
             */
            public Builder(long whoAdded, long entityId, @Nonnull TraversalMode traversalMode) {
                traversalState = new TraversalState(whoAdded, entityId, traversalMode, 0);
            }

            /**
             * Set depth and return the immutable {@link TraversalState} object.
             *
             * @param depth the depth of the immutable {@link TraversalState} object
             * @return the immutable {@link TraversalState} object
             */
            public TraversalState withDepth(int depth) {
                traversalState.depth = depth;
                return traversalState;
            }
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
        AGGREGATED_BY,
        /**
         * We are traversing controllers.
         */
        CONTROLLED_BY,
        /**
         * Traversal must stop here.
         */
        STOP
    }

    /**
     * Class that represents Supply chain frontier. Skip entities that have been already visited
     * during calculation process, i.e. {@link TraversalState} instance which points to the same
     * entity will not be added to a frontier, this will lead to memory optimization.
     *
     * @param <E> type of the entity provided by {@link TopologyGraph}.
     */
    protected static class Frontier<E extends TopologyGraphEntity<E>>
                    extends LinkedList<TraversalState> {
        private static final Logger LOGGER = LogManager.getLogger(Frontier.class);
        private final Collection<Long> visitedEntities = new HashSet<>();
        private final TopologyGraph<E> topology;

        /**
         * Creating new {@link Frontier} instance.
         *
         * @param c collection of initial {@link TraversalState}s.
         * @param topology graph that could be used to get comprehensive entity
         *                 information.
         */
        protected Frontier(Collection<? extends TraversalState> c, TopologyGraph<E> topology) {
            super();
            this.topology = topology;
            addAll(c);
        }

        @Override
        public boolean offer(TraversalState e) {
            return add(e);
        }

        @Override
        public boolean add(TraversalState traversalState) {
            // skip already processed entities
            final boolean result = visitedEntities.add(traversalState.getEntityId());
            if (result) {
                super.add(traversalState);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("'{}' processing caused addition of '{}' into frontier",
                                    topology.getEntity(traversalState.getWhoAdded()).orElse(null),
                                    topology.getEntity(traversalState.getEntityId()).orElse(null));
                }
            }
            return result;
        }

        @Override
        public TraversalState remove() {
            if (isEmpty()) {
                throw new NoSuchElementException();
            }
            final TraversalState result = first(true);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("'{}' taken for processing",
                                topology.getEntity(result.getEntityId()).orElse(null));
            }
            return result;
        }

        @Nullable
        private TraversalState first(boolean remove) {
            final Iterator<TraversalState> it = iterator();
            if (!it.hasNext()) {
                return null;
            }
            final TraversalState result = it.next();
            if (remove) {
                it.remove();
            }
            return result;
        }

        @Override
        public TraversalState poll() {
            return first(true);
        }

        @Override
        public TraversalState element() {
            if (isEmpty()) {
                throw new NoSuchElementException();
            }
            return first(false);
        }

        @Override
        public TraversalState peek() {
            return first(false);
        }
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
        public SupplyChainNodeBuilder(int entityType, int depth) {
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

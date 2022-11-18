package com.vmturbo.topology.graph;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A graph built from the topology.
 *
 * <p>The graph should be an immutable DAG (directed acyclic graph) though that is not enforced by
 * this interface.</p>
 *
 * <p>The graph is constructed by following consumes/provides relations established by the
 * commodities bought and sold among the entities in the topology, as well as the explicit
 * connection relationships in the topology.</p>
 *
 * <p>The graph does NOT permit parallel edges. That is, an entity consuming multiple commodities
 * from the same provider results in only a single edge between the two in the graph.</p>
 *
 * <p>The objects within the graph's entities may be edited, but the graph is
 * immutable otherwise. Note that the edits made to the objects in the vertices should not modify
 * topological relationships or the graph will become out of sync with its members. This means that
 * commodities bought and sold by entities should not be modified in a way that they change which
 * entities are buying or selling commodities to each other.</p>
 *
 * <p>See {@link TopologyGraphEntity} for further details on the entities in the
 * {@link TopologyGraph}.<p/>
 *
 * @param <E> entity type
 */
@Immutable
public class TopologyGraph<E extends TopologyGraphEntity<E>> {

    private static final Logger logger = LogManager.getLogger();

    /**
     * A map permitting lookup from OID to {@link TopologyGraphEntity}.
     */
    @Nonnull
    private final Long2ObjectMap<E> graph;

    /**
     * An index permitting lookup from entity type to all the entities of that type in the graph.
     */
    @Nonnull
    private final Map<Integer, Collection<E>> entityTypeIndex;

    /**
     * Create a new topology graph. Called via the appropriate builder.
     *
     * @param graph           The graph of entities in the {@link TopologyGraph}.
     * @param entityTypeIndex entities organized by entity type
     */
    public TopologyGraph(@Nonnull final Long2ObjectMap<E> graph,
                         @Nonnull final Map<Integer, Collection<E>> entityTypeIndex) {
        this.graph = Objects.requireNonNull(graph);
        this.entityTypeIndex = Objects.requireNonNull(entityTypeIndex);
    }

    /**
     * Retrieve the {@link TopologyGraphEntity} for an entity in the graph by its OID. Returns
     * {@link Optional#empty()} if no such {@link TopologyGraphEntity} is in the graph.
     *
     * @param oid The OID of the {@link TopologyGraphEntity} in the graph.
     * @return The {@link TopologyGraphEntity} with the corresponding OID or {@link
     * Optional#empty()} if no entity with the given OID exists.
     */
    @Nonnull
    public Optional<E> getEntity(final long oid) {
        return Optional.ofNullable(graph.get(oid));
    }

    /**
     * The multi-get version of {@link TopologyGraph#getEntity(long)}.
     *
     * @param oids The OIDs to return. Returns all entities if the input set is empty. The input is
     *             a set to avoid duplicates.
     * @return A stream of {@link TopologyGraphEntity}s with the corresponding OIDS, or all entities
     * if the input is an empty set.
     */
    @Nonnull
    public Stream<E> getEntities(@Nonnull final Set<Long> oids) {
        if (oids.isEmpty()) {
            return entities();
        } else {
            return oids.stream()
                    .map(this::getEntity)
                    .filter(Optional::isPresent)
                    .map(Optional::get);
        }
    }

    /**
     * Get a stream of all {@link TopologyGraphEntity}s in the graph.
     *
     * @return A stream of all {@link TopologyGraphEntity}s in the graph.
     */
    @Nonnull
    public Stream<E> entities() {
        return graph.values().stream();
    }

    /**
     * Get the set of all {@link TopologyGraphEntity} OIDs in the graph.
     *
     * @return A stream of all {@link TopologyGraphEntity} OIDs in the graph.
     */
    @Nonnull
    public Set<Long> oids() {
        return graph.keySet();
    }

    /**
     * Edges considered for topological sorting are applied in the order specified in this list,
     * meaning that when entities targeted by edges are visited in the order of the edge types
     * appearing here.
     *
     * <p>All edges are represented by lists of targeted entities, and are chosen so that the
     * entity that carries the reference establishing the relationship in the topology broadcast is
     * at the source-end of the edges (e.g. consumers targeting producers, not the other way around,
     * becuase in the topology, consumed commodity structures hold the producer entity ids, not the
     * other way around. This choice is designed to eliminate or at least minimize the occurrence of
     * "forward references" in the broadcast. Barring opposing ordering implications among the
     * different edge suppliers for any given pair of entities, the result should be a broadcast in
     * which all referenced entities appear prior to their referencers.</p>
     *
     * <p>There is no guarantee that edges from different suppliers will be compatible, so the
     * suppliers appear in decreasing priority order. The sort algorithm has a bias toward
     * complying with higher-priority edges in preference to lower-priority edges, but it is
     * by no means guaranteed. We do not generally expect the different types of edges to disagree,
     * in any case.</p>
     */
    private final List<TopsortEdgeSupplier<E>> standardEdgeSuppliers = ImmutableList.of(
            // consumers carry references to producers
            TopologyGraph::getProviders,
            // aggregated entities carry references to their aggregators
            TopologyGraph::getAggregators,
            // controlled entities carry references to their controllers
            TopologyGraph::getControllers,
            // owners carry references to their owned entities
            TopologyGraph::getOwnedEntities,
            // normal connections are considered outbound in the referncing entity, and inbound
            // in the referenced entity
            TopologyGraph::getOutboundAssociatedEntities
    );

    /**
     * Get a stream of all {@link TopologyGraphEntity}s in the graph, sorted topologically, using
     * the standard edge suppliers.
     *
     * @return the sorted stream of entities
     */
    public Stream<E> topSort() {
        return topSort(standardEdgeSuppliers);
    }

    /**
     * Get a stream of all {@link TopologyGraphEntity}s in the graph, sorted topologically so that
     * each entity appears after any predecessors indicate by the edge suppliers.
     *
     * @param prioritizedEdgeSuppliers list of edge suppliers in decreasing priority order
     * @return the sorted stream of entities
     */
    @Nonnull
    public Stream<E> topSort(List<TopsortEdgeSupplier<E>> prioritizedEdgeSuppliers) {
        Stopwatch watch = Stopwatch.createStarted();
        TopSortContext<E> context = new TopSortContext<E>(this, prioritizedEdgeSuppliers);
        entities().forEach(e -> visitForSort(e.getOid(), context));
        logger.info("Topological sort completed in {}", watch);
        return context.getResults();
    }

    /**
     * Return the types of entities contained in the graph.
     *
     * @return set of entity types present in the graph
     */
    @Nonnull
    public Set<Integer> entityTypes() {
        return Collections.unmodifiableSet(entityTypeIndex.keySet());
    }

    /**
     * Get a stream of all {@link TopologyGraphEntity}s in the graph of a given type. Implementation
     * note: This call fetches entities of a given type in constant time.
     *
     * @param entityType The type of entities to be retrieved.
     * @return a stream of all {@link TopologyGraphEntity}s in the graph of the given type. If no
     * entities of the given type are present, returns an empty stream.
     */
    @Nonnull
    public Stream<E> entitiesOfType(@Nonnull final EntityType entityType) {
        return entitiesOfType(entityType.getNumber());
    }

    /**
     * Get a stream of all {@link TopologyGraphEntity}s in the graph of a given type specified by
     * the type number. Implementation note: This call fetches entities of a given type in constant
     * time.
     *
     * @param entityTypeNumber The number of the {@link EntityType} of entities to be retrieved.
     * @return a stream of all {@link TopologyGraphEntity}s in the graph of the given type. If no
     * entities of the given type are present, returns an empty stream.
     */
    @Nonnull
    public Stream<E> entitiesOfType(@Nonnull final Integer entityTypeNumber) {
        final Collection<E> entitiesOfType = entityTypeIndex.get(entityTypeNumber);
        return entitiesOfType == null ? Stream.empty() : entitiesOfType.stream();
    }

    /**
     * Get the number of {@link TopologyGraphEntity}s in the graph of a given type specified by
     * the type number.
     *
     * @param entityTypeNumber The number of the {@link EntityType} of entities to be retrieved.
     * @return the number of {@link TopologyGraphEntity}s in the graph of the given type.
     */
    public int entitiesOfTypeCount(@Nonnull final Integer entityTypeNumber) {
        final Collection<E> entitiesOfType = entityTypeIndex.get(entityTypeNumber);
        return entitiesOfType == null ? 0 : entitiesOfType.size();
    }

    /**
     * Retrieve all consumers for a given entity in the graph.
     *
     * <p>If no {@link TopologyGraphEntity} with the given OID exists, returns empty.</p>
     *
     * <p>An entity is a consumer of another entity if it buys a commodity from
     * that entity.</p>
     *
     * @param oid The OID of the {@link TopologyGraphEntity} in the graph.
     * @return All consumers for a {@link TopologyGraphEntity} in the graph by its OID.
     */
    @Nonnull
    public Stream<E> getConsumers(long oid) {
        // Because this is high-performance code, do not call getEntity which allocates
        // an additional object.
        final E entity = graph.get(oid);
        return entity == null ? Stream.empty() : getConsumers(entity);
    }

    /**
     * Get the consumers for a {@link TopologyGraphEntity} in the graph.
     *
     * @param entity The {@link TopologyGraphEntity} whose consumers should be retrieved.
     * @return The consumers for a {@link TopologyGraphEntity} in the graph.
     */
    @Nonnull
    public Stream<E> getConsumers(@Nonnull final E entity) {
        return entity.getConsumers().stream();
    }

    /**
     * Retrieve all providers for a given entity in the graph. If no {@link TopologyGraphEntity}
     * with the given OID exists, returns empty.
     *
     * <p>An entity is a provider of another entity if the other entity buys a commodity from this
     * one.</p>
     *
     * @param oid The OID of the {@link TopologyGraphEntity} in the graph.
     * @return All providers for a {@link TopologyGraphEntity} in the graph by its OID.
     */
    @Nonnull
    public Stream<E> getProviders(long oid) {
        // Because this is high-performance code, do not call getEntity which allocates
        // an additional object.
        final E topologyEntity = graph.get(oid);
        return topologyEntity == null ? Stream.empty() : getProviders(topologyEntity);
    }

    /**
     * Get the providers for a {@link TopologyGraphEntity} in the graph.
     *
     * @param topologyEntity The {@link TopologyGraphEntity} whose providers should be retrieved.
     * @return The providers for a {@link TopologyGraphEntity} in the graph.
     */
    @Nonnull
    public Stream<E> getProviders(@Nonnull final E topologyEntity) {
        return topologyEntity.getProviders().stream();
    }

    /**
     * Get the entities that are aggregated or owned by a given {@link TopologyGraphEntity} in the
     * graph.
     *
     * @param topologyEntity The {@link TopologyGraphEntity} whose aggregated and owned entities
     *                       should be retrieved.
     * @return The entities that are aggregated and owned by a {@link TopologyGraphEntity} in the
     * graph.
     */
    @Nonnull
    public Stream<E> getOwnedOrAggregatedEntities(@Nonnull final E topologyEntity) {
        return Stream.concat(topologyEntity.getOwnedEntities().stream(), topologyEntity.getAggregatedEntities().stream());
    }

    /**
     * Get the owner and aggregators of a given {@link TopologyGraphEntity} in the graph.
     *
     * @param topologyEntity The {@link TopologyGraphEntity} whose owner and aggregators should be
     *                       retrieved.
     * @return The aggregators and owner of the {@link TopologyGraphEntity}.
     */
    @Nonnull
    public Stream<E> getOwnersOrAggregators(@Nonnull final E topologyEntity) {
        return Stream.concat(topologyEntity.getAggregators().stream(), getOwner(topologyEntity));
    }

    /**
     * Get all aggregators and controllers of this entity.
     * This is needed to ensure backwards compatibility of reported relationship between
     * containers and container specs.
     * After https://vmturbo.atlassian.net/browse/OM-71015 is released it can happen that there
     * are k8s probes which report older relationships ie. AggregatedBy and newer probes which
     * report ControlledBy between containers and container specs. We need to account for both.
     * As of now the probe would report one or the other, so its ok to get a union of both sets.
     * TODO: Remove this and its respective usage when all users move to version 8.2.3+ of
     *   Turbonomic and Kubeturbo in ALL of their environments.
     *
     * @param topologyEntity the {@link TopologyGraphEntity} whose aggregators and controllers
     *                       should be retrieved.
     * @return all aggregator and controller entities
     */
    @Nonnull
    public Stream<E> getAggregatorsAndControllers(@Nonnull final E topologyEntity) {
        return topologyEntity.getAggregatorsAndControllers().stream();
    }

    /**
     * Get the entities that are owned by a given {@link TopologyGraphEntity} in the graph.
     *
     * @param topologyEntity The {@link TopologyGraphEntity} whose owned entities should be
     *                       retrieved.
     * @return The entities that are aggregated and owned by a {@link TopologyGraphEntity} in the
     * graph.
     */
    @Nonnull
    public Stream<E> getOwnedEntities(@Nonnull final E topologyEntity) {
        return topologyEntity.getOwnedEntities().stream();
    }

    /**
     * Get the owner of a given {@link TopologyGraphEntity} in the graph.
     *
     * @param topologyEntity The {@link TopologyGraphEntity} whose owner should be retrieved.
     * @return The owner of the {@link TopologyGraphEntity}.
     */
    @Nonnull
    public Stream<E> getOwner(@Nonnull final E topologyEntity) {
        return topologyEntity.getOwner().map(Stream::of).orElseGet(Stream::empty);
    }

    /**
     * Get the entities that are aggregated by a given {@link TopologyGraphEntity} in the graph.
     *
     * @param topologyEntity The {@link TopologyGraphEntity} whose aggregated entities should be
     *                       retrieved.
     * @return The entities that are aggregated by a {@link TopologyGraphEntity} in the graph.
     */
    @Nonnull
    public Stream<E> getAggregatedEntities(@Nonnull final E topologyEntity) {
        return topologyEntity.getAggregatedEntities().stream();
    }

    /**
     * Get the aggregators of a given {@link TopologyGraphEntity} in the graph.
     *
     * @param topologyEntity The {@link TopologyGraphEntity} whose aggregators should be retrieved.
     * @return The aggregators of the {@link TopologyGraphEntity}.
     */
    @Nonnull
    public Stream<E> getAggregators(@Nonnull final E topologyEntity) {
        return topologyEntity.getAggregators().stream();
    }


    /**
     * Get the entities to which a given {@link TopologyGraphEntity} is connected in the graph, via
     * an outbound "normal" connection.
     *
     * @param topologyEntity The {@link TopologyGraphEntity} whose connected entities should be
     *                       retrieved
     * @return The connected entities
     */
    @Nonnull
    public Stream<E> getOutboundAssociatedEntities(@Nonnull final E topologyEntity) {
        return topologyEntity.getOutboundAssociatedEntities().stream();
    }

    /**
     * Get the entities connected to a given {@link TopologyGraphEntity} in the graph, via an
     * inbound "normal" connection.
     *
     * @param topologyEntity The {@link TopologyGraphEntity} whose connected entities should be
     *                       retrieved
     * @return The connected entities
     */
    @Nonnull
    public Stream<E> getInboundAssociatedEntities(@Nonnull final E topologyEntity) {
        return topologyEntity.getInboundAssociatedEntities().stream();
    }

    /**
     * Get the entities that are controlled by a given {@link TopologyGraphEntity} in the graph.
     *
     * @param topologyEntity The {@link TopologyGraphEntity} whose controlled entities should be retrieved.
     * @return The entities that are controlled by a {@link TopologyGraphEntity} in the graph.
     */
    @Nonnull
    public Stream<E> getControlledEntities(@Nonnull final E topologyEntity) {
        return topologyEntity.getControlledEntities().stream();
    }

    /**
     * Get the controllers of a given {@link TopologyGraphEntity} in the graph.
     *
     * @param topologyEntity The {@link TopologyGraphEntity} whose controllers should be retrieved.
     * @return The controllers of the {@link TopologyGraphEntity}.
     */
    @Nonnull
    public Stream<E> getControllers(@Nonnull final E topologyEntity) {
        return topologyEntity.getControllers().stream();
    }

    /**
     * Get the number of entities in the graph.
     *
     * @return The number of entities in the graph.
     */
    public int size() {
        return graph.size();
    }

    /**
     * Render the graph as a string.
     *
     * @return The graph output in the format OID1: (list of consumers) -> (list of providers) OID2:
     * (list of consumers) -> (list of providers) etc...
     */
    @Override
    public String toString() {
        return graph.values().stream()
                .map(e -> e + ": ("
                        + e.getConsumers().stream().map(Object::toString).collect(Collectors.joining(", ")) + ") -> ("
                        + e.getProviders().stream().map(Object::toString).collect(Collectors.joining(", ")) + ")")
                .collect(Collectors.joining("\n"));
    }

    private static <V extends TopologyGraphEntity<V>> void visitForSort(long oid, TopSortContext<V> context) {
        if (!context.isPlaced(oid)) {
            if (context.visit(oid)) {
                context.getPredecessors(oid).forEach(p -> visitForSort(p.getOid(), context));
                context.place(oid);
                context.leave(oid);
            }
        }
    }

    /**
     * This class maintains bookkeeping info required by the topological sort algorithm during its
     * graph walk.
     *
     * @param <E> entity type
     */
    private static class TopSortContext<E extends TopologyGraphEntity<E>> {
        public static final int MAX_CYCLE_ENTITIES_TO_LOG = 10;
        private final TopologyGraph<E> graph;
        private final List<TopsortEdgeSupplier<E>> edgeSuppliers;
        /** this list will accumulate nodes in their final sorted order, during the graph walk. */
        private final LongList result;
        /**
         * keep track of nodes that have already been placed, to avoid re-visiting later in walk.
         */
        private final LongSet placed;
        /**
         * keep track of nodes that are currently being visited; if they're encoutnered a second
         * tiem during htat visit, that indicates a cycle involving the visited node.
         */
        private final LongSet visiting = new LongOpenHashSet();
        /* keep track of nodes that were detected as participating in cycles */
        private final LongSet idsInCycles = new LongOpenHashSet();

        TopSortContext(TopologyGraph<E> graph, List<TopsortEdgeSupplier<E>> edgeSuppliers) {
            this.graph = graph;
            this.edgeSuppliers = edgeSuppliers;
            this.result = new LongArrayList(graph.size());
            this.placed = new LongOpenHashSet(graph.size());
        }

        public boolean isPlaced(long id) {
            return placed.contains(id);
        }

        /* when we vinish a node visit, we place that node as next in result, and mark it placed */
        public void place(long id) {
            result.add(id);
            placed.add(id);
        }

        /*
         * record that we're currently visiting this node, and perform cycle check.
         */
        public boolean visit(long id) {
            if (!visiting.add(id)) {
                // this means we have a cycle involving this node, so the graph is not a DAG.
                // We'll treat this as if the edge we just traversed to get here had not been
                // in the graph, i.e. we'll do nothing. Our current visit to this node will
                // record it in the sorted array as we unwind.
                idsInCycles.add(id);
                return false;
            } else {
                visiting.add(id);
                return true;
            }
        }

        /**
         * no longer visiting this node.
         *
         * @param id entity id of entity no longer visited
         */
        public void leave(long id) {
            visiting.remove(id);
        }

        /**
         * get the list of nodes to be visited before placing this node.
         *
         * @param id entity id of entity being visited
         * @return list of entities that should sort before this entity based on direct edges
         */
        public Stream<E> getPredecessors(long id) {
            return graph.getEntity(id).map(e ->
                    edgeSuppliers.stream()
                            .flatMap(es -> es.getPredecessors(graph, e))
                            .distinct())
                    .orElse(Stream.empty());
        }

        /** log any cycles that may have shown up in this graph. */
        public void logCycles() {
            final List<String> descriptions = idsInCycles.stream()
                    .limit(MAX_CYCLE_ENTITIES_TO_LOG)
                    .map(graph::getEntity)
                    .map(optE -> optE.map(e ->
                            String.format("%d[%s]", e.getOid(), e.getDisplayName())))
                    .map(desc -> desc.orElse("?"))
                    .collect(Collectors.toList());
            logger.warn("Visits to {} entities resulted in detections of cycles. Some examples are: {}",
                    idsInCycles.size(), descriptions);

        }

        /**
         * finished top-level visit of all nodes; do some checks and stream back results.
         *
         * @return stream of entities ordered as so that every entity follows its predecessors
         */
        public Stream<E> getResults() {
            if (!idsInCycles.isEmpty()) {
                logCycles(); // if any
            }
            if (result.size() != graph.size()) {
                logger.error(
                        "Sorted entity stream size {} differs from graph size {}; sending unsorted entities",
                        result.size(), graph.size());
                return graph.entities();

            }
            return result.stream()
                    .map(graph::getEntity)
                    .filter(Optional::isPresent)
                    .map(Optional::get);
        }
    }

    /**
     * Interface for a supplier of edges incident on a given {@link TopologyGraphEntity} in the
     * graph.
     *
     * @param <E> node type
     */
    @FunctionalInterface
    public interface TopsortEdgeSupplier<E extends TopologyGraphEntity<E>> {

        /**
         * Obtain a stream of nodes connnected to a given {@link TopologyGraphEntity} in the graph,
         * based on some determination of relevant edges.
         *
         * @param graph  The graph
         * @param entity The node whose connected nodes are requested
         * @return a stream of the connected nodes
         */
        Stream<E> getPredecessors(TopologyGraph<E> graph, E entity);
    }
}

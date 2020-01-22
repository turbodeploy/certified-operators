package com.vmturbo.topology.graph;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A graph built from the topology.
 * The graph should be an immutable DAG (directed acyclic graph) though that is not enforced by this interface.
 *
 * The graph is constructed by following consumes/provides relations established by the commodities
 * bought and sold among the entities in the topology, as well as the explicit
 * connection relationships in the topology.
 *
 * The graph does NOT permit parallel edges. That is, an entity consuming multiple commodities
 * from the same provider results in only a single edge between the two in the graph.
 *
 * The objects within the graph's entities may be edited, but the graph is
 * immutable otherwise. Note that the edits made to the objects in the vertices
 * should not modify topological relationships or the graph will become out of synch with its members.
 * This means that commodities bought and sold by entities should not be modified in a way that they change
 * which entities are buying or selling commodities to each other.
 *
 * See {@link TopologyGraphEntity} for further details on the entities in the {@link TopologyGraph}.
 */
@Immutable
public class TopologyGraph<ENTITY extends TopologyGraphEntity<ENTITY>> {

    /**
     * A map permitting lookup from OID to {@link TopologyGraphEntity}.
     */
    @Nonnull private final Map<Long, ENTITY> graph;

    /**
     * An index permitting lookup from entity type to all the entities of that type in the graph.
     */
    @Nonnull private final Map<Integer, Collection<ENTITY>> entityTypeIndex;

    /**
     * Create a new topology graph. Called via the appropriate builder.
     *
     * @param graph The graph of entities in the {@link TopologyGraph}.
     */
    public TopologyGraph(@Nonnull final Map<Long, ENTITY> graph,
                         @Nonnull final Map<Integer, Collection<ENTITY>> entityTypeIndex) {
        this.graph = Objects.requireNonNull(graph);
        this.entityTypeIndex = Objects.requireNonNull(entityTypeIndex);
    }

    /**
     * Retrieve the {@link TopologyGraphEntity} for an entity in the graph by its OID.
     * Returns {@link Optional#empty()} if no such {@link TopologyGraphEntity} is in the graph.
     *
     * @param oid The OID of the {@link TopologyGraphEntity} in the graph.
     * @return The {@link TopologyGraphEntity} with the corresponding OID or {@link Optional#empty()} if no
     *         entity with the given OID exists.
     */
    @Nonnull
    public Optional<ENTITY> getEntity(final long oid) {
        return Optional.ofNullable(graph.get(oid));
    }

    /**
     * The multi-get version of {@link TopologyGraph#getEntity(long)}.
     *
     * @param oids The OIDs to return. Returns all entities if the input set is empty.
     *             The input is a set to avoid duplicates.
     * @return A stream of {@link TopologyGraphEntity}s with the corresponding OIDS, or all entities
     *         if the input is an empty set.
     */
    @Nonnull
    public Stream<ENTITY> getEntities(@Nonnull final Set<Long> oids) {
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
    public Stream<ENTITY> entities() {
        return graph.values().stream();
    }

    /**
     * Return the types of entities contained in the graph.
     */
    @Nonnull
    public Set<Integer> entityTypes() {
        return Collections.unmodifiableSet(entityTypeIndex.keySet());
    }

    /**
     * Get a stream of all {@link TopologyGraphEntity}s in the graph of a given type.
     * Implementation note: This call fetches entities of a given type in constant time.
     *
     * @param entityType The type of entities to be retrieved.
     * @return a stream of all {@link TopologyGraphEntity}s in the graph of the given type.
     *         If no entities of the given type are present, returns an empty stream.
     */
    @Nonnull
    public Stream<ENTITY> entitiesOfType(@Nonnull final EntityType entityType) {
        return entitiesOfType(entityType.getNumber());
    }

    /**
     * Get a stream of all {@link TopologyGraphEntity}s in the graph of a given type specified by the type number.
     * Implementation note: This call fetches entities of a given type in constant time.
     *
     * @param entityTypeNumber The number of the {@link EntityType} of entities to be retrieved.
     * @return a stream of all {@link TopologyGraphEntity}s in the graph of the given type.
     *         If no entities of the given type are present, returns an empty stream.
     */
    @Nonnull
    public Stream<ENTITY> entitiesOfType(@Nonnull final Integer entityTypeNumber) {
        final Collection<ENTITY> entitiesOfType = entityTypeIndex.get(entityTypeNumber);
        return entitiesOfType == null ? Stream.empty() : entitiesOfType.stream();
    }

    /**
     * Retrieve all consumers for a given entity in the graph.
     * If no {@link TopologyGraphEntity} with the given OID exists, returns empty.
     *
     * An entity is a consumer of another entity if it buys a commodity from
     * that entity.
     *
     * @param oid The OID of the {@link TopologyGraphEntity} in the graph.
     * @return All consumers for a {@link TopologyGraphEntity} in the graph by its OID.
     */
    @Nonnull
    public Stream<ENTITY> getConsumers(long oid) {
        // Because this is high-performance code, do not call getEntity which allocates
        // an additional object.
        final ENTITY entity = graph.get(oid);
        return entity == null ? Stream.empty() : getConsumers(entity);
    }

    /**
     * Get the consumers for a {@link TopologyGraphEntity} in the graph.
     *
     * @param entity The {@link TopologyGraphEntity} whose consumers should be retrieved.
     * @return The consumers for a {@link TopologyGraphEntity} in the graph.
     */
    @Nonnull
    public Stream<ENTITY> getConsumers(@Nonnull final ENTITY entity) {
        return entity.getConsumers().stream();
    }

    /**
     * Retrieve all providers for a given entity in the graph.
     * If no {@link TopologyGraphEntity} with the given OID exists, returns empty.
     *
     * An entity is a provider of another entity if the other entity
     * buys a commodity from this one.
     *
     * @param oid The OID of the {@link TopologyGraphEntity} in the graph.
     * @return All providers for a {@link TopologyGraphEntity} in the graph by its OID.
     */
    @Nonnull
    public Stream<ENTITY> getProviders(Long oid) {
        // Because this is high-performance code, do not call getEntity which allocates
        // an additional object.
        final ENTITY topologyEntity = graph.get(oid);
        return topologyEntity == null ? Stream.empty() : getProviders(topologyEntity);
    }

    /**
     * Get the providers for a {@link TopologyGraphEntity} in the graph.
     *
     * @param topologyEntity The {@link TopologyGraphEntity} whose providers should be retrieved.
     * @return The providers for a {@link TopologyGraphEntity} in the graph.
     */
    @Nonnull
    public Stream<ENTITY> getProviders(@Nonnull final ENTITY topologyEntity) {
        return topologyEntity.getProviders().stream();
    }

    /**
     * Get the entities that are aggregated or owned by a given {@link TopologyGraphEntity} in the graph.
     *
     * @param topologyEntity The {@link TopologyGraphEntity} whose aggregated and owned entities
     *                       should be retrieved.
     * @return The entities that are aggregated and owned by a {@link TopologyGraphEntity} in the graph.
     */
    @Nonnull
    public Stream<ENTITY> getOwnedOrAggregatedEntities(@Nonnull final ENTITY topologyEntity) {
        return topologyEntity.getAggregatedAndOwnedEntities().stream();
    }

    /**
     * Get the owner and aggregators of a given {@link TopologyGraphEntity} in the graph.
     *
     * @param topologyEntity The {@link TopologyGraphEntity} whose owner and aggregators should be retrieved.
     * @return The aggregators and owner of the {@link TopologyGraphEntity}.
     */
    @Nonnull
    public Stream<ENTITY> getOwnersOrAggregators(@Nonnull final ENTITY topologyEntity) {
        return topologyEntity.getAggregatorsAndOwner().stream();
    }

    /**
     * Get the entities that are owned by a given {@link TopologyGraphEntity} in the graph.
     *
     * @param topologyEntity The {@link TopologyGraphEntity} whose owned entities should be retrieved.
     * @return The entities that are aggregated and owned by a {@link TopologyGraphEntity} in the graph.
     */
    @Nonnull
    public Stream<ENTITY> getOwnedEntities(@Nonnull final ENTITY topologyEntity) {
        return topologyEntity.getOwnedEntities().stream();
    }

    /**
     * Get the owner of a given {@link TopologyGraphEntity} in the graph.
     *
     * @param topologyEntity The {@link TopologyGraphEntity} whose owner should be retrieved.
     * @return The owner of the {@link TopologyGraphEntity}.
     */
    @Nonnull
    public Stream<ENTITY> getOwner(@Nonnull final ENTITY topologyEntity) {
        return topologyEntity.getOwner().map(Stream::of).orElseGet(Stream::empty);
    }

    /**
     * Get the entities that are aggregated by a given {@link TopologyGraphEntity} in the graph.
     *
     * @param topologyEntity The {@link TopologyGraphEntity} whose aggregated entities should be retrieved.
     * @return The entities that are aggregated by a {@link TopologyGraphEntity} in the graph.
     */
    @Nonnull
    public Stream<ENTITY> getAggregatedEntities(@Nonnull final ENTITY topologyEntity) {
        return topologyEntity.getAggregatedEntities().stream();
    }

    /**
     * Get the aggregators of a given {@link TopologyGraphEntity} in the graph.
     *
     * @param topologyEntity The {@link TopologyGraphEntity} whose aggregators should be retrieved.
     * @return The aggregators of the {@link TopologyGraphEntity}.
     */
    @Nonnull
    public Stream<ENTITY> getAggregators(@Nonnull final ENTITY topologyEntity) {
        return topologyEntity.getAggregators().stream();
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
     * @return The graph output in the format
     *         OID1: (list of consumers) -> (list of providers)
     *         OID2: (list of consumers) -> (list of providers)
     *         etc...
     */
    @Override
    public String toString() {
        return graph.entrySet().stream()
            .map(entry -> entry.getValue() + ": (" +
                entry.getValue().getConsumers().stream().map(Object::toString).collect(Collectors.joining(", ")) + ") -> (" +
                entry.getValue().getProviders().stream().map(Object::toString).collect(Collectors.joining(", ")) + ")")
            .collect(Collectors.joining("\n"));
    }
}

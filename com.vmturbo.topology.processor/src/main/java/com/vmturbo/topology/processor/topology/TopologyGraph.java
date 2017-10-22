package com.vmturbo.topology.processor.topology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;

/**
 * A graph built from the topology.
 * The graph should be an immutable DAG (directed acyclic graph) though that is not enforced by this interface.
 *
 * The graph is constructed by following consumes/produces relations
 * among the entities in the graph.
 *
 * The graph does NOT permit parallel edges. That is, an entity consuming multiple commodities
 * from the same producer results in only a single edge between the two in the graph.
 *
 * The TopologyEntityDTO.Builder objects within the graph's vertices may be edited, but the graph is
 * immutable otherwise. Note that the edits made to the TopologyEntityDTO.Builder objects in the vertices
 * should not modify topological relationships or the graph will become out of synch with its members.
 *
 */
public class TopologyGraph {

    /**
     * A map permitting lookup from OID to vertex.
     */
    @Nonnull private final Map<Long, Vertex> graph;

    /**
     * Create a new topology graph.
     *
     * Note that graph construction does not validate the consistency of the input - for example,
     * if an entity in the input is buying from an entity not in the input, no error will be
     * generated.
     *
     * @param topologyMap A map of the topology, with the key being OID and value being the corresponding entity.
     */
    public TopologyGraph(@Nonnull final Map<Long, TopologyEntityDTO.Builder> topologyMap) {
        graph = new HashMap<>();

        topologyMap.entrySet()
            .forEach(mapEntry -> {
                final TopologyEntityDTO.Builder topologyEntityDTO = mapEntry.getValue();
                if (mapEntry.getKey() != topologyEntityDTO.getOid()) {
                    throw new IllegalArgumentException("Map key " + mapEntry.getValue() +
                        " does not match OID value: " + topologyEntityDTO.getOid());
                }
                addVertex(topologyEntityDTO, topologyMap);
            });
    }

    /**
     * Retrieve the vertex for an entity in the graph by its OID.
     * Returns {@link Optional#empty()} if the no such vertex is in the graph.
     *
     * @param oid The OID of the vertex in the graph.
     * @return The vertex with the corresponding OID.
     */
    public Optional<Vertex> getVertex(long oid) {
        return Optional.ofNullable(graph.get(oid));
    }

    /**
     * Get a stream of all vertices in the graph.
     *
     * @return A stream of all vertices in the graph.
     */
    public Stream<Vertex> vertices() {
        return graph.values().stream();
    }

    /**
     * Retrieve all consumers for a given entity in the graph.
     * If no vertex with the given OID exists, returns empty.
     *
     * An entity is a consumer of another entity if it buys a commodity from
     * that entity.
     *
     * @param oid The OID of the vertex in the graph.
     * @return All consumers for a vertex in the graph by its OID.
     */
    @Nonnull
    public Stream<Vertex> getConsumers(long oid) {
        // Because this is high-performance code, do not call getVertex which allocates
        // an additional object.
        final Vertex vertex = graph.get(oid);
        return vertex == null ? Stream.empty() : getConsumers(vertex);
    }

    /**
     * Get the consumers for a vertex in the graph.
     *
     * @param vertex The vertex whose consumers should be retrieved.
     * @return The consumers for a vertex in the graph.
     */
    @Nonnull
    public Stream<Vertex> getConsumers(@Nonnull final Vertex vertex) {
        return vertex.consumers.stream();
    }

    /**
     * Retrieve all producers for a given entity in the graph.
     * If no vertex with the given OID exists, returns empty.
     *
     * An entity is a producer of another entity if the other entity
     * buys a commodity from this one.
     *
     * @param oid The OID of the vertex in the graph.
     * @return All producers for a vertex in the graph by its OID.
     */
    @Nonnull
    public Stream<Vertex> getProducers(Long oid) {
        // Because this is high-performance code, do not call getVertex which allocates
        // an additional object.
        final Vertex vertex = graph.get(oid);
        return vertex == null ? Stream.empty() : getProducers(vertex);
    }

    /**
     * Get the producers for a vertex in the graph.
     *
     * @param vertex The vertex whose producers should be retrieved.
     * @return The producers for a vertex in the graph.
     */
    @Nonnull
    public Stream<Vertex> getProducers(@Nonnull final Vertex vertex) {
        return vertex.producers.stream();
    }

    /**
     * Get the number of vertices in the graph.
     *
     * @return The number of vertices in the graph.
     */
    public int vertexCount() {
        return graph.size();
    }

    /**
     * @return The graph output in the format
     *         OID1: (list of consumers) -> (list of producers)
     *         OID2: (list of consumers) -> (list of producers)
     *         etc...
     */
    @Override
    public String toString() {
        return graph.entrySet().stream()
            .map(entry -> entry.getValue() + ": (" +
                entry.getValue().consumers.stream().map(Object::toString).collect(Collectors.joining(", ")) + ") -> (" +
                entry.getValue().producers.stream().map(Object::toString).collect(Collectors.joining(", ")) + ")")
            .collect(Collectors.joining("\n"));
    }

    /**
     * Add a vertex corresponding to the input {@link TopologyEntityDTO}.
     * Adds consumes edges in the graph for all entities the input is consuming from.
     * Adds produces edges in the graph for all entities providing commodities this entity is consuming.
     *
     * Clients should never attempt to add a vertex for an entity already in the graph.
     * This may not be checked.
     *
     * @param topologyEntityDTO The entity to add a vertex for.
     */
    private void addVertex(@Nonnull final TopologyEntityDTO.Builder topologyEntityDTO,
                           @Nonnull final Map<Long, TopologyEntityDTO.Builder> topologyMap) {
        final Vertex vertex = getOrCreateVertex(topologyEntityDTO);

        for (Long producerOid : getCommodityBoughtProviderIds(topologyEntityDTO)) {
            final Vertex producer = getOrCreateVertex(topologyMap.get(producerOid));

            // Note that producers and consumers are lists, but we do not perform an explicit check
            // that the edge does not already exist. That is because such a check is unnecessary on
            // properly formed input. The producers are pulled from a set, so they must be unique,
            // and because the vertices themselves must be unique by OID (keys in constructor map are unique
            // plus check that key==OID in the constructor ensures this as long as OIDs are unique),
            // we are guaranteed that:
            //
            // 1. the producer cannot already be in the vertex's list of producers
            // 2. the vertex cannot already be in the producer's list of consumers
            //
            // Having an explicit check for this invariant given the current implementation would
            // be both expensive and redundant. However, keep this requirement in mind if making changes
            // to the implementation.
            vertex.producers.add(producer);
            producer.consumers.add(vertex);
        }
    }

    /**
     * Get a set of provider ids of entity's commodity bought list. And all these provider ids will
     * be producers of this entity. For commodity bought without provider id will be filtered out.
     *
     * @param topologyEntityDTO The entity to add a vertex for.
     * @return A set of provider ids.
     */
    private Set<Long> getCommodityBoughtProviderIds(@Nonnull final TopologyEntityDTO.Builder topologyEntityDTO) {
        return topologyEntityDTO.getCommoditiesBoughtFromProvidersList().stream()
            .filter(CommoditiesBoughtFromProvider::hasProviderId)
            .map(CommoditiesBoughtFromProvider::getProviderId)
            .collect(Collectors.toSet());
    }

    /**
     * Get the vertex corresponding to an entity from the graph, or if it does not exist,
     * create one and insert it into the graph.
     *
     * @param topologyEntityDTO The entity whose corresponding vertex should be looked up.
     * @return The retrieved or newly created vertex for the entity.
     */
    private Vertex getOrCreateVertex(@Nonnull final TopologyEntityDTO.Builder topologyEntityDTO) {
        return getVertex(topologyEntityDTO.getOid()).orElseGet(() -> {
            final Vertex vertex = new Vertex(topologyEntityDTO);
            graph.put(vertex.getOid(), vertex);
            return vertex;
        });
    }

    /**
     * A node in the TopologyGraph.
     *
     * Contains information about the OID and entityType for an entity in the topology.
     * Vertices are equivalent when their OIDs are equal.
     *
     * The TopologyEntityDTO.Builder within a vertex may be edited but the vertex is immutable otherwise.
     */
    public static class Vertex {

        /**
         * A builder for the entity in the topology corresponding to this vertex.
         */
        private TopologyEntityDTO.Builder entityBuilder;

        /**
         * The set of all vertices in the graph that consume from this vertex.
         */
        private final List<Vertex> consumers;

        /**
         * The set of all vertices in the graph that produce for this vertex.
         */
        private final List<Vertex> producers;

        public Vertex(@Nonnull final TopologyEntityDTO.Builder entity) {
            this.entityBuilder = Objects.requireNonNull(entity);
            this.consumers = new ArrayList<>();
            this.producers = new ArrayList<>();
        }

        /**
         * Get the OID for this vertex.
         *
         * @return the OID for this vertex.
         */
        public long getOid() {
            return entityBuilder.getOid();
        }

        /**
         * Get the entityType. This field corresponds to {@link TopologyEntityDTO#getEntityType()}
         * Stored here as an optimization to avoid a lookup because it is used so commonly.
         *
         * @return The entityType for the entityBuilder corresponding to this node.
         */
        public int getEntityType() {
            return entityBuilder.getEntityType();
        }

        /**
         * Get a builder for the entity associated with this vertex.
         * The builder may be mutated to modify the properties of the entity.
         *
         * DO NOT modify the providers or consumers of the entity or the graph will be invalidated.
         *
         * @return The property information for the entity associated with this vertex.
         */
        @Nonnull
        public TopologyEntityDTO.Builder getTopologyEntityDtoBuilder() {
            return entityBuilder;
        }

        @Override
        public String toString() {
            return "(oid: " + getOid() + ", entityType: " + getEntityType() + ")";
        }
    }
}

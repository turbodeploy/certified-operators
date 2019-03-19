package com.vmturbo.topology.processor.topology;

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
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;

/**
 * A graph built from the topology.
 * The graph should be an immutable DAG (directed acyclic graph) though that is not enforced by this interface.
 *
 * The graph is constructed by following consumes/provides relations established by the commodities
 * bought and sold among the entities in the topology.
 *
 * The graph does NOT permit parallel edges. That is, an entity consuming multiple commodities
 * from the same provider results in only a single edge between the two in the graph.
 *
 * The TopologyEntityDTO.Builder objects within the graph's entities may be edited, but the graph is
 * immutable otherwise. Note that the edits made to the TopologyEntityDTO.Builder objects in the vertices
 * should not modify topological relationships or the graph will become out of synch with its members.
 * This means that commodities bought and sold by entities should not be modified in a way that they change
 * which entities are buying or selling commodities to each other.
 *
 * See {@link TopologyEntity} for further details on the entities in the {@link TopologyGraph}.
 */
public class TopologyGraph {

    /**
     * A map permitting lookup from OID to {@link TopologyEntity}.
     */
    @Nonnull private final Map<Long, TopologyEntity> graph;

    /**
     * An index permitting lookup from entity type to all the entities of that type in the graph.
     */
    @Nonnull private final Map<Integer, List<TopologyEntity>> entityTypeIndex;

    /**
     * Create a new topology graph. Called via the appropriate builder.
     *
     * @param graph The graph of entities in the {@link TopologyGraph}.
     */
    private TopologyGraph(@Nonnull final Map<Long, TopologyEntity> graph,
                          @Nonnull final Map<Integer, List<TopologyEntity>> entityTypeIndex) {
        this.graph = Objects.requireNonNull(graph);
        this.entityTypeIndex = Objects.requireNonNull(entityTypeIndex);
    }

    /**
     * Retrieve the {@link TopologyEntity} for an entity in the graph by its OID.
     * Returns {@link Optional#empty()} if no such {@link TopologyEntity} is in the graph.
     *
     * @param oid The OID of the {@link TopologyEntity} in the graph.
     * @return The {@link TopologyEntity} with the corresponding OID or {@link Optional#empty()} if no
     *         entity with the given OID exists.
     */
    public Optional<TopologyEntity> getEntity(final long oid) {
        return Optional.ofNullable(graph.get(oid));
    }

    /**
     * Get a stream of all {@link TopologyEntity}s in the graph.
     *
     * @return A stream of all {@link TopologyEntity}s in the graph.
     */
    public Stream<TopologyEntity> entities() {
        return graph.values().stream();
    }

    /**
     * Get a stream of all {@link TopologyEntity}s in the graph of a given type.
     * Implementation note: This call fetches entities of a given type in constant time.
     *
     * @param entityType The type of entities to be retrieved.
     * @return a stream of all {@link TopologyEntity}s in the graph of the given type.
     *         If no entities of the given type are present, returns an empty stream.
     */
    public Stream<TopologyEntity> entitiesOfType(@Nonnull final EntityType entityType) {
        return entitiesOfType(entityType.getNumber());
    }

    /**
     * Get a stream of all {@link TopologyEntity}s in the graph of a given type specified by the type number.
     * Implementation note: This call fetches entities of a given type in constant time.
     *
     * @param entityTypeNumber The number of the {@link EntityType} of entities to be retrieved.
     * @return a stream of all {@link TopologyEntity}s in the graph of the given type.
     *         If no entities of the given type are present, returns an empty stream.
     */
    public Stream<TopologyEntity> entitiesOfType(@Nonnull final Integer entityTypeNumber) {
        final List<TopologyEntity> entitiesOfType = entityTypeIndex.get(entityTypeNumber);
        return entitiesOfType == null ? Stream.empty() : entitiesOfType.stream();
    }

    /**
     * Retrieve all consumers for a given entity in the graph.
     * If no {@link TopologyEntity} with the given OID exists, returns empty.
     *
     * An entity is a consumer of another entity if it buys a commodity from
     * that entity.
     *
     * @param oid The OID of the {@link TopologyEntity} in the graph.
     * @return All consumers for a {@link TopologyEntity} in the graph by its OID.
     */
    @Nonnull
    public Stream<TopologyEntity> getConsumers(long oid) {
        // Because this is high-performance code, do not call getEntity which allocates
        // an additional object.
        final TopologyEntity entity = graph.get(oid);
        return entity == null ? Stream.empty() : getConsumers(entity);
    }

    /**
     * Get the consumers for a {@link TopologyEntity} in the graph.
     *
     * @param entity The {@link TopologyEntity} whose consumers should be retrieved.
     * @return The consumers for a {@link TopologyEntity} in the graph.
     */
    @Nonnull
    public Stream<TopologyEntity> getConsumers(@Nonnull final TopologyEntity entity) {
        return entity.getConsumers().stream();
    }

    /**
     * Retrieve all providers for a given entity in the graph.
     * If no {@link TopologyEntity} with the given OID exists, returns empty.
     *
     * An entity is a provider of another entity if the other entity
     * buys a commodity from this one.
     *
     * @param oid The OID of the {@link TopologyEntity} in the graph.
     * @return All providers for a {@link TopologyEntity} in the graph by its OID.
     */
    @Nonnull
    public Stream<TopologyEntity> getProviders(Long oid) {
        // Because this is high-performance code, do not call getEntity which allocates
        // an additional object.
        final TopologyEntity topologyEntity = graph.get(oid);
        return topologyEntity == null ? Stream.empty() : getProviders(topologyEntity);
    }

    /**
     * Get the providers for a {@link TopologyEntity} in the graph.
     *
     * @param topologyEntity The {@link TopologyEntity} whose providers should be retrieved.
     * @return The providers for a {@link TopologyEntity} in the graph.
     */
    @Nonnull
    public Stream<TopologyEntity> getProviders(@Nonnull final TopologyEntity topologyEntity) {
        return topologyEntity.getProviders().stream();
    }

    /**
     * Get the entities that are connected to the given {@link TopologyEntity} in the graph.
     *
     * @param oid Oid of the {@link TopologyEntity} whose connectedFromEntities should be retrieved.
     * @return The entities that are connected to a {@link TopologyEntity} in the graph.
     */
    @Nonnull
    public Stream<TopologyEntity> getConnectedFromEntities(@Nonnull final Long oid) {
        final TopologyEntity topologyEntity = graph.get(oid);
        return topologyEntity == null ? Stream.empty() : getConnectedFromEntities(topologyEntity);
    }

    /**
     * Get the entities that are connected to the given {@link TopologyEntity} in the graph.
     *
     * @param topologyEntity The {@link TopologyEntity} whose connectedFromEntities should be retrieved.
     * @return The entities that are connected to the {@link TopologyEntity} in the graph.
     */
    @Nonnull
    public Stream<TopologyEntity> getConnectedFromEntities(@Nonnull final TopologyEntity topologyEntity) {
        return topologyEntity.getConnectedFromEntities().stream();
    }

    /**
     * Get the specific type of entities which are connected to the given entity.
     *
     * @param oid the oid of the entity to get connected from entities for
     * @param entityType type of the connectedFrom entities to get
     * @return Stream of the given type of entities that are connected to the {@link TopologyEntity} in the graph.
     */
    public Stream<TopologyEntityDTO.Builder> getConnectedFromEntitiesOfType(@Nonnull Long oid,
                                                                            @Nonnull Integer entityType) {
        return getConnectedFromEntities(oid)
            .filter(entity -> entity.getEntityType() == entityType)
            .map(TopologyEntity::getTopologyEntityDtoBuilder);
    }

    /**
     * Get the entities that given {@link TopologyEntity} in the graph is connected to.
     *
     * @param oid Oid of the {@link TopologyEntity} whose connectedToEntities should be retrieved.
     * @return The entities that {@link TopologyEntity} are connected to in the graph.
     */
    @Nonnull
    public Stream<TopologyEntity> getConnectedToEntities(@Nonnull final Long oid) {
        final TopologyEntity topologyEntity = graph.get(oid);
        return topologyEntity == null ? Stream.empty() : getConnectedToEntities(topologyEntity);
    }

    /**
     * Get the entities that given {@link TopologyEntity} in the graph are connected to.
     *
     * @param topologyEntity The {@link TopologyEntity} whose connectedToEntities should be retrieved.
     * @return The entities that {@link TopologyEntity} are connected to in the graph.
     */
    @Nonnull
    public Stream<TopologyEntity> getConnectedToEntities(@Nonnull final TopologyEntity topologyEntity) {
        return topologyEntity.getConnectedToEntities().stream();
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

    /**
     * Create a new topology graph from a map of OID -> {@link TopologyEntity.Builder}.
     * Note that the builders in the map should have their lastUpdatedTime set, but the consumer and provider
     * relationships should be empty because graph construction will create these relationships.
     *
     * Note that graph construction does not validate the consistency of the input - for example,
     * if an entity in the input is buying from an entity not in the input, no error will be
     * generated. This validation should occur earlier (ie in stitching).
     *
     * @param topologyBuilderMap A map of the topology as represented by builders for TopologyEntities,
     *                           The builders in the map should have their lastUpdatedTime set but their consumer
     *                           and provider relationships will be set up during graph construction.
     *                           The keys in the map are entity OIDs.
     */
    public static TopologyGraph newGraph(@Nonnull final Map<Long, TopologyEntity.Builder> topologyBuilderMap) {
        return new Builder(topologyBuilderMap).build();
    }

    /**
     * A builder for a {@Link TopologyGraph}.
     */
    private static class Builder {
        @Nonnull
        final Map<Long, TopologyEntity.Builder> topologyBuilderMap;

        private Builder(@Nonnull final Map<Long, TopologyEntity.Builder> topologyBuilderMap) {
            this.topologyBuilderMap = Objects.requireNonNull(topologyBuilderMap);
        }

        /**
         * Build a new {@link TopologyGraph} from this builder.
         * Builders should not be re-used more than once.
         *
         * Note that an entity is unable to participate in multiple graphs simultaneously.
         *
         * @return A {@link TopologyGraph} built from this builder.
         */
        public TopologyGraph build() {
            // Clear any previously established consumers and providers because these relationships
            // will be set up from scratch while constructing the graph.
            topologyBuilderMap.values().forEach(TopologyEntity.Builder::clearConsumersAndProviders);

            topologyBuilderMap.forEach((oid, entity) -> {
                if (oid != entity.getOid()) {
                    throw new IllegalArgumentException("Map key " + entity +
                        " does not match OID value: " + entity.getOid());
                }
                addConsumerSideRelationships(entity);
                addConnectedFromSideRelationships(entity);
            });

            final Map<Long, TopologyEntity> graph = topologyBuilderMap.values().stream()
                .collect(Collectors.toMap(TopologyEntity.Builder::getOid, TopologyEntity.Builder::build));
            final Map<Integer, List<TopologyEntity>> entityTypeIndex = graph.values().stream()
                .collect(Collectors.groupingBy(TopologyEntity::getEntityType));

            return new TopologyGraph(graph, entityTypeIndex);
        }

        /**
         * Set up the consumer-side relationships for a {@link TopologyEntity}. That is, for each entity that this
         * entity consumes from, add those entities as providers to this entity, and add this entity as a consumer
         * of those entities.
         *
         * Note that because of the symmetry of the consumer-provider relationships, by adding relations following
         * the consumer-side for all entities, we guarantee that the the provider-side relationships when calling
         * for this entity will be set up at other points when this method is called on the consumers of this entity.
         *
         * Clients should never attempt to add a {@link TopologyEntity} for an entity already in the graph.
         * This may not be checked.
         *
         * @param topologyEntity The entity whose relationships should be added.
         */
        private void addConsumerSideRelationships(@Nonnull final TopologyEntity.Builder topologyEntity) {
            for (Long providerOid : getCommodityBoughtProviderIds(topologyEntity.getEntityBuilder())) {
                final TopologyEntity.Builder providerEntity = topologyBuilderMap.get(providerOid);
                // The provider might be null if the entity has unplaced commodities (e.g. for clones)
                if (providerEntity != null) {
                    // Note that providers and consumers are lists, but we do not perform an explicit check
                    // that the edge does not already exist. That is because such a check is unnecessary on
                    // properly formed input. The providers are pulled from a set, so they must be unique,
                    // and because the entities themselves must be unique by OID (keys in constructor map are unique
                    // plus check that key==OID in the constructor ensures this as long as OIDs are unique),
                    // we are guaranteed that:
                    //
                    // 1. the provider cannot already be in the TopologyEntity's list of providers
                    // 2. the TopologyEntity cannot already be in the provider's list of consumers
                    //
                    // Having an explicit check for this invariant given the current implementation would
                    // be both expensive and redundant. However, keep this requirement in mind if making changes
                    // to the implementation.
                    topologyEntity.addProvider(providerEntity);
                    providerEntity.addConsumer(topologyEntity);
                } else if (providerOid >= 0) {
                    // Clones have negative OIDs for their unplaced commodities.
                    // If the OID is non-negative (and, thus, a valid ID), this means
                    // the input topology is inconsistent.
                    throw new IllegalArgumentException("Illegal topology! Entity " +
                        topologyEntity.getOid() + " (name " + topologyEntity.getEntityBuilder().getDisplayName() +
                        ") is consuming a commodity from non-existing provider " + providerOid);
                }
            }
        }

        /**
         * Set up the connnetedFrom-side relationships for a {@link TopologyEntity}. That is, for
         * each entity that this entity is connected to, add those entities as connectedToEntities
         * to this entity, and add this entity as a connectedFromEntity of those entities.
         */
        private void addConnectedFromSideRelationships(@Nonnull final TopologyEntity.Builder topologyEntity) {
            topologyEntity.getEntityBuilder().getConnectedEntityListList().forEach(connectedEntity -> {
                final TopologyEntity.Builder connectedToEntity =
                        topologyBuilderMap.get(connectedEntity.getConnectedEntityId());
                topologyEntity.addConnectedTo(connectedToEntity);
                connectedToEntity.addConnectedFrom(topologyEntity);
            });
        }

        /**
         * Get a set of provider ids of entity's commodity bought list. And all these provider ids will
         * be providers of this entity. For commodity bought without provider id will be filtered out.
         *
         * @param topologyEntityDTO The entity to add a {@link TopologyEntity} for.
         * @return A set of provider ids.
         */
        private Set<Long> getCommodityBoughtProviderIds(@Nonnull final TopologyEntityDTO.Builder topologyEntityDTO) {
            return topologyEntityDTO.getCommoditiesBoughtFromProvidersList().stream()
                .filter(CommoditiesBoughtFromProvider::hasProviderId)
                .map(CommoditiesBoughtFromProvider::getProviderId)
                .collect(Collectors.toSet());
        }
    }
}

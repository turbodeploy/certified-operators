package com.vmturbo.topology.processor.stitching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.stitching.StitchingGraph;
import com.vmturbo.stitching.StitchingOperationResult.CommoditiesBoughtChange;
import com.vmturbo.stitching.StitchingOperationResult.RemoveEntityChange;

/**
 * A mutable graph built from the forest of partial topologies discovered by individual targets.
 * The graph should be a mutable DAG (though the DAG-ness is not enforced by this class).
 *
 * The graph is constructed by following consumes/produces relations
 * among the entities in the graph.
 *
 * The graph does NOT permit parallel edges. That is, an entity consuming multiple commodities
 * from the same provider results in only a single edge between the two in the graph.
 *
 * The EntityDTO.Builder objects within the graph's vertices may be edited, but edits to buying/selling
 * relationships on the DTOs are NOT automatically reflected in the relationships available on the
 * graph itself.
 *
 * After the graph is initially constructed, it may be mutated via calls to the
 * {@link #removeEntity(RemoveEntityChange)} and {@link #updateCommoditiesBought(CommoditiesBoughtChange)}
 * methods.
 *
 * Note that because a multiple EntityDTOs may map to the same localId, requesting a vertex
 * by localId returns a stream of EntityDTOs rather than a single one.
 *
 * Mutations permitted to the graph:
 * 1. Removing an entity - Removing an entity from the graph propagates a change to all buyers of commodities
 *                         from the entity being removed.
 * 2. Commodities bought - Commodities bought by an entity may be added or removed. These changes will
 *                         automatically be propagated to the sellers of the commodities.
 *
 * Mutations NOT permitted to the graph:
 * 1. The creation of new entities.
 * 2. Commodities sold - No destructive mutations are permitted to commodities sold (that is, changes
 *                       that would change or remove relationships to buyers of the commodities being changed).
 *                       If a use case for this arises, we may consider supporting it in the future.
 *
 * TODO: Support entities with the same localId discovered by two different targets.
 */
@NotThreadSafe
public class TopologyStitchingGraph implements StitchingGraph {

    private static final Logger logger = LogManager.getLogger();

    /**
     * A map permitting lookup from localId to vertex.
     */
    @Nonnull
    private final Map<String, Vertex> graph;

    /**
     * Create a new stitchingGraph.
     *
     * Note that stitchingGraph construction does not validate the consistency of the input - for example,
     * if an entity in the input is buying from an entity not in the input, no error will be
     * generated.
     */
    public TopologyStitchingGraph(int expectedSize) {
        graph = new HashMap<>(expectedSize);
    }

    /**
     * Get a stream of all vertices in the stitchingGraph.
     *
     * @return A stream of all vertices in the stitchingGraph.
     */
    public Stream<Vertex> vertices() {
        return graph.values().stream();
    }

    /**
     * Retrieve the vertex for an entity in the stitchingGraph by its localId.
     * Returns {@link Optional#empty()} if the no such vertex is in the stitchingGraph.
     *
     * @param localId The target-localId of the vertex in the stitchingGraph.
     * @return The vertex with the corresponding localId.
     */
    public Optional<Vertex> getVertex(String localId) {
        return Optional.ofNullable(graph.get(localId));
    }

    /**
     * Retrieve all consumers for a given entity in the stitchingGraph.
     * If no vertex with the given localId exists, returns empty.
     *
     * An entity is a consumer of another entity if it buys a commodity from
     * that entity.
     *
     * @param localId The target-local ID of the vertex in the stitchingGraph.
     * @return All consumers for a vertex in the stitchingGraph by its localId.
     */
    @Nonnull
    public Stream<EntityDTO.Builder> getConsumerEntities(@Nonnull final String localId) {
        return getConsumers(localId)
            .flatMap(consumer -> consumer.getStitchingData().stream()
                .map(StitchingEntityData::getEntityDtoBuilder));
    }

    /**
     * Get the consumers for a vertex in the stitchingGraph.
     *
     * @param vertex The vertex whose consumers should be retrieved.
     * @return The consumers for a vertex in the stitchingGraph.
     */
    @Nonnull
    public Stream<Vertex> getConsumers(@Nonnull final Vertex vertex) {
        return vertex.consumers.stream();
    }

    /**
     * Get the consumers for a localId in the stitchingGraph.
     *
     * @param localId The localId of the vertex whose consumers should be retrieved.
     * @return The consumers for a vertex in the stitchingGraph.
     */
    @Nonnull
    public Stream<Vertex> getConsumers(@Nonnull final String localId) {
        final Vertex vertex = graph.get(localId);
        return vertex == null ? Stream.empty() : vertex.consumers.stream();
    }

    /**
     * Retrieve all providers for a given entity in the stitchingGraph.
     * If no vertex with the given localId exists, returns empty.
     *
     * An entity is a provider of another entity if the other entity
     * buys a commodity from this one.
     *
     * @param localId The target-local ID of the vertex in the stitchingGraph.
     * @return All providers for a vertex in the stitchingGraph by its localId.
     */
    @Nonnull
    public Stream<EntityDTO.Builder> getProviderEntities(String localId) {
        return getProviders(localId)
            .flatMap(provider -> provider.getStitchingData().stream()
                .map(StitchingEntityData::getEntityDtoBuilder));
    }

    /**
     * Get the providers for a vertex in the stitchingGraph.
     *
     * @param vertex The vertex whose providers should be retrieved.
     * @return The providers for a vertex in the stitchingGraph.
     */
    @Nonnull
    public Stream<Vertex> getProviders(@Nonnull final Vertex vertex) {
        return vertex.providers.stream();
    }

    /**
     * Get the providers for a  in the stitchingGraph.
     *
     * @param localId The local ID of the vertex whose providers should be retrieved.
     * @return The providers for a vertex in the stitchingGraph.
     */
    @Nonnull
    public Stream<Vertex> getProviders(@Nonnull final String localId) {
        final Vertex vertex = graph.get(localId);
        return vertex == null ? Stream.empty() : vertex.providers.stream();
    }

    /**
     * Get the number of vertices in the stitchingGraph.
     *
     * @return The number of vertices in the stitchingGraph.
     */
    public int vertexCount() {
        return graph.size();
    }

    /**
     * Look up the target ID for a given {@link EntityDTO} in the graph.
     *
     * @param entityBuilder The builder whose target ID should be looked up.
     * @return The targetID of the target that originally discovered by the builder.
     *         Returns {@link Optional#empty()} if the builder is not known to the graph.
     */
    public Optional<Long> getTargetId(@Nonnull final EntityDTO.Builder entityBuilder) {
        return getVertex(entityBuilder.getId())
            .flatMap(vertex -> vertex.getStitchingData().stream()
                // The faster reference equals is safe here. Doing a full equals on these DTOs is expensive.
                .filter(stitchingData -> stitchingData.getEntityDtoBuilder() == entityBuilder)
                .findFirst()
                .map(StitchingEntityData::getTargetId));
    }

    /**
     * Get the builder for the {@link EntityDTO} with the given target-localId discovered by the
     * target with the given targetId.
     *
     * @param localId The target-localId for the {@link EntityDTO} that should be returned.
     * @param targetId The ID of the target that discovered the {@link EntityDTO} whose builder
     *                 should be returned.
     * @return the builder for the {@link EntityDTO} with the given localId discovered by the
     *         target with the given targetId. Returns {@link Optional#empty()} if no such
     *         entity builder can be found.
     */
    public Optional<EntityDTO.Builder> getEntityBuilder(@Nonnull final String localId,
                                                        final long targetId) {
        return getVertex(localId)
            .flatMap(vertex -> vertex.getStitchingData().stream()
                .filter(stitchingData -> stitchingData.getTargetId() == targetId)
                .findFirst()
                .map(StitchingEntityData::getEntityDtoBuilder));
    }

    /**
     * Add a vertex corresponding to the input {@link StitchingEntityData}.
     * Adds consumes edges in the stitchingGraph for all entities the input is consuming from.
     * Adds produces edges in the stitchingGraph for all entities providing commodities this entity is consuming.
     *
     * Clients should never attempt to add a vertex for an entity already in the stitchingGraph.
     * This may not be checked.
     *
     * @param entityData The entity to add a vertex for.
     * @param entityMap The map of localId -> {@link StitchingEntityData} for the target that discovered
     *                  this entity.
     */
    public void addStitchingData(@Nonnull final StitchingEntityData entityData,
                                 @Nonnull final Map<String, StitchingEntityData> entityMap) {
        final Vertex vertex = getOrCreateVertexAndAdd(entityData);

        for (CommodityBought commodityBought : entityData.getEntityDtoBuilder().getCommoditiesBoughtList()) {
            final String providerId = commodityBought.getProviderId();
            final Vertex provider = getOrCreateVertexAndAdd(entityMap.get(providerId));
            if (!provider.getLocalId().equals(providerId)) {
                throw new IllegalArgumentException("Map key " + providerId +
                    " does not match localId value: " + provider.getLocalId());
            }

            vertex.providers.add(provider);
            provider.consumers.add(vertex);
        }
    }

    /**
     * Remove a {@link EntityDTO} from the graph as specified by the {@link RemoveEntityChange}.
     *
     * Each vertex in the graph corresponds to a unique localId in the topology.
     * Because multiple {@link EntityDTO}s may map to the same localId/vertex, removing an
     * {@link EntityDTO} does not guarantee that the associated vertex in the graph is removed.
     * However, if the {@link EntityDTO} is the last entity associated with the vertex, removing that
     * entity removes the associated vertex completely from the graph.
     *
     * For each vertex in the graph that has a provider or consumer relation to the entity-vertex being
     * removed, if that relation existed solely because it was buying from or selling to the {@link EntityDTO}
     * being removed, those vertices are updated by deleting their relationships to the vertex
     * whose entity is being removed.
     *
     * Entities known to be buying from the removed {@link EntityDTO} have their commodities updated
     * so that the commodities they used to buy from the {@link EntityDTO} are also removed.
     *
     * Attempting to remove an entity not in the graph is treated as a no-op.
     *
     * @param removal The removal instruction specifying the {@link EntityDTO} to be removed.
     * @return The set of localIds for all entities affected by the removal. The localId of the removed
     *         entity is always in this set unless no entity was actually removed.
     */
    public Set<String> removeEntity(@Nonnull final RemoveEntityChange removal) {
        final String localId = removal.entityBuilder.getId();
        final Optional<Long> optionalTargetId = getTargetId(removal.entityBuilder);
        if (!optionalTargetId.isPresent()) {
            // Treat as a no-op. Imagine a fake disk-array is connected to multiple storages which are each
            // stitched. Imagine we remove this same fake disk array each time we see it connected to a
            // storage we are stitching, resulting in multiple removal calls. This is fine, and results
            // in the expected behavior of the disk array no longer being in the topology, which is
            // why removing an entity that is no longer there is treated so lightly.
            logger.debug("Unable to remove data for removal with entity {}",
                removal.entityBuilder.getId());
            return Collections.emptySet();
        }

        final Long targetId = optionalTargetId.get();
        final Vertex vertex = graph.get(localId);

        logger.debug("Removing {}-{}", removal.entityBuilder.getEntityType(),
            removal.entityBuilder.getId());
        final String removalId = removal.entityBuilder.getId();
        final List<Vertex> consumersNoLongerBuying = new ArrayList<>();
        final Set<String> affectedEntityIds = new HashSet<>();
        affectedEntityIds.add(localId);

        // Remove consumer commodities bought from the entity being removed.
        // Only consider entities discovered by the same target that were discovered
        // by the target being removed.
        // TODO: DavidBlinn (10/31/2017) Consider allowing an option on the removal to specify
        // TODO: whether or which associated commodities should be removed.
        getConsumers(vertex)
            .flatMap(v -> v.getStitchingData().stream())
            .filter(stitchingData -> targetId.equals(stitchingData.getTargetId()))
            .map(StitchingEntityData::getEntityDtoBuilder)
            .forEach(consumer -> {
                Optional<EntityDTO> original = Optional.empty();

                final List<CommodityBought> toRetain = consumer.getCommoditiesBoughtList().stream()
                    .filter(commodityBought -> !commodityBought.getProviderId().equals(removalId))
                    .collect(Collectors.toList());
                if (toRetain.size() != consumer.getCommoditiesBoughtList().size()) {
                    original = Optional.of(consumer.build());

                    consumer.clearCommoditiesBought();
                    consumer.addAllCommoditiesBought(toRetain);
                }

                original.ifPresent(originalConsumer -> {
                    final Vertex consumerVertex = graph.get(consumer.getId());
                    final RelationshipChanges relationshipChanges =
                        computeRelationshipChanges(consumerVertex, consumer, originalConsumer);
                    if (!relationshipChanges.removedProviders.isEmpty()) {
                        consumersNoLongerBuying.add(consumerVertex);
                        affectedEntityIds.add(consumerVertex.getLocalId());
                    }
                });
            });

        consumersNoLongerBuying.forEach(consumerVertex -> consumerVertex.providers.remove(vertex));
        vertex.remove(removal.entityBuilder);
        if (vertex.isEmpty()) {
            graph.remove(localId);
        }

        return affectedEntityIds;
    }

    /**
     * Update the relationships for an {@link EntityDTO} in the graph as specified by
     * the {@link CommoditiesBoughtChange}.
     *
     * A {@link CommoditiesBoughtChange} contains an update method that modifies the {@link EntityDTO}
     * in the update by changing which commodities it buys and who it buys them from.
     *
     * The update works by analyzing the relationships of the entity before and after applying
     * the update function.
     *
     * Attempting to update an entity not in the graph results in an {@link UnknownEntityException}.
     *
     * If the updates to the commodities bought results in changes to usage values of commodities sold,
     * it is up to the {@link CommoditiesBoughtChange#updateMethod} to carry out those changes,
     * they are NOT automatically performed by applying the change to the graph.
     *
     * Note that the graph does NOT currently support destructive changes to commodities sold.
     *
     * @param update The update to apply to the graph.
     * @return The set of local ids for all entities affected by the update. The localID of the updated
     *         entity is always in this set. Other entities are included in the set if they now sell
     *         to the entity when they did not before or if they no longer buy sell to the entity
     *         when they used to do so. Note that because relationships are established by buying
     *         commodities that only the entity in the update can have NEW buying relationships.
     * @throws UnknownEntityException if the entity being updated is not in the graph.
     */
    public Set<String> updateCommoditiesBought(@Nonnull final CommoditiesBoughtChange update) {
        final String localId = update.entityBuilder.getId();
        final Vertex vertex = graph.get(localId);
        if (vertex == null) {
            throw new UnknownEntityException("No vertex for id: " + localId);
        }

        final Set<String> affectedEntityIds = new HashSet<>();
        affectedEntityIds.add(vertex.getLocalId());
        logger.debug("Updating {}-{}", update.entityBuilder.getEntityType(),
            update.entityBuilder.getId());

        // Apply the update
        final EntityDTO original = update.entityBuilder.build();
        update.updateMethod.accept(update.entityBuilder);
        final RelationshipChanges relationshipChanges =
            computeRelationshipChanges(vertex, update.entityBuilder, original);

        // Remove all the relationship on the original not in the updated
        relationshipChanges.removedProviders.forEach(provider -> {
            final Vertex providerVertex = graph.get(provider);
            affectedEntityIds.add(providerVertex.getLocalId());

            providerVertex.consumers.remove(vertex);
            vertex.providers.remove(providerVertex);
        });

        // Add all relationships on the updated not in the original
        relationshipChanges.newProviders.forEach(provider -> {
            final Vertex providerVertex = graph.get(provider);
            affectedEntityIds.add(providerVertex.getLocalId());

            providerVertex.consumers.add(vertex);
            vertex.providers.add(providerVertex);
        });

        return affectedEntityIds;
    }

    /**
     * Compute an update to a vertex describing the changes made to the stitching builder
     * from its original state.
     *
     * @param vertex The vertex being updated.
     * @param postMutationEntity The entity or builder for the {@link EntityDTO} reflecting the state of the
     *                           entity after it has been affected by a modification.
     * @param preMutationEntity The original state prior to updating the {@link EntityDTO}
     *                          described by the stitching builder before.
     * @return An {@link RelationshipChanges} relationships that changed on the {@link Vertex}.
     */
    @Nonnull
    private RelationshipChanges computeRelationshipChanges(@Nonnull final Vertex vertex,
                                                           @Nonnull final EntityDTOOrBuilder postMutationEntity,
                                                           @Nonnull final EntityDTO preMutationEntity) {
        final Set<String> preMutationProviders = preMutationEntity.getCommoditiesBoughtList().stream()
            .filter(CommodityBought::hasProviderId)
            .map(CommodityBought::getProviderId)
            .collect(Collectors.toSet());
        final Set<String> postMutationProviders = postMutationEntity.getCommoditiesBoughtList().stream()
            .filter(CommodityBought::hasProviderId)
            .map(CommodityBought::getProviderId)
            .collect(Collectors.toSet());

        final Set<String> otherProviders = vertex.getStitchingData().stream()
            .filter(data -> data.getEntityDtoBuilder() != postMutationEntity)
            .flatMap(data -> data.getEntityDtoBuilder().getCommoditiesBoughtList().stream()
                .filter(CommodityBought::hasProviderId)
                .map(CommodityBought::getProviderId))
            .collect(Collectors.toSet());
        preMutationProviders.addAll(otherProviders);
        postMutationProviders.addAll(otherProviders);

        final Set<String> removedProviders = Sets.difference(preMutationProviders, postMutationProviders);
        final Set<String> addedProviders = Sets.difference(postMutationProviders, preMutationProviders);

        return new RelationshipChanges(removedProviders, addedProviders);
    }

    /**
     * Get the vertex corresponding to an entity from the stitchingGraph, or if it does not exist,
     * create one and insert it into the stitchingGraph.
     *
     * @param entityData The entity whose corresponding vertex should be looked up.
     * @return The retrieved or newly created vertex for the entity.
     */
    private Vertex getOrCreateVertexAndAdd(@Nonnull final StitchingEntityData entityData) {
        final String localId = entityData.getEntityDtoBuilder().getId();

        final Vertex vertex = getVertex(localId).orElseGet(() -> {
            final Vertex newVertex = new Vertex(entityData);
            graph.put(newVertex.getLocalId(), newVertex);
            return newVertex;
        });

        if (!vertex.contains(entityData)) {
            vertex.add(entityData);
        }
        return vertex;
    }

    /**
     * A node in the TopologyGraph.
     *
     * Contains information about the localId and entityType for an entity in the topology.
     * Vertices are equivalent when their localIds are equal.
     *
     * The TopologyEntityDTO.Builder within a vertex may be edited but the vertex is immutable otherwise.
     */
    public static class Vertex {

        /**
         * The list of all stitching data that maps to this vertex.
         * All stitching data with the same localId map to the same vertex.
         */
        private final List<StitchingEntityData> stitchingData;

        /**
         * The set of all vertices in the stitchingGraph that consume from this vertex.
         */
        private final Set<Vertex> consumers;

        /**
         * The set of all vertices in the stitchingGraph that produce for this vertex.
         */
        private final Set<Vertex> providers;

        private final String localId;

        public Vertex(@Nonnull final StitchingEntityData stitchingData) {
            Objects.requireNonNull(stitchingData);

            this.stitchingData = new ArrayList<>();
            this.stitchingData.add(stitchingData);

            this.consumers = new HashSet<>();
            this.providers = new HashSet<>();
            this.localId = stitchingData.getEntityDtoBuilder().getId();
        }

        public void add(@Nonnull final StitchingEntityData stitchingEntityData) {
            Preconditions.checkArgument(stitchingEntityData.getEntityDtoBuilder().getId().equals(localId));

            this.stitchingData.add(stitchingEntityData);
        }

        public boolean contains(@Nonnull final StitchingEntityData stitchingEntityData) {
            return this.stitchingData.contains(stitchingEntityData);
        }

        /**
         * Remove the {@link StitchingEntityData} associated with the given builder.
         * If the vertex does not know about such a builder, it is not removed and returns
         * {@link Optional#empty()} instead.
         *
         * @param entityBuilder The builder to remove.
         * @return The {@link StitchingEntityData} whose builder was requested to be removed.
         *         If no such builder is found, returns {@link Optional#empty()}.
         */
        public Optional<StitchingEntityData> remove(@Nonnull final EntityDTO.Builder entityBuilder) {
            return remove(stitchingEntityData -> stitchingEntityData.getEntityDtoBuilder() == entityBuilder);
        }

        public boolean isEmpty() {
            return stitchingData.isEmpty();
        }

        /**
         * Get the localId for this vertex.
         *
         * @return the localId for this vertex.
         */
        public String getLocalId() {
            return localId;
        }

        /**
         * Get a builder for the entity associated with this vertex.
         * The builder may be mutated to modify the properties of the entity.
         *
         * DO NOT modify the providers or consumers of the entity or the stitchingGraph will be invalidated.
         *
         * @return The property information for the entity associated with this vertex.
         */
        @Nonnull
        public List<StitchingEntityData> getStitchingData() {
            return stitchingData;
        }

        @Override
        public String toString() {
            return "(localId: " + getLocalId() + "[" + stitchingData.size() + "]" + ")";
        }

        private Optional<StitchingEntityData> remove(@Nonnull final Predicate<StitchingEntityData> test) {
            for (Iterator<StitchingEntityData> it = stitchingData.iterator(); it.hasNext();) {
                final StitchingEntityData stitchingEntityData = it.next();
                if (test.test(stitchingEntityData)) {
                    it.remove();
                    return Optional.of(stitchingEntityData);
                }
            }

            return Optional.empty();
        }
    }

    /**
     * The relationships updated by a particular mutation to the graph.
     */
    @Immutable
    private static class RelationshipChanges {
        /**
         * The providers removed by a relationship update.
         */
        public final Set<String> removedProviders;

        /**
         * The providers added by a relationship update.
         */
        public final Set<String> newProviders;

        public RelationshipChanges(final Set<String> removedProviders,
                                   final Set<String> newProviders) {
            this.removedProviders = removedProviders;
            this.newProviders = newProviders;
        }
    }

    /**
     * An exception that occurs when trying to update an entity not in the graph.
     */
    public class UnknownEntityException extends RuntimeException {
        public UnknownEntityException(@Nonnull final String message) {
            super(message);
        }
    }
}

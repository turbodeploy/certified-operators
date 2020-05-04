package com.vmturbo.topology.processor.stitching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntityToAdd;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.conversions.SdkToTopologyEntityConverter;
import com.vmturbo.topology.processor.identity.IdentityMetadataMissingException;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderException;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * A context object that contains the data necessary to perform stitching.
 *
 * A {@link StitchingContext} contains acceleration structures that permit rapid lookup
 * of various groups of entities by {@link EntityType} and also by the target that
 * discovered that entity.
 *
 * Stitching proceeds on a per-target basis. When stitching, we generally want to find
 * all the entities of a given {@link EntityType} discovered by that target (ie that
 * target's "internal entities") as well as finding all entities of a given
 * {@link EntityType} discovered by targets other than a particular target (ie that
 * target's "external entities). The context contains methods for looking up
 * collections of these entities.
 *
 * A {@link StitchingContext} also contains a {@link TopologyStitchingGraph} that can
 * be used for traversal to find the consumers and providers of another entity in the graph.
 *
 * Construct a {@link StitchingContext} via its builder. Once a {@link StitchingContext} has
 * been built, no additional entities can be added to the context.
 */
@NotThreadSafe
public class StitchingContext {
    /**
     * A graph of all entities, permitting traversal on consumers and providers as well as certain
     * kinds of mutation. See {@link TopologyStitchingGraph} for further details.
     */
    private final TopologyStitchingGraph stitchingGraph;

    /**
     * A map of EntityType -> Map<TargetId, List<Entities of the given type discovered by that target>>
     */
    private final Map<EntityType, Map<Long, List<TopologyStitchingEntity>>> entitiesByEntityTypeAndTarget;

    /**
     * The target store which contains target specific information for the entity.
     */
    private final TargetStore targetStore;

    /**
     * Identity provider to assign ID's to new entities.
     */
    private final IdentityProvider identityProvider;

    /**
     * Cache in which we can lookup which commodities are resold commodities.
     */
    private final ResoldCommodityCache resoldCommodityCache;

    private static final Logger logger = LogManager.getLogger();

    /**
     * Create a new {@link StitchingContext}.
     *
     * @param stitchingGraph the graph of all entities to be stitched.
     * @param entitiesByEntityTypeAndTarget A map of EntityType ->
     *                                      Map<TargetId, List<Entities of the given type discovered by that target>>
     * @param targetStore store of targets.
     * @param identityProvider Identity provider to use to assign identities for entities created during stitching.
     * @param resoldCommodityCache A cache for looking up which commodities are resold based on probe
     *                             supply chains.
     */
    private StitchingContext(@Nonnull final TopologyStitchingGraph stitchingGraph,
                             @Nonnull final Map<EntityType, Map<Long, List<TopologyStitchingEntity>>> entitiesByEntityTypeAndTarget,
                             @Nonnull final TargetStore targetStore,
                             @Nonnull final IdentityProvider identityProvider,
                             @Nonnull final ResoldCommodityCache resoldCommodityCache) {
        this.stitchingGraph = Objects.requireNonNull(stitchingGraph);
        this.entitiesByEntityTypeAndTarget = Objects.requireNonNull(entitiesByEntityTypeAndTarget);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.resoldCommodityCache = Objects.requireNonNull(resoldCommodityCache);
    }

    /**
     * Create a new builder for constructing a {@link StitchingContext}.
     *
     * @param entityCount The number of entities to be added to the context when it is built.
     * @param targetStore The targetStore to use when constructing the stitching context.
     * @return a new builder for constructing a {@link StitchingContext}.
     */
    public static Builder newBuilder(int entityCount,
                                     @Nonnull final TargetStore targetStore) {
        return new Builder(entityCount, targetStore);
    }

    /**
     * Get the {@link TopologyStitchingGraph} that contains a graph of all entities in the combined topology.
     * This graph will be mutated during the course of stitching.
     *
     * @return The {@link TopologyStitchingGraph} for use in stitching.
     */
    @Nonnull
    public TopologyStitchingGraph getStitchingGraph() {
        return stitchingGraph;
    }

    public Optional<TopologyStitchingEntity> getEntity(@Nonnull final EntityDTO.Builder entityBuilder) {
        return stitchingGraph.getEntity(entityBuilder);
    }

    public boolean hasEntity(@Nonnull final StitchingEntity entity) {
        return stitchingGraph.getEntity(entity.getEntityBuilder()).isPresent();
    }

    /**
     * Get a stream of all entities of a given type discovered by a target (ie the "internal entities"
     * for a target of a given type).
     *
     * @param entityType The entity type of the entities to retrieve.
     * @param targetId The target ID of the target that discovered the entities to retrieve.
     * @return a stream of all entities of a given type discovered by a target.
     *         Returns {@link Stream#empty()} if the context does not know about the target.
     */
    @Nonnull
    public Stream<TopologyStitchingEntity> internalEntities(@Nonnull final EntityType entityType,
                                                            @Nonnull final Long targetId) {
        final Map<Long, List<TopologyStitchingEntity>> entitiesByTarget =
            entitiesByEntityTypeAndTarget.get(entityType);

        if (entitiesByTarget == null) {
            return Stream.empty();
        } else {
            final List<TopologyStitchingEntity> entities = entitiesByTarget.get(targetId);
            return entities == null ? Stream.empty() : entities.stream();
        }
    }

    /**
     * Get a stream of all entities discovered by a target.
     *
     * @param targetId The id of the target that discovered the entities to retrieve.
     * @return a stream of all entities discovered by a single target.
     *         Returns {@link Stream#empty()} if the context does not know about the target.
     */
    @Nonnull
    public Stream<TopologyStitchingEntity> internalEntities(@Nonnull final Long targetId) {
        return entitiesByEntityTypeAndTarget.values().stream()
            .flatMap(entitiesByTarget -> {
                final List<TopologyStitchingEntity> targetEntities = entitiesByTarget.get(targetId);
                return targetEntities == null ? Stream.empty() : targetEntities.stream();
            });
    }

    /**
     * Get a stream of all entities of a given target discovered by targets OTHER than the given one (ie
     * the "external entities" for a target of a given type).
     *
     * @param entityType The entity type of the entities to retrieve.
     * @param targetId The target ID of the target that discovered the entities to retrieve.
     * @return a stream of all entities of a given type discovered by other targets.
     *         Returns all entities of the given type if the context does not know about the target.
     */
    @Nonnull
    public Stream<TopologyStitchingEntity> externalEntities(@Nonnull final EntityType entityType,
                                                            @Nonnull final Long targetId) {
        final Map<Long, List<TopologyStitchingEntity>> entitiesByTarget = entitiesByEntityTypeAndTarget.get(entityType);
        if (entitiesByTarget == null) {
            return Stream.empty();
        } else {
            return entitiesByTarget.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(targetId))
                .flatMap(entry -> entry.getValue().stream());
        }
    }

    /**
     * Get a stream of all entities of a given type.
     *
     * @param entityType The type of the entity for which all entities should be retrieved.
     * @return a stream of all entities of the given type.
     */
    @Nonnull
    public Stream<TopologyStitchingEntity> getEntitiesOfType(@Nonnull final EntityType entityType) {
        final Map<Long, List<TopologyStitchingEntity>> entitiesByTarget = entitiesByEntityTypeAndTarget.get(entityType);
        if (entitiesByTarget == null) {
            return Stream.empty();
        } else {
            return entitiesByTarget.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream());
        }
    }

    /**
     * Get a count of the number of entities by their entity type in the context.
     *
     * @return a count of the number of entities by their entity type in the context.
     */
    @Nonnull
    public Map<EntityType, Integer> entityTypeCounts() {
        return entitiesByEntityTypeAndTarget.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey,
                entry -> entry.getValue().values().stream()
                    .mapToInt(List::size)
                    .sum()));
    }

    /**
     * Get a count of the number of entities for each target in the context.
     *
     * @return A map of targetId -> <number of entities discovered by that target>
     */
    @Nonnull
    public Map<Long, Integer> targetEntityCounts() {
        final Map<Long, Integer> targetEntityCounts = new HashMap<>();
        entitiesByEntityTypeAndTarget.values().forEach(targetMap ->
            targetMap.forEach((targetId, entities) -> {
                final Integer countSoFar = targetEntityCounts.get(targetId);
                if (countSoFar == null)
                    targetEntityCounts.put(targetId, entities.size());
                else
                    targetEntityCounts.put(targetId, entities.size() + countSoFar);
            }));

        return targetEntityCounts;
    }

    /**
     * Remove an entity from the stitching context and the {@link TopologyStitchingGraph} in the context.
     *
     * @param toRemove The entity to remove from the context and graph.
     * @return True if the entity was contained by the context and removed, false otherwise.
     */
    public boolean removeEntity(@Nonnull final TopologyStitchingEntity toRemove) {
        if (!stitchingGraph.removeEntity(toRemove).isEmpty()) {
            final Map<Long, List<TopologyStitchingEntity>> entitiesOfTypeByTarget =
                entitiesByEntityTypeAndTarget.get(toRemove.getEntityType());
            boolean removed = false;

            if (entitiesOfTypeByTarget != null) {
                final List<TopologyStitchingEntity> stitchingBuilders =
                    entitiesOfTypeByTarget.get(toRemove.getTargetId());
                removed = stitchingBuilders.remove(toRemove);
            }

            if (!removed) {
                logger.error("Illegal state: an entity is in the stitching graph but not the stitching context");
                throw new IllegalStateException("This should never happen!");
            }

            return true;
        }

        return false;
    }

    /**
     * Add a list of new entities to the existing graph, and set up the relationship with existing
     * entities provided by {@link EntityToAdd}.
     *
     * @param entitiesToAdd the list of {@link EntityToAdd} which wraps the EntityDTO to be added,
     *                      and related consumer, provider
     * @return list of new {@link TopologyStitchingEntity}
     */
    public List<TopologyStitchingEntity> addEntities(@Nonnull List<EntityToAdd> entitiesToAdd) {
        try {
            // group entities by probe id to improve performance when assigning oids
            final Map<Long, List<EntityDTO>> entitiesToAddByProbeId = entitiesToAdd.stream()
                .collect(Collectors.groupingBy(entity -> {
                    Optional<Target> target = targetStore.getTarget(entity.getConsumer().getTargetId());
                    if (!target.isPresent()) {
                        throw new IllegalStateException("No target found for id: " +
                            entity.getConsumer().getTargetId());
                    }
                    return target.get().getProbeId();
                }, Collectors.mapping(EntityToAdd::getEntityDTO, Collectors.toList())));
            // create a mapping from EntityDTO to EntityToAdd
            final Map<EntityDTO, EntityToAdd> entityToAddByDTO = entitiesToAdd.stream()
                .collect(Collectors.toMap(EntityToAdd::getEntityDTO, Function.identity()));
            // list to keep new added entity
            List<TopologyStitchingEntity> newTopologyEntities = new ArrayList<>();
            for (Map.Entry<Long, List<EntityDTO>> entry : entitiesToAddByProbeId.entrySet()) {
                final Map<Long, EntityDTO> oidToEntityDTO = identityProvider.getIdsForEntities(
                    entry.getKey(), entry.getValue());
                oidToEntityDTO.forEach((oid, dto) -> {
                    EntityToAdd entity = entityToAddByDTO.get(dto);
                    TopologyStitchingEntity consumer = (TopologyStitchingEntity)entity.getConsumer();
                    TopologyStitchingEntity provider = (TopologyStitchingEntity)entity.getProvider();
                    // create new TopologyStitchingEntity
                    TopologyStitchingEntity newEntity = stitchingGraph.getOrCreateStitchingEntity(
                        StitchingEntityData.newBuilder(dto.toBuilder())
                            .oid(oid)
                            .targetId(consumer.getTargetId())
                            .lastUpdatedTime(consumer.getLastUpdatedTime())
                            .build());
                    // set up connected relationship: consumer --> new entity --> provider
                    consumer.addConnectedTo(entity.getConnectionType(), newEntity);
                    newEntity.addConnectedTo(entity.getConnectionType(), provider);
                    // if it is volume, we also need to set volume id on the related CommodityBought of consumer
                    if (dto.getEntityType() == EntityType.VIRTUAL_VOLUME) {
                        consumer.getCommodityBoughtListByProvider().get(provider).forEach(
                            commoditiesBought -> commoditiesBought.setVolumeId(oid));
                    }
                    // add new entity to stitching graph
                    final Map<Long, List<TopologyStitchingEntity>> existingEntitiesOfTypeByTarget =
                        entitiesByEntityTypeAndTarget.computeIfAbsent(dto.getEntityType(), eType -> new HashMap<>());
                    final List<TopologyStitchingEntity> targetEntitiesForType =
                        existingEntitiesOfTypeByTarget.computeIfAbsent(consumer.getTargetId(), type -> new ArrayList<>());
                    targetEntitiesForType.add(newEntity);
                    // add new entity to the result
                    newTopologyEntities.add(newEntity);
                });
            }
            return newTopologyEntities;
        } catch (IdentityUninitializedException | IdentityMetadataMissingException | IdentityProviderException e) {
            throw new IllegalStateException("Exception while adding new entities to stitching graph: " + e);
        }
    }

    /**
     * After stitching, this should return a valid, well-formed topology.
     *
     * @return The entities in the {@link StitchingContext}, arranged by ID.
     */
    @Nonnull
    public Map<Long, TopologyEntity.Builder> constructTopology() {
        /**
         * If this line throws an exception, it indicates an error in stitching. If stitching is
         * successful it should merge down all entities with duplicate OIDs into a single entity.
         *
         * If multiple entities have the same OID, we log it as an error and pick one to use at random.
         */
        return stitchingGraph.entities()
            .collect(Collectors.toMap(TopologyStitchingEntity::getOid,
                stitchingEntity -> {
                    final TopologyEntityDTO.Builder builder =
                        SdkToTopologyEntityConverter.newTopologyEntityDTO(stitchingEntity, resoldCommodityCache)
                            .setOrigin(Origin.newBuilder()
                                .setDiscoveryOrigin(stitchingEntity.buildDiscoveryOrigin()));
                    return TopologyEntity.newBuilder(builder);
                },
                (oldValue, newValue) -> {
                    logger.error("Multiple entities with oid {}. Keeping the first.", oldValue.getOid());
                    return oldValue;
                }
            ));
    }

    @Nonnull
    public Map<EntityType, Map<Long, List<TopologyStitchingEntity>>> getEntitiesByEntityTypeAndTarget() {
        return Collections.unmodifiableMap(entitiesByEntityTypeAndTarget);
    }

    /**
     * Get a count of the number of entities in the context.
     *
     * @return The number of entities in the context.
     */
    public int size() {
        return stitchingGraph.entityCount();
    }

    /**
     * A builder for constructing a stitching graph.
     *
     * Entities can be added to the context via the builder. Once the {@link StitchingContext} is built
     * via the builder, no additional entities can be added to the context.
     */
    public static class Builder {
        private final TopologyStitchingGraph stitchingGraph;

        private final Map<EntityType, Map<Long, List<TopologyStitchingEntity>>> entitiesByEntityTypeAndTarget;

        private TargetStore targetStore;

        private IdentityProvider identityProvider;

        private ResoldCommodityCache resoldCommodityCache;

        private Builder(final int entityCount, @Nonnull final TargetStore targetStore) {
            this.stitchingGraph = new TopologyStitchingGraph(entityCount);
            this.targetStore = Objects.requireNonNull(targetStore);
            this.resoldCommodityCache = new ResoldCommodityCache(targetStore);
            entitiesByEntityTypeAndTarget = new EnumMap<>(EntityType.class);
        }

        public Builder setIdentityProvider(final IdentityProvider identityProvider) {
            this.identityProvider = identityProvider;
            return this;
        }

        public StitchingContext build() {
            return new StitchingContext(stitchingGraph, entitiesByEntityTypeAndTarget, targetStore,
                identityProvider, resoldCommodityCache);
        }

        /**
         * Add an entity to the {@link StitchingContext}. The entity will be added to both the acceleration
         * structures and graph used by the {@link StitchingContext}.
         *
         * No attempt is made at de-duplication. If this call is made twice for the same entity, that entity
         * will be added twice.
         *
         * @param stitchingEntityData stitching entity to add.
         * @param targetIdMap A map of target-localId -> StitchingEntityData for all entities discovered by
         *                    the target that discovered the {@link StitchingEntityData} to add.
         */
        public void addEntity(@Nonnull final StitchingEntityData stitchingEntityData,
                              @Nonnull final Map<String, StitchingEntityData> targetIdMap) {
            final TopologyStitchingEntity stitchingEntity =
                    stitchingGraph.addStitchingData(stitchingEntityData, targetIdMap);

            final EntityDTO.Builder stitchingBuilder = stitchingEntityData.getEntityDtoBuilder();
            final EntityType entityType = stitchingBuilder.getEntityType();
            final long targetId = stitchingEntityData.getTargetId();

            final Map<Long, List<TopologyStitchingEntity>> entitiesOfTypeByTarget =
                entitiesByEntityTypeAndTarget.computeIfAbsent(entityType, eType -> new HashMap<>());

            final List<TopologyStitchingEntity> targetEntitiesForType =
                entitiesOfTypeByTarget.computeIfAbsent(targetId, type -> new ArrayList<>());
            targetEntitiesForType.add(stitchingEntity);

            // Remove all commodities on the builder so that nobody interacts with them by mistake.
            // Interact with commodities directly via the StitchingEntity.
            stitchingEntity.getEntityBuilder().clearCommoditiesSold();
            stitchingEntity.getEntityBuilder().clearCommoditiesBought();
        }
    }
}

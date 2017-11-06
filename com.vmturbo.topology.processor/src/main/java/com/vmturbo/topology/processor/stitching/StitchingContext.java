package com.vmturbo.topology.processor.stitching;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingOperationResult;
import com.vmturbo.stitching.StitchingOperationResult.CommoditiesBoughtChange;
import com.vmturbo.stitching.StitchingOperationResult.RemoveEntityChange;
import com.vmturbo.stitching.StitchingOperationResult.StitchingChange;
import com.vmturbo.topology.processor.conversions.Converter;
import com.vmturbo.topology.processor.topology.TopologyGraph;

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
    private final Map<EntityType, Map<Long, List<EntityDTO.Builder>>> entitiesByEntityTypeAndTarget;

    private static final Logger logger = LogManager.getLogger();

    /**
     * Create a new {@link StitchingContext}.
     *
     * @param stitchingGraph the graph of all entities to be stitched.
     * @param  entitiesByEntityTypeAndTarget A map of EntityType ->
     *                                       Map<TargetId, List<Entities of the given type discovered by that target>>
     */
    private StitchingContext(@Nonnull final TopologyStitchingGraph stitchingGraph,
                             @Nonnull final Map<EntityType, Map<Long, List<EntityDTO.Builder>>>
                                 entitiesByEntityTypeAndTarget) {
        this.stitchingGraph = Objects.requireNonNull(stitchingGraph);
        this.entitiesByEntityTypeAndTarget = Objects.requireNonNull(entitiesByEntityTypeAndTarget);
    }

    /**
     * Create a new builder for constructing a {@link StitchingContext}.
     *
     * @param entityCount The number of entities to be added to the context when it is built.
     * @return a new builder for constructing a {@link StitchingContext}.
     */
    public static Builder newBuilder(int entityCount) {
        return new Builder(entityCount);
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

    /**
     * Get a stream of all entities of a given type discovered by a target (ie the "internal entities"
     * for a target of a given type).
     *
     * @param entityType The entity type of the entities to retrieve.
     * @param targetId The target ID of the target that discovered the entities to retrieve.
     * @return a stream of all entities of a given type discovered by a target.
     *         Returns {@link Optional#empty()} if the context does not know about the target.
     */
    @Nonnull
    public Stream<EntityDTO.Builder> internalEntities(@Nonnull final EntityType entityType,
                                                      @Nonnull final Long targetId) {
        final Map<Long, List<EntityDTO.Builder>> entitiesByTarget = entitiesByEntityTypeAndTarget.get(entityType);
        if (entitiesByTarget == null) {
            return Stream.empty();
        } else {
            final List<EntityDTO.Builder> entities = entitiesByTarget.get(targetId);
            return entities == null ? Stream.empty() : entities.stream();
        }
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
    public Stream<EntityDTO.Builder> externalEntities(@Nonnull final EntityType entityType,
                                                      @Nonnull final Long targetId) {
        final Map<Long, List<EntityDTO.Builder>> entitiesByTarget = entitiesByEntityTypeAndTarget.get(entityType);
        if (entitiesByTarget == null) {
            return Stream.empty();
        } else {
            return entitiesByTarget.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(targetId))
                .flatMap(entry -> entry.getValue().stream());
        }
    }

    /**
     * Apply the {@link StitchingOperationResult} to modify the context and its graph
     * according to the {@link StitchingChange}s in the result.
     *
     * @param result The {@link StitchingOperationResult} whose changes should be applied.
     */
    public void applyStitchingResult(@Nonnull final StitchingOperationResult result) {
        logger.debug("Applying stitching result with {} changes", result.getChanges().size());

        for (StitchingChange stitchingChange : result.getChanges()) {
            // Consider visitor pattern here?
            if (stitchingChange instanceof CommoditiesBoughtChange) {
                final CommoditiesBoughtChange update = (CommoditiesBoughtChange)stitchingChange;
                performUpdate(update);
            } else {
                performRemoval((RemoveEntityChange)stitchingChange);
            }
        }
    }

    /**
     * Construct a {@link TopologyGraph} composed of the entities in the {@link StitchingContext}.
     *
     * After stitching, this should return a valid, well-formed topology.
     *
     * @return A {@link TopologyGraph} composed of the entities in the {@link StitchingContext}.
     */
    @Nonnull
    public TopologyGraph constructTopology() {
        /**
         * If this line throws an {@link IllegalStateException} it means that there are duplicate
         * localIds in the post-stitched topology. Note that a map containing localId -> OID
         * for an individual target is insufficient because after stitching entities discovered by
         * one target may be buying from entities discovered from different targets.
         *
         * TODO: (DavidBlinn 11/2/2017) Figure out, if possible, a more robust way of performing
         * TODO: this conversion.
         */
        final Map<String, Long> localIdToOidMap = stitchingGraph.vertices()
            .flatMap(vertex -> vertex.getStitchingData().stream())
            .collect(Collectors.toMap(
                StitchingEntityData::getLocalId,
                StitchingEntityData::getOid));

        /**
         * If this line throws an exception, it indicates an error in stitching. If stitching is
         * successful it should merge down all entities with duplicate OIDs into a single entity.
         */
        final Map<Long, TopologyEntityDTO.Builder> entityMap =
            stitchingGraph.vertices()
            .flatMap(vertex -> vertex.getStitchingData().stream())
            .collect(Collectors.toMap(
                StitchingEntityData::getOid,
                stitchingEntityData -> Converter.newTopologyEntityDTO(
                    stitchingEntityData.getEntityDtoBuilder(),
                    stitchingEntityData.getOid(),
                    localIdToOidMap)));

        return new TopologyGraph(entityMap);
    }

    /**
     * Get a count of the number of entities in the context.
     *
     * @return The number of entities in the context.
     */
    public int size() {
        return entitiesByEntityTypeAndTarget.values().stream()
            .flatMap(targetMap -> targetMap.values().stream())
            .mapToInt(List::size)
            .sum();
    }

    /**
     * A builder for constructing a stitching graph.
     *
     * Entities can be added to the context via the builder. Once the {@link StitchingContext} is built
     * via the builder, no additional entities can be added to the context.
     */
    public static class Builder {
        private final TopologyStitchingGraph stitchingGraph;

        private final Map<EntityType, Map<Long, List<EntityDTO.Builder>>> entitiesByEntityTypeAndTarget;

        private Builder(final int entityCount) {
            this.stitchingGraph = new TopologyStitchingGraph(entityCount);
            entitiesByEntityTypeAndTarget = new EnumMap<>(EntityType.class);
        }

        public StitchingContext build() {
            return new StitchingContext(stitchingGraph, entitiesByEntityTypeAndTarget);
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
            stitchingGraph.addStitchingData(stitchingEntityData, targetIdMap);

            final EntityDTO.Builder stitchingBuilder = stitchingEntityData.getEntityDtoBuilder();
            final EntityType entityType = stitchingBuilder.getEntityType();
            final long targetId = stitchingEntityData.getTargetId();

            final Map<Long, List<EntityDTO.Builder>> entitiesOfTypeByTarget =
                entitiesByEntityTypeAndTarget.computeIfAbsent(entityType, eType -> new HashMap<>());

            final List<EntityDTO.Builder> targetEntitiesForType =
                entitiesOfTypeByTarget.computeIfAbsent(targetId, type -> new ArrayList<>());
            targetEntitiesForType.add(stitchingBuilder);
        }
    }

    @VisibleForTesting
    void performRemoval(@Nonnull final RemoveEntityChange removal) {
        final Optional<Long> relatedTargetId = stitchingGraph.getTargetId(removal.entityBuilder);
        relatedTargetId.ifPresent(targetId -> {
            stitchingGraph.removeEntity(removal);

            final Map<Long, List<EntityDTO.Builder>> entitiesOfTypeByTarget =
                entitiesByEntityTypeAndTarget.get(removal.entityBuilder.getEntityType());
            if (entitiesOfTypeByTarget != null) {
                final List<EntityDTO.Builder> stitchingBuilders = entitiesOfTypeByTarget.get(targetId);
                stitchingBuilders.remove(removal.entityBuilder);
            } else {
                throw new IllegalStateException("This should never happen!");
            }
        });
    }

    @VisibleForTesting
    void performUpdate(@Nonnull final CommoditiesBoughtChange update) {
        stitchingGraph.updateCommoditiesBought(update);
    }
}

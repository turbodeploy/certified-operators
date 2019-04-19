package com.vmturbo.topology.processor.stitching;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.EntityPipelineErrors.StitchingErrorCode;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.stitching.utilities.CommoditiesBought;
import com.vmturbo.topology.processor.conversions.SdkToTopologyEntityConverter;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity.CommoditySold;

/**
 * A mutable graph built from the forest of partial topologies discovered by individual targets.
 * The graph should be a mutable DAG (though the DAG-ness is not enforced by this class).
 *
 * The graph is constructed by following consumes/provides relations among the entities in the graph.
 *
 * The graph does NOT permit parallel edges. That is, an entity consuming multiple commodities
 * from the same provider results in only a single edge between the two in the graph.
 *
 * The EntityDTO.Builder objects within the graph's {@link TopologyStitchingEntity}s may be edited,
 * but edits to buying/selling relationships on the DTOs are NOT automatically reflected in the
 * relationships available on the graph itself.
 *
 * After the graph is initially constructed, it may be mutated in specifically allowed ways.
 *
 * Mutations permitted to the graph:
 * 1. Removing an entity - Removing an entity from the graph propagates a change to all buyers of commodities
 *                         from the entity being removed.
 * 2. Commodities bought - Commodities bought by an entity may be added or removed. These changes will
 *                         automatically be propagated to the sellers of the commodities.
 * 3. The creation of new entities - Adding a new entity to the graph during the pre-stitching stage
 *
 * Mutations NOT permitted to the graph:
 * 1. Commodities sold - No destructive mutations are permitted to commodities sold (that is, changes
 *                       that would change or remove relationships to buyers of the commodities being changed).
 *                       If a use case for this arises, we may consider supporting it in the future.
 *
 * Note that the graph itself only contains methods for constructing the graph. Mutate the graph and
 * the entities within the graph via the appropriate classes in {@link TopologyStitchingChanges}. See
 * also {@link StitchingResultBuilder} which contains methods for constructing and queueing
 * change objects during the stitching processing phase.
 *
 * The graph retains references to its constituent {@link TopologyStitchingEntity}s via a map that uses
 * object-references to the builders to the {@link EntityDTO} objects that describe the entities associated
 * with each individual {@link TopologyStitchingEntity}.
 */
@NotThreadSafe
public class TopologyStitchingGraph {

    private static final Logger logger = LogManager.getLogger();

    /**
     * A map permitting lookup from the original entity builder to its corresponding stitching entity.
     */
    @Nonnull
    private final Map<EntityDTO.Builder, TopologyStitchingEntity> stitchingEntities;

    /**
     * Create a new stitchingGraph.
     *
     * Note that stitchingGraph construction does not validate the consistency of the input - for example,
     * if an entity in the input is buying from an entity not in the input, no error will be
     * generated.
     */
    public TopologyStitchingGraph(int expectedSize) {
        stitchingEntities = new IdentityHashMap<>(expectedSize);
    }

    /**
     * Get a stream of all {@link TopologyStitchingEntity}s in the stitchingGraph.
     *
     * @return A stream of all {@link TopologyStitchingEntity}s in the stitchingGraph.
     */
    public Stream<TopologyStitchingEntity> entities() {
        return stitchingEntities.values().stream();
    }

    /**
     * Retrieve the {@link TopologyStitchingEntity} for an entity in the stitchingGraph by its
     * associated entity builder. Returns {@link Optional#empty()} if the no such {@link TopologyStitchingEntity}
     * is in the stitchingGraph.
     *
     * @param entityBuilder The builder for the entity of the {@link TopologyStitchingEntity} in the stitchingGraph.
     * @return The {@link TopologyStitchingEntity} associated with the corresponding entityBuilder.
     */
    public Optional<TopologyStitchingEntity> getEntity(EntityDTO.Builder entityBuilder) {
        return Optional.ofNullable(stitchingEntities.get(entityBuilder));
    }

    /**
     * Get the number of entities in the stitchingGraph.
     *
     * @return The number of entities in the stitchingGraph.
     */
    public int entityCount() {
        return stitchingEntities.size();
    }

    /**
     * Look up the target ID for a given {@link EntityDTO} in the graph.
     *
     * @param entityBuilder The builder whose target ID should be looked up.
     * @return The targetID of the target that originally discovered by the builder.
     *         Returns {@link Optional#empty()} if the builder is not known to the graph.
     */
    public Optional<Long> getTargetId(@Nonnull final EntityDTO.Builder entityBuilder) {
        return getEntity(entityBuilder)
            .map(TopologyStitchingEntity::getTargetId);
    }

    /**
     * Add a {@link TopologyStitchingEntity} corresponding to the input {@link StitchingEntityData}.
     * Adds consumes edges in the stitchingGraph for all entities the input is consuming from.
     * Adds produces edges in the stitchingGraph for all entities providing commodities this entity is consuming.
     *
     * Clients should never attempt to add a {@link TopologyStitchingEntity} for an entity already in the
     * stitchingGraph. This may not be checked.
     *
     * @param entityData The entity to add a {@link TopologyStitchingEntity} for.
     * @param entityMap The map of localId -> {@link StitchingEntityData} for the target that discovered
     *                  this entity.
     */
    public TopologyStitchingEntity addStitchingData(@Nonnull final StitchingEntityData entityData,
                                 @Nonnull final Map<String, StitchingEntityData> entityMap) {
        final TopologyStitchingEntity entity = getOrCreateStitchingEntity(entityData);
        final EntityDTO.Builder entityDtoBuilder = entityData.getEntityDtoBuilder();

        final Map<StitchingErrorCode, MutableInt> errorsByCategory = new HashMap<>();

        if (!entityDtoBuilder.getCommoditiesBoughtList().isEmpty()) {
            final List<Integer> invalidCommBought = new ArrayList<>();
            for (int i = 0; i < entityDtoBuilder.getCommoditiesBoughtCount(); ++i) {
                final EntityDTO.CommodityBought commodityBought = entityDtoBuilder.getCommoditiesBought(i);
                final String providerId = commodityBought.getProviderId();
                final StitchingEntityData providerData = entityMap.get(providerId);
                if (providerData == null) {
                    // This is a pretty serious error if it happens, so it's worth the error level.
                    logger.error("Entity {} (local id: {}) buying commodities from non-existing provider {}",
                        entityData.getOid(), entityData.getLocalId(), providerId);
                    errorsByCategory.computeIfAbsent(StitchingErrorCode.INVALID_COMM_BOUGHT,
                        k -> new MutableInt(0)).increment();
                    invalidCommBought.add(i);
                    continue;
                }

                final TopologyStitchingEntity provider = getOrCreateStitchingEntity(providerData);
                if (!provider.getLocalId().equals(providerId)) {
                    // The only way this would happen is if there is already a different entity
                    // in the stitching graph.
                    // Serious error, and the IDs will be helpful to diagnose where it came
                    // from.
                    logger.error("Entity {} (local id: {}) - Map key {} does not match provider localId value {}",
                        entityData.getOid(), entityData.getLocalId(), providerId, provider.getLocalId());
                    logger.debug("Provider entity: {}",
                        entityData.getEntityDtoBuilder(), provider.getEntityBuilder());
                    errorsByCategory.computeIfAbsent(StitchingErrorCode.INCONSISTENT_KEY,
                        k -> new MutableInt(0)).increment();
                    errorsByCategory.computeIfAbsent(StitchingErrorCode.INVALID_COMM_BOUGHT,
                        k -> new MutableInt(0)).increment();
                    invalidCommBought.add(i);
                    continue;
                }

                // add commodity bought for this provider, there may be multiple set of commodity
                // bought from same provider
                final Long volumeId;
                if (!commodityBought.hasSubDivision()) {
                    volumeId = null;
                } else {
                    String volumeLocalId = commodityBought.getSubDivision().getSubDivisionId();
                    StitchingEntityData volumeData = entityMap.get(volumeLocalId);
                    if (volumeData == null) {
                        // Serious error, and the IDs will be helpful to diagnose where it came
                        // from.
                        logger.error("Entity {} (local id: {}) connected to non-existing sub-division {}.",
                            entity.getOid(), entityData.getLocalId(), volumeLocalId);
                        errorsByCategory.computeIfAbsent(StitchingErrorCode.INVALID_COMM_BOUGHT,
                            k -> new MutableInt(0)).increment();
                        invalidCommBought.add(i);
                        continue;
                    }
                    volumeId = volumeData.getOid();
                }

                entity.addProviderCommodityBought(provider, new CommoditiesBought(
                    commodityBought.getBoughtList().stream()
                        .map(CommodityDTO::toBuilder)
                        .collect(Collectors.toList()), volumeId));

                provider.addConsumer(entity);
            }

            // Remove in reverse order so that all indices continue to be valid.
            // i.e. if we want to remove index 1 and 3, removing index 1 first will mean index
            // 3 needs to be re-interpreted as index 2. Reverse order also reduces shifting.
            for (final Integer invalidIdx : Lists.reverse(invalidCommBought)) {
                entityDtoBuilder.removeCommoditiesBought(invalidIdx);
            }
        }

        if (!entityDtoBuilder.getCommoditiesSoldList().isEmpty()) {
            final List<Integer> invalidCommSold = new ArrayList<>();
            for (int i = 0; i < entityDtoBuilder.getCommoditiesSoldCount(); ++i) {
                final CommodityDTO commoditySold = entityDtoBuilder.getCommoditiesSold(i);
                final Optional<String> accessingIdOpt =
                    SdkToTopologyEntityConverter.parseAccessKey(commoditySold);
                final TopologyStitchingEntity accessingEntity;
                if (accessingIdOpt.isPresent()) {
                    final String accessingLocalId = accessingIdOpt.get();
                    final StitchingEntityData accessingData = entityMap.get(accessingLocalId);
                    if (accessingData == null) {
                        // Serious error, and the IDs will be helpful to diagnose where it came
                        // from.
                        logger.error("Entity {} (local id: {}) accessed by entity {} which does not exist.",
                            entity.getOid(), entityData.getLocalId(), accessingLocalId);
                        errorsByCategory.computeIfAbsent(StitchingErrorCode.INVALID_COMM_SOLD,
                            k -> new MutableInt(0)).increment();
                        invalidCommSold.add(i);
                        continue;
                    }

                    accessingEntity = getOrCreateStitchingEntity(accessingData);
                    if (!accessingEntity.getLocalId().equals(accessingLocalId)) {
                        // Serious error, and the IDs will be helpful to diagnose where it came
                        // from.
                        logger.error("Entity {} (local id: {}) - Map key {} does not " +
                            "match accessing localId value {}", entityData.getOid(),
                            entityData.getLocalId(), accessingLocalId, accessingEntity.getLocalId());
                        logger.debug("Accessing entity: {}",
                            entityData.getEntityDtoBuilder(), accessingEntity.getEntityBuilder());
                        errorsByCategory.computeIfAbsent(StitchingErrorCode.INVALID_COMM_SOLD,
                            k -> new MutableInt(0)).increment();
                        errorsByCategory.computeIfAbsent(StitchingErrorCode.INCONSISTENT_KEY,
                            k -> new MutableInt(0)).increment();
                        invalidCommSold.add(i);
                        continue;
                    }
                } else {
                    accessingEntity = null;
                }

                entity.getTopologyCommoditiesSold()
                    .add(new CommoditySold(commoditySold.toBuilder(), accessingEntity));
            }

            for (final Integer invalidIdx : Lists.reverse(invalidCommSold)) {
                entityDtoBuilder.removeCommoditiesSold(invalidIdx);
            }
        }

        // add connected entity, this is currently only used by cloud entities (AWS/Azure)
        // cloud entities use layeredOver to represent normal connection, and consistsOf to
        // represent owns connection. some entities may used layeredOver for other purposes,
        // we don't want to add connected for them, so we need a check here to see if we should
        // use connectedTo.
        // layeredOver means normal connection
        if (entityData.supportsConnectedTo()) {
            if (!entityDtoBuilder.getLayeredOverList().isEmpty()) {
                final Map<String, TopologyStitchingEntity> validLayeredOverByLocalId =
                    entityDtoBuilder.getLayeredOverList().stream()
                        .map(layeredOverId -> {
                            final StitchingEntityData layeredOverData = entityMap.get(layeredOverId);
                            if (layeredOverData == null) {
                                // The final list of invalid entities gets printed at error-level
                                // below, so this can be at debug.
                                logger.debug("Entity {} (local id: {}) - layered over entity {}" +
                                    " which does not exist.", entity.getOid(), entity.getLocalId(),
                                    layeredOverId);
                                errorsByCategory.computeIfAbsent(StitchingErrorCode.INVALID_LAYERED_OVER,
                                    k -> new MutableInt(0)).increment();
                                return null;
                            }

                            final TopologyStitchingEntity layeredOverEntity = getOrCreateStitchingEntity(layeredOverData);
                            if (!layeredOverEntity.getLocalId().equals(layeredOverId)) {
                                // The final list of invalid entities gets printed at error-level
                                // below, so this can be at debug.
                                logger.debug("Entity {} (local id: {}) - Map key {} does not " +
                                        "match layered over localId value {}", entityData.getOid(),
                                    entityData.getLocalId(), layeredOverId, layeredOverEntity.getLocalId());
                                errorsByCategory.computeIfAbsent(StitchingErrorCode.INVALID_LAYERED_OVER,
                                    k -> new MutableInt(0)).increment();
                                errorsByCategory.computeIfAbsent(StitchingErrorCode.INCONSISTENT_KEY,
                                    k -> new MutableInt(0)).increment();
                                return null;
                            }
                            return layeredOverEntity;
                        })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toMap(TopologyStitchingEntity::getLocalId, Function.identity()));

                if (validLayeredOverByLocalId.size() < entityDtoBuilder.getLayeredOverCount()) {
                    // Some are invalid!
                    // This is a pretty serious error, so it's worth logging at error-level.
                    // We want to see the invalid entity IDs because that can be helpful in figuring
                    // out the issue.
                    logger.error("Entity {} (local id: {}) layered over invalid entities: {}",
                        entity::getOid,
                        entity::getLocalId,
                        () -> Collections2.filter(entityDtoBuilder.getLayeredOverList(),
                            e -> !validLayeredOverByLocalId.containsKey(e)));

                    entityDtoBuilder.clearLayeredOver();
                    entityDtoBuilder.addAllLayeredOver(validLayeredOverByLocalId.keySet());
                }

                validLayeredOverByLocalId.values().forEach(layeredOverEntity -> {
                    entity.addConnectedTo(ConnectionType.NORMAL_CONNECTION, layeredOverEntity);
                    layeredOverEntity.addConnectedFrom(ConnectionType.NORMAL_CONNECTION, entity);
                });
            }

            // consistsOf means owns connection
            entityDtoBuilder.getConsistsOfList().forEach(connectedEntityId -> {
                final Map<String, TopologyStitchingEntity> validConsistsOf =
                    entityDtoBuilder.getConsistsOfList().stream()
                        .map(consistsOfId -> {
                            final StitchingEntityData consistsOfData = entityMap.get(consistsOfId);
                            if (consistsOfData == null) {
                                // The final list of invalid entities gets printed at error-level
                                // below, so this can be at debug.
                                logger.debug("Entity {} (local id: {}) - consists of entity {}" +
                                        " which does not exist.", entity.getOid(), entity.getLocalId(),
                                    consistsOfId);
                                errorsByCategory.computeIfAbsent(StitchingErrorCode.INVALID_CONSISTS_OF,
                                    k -> new MutableInt(0)).increment();
                                return null;
                            }

                            final TopologyStitchingEntity consistsOfEntity = getOrCreateStitchingEntity(consistsOfData);
                            if (!consistsOfEntity.getLocalId().equals(consistsOfId)) {
                                // The final list of invalid entities gets printed at error-level
                                // below, so this can be at debug.
                                logger.debug("Entity {} (local id: {}) - Map key {} does not " +
                                        "match consists-of localId value {}", entityData.getOid(),
                                    entityData.getLocalId(), consistsOfId, consistsOfEntity.getLocalId());
                                errorsByCategory.computeIfAbsent(StitchingErrorCode.INVALID_CONSISTS_OF,
                                    k -> new MutableInt(0)).increment();
                                errorsByCategory.computeIfAbsent(StitchingErrorCode.INCONSISTENT_KEY,
                                    k -> new MutableInt(0)).increment();
                                return null;
                            }
                            return consistsOfEntity;
                        })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toMap(TopologyStitchingEntity::getLocalId, Function.identity()));

                if (validConsistsOf.size() < entityDtoBuilder.getConsistsOfCount()) {
                    // Some are invalid!
                    // This is a pretty serious error, so it's worth logging at error-level.
                    // We want to see the invalid entity IDs because that can be helpful in figuring
                    // out the issue.
                    logger.error("Entity {} (local id: {}) consists of invalid entities: {}",
                        entity::getOid,
                        entity::getLocalId,
                        () -> Collections2.filter(entityDtoBuilder.getConsistsOfList(),
                            e -> !validConsistsOf.containsKey(e)));

                    entityDtoBuilder.clearConsistsOf();
                    entityDtoBuilder.addAllConsistsOf(validConsistsOf.keySet());
                }

                validConsistsOf.values().forEach(consistsOfEntity -> {
                    entity.addConnectedTo(ConnectionType.OWNS_CONNECTION, consistsOfEntity);
                    consistsOfEntity.addConnectedFrom(ConnectionType.OWNS_CONNECTION, entity);
                });
            });
        }

        // Log and record the high-level error summary.
        if (!errorsByCategory.isEmpty()) {
            logger.debug("Invalid entity added to stitching graph " +
                "(see above for specific errors): {}", entityData.getEntityDtoBuilder());
            errorsByCategory.forEach((category, numErrors) -> {
                entity.recordError(category);
                Metrics.ERROR_COUNT.labels(category.name().toLowerCase()).increment(numErrors.doubleValue());
            });
        }


        return entity;
    }

    /**
     * Remove a {@link TopologyStitchingEntity} from the graph.
     *
     * For each {@link TopologyStitchingEntity} in the graph that has a provider or consumer relation to
     * the {@link TopologyStitchingEntity} being removed, those {@link TopologyStitchingEntity}s are updated
     * by deleting their relationships to the {@link TopologyStitchingEntity} that is being removed.
     *
     * Entities known to be buying from the removed {@link TopologyStitchingEntity} have their commodities updated
     * so that the commodities they used to buy from the {@link TopologyStitchingEntity} are also removed.
     *
     * Attempting to remove an entity not in the graph is treated as a no-op.
     *
     * @param toRemove The entity to remove.
     * @return The set of localIds for all entities affected by the removal. The localId of the removed
     *         entity is always in this set unless no entity was actually removed.
     */
    public Set<TopologyStitchingEntity>
    removeEntity(@Nonnull final TopologyStitchingEntity toRemove) {
        final Set<TopologyStitchingEntity> affected = new HashSet<>();

        final TopologyStitchingEntity removedEntity = stitchingEntities.remove(toRemove.getEntityBuilder());
        if (removedEntity != null) {
            Preconditions.checkArgument(toRemove == removedEntity); // Must be equal by reference.
            affected.add(toRemove);

            // Fix up relationships on providers and consumers, recording every entity affected by the change.
            toRemove.getTopologyProviders().forEach(provider -> {
                provider.removeConsumer(toRemove);
                affected.add(provider);
            });
            toRemove.getTopologyConsumers().forEach(consumer -> {
                consumer.removeProvider(toRemove);
                affected.add(consumer);
            });

            toRemove.clearConsumers();
            toRemove.clearProviders();
        }

        return affected;
    }

    /**
     * Get the {@link TopologyStitchingEntity} corresponding to an entity from the stitchingGraph, or if
     * it does not exist, create one and insert it into the stitchingGraph.
     *
     * @param entityData The entity whose corresponding {@link TopologyStitchingEntity} should be looked up
     *                   or created.
     * @return The retrieved or newly created {@link TopologyStitchingEntity} for the entity.
     */
    public TopologyStitchingEntity getOrCreateStitchingEntity(@Nonnull final StitchingEntityData entityData) {
        return getEntity(entityData.getEntityDtoBuilder()).orElseGet(() -> {
            final TopologyStitchingEntity newStitchingEntity = new TopologyStitchingEntity(entityData);
            stitchingEntities.put(newStitchingEntity.getEntityBuilder(), newStitchingEntity);
            return newStitchingEntity;
        });
    }

    private static class Metrics {

        private static final DataMetricCounter ERROR_COUNT = DataMetricCounter.builder()
            .withName("tp_stitching_graph_error_count")
            .withHelp("The number of errors when constructing the stitching graph.")
            .withLabelNames("type")
            .build()
            .register();
    }
}

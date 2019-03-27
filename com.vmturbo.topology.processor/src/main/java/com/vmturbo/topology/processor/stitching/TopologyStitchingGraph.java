package com.vmturbo.topology.processor.stitching;

import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
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

        for (EntityDTO.CommodityBought commodityBought : entityDtoBuilder.getCommoditiesBoughtList()) {
            final String providerId = commodityBought.getProviderId();
            final StitchingEntityData providerData = entityMap.get(providerId);
            if (providerData == null) {
                // TODO (DavidBlinn 12/1/2017): Roll back all entities provided by the target that added
                // this entity because its data is unreliable.
                throw new IllegalArgumentException("Entity " + entityDtoBuilder +
                    " is buying from entity " + providerId + " which does not exist.");
            }

            final TopologyStitchingEntity provider = getOrCreateStitchingEntity(providerData);
            if (!provider.getLocalId().equals(providerId)) {
                throw new IllegalArgumentException("Map key " + providerId +
                    " does not match provider localId value: " + provider.getLocalId());
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
                    throw new IllegalArgumentException("Entity " + entityDtoBuilder +
                            " is related to volume " + volumeLocalId + " which does not exist.");
                }
                volumeId = volumeData.getOid();
            }

            entity.addProviderCommodityBought(provider, new CommoditiesBought(
                    commodityBought.getBoughtList().stream()
                            .map(CommodityDTO::toBuilder)
                            .collect(Collectors.toList()), volumeId));

            provider.addConsumer(entity);
        }

        for (CommodityDTO commoditySold : entityDtoBuilder.getCommoditiesSoldList()) {
            final TopologyStitchingEntity accessing = SdkToTopologyEntityConverter.parseAccessKey(commoditySold).map(accessingLocalId -> {
                final StitchingEntityData accessingData = entityMap.get(accessingLocalId);
                if (accessingData == null) {
                    // TODO (DavidBlinn 12/1/2017): Roll back all entities provided by the target that added
                    // this entity because its data is unreliable.
                    throw new IllegalArgumentException("Entity " + entityDtoBuilder +
                        " accesses entity " + accessingLocalId + " which does not exist.");
                }

                final TopologyStitchingEntity accessEntity = getOrCreateStitchingEntity(accessingData);
                if (!accessEntity.getLocalId().equals(accessingLocalId)) {
                    throw new IllegalArgumentException("Map key " + accessingLocalId +
                        " does not match accessing localId value: " + accessEntity.getLocalId());
                }

                return accessEntity;
            }).orElse(null);
            entity.getTopologyCommoditiesSold().add(new CommoditySold(commoditySold.toBuilder(), accessing));
        }

        // add connected entity, this is currently only used by cloud entities (AWS/Azure)
        // cloud entities use layeredOver to represent normal connection, and consistsOf to
        // represent owns connection. some entities may used layeredOver for other purposes,
        // we don't want to add connected for them, so we need a check here to see if we should
        // use connectedTo.
        // layeredOver means normal connection
        if (entityData.supportsConnectedTo()) {
            entityDtoBuilder.getLayeredOverList().forEach(connectedEntityId -> {
                final StitchingEntityData connectedEntityData = entityMap.get(connectedEntityId);
                if (connectedEntityData == null) {
                    throw new IllegalArgumentException(
                            "Entity " + entityDtoBuilder + " is connected to entity " +
                                    connectedEntityId + " which does not exist.");
                }

                final TopologyStitchingEntity connectedEntity = getOrCreateStitchingEntity(connectedEntityData);
                if (!connectedEntity.getLocalId().equals(connectedEntityId)) {
                    throw new IllegalArgumentException("Map key " + connectedEntityId +
                            " does not match connected entity localId value: " + connectedEntity.getLocalId());
                }
                entity.addConnectedTo(ConnectionType.NORMAL_CONNECTION, connectedEntity);
                connectedEntity.addConnectedFrom(ConnectionType.NORMAL_CONNECTION, entity);
            });
            // consistsOf means owns connection
            entityDtoBuilder.getConsistsOfList().forEach(connectedEntityId -> {
                final StitchingEntityData connectedEntityData = entityMap.get(connectedEntityId);
                if (connectedEntityData == null) {
                    throw new IllegalArgumentException(
                            "Entity " + entityDtoBuilder + " is connected to entity " +
                                    connectedEntityId + " which does not exist.");
                }

                final TopologyStitchingEntity connectedEntity = getOrCreateStitchingEntity(connectedEntityData);
                if (!connectedEntity.getLocalId().equals(connectedEntityId)) {
                    throw new IllegalArgumentException("Map key " + connectedEntityId +
                            " does not match connected entity localId value: " + connectedEntity.getLocalId());
                }
                entity.addConnectedTo(ConnectionType.OWNS_CONNECTION, connectedEntity);
                connectedEntity.addConnectedFrom(ConnectionType.OWNS_CONNECTION, entity);
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
}

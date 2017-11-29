package com.vmturbo.stitching;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * An entity capable of being stitched.
 *
 * Provides access to the source builder for an entity and its properties as provided by the probe that
 * discovered it and mutated by any previously run stitching operations. Note that the commodities
 * of the entity have been stripped off this builder. Access the commodities directly via the
 * {@link StitchingEntity} interface.
 *
 * Also provides access to information about the target that discovered the entity, the OID (object identifier)
 * of the entity.
 *
 * Access the consumers and providers of a {@link StitchingEntity} to traverse the graph from that entity.
 *
 * Mutations to stitching entities during stitching should be tracked via an appropriate
 * {@link com.vmturbo.stitching.StitchingOperationResult.StitchingChange} on a {@link StitchingOperationResult}
 * object. Mutating a {@link StitchingEntity} directly may not propagate that change
 * to all objects that need to track the change. See {@link StitchingOperationResult} for additional details.
 */
public interface StitchingEntity {
    /**
     * Get the builder for the {@link EntityDTO} as discovered by the target that discovered by this probe
     * and modified by earlier stitching operations.
     *
     * @return The builder for the {@link EntityDTO} for this entity.
     */
    @Nonnull
    EntityDTO.Builder getEntityBuilder();

    /**
     * Get the {@link StitchingEntity}s providing commodities to this {@link StitchingEntity}.
     *
     * A provider is defined as an entity from which this entity is buying commodities.
     * It is possible for a provider to appear multiple times in this stream if the provider is supplying
     * multiple groupings of commodities to this entity.
     *
     * @return The {@link StitchingEntity}s providing commodities to this entity.
     */
    Set<StitchingEntity> getProviders();

    /**
     * Get the {@link StitchingEntity}s that are buying commodities from this {@link StitchingEntity}.
     *
     * If an entity is buying commodities from another entity, the buyer is the consumer of the supplier.
     * A consumer will never appear more than once in the stream of consumers.
     *
     * @return The {@link StitchingEntity}s buying commodities from this entity.
     */
    Set<StitchingEntity> getConsumers();

    /**
     * Get the {@link EntityType} of this {@link StitchingEntity}.
     *
     * @return the {@link EntityType} of this {@link StitchingEntity}.
     */
    default EntityType getEntityType() {
        return getEntityBuilder().getEntityType();
    }

    /**
     * Get the target-local ID of this {@link StitchingEntity}.
     *
     * This ID is NOT guaranteed to be unique within the topology.
     * Two entities with the same target-local ID are not necessarily the same.
     *
     * @return the target-local ID of this {@link StitchingEntity}.
     */
    default String getLocalId() {
        return getEntityBuilder().getId();
    }

    /**
     * Get the display name of an entity.
     *
     * @return the display name of an entity.
     */
    default String getDisplayName() {
        return getEntityBuilder().getDisplayName();
    }

    /**
     * Get the OID (Object ID) of the entity.
     *
     * Multiple {@link StitchingEntity}s in the topology may have the same OID during stitching,
     * but they will always refer to the same real-world object. {@link StitchingOperation}s should
     * resolve these multiple definitions of this single entity into a single definition by
     * the completion of stitching.
     *
     * @return The OID (Object ID) of the entity.
     */
    long getOid();

    /**
     * Get the ID of the target that discovered this {@link StitchingEntity}.
     *
     * @return The ID of the target that discovered this {@link StitchingEntity}.
     */
    long getTargetId();

    /**
     * Get the time that the data for this entity was last updated.
     * Important note: This is the time that TopologyProcessor received this data from the probe, not the actual
     * time that the probe retrieved the information from the target.
     *
     * This field may be used as a heuristic for the recency of the data in the absence of better information.
     * The time is in "computer time" and not necessarily UTC, however, times on {@link StitchingEntity}s
     * are comparable. See {@link System#currentTimeMillis()} for further details.
     *
     * @return The time that the data for this entity was last updated.
     */
    long getLastUpdatedTime();

    /**
     * The commodities sold by this entity.
     *
     * Currently no mutations are allowed to commodities sold.
     * TODO: Appropriately support mutations to commodities sold when use cases are identified.
     *
     * @return The commodities sold by this entity.
     */
    Stream<CommodityDTO> getCommoditiesSold();

    /**
     * Get the commodities bought by this entity, indexed by the provider
     * of those commodities.
     *
     * Mutations to this map may affect relations with other entities in the topology
     * (ie providers of the commodities in the map may need to be notified of new or removed
     * consumers).
     *
     * Prefer using the appropriate helper utilities in the {@link com.vmturbo.stitching.utilities}
     * package to ensure that updates to commodities are performed appropriately.
     *
     * TODO: Support the possibility of entities buying multiple groupings of commodities from the
     * TODO: same provider.
     *
     * @return the commodities bought by this entity, indexed by the provider of those commodities.
     */
    Map<StitchingEntity, List<CommodityDTO>> getCommoditiesBoughtByProvider();

    /**
     * Remove an entity as a provider to this entity. If the provider is successfully
     * removed, returns the {@link List} of all commodities that this entity was
     * buying from the provider that this entity will no longer be buying goods from.
     *
     * TODO: Support multiple commodity groupings being bought from the same entity.
     *
     * @param entity The entity providing commodities that this entity should stop
     *               buying commodities from.
     * @return If the input entity was a provider, returns an {@link Optional} with
     *         the list of commodities no longer being bought from the provider. If
     *         the entity was not a provider, returns {@link Optional#empty()}.
     */
    Optional<List<CommodityDTO>> removeProvider(@Nonnull final StitchingEntity entity);

    /**
     * Check if this entity is providing commodities to another {@link StitchingEntity}.
     *
     * @param entity The entity to check if it is providing commodities to this entity.
     * @return True if the entity is providing commodities to this entity, false otherwise.
     */
    boolean hasProvider(@Nonnull final StitchingEntity entity);

    /**
     * Check if this entity is consuming commodities from another {@link StitchingEntity}.
     *
     * @param entity The entity to check if it is consuming commodities from this entity.
     * @return True if the entity is consuming commodities to this entity, false otherwise.
     */
    boolean hasConsumer(@Nonnull final StitchingEntity entity);
}

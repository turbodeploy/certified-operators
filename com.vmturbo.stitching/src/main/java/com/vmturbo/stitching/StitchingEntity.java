package com.vmturbo.stitching;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologicalChangelog.TopologicalChange;
import com.vmturbo.stitching.journal.JournalableEntity;

/**
 * An entity capable of being stitched.
 *
 * Provides access to the source builder for an entity and its properties as provided by the probe that
 * discovered it and mutated by any previously run stitching calculations or operations. Note that the commodities
 * of the entity have been stripped off this builder. Access the commodities directly via the
 * {@link StitchingEntity} interface.
 *
 * Also provides access to information about the target that discovered the entity, the OID (object identifier)
 * of the entity.
 *
 * Access the consumers and providers of a {@link StitchingEntity} to traverse the graph from that entity.
 *
 * Mutations to stitching entities during stitching should be tracked via an appropriate
 * {@link TopologicalChange} on a {@link TopologicalChangelog}
 * object. Mutating a {@link StitchingEntity} directly may not propagate that change
 * to all objects that need to track the change. See {@link TopologicalChangelog} for additional details.
 */
public interface StitchingEntity extends JournalableEntity<StitchingEntity> {
    /**
     * Get the builder for the {@link EntityDTO} as discovered by the target that discovered by this probe
     * and modified by earlier stitching calculations or operations.
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
    @Nonnull
    @Override
    default String getDisplayName() {
        return getEntityBuilder().getDisplayName();
    }

    /**
     * Get the OID (Object ID) of the entity.
     *
     * Multiple {@link StitchingEntity}s in the topology may have the same OID during stitching,
     * but they will always refer to the same real-world object. {@link PreStitchingOperation}s and
     * {@link StitchingOperation}s should resolve these multiple definitions of this single entity
     * into a single definition by the completion of stitching.
     *
     * @return The OID (Object ID) of the entity.
     */
    long getOid();

    /**
     * Get the ID of the target that discovered this {@link StitchingEntity}.
     *
     * Note that if the entity has been merged with one or more other entities during stitching
     * (meaning the entity was discovered by multiple targets), those targetIds are available through
     * information on {@link #getMergeInformation()}.
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
     * Update the time at which the data for this entity was last updated if that time is
     * more recent.
     *
     * If the input updateTime is not more recent than the current updateTime according to the
     * {@link #getLastUpdatedTime()} method, this {@link StitchingEntity} will retain its
     * current updateTime instead of adopting the input time.
     *
     * @param updateTime The update time to adopt if newer than the current update time.
     * @return true if the input updateTime is newer than the current update time,
     *         false otherwise.
     */
    boolean updateLastUpdatedTime(final long updateTime);

    /**
     * The commodities sold by this entity.
     *
     * Currently no mutations are allowed to commodities sold.
     *
     * @return The commodities sold by this entity.
     */
    Stream<CommodityDTO.Builder> getCommoditiesSold();

    /**
     * Have this {@link StitchingEntity} sell a new commodity.
     * Note that adding a new commodity sold does NOT automatically make any other entity buy this new commodity.
     * To have other entities buy this commodity, you must add buying relationships.
     *
     * @param commoditySold The commodity to be sold by this stitching entity.
     * @param accesses Usually empty. For DSPM_ACCESS commodity this is the PhysicalMachine associated
     *                 with the Storage that sells this commodity. For DATASTORE commodity this is the
     *                 Storage associated with the PhysicalMachine that sells this commodity.
     *                 Empty otherwise.
     */
    void addCommoditySold(@Nonnull final CommodityDTO.Builder commoditySold,
                          @Nonnull final Optional<StitchingEntity> accesses);

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
    Map<StitchingEntity, List<CommodityDTO.Builder>> getCommoditiesBoughtByProvider();

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
    Optional<List<CommodityDTO.Builder>> removeProvider(@Nonnull final StitchingEntity entity);

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

    /**
     * Get an unmodifiable view of information about the entities that were merged onto
     * this entity during stitching.
     *
     * If no entities were merged onto this entity, will return an empty list.
     * No guarantees are made about the order of the information in the list.
     *
     * @return an unmodifiable view of information about the entities that were merged onto
     *         this entity during stitching.
     */
    @Nonnull
    List<StitchingMergeInformation> getMergeInformation();

    /**
     * Add tracking information for another {@link StitchingEntity} that was merged onto this one.
     *
     * Attempting to add the same information multiple times will have no effect after the first addition.
     *
     * @param mergeInformation the {@Link StitchingMergeInformation} to be added.
     * @return true if the information was successfully added, false otherwise.
     */
    boolean addMergeInformation(@Nonnull final StitchingMergeInformation mergeInformation);

    /**
     * Add a list of {@link StitchingMergeInformation} for entities that were merged onto this entity.
     *
     * Equivalent to calling {@link #addMergeInformation} for each element of the input list.
     *
     * @param mergeInformation The list of {@link StitchingMergeInformation} to be added.
     */
    void addAllMergeInformation(@Nonnull final List<StitchingMergeInformation> mergeInformation);

    /**
     * True if this {@link StitchingEntity}'s merge information contains at least one entry.
     *
     * @return True if this {@link StitchingEntity}'s merge information contains at least one entry,
     *         false otherwise.
     */
    boolean hasMergeInformation();

    @Nonnull
    @Override
    default Stream<Long> getDiscoveringTargetIds() {
        return Stream.concat(Stream.of(getTargetId()), getMergeInformation().stream()
                .map(StitchingMergeInformation::getTargetId));
    }

    /**
     * Get a {@link DiscoveryOrigin} object representing when this entity was last updated and by which
     * target(s).
     *
     * @return {@link DiscoveryOrigin} for this entity.
     */
    default DiscoveryOrigin buildDiscoveryOrigin() {
        return DiscoveryOriginBuilder.discoveredBy(getTargetId())
            .withMergeFromTargetIds(getMergeInformation().stream()
                .map(StitchingMergeInformation::getTargetId))
            .lastUpdatedAt(getLastUpdatedTime());
    }
}

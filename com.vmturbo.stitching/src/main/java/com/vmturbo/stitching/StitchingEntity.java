package com.vmturbo.stitching;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.common.protobuf.topology.StitchingErrors;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.EntityPipelineErrors;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.EntityPipelineErrors.StitchingErrorCode;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologicalChangelog.TopologicalChange;
import com.vmturbo.stitching.journal.JournalableEntity;
import com.vmturbo.stitching.utilities.CommoditiesBought;
import com.vmturbo.stitching.utilities.DTOFieldAndPropertyHandler;

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
     * Get the connected {@link StitchingEntity} instances that this entity is "connected to"
     * grouped by connection type.
     *
     * <p>This relationship was added to support cloud entities that are connected to other entities
     * but do not buy or sell commodities. For example, a BusinessAccount may be connected to
     * other BusinessAccounts, a VM may be connected to an AvailabilityZone, and so on, and may
     * own Workload entities such as VMs, Databases, etc.</p>
     *
     * <p>This method returns all types of outwards connections: these include normal connections,
     * owned, aggregated entities and controlled entities.</p>
     *
     * @return the map from {@link ConnectionType} to set of {@link StitchingEntity} instances that
     * this entity is connected to.
     */
    default Map<ConnectionType, Set<StitchingEntity>> getConnectedToByType() {
        return Collections.emptyMap();
    }

    /**
     * Get the connected {@link StitchingEntity} instances that are "connectedTo" this entity
     * grouped by connection type.
     *
     * <p>This is the inverse of the connectedTo relationship and it used by cloud probes as well
     * as Storage Browsing probes.</p>
     *
     * <p>This method returns all types of inwards connections: these include normal connections,
     * the owner, aggregating entities and controlling entities.</p>
     *
     * @return the map from {@link ConnectionType} to set of {@link StitchingEntity} instances that
     * are connectedTo this entity.
     */
    default Map<ConnectionType, Set<StitchingEntity>> getConnectedFromByType() {
        return Collections.emptyMap();
    }

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
     * Get the stitching errors encountered by this specific entity during stitching.
     */
    @Nonnull
    StitchingErrors getStitchingErrors();

    /**
     * Record an error that this entity encountered during any part of stitching.
     *
     * @param errorCode The code of the error.
     */
    void recordError(@Nonnull StitchingErrorCode errorCode);

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
    boolean updateLastUpdatedTime(long updateTime);

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
    void addCommoditySold(@Nonnull CommodityDTO.Builder commoditySold,
                          @Nonnull Optional<StitchingEntity> accesses);

    /**
     * Get the commodities bought list by this entity, indexed by the provider of those commodities.
     * For most cases, there is one set of commodity bought for one provider, but there is use case
     * that multiple commodity bought exist for same provider.
     *
     * For example: cloud VM may buy multiple set of commodities bought from same storage tier,
     * depending how may volumes are attached to it.
     *
     * Mutations to this map may affect relations with other entities in the topology
     * (ie providers of the commodities in the map may need to be notified of new or removed
     * consumers).
     *
     * Prefer using the appropriate helper utilities in the {@link com.vmturbo.stitching.utilities}
     * package to ensure that updates to commodities are performed appropriately.
     *
     * @return the commodities bought list by this entity, indexed by the provider of those commodities.
     */
    Map<StitchingEntity, List<CommoditiesBought>> getCommodityBoughtListByProvider();

    /**
     * Find the matching bought commodities set on this entity based on the provider and
     * commoditiesBought, since there may be multiple set of CommoditiesBought.
     *
     * @param provider the provider from which to find commodity bought
     * @param commoditiesBought the {@link CommoditiesBought} instance used to match the
     * CommoditiesBought on this entity
     * @return matched CommoditiesBought wrapped by Optional, or empty if not existing.
     */
    Optional<CommoditiesBought> getMatchingCommoditiesBought(@Nonnull StitchingEntity provider,
            @Nonnull CommoditiesBought commoditiesBought);

    /**
     * Remove an entity as a provider to this entity. If the provider is successfully
     * removed, returns the {@link List} of all commodities that this entity was
     * buying from the provider that this entity will no longer be buying goods from.
     *
     * @param entity The entity providing commodities that this entity should stop
     *               buying commodities from.
     * @return If the input entity was a provider, returns an {@link Optional} with
     *         the list of commodities no longer being bought from the provider. If
     *         the entity was not a provider, returns {@link Optional#empty()}.
     */
    Optional<List<CommoditiesBought>> removeProvider(@Nonnull StitchingEntity entity);

    /**
     * Remove the connection from this entity to an entity this entity is connected to.
     *
     * @param connectedTo The entity this entity is connected to.
     * @param type The type of connection to remove.
     * @return True if the connection was successfully removed. False if there was no connection
     *         of the right type to remove.
     */
    boolean removeConnection(@Nonnull StitchingEntity connectedTo,
                             @Nonnull ConnectionType type);

    /**
     * Check if this entity is providing commodities to another {@link StitchingEntity}.
     *
     * @param entity The entity to check if it is providing commodities to this entity.
     * @return True if the entity is providing commodities to this entity, false otherwise.
     */
    boolean hasProvider(@Nonnull StitchingEntity entity);

    /**
     * Check if this entity is consuming commodities from another {@link StitchingEntity}.
     *
     * @param entity The entity to check if it is consuming commodities from this entity.
     * @return True if the entity is consuming commodities to this entity, false otherwise.
     */
    boolean hasConsumer(@Nonnull StitchingEntity entity);

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
    boolean addMergeInformation(@Nonnull StitchingMergeInformation mergeInformation);

    /**
     * Add a list of {@link StitchingMergeInformation} for entities that were merged onto this entity.
     *
     * Equivalent to calling {@link #addMergeInformation} for each element of the input list.
     *
     * @param mergeInformation The list of {@link StitchingMergeInformation} to be added.
     */
    void addAllMergeInformation(@Nonnull List<StitchingMergeInformation> mergeInformation);

    /**
     * True if this {@link StitchingEntity}'s merge information contains at least one entry.
     *
     * @return True if this {@link StitchingEntity}'s merge information contains at least one entry,
     *         false otherwise.
     */
    boolean hasMergeInformation();

    /**
     * Indicates an entity that only exists to push data and/or relationships in stitching.  This
     * entity should be removed in the event that it doesn't get stitched.
     *
     * @return Boolean value indicating whether or not to remove this entity in the event that it
     * doesn't get stitched.
     */
    default boolean removeIfUnstitched() {
        return false;
    }

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
        String localName = DTOFieldAndPropertyHandler.getVendorId(getEntityBuilder());
        if (StringUtils.isBlank(localName)) {
            localName = getEntityBuilder().getId();
        }
        return DiscoveryOriginBuilder.discoveredBy(getTargetId(), localName)
            .withMerge(getMergeInformation().stream())
            .lastUpdatedAt(getLastUpdatedTime());
    }

    /**
     * Get a {@link EntityPipelineErrors} object representing the stitching errors encountered by
     * this entity. Note - other stages further down the topology pipeline may edit this errors
     * message to add non-stitching related error information.
     *
     * @return If this entity had stitching errors, returns an optional {@link EntityPipelineErrors}
     *         with a non-zero value in {@link EntityPipelineErrors#getStitchingErrors()}.
     *         Otherwise, returns an empty optional.
     */
    @Nonnull
    default StitchingErrors combinedEntityErrors() {
        final StitchingErrors combinedError = new StitchingErrors();
        combinedError.add(getStitchingErrors());
        getMergeInformation().stream()
            .map(StitchingMergeInformation::getError)
            .forEach(combinedError::add);
        return combinedError;
    }
}

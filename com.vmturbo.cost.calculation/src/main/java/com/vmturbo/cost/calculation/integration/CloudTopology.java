package com.vmturbo.cost.calculation.integration;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Represents the subset of the topology that lives in the cloud, and provides methods required
 * to navigate the topology. This is a much simpler version of the topology graph we use for
 * group resolution, because we only need to consider specific kinds of relationships.
 *
 * @param <ENTITY_CLASS> The class used to represent entities in the topology. For example,
 *                      {@link TopologyEntityDTO} for the realtime topology.
 */
public interface CloudTopology<ENTITY_CLASS> {

    /**
     * Return an unmodifiable map of the entities in the topology.
     * Note: Use this as a last resort - use the other methods on the topology whenever possible.
     * e.g. don't get the map and look up the entity, use {@link CloudTopology#getEntity(long)}.
     *
     * @return A map of entities, arranged by ID.
     */
    @Nonnull
    Map<Long, ENTITY_CLASS> getEntities();

    /**
     * Get the entity associated with an ID.
     *
     * @param entityId The ID of the entity.
     * @return An optional containing the entity, or an empty optional if the ID is not found.
     */
    @Nonnull
    Optional<ENTITY_CLASS> getEntity(final long entityId);

    /**
     * Get the compute tier associated with an entity.
     *
     * Only finds the immediately connected compute tier. For example, suppose an APPLICATION
     * connected to a VM connected to a COMPUTE TIER. Calling this method with the APPLICATION's
     * ID will return an empty optional.
     *
     * @param entityId The ID of the entity.
     * @return An optional containing the compute tier entity, or an empty optional if the ID is
     *  not found, or if there is no compute tier directly associated with the entity.
     */
    @Nonnull
    Optional<ENTITY_CLASS> getComputeTier(final long entityId);

    /**
     * Get the database tier associated with an entity.
     *
     * Only finds the immediately connected database tier. For example, suppose a DATABASE is
     * connected to a DATABASE TIER. Calling this method with the DATABASE's
     * ID will return the associated databaseTier.
     *
     * @param entityId The ID of the entity.
     * @return An optional containing the database tier entity, or an empty optional if the ID is
     *  not found, or if there is no database tier directly associated with the entity.
     */
    @Nonnull
    Optional<ENTITY_CLASS> getDatabaseTier(final long entityId);

    /**
     * Get the storage tier associated with an entity.
     *
     * Only finds the immediately connected storage tier. For example, suppose a VIRTUAL_MACHINE is
     * connected to a STORAGE TIER. Calling this method with the VIRTUAL_MACHINE's
     * ID will return the associated databaseTier.
     *
     * @param entityId The ID of the entity.
     * @return An optional containing the storage tier entity, or an empty optional if the ID is
     *  not found, or if there is no storage tier directly associated with the entity.
     */
    @Nonnull
    Optional<ENTITY_CLASS> getStorageTier(final long entityId);

    /**
     * Get the volumes connected to an entity.
     *
     * Only finds the immediately connected volumes.
     *
     * @param entityId The ID of the entity.
     * @return A collection of volumes connected to the entity, or an empty collection if there
     *    are none.
     */
    @Nonnull
    Collection<ENTITY_CLASS> getConnectedVolumes(final long entityId);

    /**
     * Get the region associated with an entity.
     *
     * This method should "pass through" availability zones. For example, suppose a VM is
     * connected to an AVAILABILITY ZONE, which is connected to a REGION. This method should
     * return the REGION given the ID of the VM.
     *
     * @param entityId The ID of the entity.
     * @return An optional containing the region this entity is in, or an empty optional if the ID
     *  is not found, or there is no associated region. Note: for cloud entities there should always
     *  be SOME associated region unless the topology is malformed.
     */
    @Nonnull
    Optional<ENTITY_CLASS> getConnectedRegion(final long entityId);

    /**
     * Get the availability zone associated with an entity.
     *
     * This method should return AVAILABILITY ZONE which is directly connected with the entity.
     *
     * @param entityId The ID of the entity.
     * @return An optional containing the availability zone this entity is in, or an empty optional
     *         if the ID is not found, or there is no associated availability zone.
     */
    @Nonnull
    Optional<ENTITY_CLASS> getConnectedAvailabilityZone(final long entityId);

    /**
     * Get the owner of a particular entity.
     *
     * The "owner" is the entity that has an "OWNS" connection to the entity. Typically this
     * will be a business account.
     *
     * This method does not search for the owner recursively. i.e. if entity A owns entity B, and
     * entity B is connected to entity C, calling this method on entity C will return an empty
     * optional.
     *
     * @param entityId The ID of the entity.
     * @return An optional containing the owner of this entity, or an empty optional if the ID
     * is not found, or there is no associated owner.
     */
    @Nonnull
    Optional<ENTITY_CLASS> getOwner(final long entityId);

    /**
     * Get the service a particular entity belongs to. If called with the ID of a service, this is
     * equivalent to {@link CloudTopology#getEntity(long)}.
     *
     * This method does not search for the owner recursively. Typically services are connected to
     * tiers. To get the service associated with, say, a VM, the caller has to find the ID(s) of the
     * tier(s) the VM buys from, and then call this method.
     *
     * @param entityId The ID of the entity. This can be the ID of a consumer from a service, or the
     *                 ID of a service.
     * @return An optional containing the service connected to this entity, or an empty optional if
     * the ID is not found, or there is no associated service.
     */
    @Nonnull
    Optional<ENTITY_CLASS> getConnectedService(final long entityId);

    /**
     * Get the size of the topology.
     *
     * @return The size of the topology.
     */
    int size();

    /*
     * Get all regions in the topology.
     */
    @Nonnull
    List<ENTITY_CLASS> getAllRegions();

    /*
     * Get all entities of a particular type in the topology.
     */
    @Nonnull
    List<ENTITY_CLASS> getAllEntitesOfType(int entityType);

}

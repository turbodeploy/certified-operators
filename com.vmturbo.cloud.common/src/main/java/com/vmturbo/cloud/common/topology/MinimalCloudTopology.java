package com.vmturbo.cloud.common.topology;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.group.api.GroupAndMembers;

/**
 * A cloud topology, providing the minimal set of operations that may be performed on compact
 * topology entities.
 *
 * @param <ENTITY_CLASS> The entity class type
 */
public interface MinimalCloudTopology<ENTITY_CLASS> {

    /**
     * Gets the map of cloud entities, indexed by their OID.
     *
     * @return An immutable map of entities, indexed by their OID.
     */
    @Nonnull
    Map<Long, ENTITY_CLASS> getEntities();

    /**
     * Searches for the entity in the topology, returning it if found.
     *
     * @param entityId The target entity ID.
     * @return An optional containing the entity, if found. the optional will be entity if no
     * entity can be found.
     */
    @Nonnull
    Optional<ENTITY_CLASS> getEntity(long entityId);

    /**
     * Checks if the entity exists within the topology.
     *
     * @param entityOid The target entity OID.
     * @return True, if the entity ID is found within the topology. False otherwise.
     */
    boolean entityExists(long entityOid);

    /**
     * Determines whether the target entity is powered on.
     * @param entityOid The target entity OID.
     * @return An optional containing true, if the entity exists and is powered on. THe optional will
     * contain false, if the entity exists and is not powered on. The optional will be empty, if the
     * entity does not exist in the topology.
     */
    @Nonnull
    Optional<Boolean> isEntityPoweredOn(long entityOid);

    /**
     * Resolves the billing family associated with an account, through the group service.
     * @param accountOid The target account OID.
     * @return The {@link GroupAndMembers} instance representing the associated billing family,
     * if one is found.
     */
    @Nonnull
    Optional<GroupAndMembers> getBillingFamilyForAccount(long accountOid);


    /**
     * A factory class for creating {@link MinimalCloudTopology}.
     * @param <ENTITY_CLASS> The entity class type.
     */
    interface MinimalCloudTopologyFactory<ENTITY_CLASS> {

        /**
         * Creates a minimal cloud topology, based on the provided {@code entities}.
         *
         * @param entities A stream of entities to include in the topology. The entities do not need
         *                 to be filtered by environment type (this is the responsibility of the
         *                 cloud topology).
         * @return The newly created cloud topology.
         */
        @Nonnull
        MinimalCloudTopology<ENTITY_CLASS> createCloudTopology(@Nonnull Stream<ENTITY_CLASS> entities);
    }
}

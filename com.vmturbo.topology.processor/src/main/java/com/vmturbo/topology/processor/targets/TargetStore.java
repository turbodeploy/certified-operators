package com.vmturbo.topology.processor.targets;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.identity.exceptions.IdentifierConflictException;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.topology.TopologyHandler;

/**
 * Interface for CRUD operations on registered targets.
 */
public interface TargetStore {

    /**
     * Gets the target if it exists in the store.
     *
     * @param targetId OID of the target to look for.
     * @return The target information, or an empty optional if none found.
     */
    @Nonnull
    Optional<Target> getTarget(final long targetId);

    /**
     * Get the name of a target if it exists.
     *
     * @param targetId OID of the target to look for.
     * @return The name of the target, or an empty optional if the target is not found or has no name.
     */
    @Nonnull
    Optional<String> getTargetAddress(final long targetId);

    /**
     * Retrieve all stored targets.
     *
     * @return A list of all targets registered in the store.
     */
    @Nonnull
    List<Target> getAll();

    /**
     * Stores the information for a new target.
     * If the target described by the spec already exists the
     * method succeeds and assigns a new ID.
     *
     * @param spec Target information.
     * @return The newly created target.
     * @throws InvalidTargetException If the target spec is invalid.
     * @throws DuplicateTargetException If the target is already exist.
     * @throws IdentityStoreException If no old or new oid fetched.
     */
    @Nonnull
    Target createTarget(@Nonnull final TargetSpec spec) throws InvalidTargetException,
            DuplicateTargetException, IdentityStoreException;

    /**
     * Retores the information from serialized string with given oid. Does not validate account values.
     *
     * @param targetId the target identifier
     * @param spec Target information
     * @return The newly created target
     * @throws InvalidTargetException If the target spec is invalid.
     */
    @Nonnull
    Target restoreTarget(long targetId, @Nonnull final TargetSpec spec) throws InvalidTargetException;

    /**
     * Creates or updates derived targets based on the target specs. If the target has already exist, we just
     * update the current one with non-identifier fields data, or create new derived target.
     *
     * @param targetSpecs List of target information.
     * @throws IdentityStoreException If fetching target identity attributes failed.
     */
    void createOrUpdateDerivedTargets(@Nonnull final List<TargetSpec> targetSpecs)
            throws IdentityStoreException;

    /**
     * Get all targets associated with a probe.
     *
     * @param probeId OID of the probe to look for.
     * @return Targets associated with the probe.
     */
    @Nonnull
    List<Target> getProbeTargets(final long probeId);

    /**
     * Updates existing target with the newly specified target spec.
     * Permits partial update of the target's configuration. That is, if a field on
     * a target is not included in the {@code updatedFields}, existing field
     * values will be used.
     *
     * @param targetId target id to change
     * @param updatedFields new data for the target
     * @return new changed target
     * @throws InvalidTargetException if target validation failed.
     * @throws TargetNotFoundException if target to be modified is absent in the store.
     * @throws IdentityStoreException if target spec update failed.
     */
    @Nonnull
    Target updateTarget(long targetId, @Nonnull Collection<AccountValue> updatedFields)
                    throws InvalidTargetException, TargetNotFoundException,
                        IdentityStoreException, IdentifierConflictException;

    /**
     * Removes existing target with the specified id from the store and trigger broadcast.
     *
     * @param targetId target id to remove
     * @return target removed
     * @throws TargetNotFoundException if target to be removed is absent in the store.
     * @throws IdentityStoreException if target to be removed is absent in the store.
     */
    @Nonnull
    Target removeTargetAndBroadcastTopology(long targetId, TopologyHandler topologyHandler,
                    Scheduler scheduler) throws TargetNotFoundException, IdentityStoreException;

    /**
     * Remove all targets from the store.
     */
    void removeAllTargets();

    /**
     * Add a listener for {@link TargetStore} events.
     *
     * @param listener The listener to add.
     */
    void addListener(@Nonnull TargetStoreListener listener);

    /**
     * Remove a listener for {@link TargetStore} events.
     *
     * @param listener The listener to remove.
     * @return True if the listener was successfully removed, false otherwise.
     */
    boolean removeListener(@Nonnull TargetStoreListener listener);

    /**
     * Get all derived target ids which belong to the specific parent target.
     *
     * @param parentTargetId The parent target id.
     * @return The set of derived target ids which belong to the parent target.
     */
    @Nonnull
    Set<Long> getDerivedTargetIds(long parentTargetId);

    /**
     * Get the probe type for a given target id.
     *
     * @param targetId the id of the target to get probe type for
     * @return SDKProbeType for the target if it exists
     */
    Optional<SDKProbeType> getProbeTypeForTarget(long targetId);
}

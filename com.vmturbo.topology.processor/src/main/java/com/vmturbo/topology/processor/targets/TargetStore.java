package com.vmturbo.topology.processor.targets;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

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
    @Nonnull Optional<Target> getTarget(final long targetId);

    /**
     * Retrieve all stored targets.
     *
     * @return A list of all targets registered in the store.
     */
    @Nonnull List<Target> getAll();

    /**
     * Stores the information for a new target.
     * If the target described by the spec already exists the
     * method succeeds and assigns a new ID.
     *
     * @param spec Target information.
     * @return The newly created target.
     * @throws InvalidTargetException If the spec is invalid.
     */
    @Nonnull Target createTarget(@Nonnull final TargetSpec spec) throws InvalidTargetException;

    /**
     * Stores the information for a new target. Does not validate account values.
     * @param targetId the target identifier
     * @param spec Target information
     * @return The newly created target
     * @throws InvalidTargetException If the target spec is invalid.
     */
    @Nonnull Target createTarget(long targetId, @Nonnull final TargetSpec spec) throws InvalidTargetException;

    /**
     * Get all targets associated with a probe.
     *
     * @param probeId OID of the probe to look for.
     * @return Targets associated with the probe.
     */
    @Nonnull List<Target> getProbeTargets(final long probeId);

    /**
     * Updates existing target with the newly specified target spec.
     * Permits partial update of the target's configuration. That is, if a field on
     * a target is not included in the {@code updatedFields}, existing field
     * values will be used.
     *
     * @param targetId target id to change
     * @param updatedFields new data for the target
     * @return new changed target
     * @throws InvalidTargetException if target validation failed
     * @throws TargetNotFoundException if target to be modified is absent in the store.
     */
    @Nonnull
    Target updateTarget(long targetId, @Nonnull Collection<AccountValue> updatedFields)
                    throws InvalidTargetException, TargetNotFoundException;

    /**
     * Removes existing target with the specified id from the store and trigger broadcast.
     *
     * @param targetId target id to remove
     * @return target removed
     * @throws TargetNotFoundException if target to be removed is absent in the store.
     */
    @Nonnull
    Target removeTargetAndBroadcastTopology(long targetId, TopologyHandler topologyHandler,
                    Scheduler scheduler) throws TargetNotFoundException;

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
}

package com.vmturbo.topology.processor.api;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.topology.processor.api.dto.TargetInputFields;

/**
 * Target controller interface is responsible for operations with targets and probes.
 */
public interface TopologyProcessor {
    /**
     * Returns all the registered probes, available currently in topology processor. Method is
     * blocked until result is returned or exception is raised.
     *
     * @return set of probe infos
     * @throws CommunicationException if persistent communication exception occurred
     */
    @Nonnull
    Set<ProbeInfo> getAllProbes() throws CommunicationException;

    /**
     * Returns probe info by probe id.
     *
     * @param id probe identifier
     * @return probe info object
     * @throws CommunicationException if persistent communication exception occurred
     * @throws TopologyProcessorException if probe is not found by the specified id
     * @throws IllegalArgumentException if {@code id} is {@code null}
     */
    @Nonnull
    ProbeInfo getProbe(long id) throws CommunicationException, TopologyProcessorException;

    /**
     * Returns all targets currently registered. Target set can be changed concurrently and the
     * result may not contain latest changes. Use {@link TargetListener} to be notified about all
     * the targets.
     *
     * @return set of registered targets.
     * @throws CommunicationException if persistent communication exception occurred
     */
    @Nonnull
    Set<TargetInfo> getAllTargets() throws CommunicationException;

    /**
     * Returns target info by target id.
     *
     * @param id target identifier
     * @return target info object
     * @throws CommunicationException if persistent communication exception occurred
     * @throws TopologyProcessorException if target is not found by the specified id
     * @throws IllegalArgumentException if {@code id} is {@code null}
     */
    @Nonnull
    TargetInfo getTarget(long id) throws CommunicationException, TopologyProcessorException;

    /**
     * Removes the target specified by id.
     *
     * @param target target id to delete
     * @throws CommunicationException if persistent communication exception occurred
     * @throws TopologyProcessorException if target with specified id does not exist
     * @throws IllegalArgumentException if specified target id is {@code null}
     */
    void removeTarget(long target) throws CommunicationException, TopologyProcessorException;

    /**
     * Changes data for the specified target.
     *
     * @param targetId target id to change
     * @param newData new target data to set for the specified target
     * @throws CommunicationException if persistent communication exception occurred
     * @throws TopologyProcessorException if target with specified id does not exist or new data is
     * not applicable.
     * @throws IllegalArgumentException if specified target id or target data is {@code null}
     */
    void modifyTarget(long targetId, @Nonnull TargetInputFields newData)
            throws CommunicationException, TopologyProcessorException;

    /**
     * Creates new target of the specified probe type and specified target configuration. TODO
     * should validation be executed automatically?
     *
     * @param probeId probe id to create target for
     * @param targetData target configuration data
     * @return id of a newly created target
     * @throws CommunicationException if persistent communication exception occurred
     * @throws TopologyProcessorException if probe with the specified id does not exist or target
     * could not be created.
     * @throws IllegalArgumentException if specified probe id or target data is {@code null}
     */
    long addTarget(long probeId, @Nonnull TargetData targetData)
            throws CommunicationException, TopologyProcessorException;

    /**
     * Triggers target validation. Method is blocked until validation has been started.
     *
     * @param targetId target to validate
     * @return validation result
     * @throws CommunicationException if persistent communication exception occurred
     * @throws TopologyProcessorException if there is no target with this id
     * @throws IllegalArgumentException if specified target id is {@code null}
     * @throws InterruptedException if thread was interrupted during the call
     */
    @Nonnull
    ValidationStatus validateTarget(long targetId)
            throws CommunicationException, TopologyProcessorException, InterruptedException;

    /**
     * Performs validation of all the targets. Method is blocked until validation has been started.
     *
     * @return set of validation responses - for each target that validation has been executed for
     * @throws CommunicationException if persistent communication exception occurred
     * @throws InterruptedException if thread was interrupted during the call
     */
    @Nonnull
    Set<ValidationStatus> validateAllTargets() throws CommunicationException, InterruptedException;

    /**
     * Performs target discovery. Entities, discovered by the call are passed to
     * {@code EntitiesListener}. Method is blocked until discovery has been started.
     *
     * @param targetId target id to discover
     * @return discover result
     * @throws CommunicationException if persistent communication exception occurred
     * @throws TopologyProcessorException if target does not exist with this id
     * @throws IllegalArgumentException if target id is {@code null}
     * @throws InterruptedException if thread was interrupted during the call
     */
    @Nonnull
    DiscoveryStatus discoverTarget(long targetId)
            throws CommunicationException, TopologyProcessorException, InterruptedException;

    /**
     * Peforms discovery of all the existing targets. Method is blocked until discovery has been
     * started.
     *
     * @return set of discovery results - one result per each target, that discovery has been
     * processed for
     * @throws CommunicationException if persistent communication exception occurred
     * @throws InterruptedException if thread was interrupted during the call
     */
    @Nonnull
    Set<DiscoveryStatus> discoverAllTargets() throws CommunicationException, InterruptedException;

    /**
     * Registers target listener.
     *
     * @param listener listener to register
     * @throws IllegalArgumentException if specified listener is {@code null}
     */
    void addTargetListener(@Nonnull TargetListener listener);

    /**
     * Registers actions listener.
     *
     * @param listener listener for notifications related to action execution.
     * @throws IllegalArgumentException if specified listener is {@code null}
     */
    void addActionListener(@Nonnull ActionExecutionListener listener);

    /**
     * Registers probe listener.
     *
     * @param listener listener for notifications related to probes.
     * @throws IllegalArgumentException if specified listener is {@code null}.
     */
    void addProbeListener(@Nonnull ProbeListener listener);

    /**
     * Registers listener to receive live topologies.
     *
     * @param listener listener to register
     * @throws IllegalArgumentException if specified listener is {@code null}
     * @throws IllegalStateException if there is no subscription to this event
     */
    void addLiveTopologyListener(@Nonnull EntitiesListener listener);

    /**
     * Registers listener to receive plan topologies.
     *
     * @param listener listener to register
     * @throws IllegalArgumentException if specified listener is {@code null}
     * @throws IllegalStateException if there is no subscription to this event
     */
    void addPlanTopologyListener(@Nonnull EntitiesListener listener);

    /**
     * Register a listener for topology summaries.
     *
     * @param listener listener to register
     * @throws IllegalArgumentException if specified listener is {@code null}
     * @throws IllegalStateException if there is no subscription to this event
     */
    void addTopologySummaryListener(@Nonnull TopologySummaryListener listener);

    /**
     * Register a listener for host state changes.
     *
     * @param listener listener to register
     * @throws IllegalArgumentException if specified listener is {@code null}
     * @throws IllegalStateException if there is no subscription to this event
     */
    void addEntitiesWithNewStatesListener(@Nonnull EntitiesWithNewStateListener listener);

}


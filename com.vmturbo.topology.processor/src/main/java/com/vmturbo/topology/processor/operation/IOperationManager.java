package com.vmturbo.topology.processor.operation;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import io.prometheus.client.Counter;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.common.dto.ActionExecution;
import com.vmturbo.topology.processor.operation.action.Action;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;

/**
 * Operation manager supplies with information about operations on targets.
 */
public interface IOperationManager {

    /**
     * Request a validation on a target. There may be only a single ongoing validation
     * at a time for a given target. Attempting a validation for a target with a current
     * ongoing validation will return the current ongoing validation.
     * Validations may take a long time and are performed asynchronously. This method throws
     * exceptions if the validation can't initiate. If a problem
     * occurs after the validation is initiated, an appropriate error will be enqueued
     * for later processing.
     *
     * @param targetId The id of the target to validate.
     * @return The {@link Validation} requested for the given target. If there was no ongoing
     * validation for the target with the same type, a new one will be created. If there was
     * an ongoing validation for the target with the same type, the existing one is returned.
     * @throws TargetNotFoundException When the requested target cannot be found.
     * @throws ProbeException When the probe associated with the target is unavailable.
     * @throws CommunicationException When the external probe cannot be reached.
     * @throws InterruptedException when the attempt to send a request to the probe is interrupted.
     */
    @Nonnull
    Validation startValidation(long targetId)
            throws ProbeException, TargetNotFoundException, CommunicationException,
            InterruptedException;

    /**
     * Returns last available validation for the specified target, either running or finished.
     *
     * @param targetId target id
     * @return last validation, or empty result, if no validations has happen
     */
    @Nonnull
    Optional<Validation> getLastValidationForTarget(long targetId);

    /**
     * Returns all the validation operations that are in progress.
     *
     * @return validations in progress
     */
    @Nonnull
    List<Validation> getAllInProgressValidations();

    /**
     * Returns timeout for validation operations. The operation is treated as expired, if during a
     * timeout period, there were no messages for the validation: either validation response or
     * keep-alive
     *
     * @return timeout for validation.
     */
    long getValidationTimeoutMs();

    /**
     * Returns validation, that is in progress.
     *
     * @param id validation id to retriege
     * @return validation operation in progress, or empty result, if the validation is finished
     * or does not exist
     */
    @Nonnull
    Optional<Validation> getInProgressValidation(long id);

    /**
     * Request a discovery on a target. There may be only a single ongoing discovery
     * at a time for a given target. Attempting a discovery for a target with a current
     * ongoing discovery will return the current ongoing discovery.
     * Discoveries may take a long time and are performed asynchronously. This method
     * throws exceptions if the discovery can't initiate. If a problem
     * occurs after the discovery is initiated, an appropriate error will be enqueued
     * for later processing.
     *
     * @param targetId The id of the target to discover.
     * @return The {@link Discovery} requested for the given target. If there was no ongoing
     * discovery
     * for the target with the same type, a new one will be created. If there was an ongoing
     * discovery
     * for the target with the same type, the existing one is returned.
     * @throws TargetNotFoundException When the requested target cannot be found.
     * @throws ProbeException When the probe associated with the target is unavailable.
     * @throws CommunicationException When the external probe cannot be reached.
     * @throws InterruptedException when the attempt to send a request to the probe is interrupted.
     */
    @Nonnull
    Discovery startDiscovery(long targetId)
            throws TargetNotFoundException, ProbeException, CommunicationException,
            InterruptedException;

    /**
     * Returns discovery operations, that are in progress.
     *
     * @return list of discoveries
     */
    @Nonnull
    List<Discovery> getInProgressDiscoveries();

    /**
     * Returns discovery, that is in progress.
     *
     * @param id discovery id to retriege
     * @return discovery operation in progress, or empty result, if the validation is finished
     * or does not exist
     */
    @Nonnull
    Optional<Discovery> getInProgressDiscovery(long id);

    /**
     * Returns last available discovery for the specified target, either running or finished.
     *
     * @param targetId target id
     * @return last discovery, or empty result, if no discoveries has happen
     */
    @Nonnull
    Optional<Discovery> getInProgressDiscoveryForTarget(long targetId);

    /**
     * Returns last available discovery for the specified target, either running or finished.
     *
     * @param targetId target id
     * @return last discovery, or empty result, if no discoveries has happen
     */
    @Nonnull
    Optional<Discovery> getLastDiscoveryForTarget(long targetId);

    /**
     * Discover a target with the same contract as {@link #startDiscovery(long)},
     * with the following exceptions:
     * 1. If a discovery is already in progress, instead of returning the existing discovery,
     * a pending discovery will be added for the target.
     * 2. If the probe associated with the target is not currently registered, a pending discovery
     * will be added for the target.
     * When a target's discovery completes or its probe registers, the pending discovery will
     * be removed and a new discovery will be initiated for the associated target.
     *
     * @param targetId The id of the target to discover.
     * @return An {@link Optional<Discovery>}. If there was no in progress discovery
     * for the target and the target's probe is registered, a new discovery will be initiated.
     * If there was an in progress discovery for the target or the target's probe is unregistered,
     * returns {@link Optional#empty()}.
     * @throws TargetNotFoundException When the requested target cannot be found.
     * @throws CommunicationException When the external probe cannot be reached.
     * @throws InterruptedException when the attempt to send a request to the probe is interrupted.
     */
    @Nonnull
    Optional<Discovery> addPendingDiscovery(long targetId)
            throws TargetNotFoundException, CommunicationException, InterruptedException;

    /**
     * Returns timeout for discovery operations. The operation is treated as expired, if during a
     * timeout period, there were no messages for the discovery: either discovery response or
     * keep-alive
     *
     * @return timeout for discovery.
     */
    long getDiscoveryTimeoutMs();

    /**
     * Request an action on a target. There can be multiple actions per target.
     * Actions may take a long time and are performed asynchronously. This method
     * throws exceptions if the action can't initiate (i.e. in case of an error
     * sending the action to the probe).
     *
     * @param actionId The id of the overarching action. This is the ID that gets
     * assigned by the Action Orchestrator.
     * @param targetId The id of the target containing the entities for the action.
     * @param actionDto The {@link ActionExecution.ActionItemDTO} describing the action to execute.
     * @return The {@link Action} requested for the target.
     * @throws TargetNotFoundException When the requested target is not found.
     * @throws ProbeException When the probe corresponding to the target is not registered.
     * @throws CommunicationException If there is an error sending the request to the probe.
     * @throws InterruptedException If there is an interrupt while sending the request to the
     * probe.
     */
    Action startAction(long actionId, long targetId,
            @Nonnull ActionExecution.ActionItemDTO actionDto)
            throws ProbeException, TargetNotFoundException, CommunicationException,
            InterruptedException;

    /**
     * Returns action, that is in progress.
     *
     * @param id action id to retriege
     * @return action operation in progress, or empty result, if the action is finished or does not
     * exist
     */
    @Nonnull
    Optional<Action> getInProgressAction(long id);

    /**
     * Returns timeout for action related operations. The operation is treated as expired, if
     * during a timeout period, there were no messages for the action: either action response
     * or action progress
     *
     * @return timeout for action execution.
     */
    long getActionTimeoutMs();

    /**
     * Check for and clear expired operations.
     */
    void checkForExpiredOperations();
}
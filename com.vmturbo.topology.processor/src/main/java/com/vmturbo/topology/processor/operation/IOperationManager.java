package com.vmturbo.topology.processor.operation;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionApprovalResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionErrorsResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.GetActionStateResponse;
import com.vmturbo.topology.processor.operation.action.Action;
import com.vmturbo.topology.processor.operation.actionapproval.ActionApproval;
import com.vmturbo.topology.processor.operation.actionapproval.ActionUpdateState;
import com.vmturbo.topology.processor.operation.actionapproval.GetActionState;
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
     * Returns current running validation for the given target.
     *
     * @param targetId target id
     * @return current validation, or empty result, if there is no validation.
     */
    @Nonnull
    Optional<Validation> getInProgressValidationForTarget(long targetId);

    /**
     * Request a discovery on a target. There may be only a single ongoing discovery
     * at a time for a given target. Attempting a discovery for a target with a current
     * ongoing discovery will return the current ongoing discovery.
     * Discoveries may take a long time and are performed asynchronously. This method
     * throws exceptions if the discovery can't initiate. If a problem
     * occurs after the discovery is initiated, an appropriate error will be enqueued
     * for later processing.
     *
     * Note: It is best to avoid holding unnecessary locks when calling this method, as it may
     *       block for an extended period while waiting for a probe operation permit.
     *
     * @param targetId The id of the target to discover.
     * @param discoveryType type of the discovery to trigger. Currently we only support FULL and
     *                      INCREMENTAL discovery
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
    Discovery startDiscovery(long targetId, DiscoveryType discoveryType)
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
     * @param id discovery id to retrieve.
     * @return discovery operation in progress, or empty result, if the validation is finished
     * or does not exist
     */
    @Nonnull
    Optional<Discovery> getInProgressDiscovery(long id);

    /**
     * Returns current running discovery for the given target.
     *
     * @param targetId target id
     * @param discoveryType type of the discovery to get current running discovery for
     * @return last discovery, or empty result, if no discoveries has happen
     */
    @Nonnull
    Optional<Discovery> getInProgressDiscoveryForTarget(long targetId, DiscoveryType discoveryType);

    /**
     * Returns last completed discovery for the specified target.
     *
     * @param targetId target id
     * @param discoveryType type of the discovery to get last completed discovery for
     * @return Last discovery or empty result if there were no discoveries.
     */
    @Nonnull
    Optional<Discovery> getLastDiscoveryForTarget(long targetId, DiscoveryType discoveryType);

    /**
     * Discover a target with the same contract as {@link #startDiscovery(long, DiscoveryType)},
     * with the following exceptions:
     * 1. If a discovery is already in progress, instead of returning the existing discovery,
     * a pending discovery will be added for the target.
     * 2. If the probe associated with the target is not currently connected, a pending discovery
     * will be added for the target.
     * When a target's discovery completes or its probe connects, the pending discovery will
     * be removed and a new discovery will be initiated for the associated target.
     *
     * @param targetId The id of the target to discover.
     * @param discoveryType type of the discovery to add. Currently we only support FULL and
     *                      INCREMENTAL discovery
     * @return An {@link Optional<Discovery>}. If there was no in progress discovery
     * for the target and the target's probe is connected, a new discovery will be initiated.
     * If there was an in progress discovery for the target or the target's probe is disconnected,
     * returns {@link Optional#empty()}.
     * @throws TargetNotFoundException When the requested target cannot be found.
     * @throws CommunicationException When the external probe cannot be reached.
     * @throws InterruptedException when the attempt to send a request to the probe is interrupted.
     */
    @Nonnull
    Optional<Discovery> addPendingDiscovery(long targetId, DiscoveryType discoveryType)
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
     * Secondary targets are targets that have discovered the destination entity in the case of a
     * cross-target move (where the destination was not discovered by the same target that
     * discovered the source entity).
     *
     * @param actionExecutionDTO action execution DTO to execute the action. OID must be set
     *          within this DTO.
     * @param targetId The id of the target containing the entities for the action.
     * @param secondaryTargetId the secondary target involved in this action, or null if no secondary
     *                          target is involved
     * @param controlAffectedEntities A set of entities directly affected by this action
     * @return The {@link Action} requested for the target.
     * @throws TargetNotFoundException When the requested target is not found.
     * @throws ProbeException When the probe corresponding to the target is not connected.
     * @throws CommunicationException If there is an error sending the request to the probe.
     * @throws InterruptedException If there is an interrupt while sending the request to the
     * probe.
     */
    Action requestActions(@Nonnull ActionExecutionDTO actionExecutionDTO,
                          long targetId,
                          @Nullable Long secondaryTargetId,
                          @Nonnull Set<Long> controlAffectedEntities)
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
     * Notify the {@link OperationManager} that a {@link Operation} completed
     * with a response returned by the probe.
     *
     * @param operation The {@link Operation} that completed.
     * @param message The message from the probe containing the response.
     * @return a Future representing pending completion of the task
     */
    Future<?> notifyDiscoveryResult(@Nonnull Discovery operation,
                                           @Nonnull DiscoveryResponse message);
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

    /**
     * Sends action approval request to a probe.
     *
     * @param targetId target to send request to
     * @param requests actions to request approval for
     * @param callback callback to receive operation result
     * @return action approval operation
     * @throws TargetNotFoundException if target not found by requested target id
     * @throws InterruptedException if current thread is interrupted
     * @throws ProbeException if the probe corresponding to the target is not connected.
     * @throws CommunicationException if there is an error sending the request to the probe.
     */
    @Nonnull
    ActionApproval approveActions(long targetId, @Nonnull Collection<ActionExecutionDTO> requests,
            @Nonnull OperationCallback<ActionApprovalResponse> callback)
            throws TargetNotFoundException, InterruptedException, ProbeException,
            CommunicationException;

    /**
     * Sends request to retrieve action states from external action approval backend.
     *
     * @param targetId target to send request to
     * @param request actions OIDs to request states for
     * @param callback callback to receive operation result
     * @return operation reflecting operation
     * @throws TargetNotFoundException if target not found by requested target id
     * @throws InterruptedException if current thread is interrupted
     * @throws ProbeException if the probe corresponding to the target is not connected.
     * @throws CommunicationException if there is an error sending the request to the probe.
     */
    @Nonnull
    GetActionState getExternalActionState(long targetId, @Nonnull Collection<Long> request,
            @Nonnull OperationCallback<GetActionStateResponse> callback)
            throws TargetNotFoundException, InterruptedException, ProbeException,
            CommunicationException;

    /**
     * Sends request to update action states on external action approval backend.
     *
     * @param targetId target to send request to
     * @param request actions states to send
     * @param callback callback to receive operation result
     * @return operation reflecting operation
     * @throws TargetNotFoundException if target not found by requested target id
     * @throws InterruptedException if current thread is interrupted
     * @throws ProbeException if the probe corresponding to the target is not connected.
     * @throws CommunicationException if there is an error sending the request to the probe.
     */
    @Nonnull
    ActionUpdateState updateExternalAction(long targetId,
            @Nonnull Collection<ActionResponse> request,
            @Nonnull OperationCallback<ActionErrorsResponse> callback)
            throws TargetNotFoundException, InterruptedException, ProbeException,
            CommunicationException;

    /**
     * Operation callback to receive success and failure for a requested operation.
     *
     * @param <T> type of operation execution result
     */
    interface OperationCallback<T> {
        /**
         * Method to be called when operation is finished successfully (response has been received
         * from a probe).
         *
         * @param response response
         */
        void onSuccess(@Nonnull T response);

        /**
         * Method to be called when operation failed to execute (response has not been received
         * from a probe).
         *
         * @param error error description
         */
        void onFailure(@Nonnull String error);
    }
}

package com.vmturbo.topology.processor.communication;

import java.time.Clock;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionApprovalRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionAuditRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionUpdateStateRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.GetActionStateRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.SetProperties;
import com.vmturbo.platform.sdk.common.MediationMessage.TargetUpdateRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ValidationRequest;
import com.vmturbo.topology.processor.operation.IOperationMessageHandler;
import com.vmturbo.topology.processor.operation.action.Action;
import com.vmturbo.topology.processor.operation.actionapproval.ActionApproval;
import com.vmturbo.topology.processor.operation.actionapproval.ActionUpdateState;
import com.vmturbo.topology.processor.operation.actionapproval.GetActionState;
import com.vmturbo.topology.processor.operation.actionaudit.ActionAudit;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.targets.Target;

/**
 * General interface of the remote mediation. All calls occur asynchronously and should return a
 * thread-safe object that can be used to track the state of the associated operation.
 */
public interface RemoteMediation {

    /**
     * Returns connected probes.
     *
     * @return set of all the probes that are currently connected.
     */
    Set<ProbeInfo> getConnectedProbes();

    /**
     * Sends discovery request. Method returns after request is sent. Result of the request
     * processing is reported to {@code responseHandler}.
     *
     * @param target target to perform request on
     * @param discoveryRequest discovery request data
     * @param responseHandler handler to accept discovery responses.
     * @return the unique mediation message id for this discovery request
     * @throws ProbeException if probe requested does not exist.
     * @throws CommunicationException if some communication error occurred.
     * @throws InterruptedException if thread is interrupted while sending request.
     */
    int sendDiscoveryRequest(@Nonnull Target target,
                              @Nonnull DiscoveryRequest discoveryRequest,
                    @Nonnull IOperationMessageHandler<Discovery> responseHandler)
                    throws ProbeException, CommunicationException, InterruptedException;

    /**
     * Sends validation request. Method returns after request is sent. Result of the request
     * processing is reported to {@code responseHandler}.
     *
     * @param target target to perform request on
     * @param validationRequest validation request data
     * @param responseHandler handler to accept validation responses.
     * @throws ProbeException if probe requested does not exist.
     * @throws CommunicationException if some communication error occurred
     * @throws InterruptedException if thread is interrupted while sending request.
     */
    void sendValidationRequest(@Nonnull Target target,
                    @Nonnull ValidationRequest validationRequest,
                    @Nonnull IOperationMessageHandler<Validation> responseHandler)
                    throws InterruptedException, ProbeException, CommunicationException;

    /**
     * Sends action request. Method returns after request is sent. Result of the request
     * processing is reported to {@code responseHandler}.
     *
     * @param target target to perform request on
     * @param actionRequest action request data
     * @param actionMessageHandler handler to accept action responses.
     * @throws ProbeException if probe requested does not exist.
     * @throws CommunicationException if some communication error occurred
     * @throws InterruptedException if thread is interrupted while sending request.
     */
    void sendActionRequest(@Nonnull Target target,
                           @Nonnull ActionRequest actionRequest,
                           @Nonnull IOperationMessageHandler<Action> actionMessageHandler)
            throws InterruptedException, ProbeException, CommunicationException;

    /**
     * Sends action approval request to an external action approval backend.
     *
     * @param target target to send message to
     * @param actionApprovalRequest request to send
     * @param messageHandler handler to receive all the messages received for this request
     * @throws InterruptedException if current thread has been interrupted
     * @throws ProbeException if probe requested does not exist
     * @throws CommunicationException if some communication error occurred
     */
    void sendActionApprovalsRequest(@Nonnull Target target,
            @Nonnull ActionApprovalRequest actionApprovalRequest,
            @Nonnull IOperationMessageHandler<ActionApproval> messageHandler)
            throws InterruptedException, ProbeException, CommunicationException;

    /**
     * Sends action state updates to external action approval backend.
     *
     * @param target target to send message to
     * @param actionUpdateStateRequest request to send
     * @param messageHandler handler to receive all the messages received for this request
     * @throws InterruptedException if current thread has been interrupted
     * @throws ProbeException if probe requested does not exist
     * @throws CommunicationException if some communication error occurred
     */
    void sendActionUpdateStateRequest(@Nonnull Target target,
            @Nonnull ActionUpdateStateRequest actionUpdateStateRequest,
            @Nonnull IOperationMessageHandler<ActionUpdateState> messageHandler)
            throws InterruptedException, ProbeException, CommunicationException;

    /**
     * Requests action state updates from external action approval backend.
     *
     * @param target target to send message to
     * @param getActionStateRequest request to send
     * @param messageHandler handler to receive all the messages received for this request
     * @throws InterruptedException if current thread has been interrupted
     * @throws ProbeException if probe requested does not exist
     * @throws CommunicationException if some communication error occurred
     */
    void sendGetActionStatesRequest(@Nonnull Target target,
            @Nonnull GetActionStateRequest getActionStateRequest,
            @Nonnull IOperationMessageHandler<GetActionState> messageHandler)
            throws InterruptedException, ProbeException, CommunicationException;

    /**
     * Sends action events for audit on the remote audit backend.
     *
     * @param target target to send message to
     * @param actionAuditRequest request to send
     * @param messageHandler handler to receive all the messages received for this request
     * @throws InterruptedException if current thread has been interrupted
     * @throws ProbeException if probe requested does not exist
     * @throws CommunicationException if some communication error occurred
     */
    void sendActionAuditRequest(@Nonnull Target target,
            @Nonnull ActionAuditRequest actionAuditRequest,
            @Nonnull IOperationMessageHandler<ActionAudit> messageHandler)
            throws InterruptedException, ProbeException, CommunicationException;

    /**
     * Sends a "set-properties" request.  No response is expected to this request.
     *
     * @param probeId probe to perform the request on.
     * @param setProperties the request.
     * @throws InterruptedException thread interrupted.
     * @throws ProbeException probe does not exist.
     * @throws CommunicationException communication error.
     */
    void sendSetPropertiesRequest(long probeId, @Nonnull SetProperties setProperties)
            throws InterruptedException, ProbeException, CommunicationException;

    /**
     * Remove message handlers and notify mediation clients about target removal.
     *
     * @param probeId probe identifier
     * @param targetId target identifier
     * @param request request to send to remote containers
     * @throws ProbeException if probe requested does not exist.
     * @throws CommunicationException if some communication error occurred
     * @throws InterruptedException if thread is interrupted while sending request.
     */
    void handleTargetRemoval(long probeId, long targetId, @Nonnull TargetUpdateRequest request)
                    throws CommunicationException, InterruptedException, ProbeException;

    /**
     * Check for expired operation handlers, expiring and removing any that have
     * exceeded their timeout. Returns the number of message handlers retained after
     * expiring handlers.
     */
    void checkForExpiredHandlers();

    /**
     * Returns clock to use for message expiration detection.
     *
     * @return clock to use
     */
    Clock getMessageHandlerExpirationClock();
}

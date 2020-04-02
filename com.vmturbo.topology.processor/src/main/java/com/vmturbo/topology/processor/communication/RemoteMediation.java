package com.vmturbo.topology.processor.communication;

import java.time.Clock;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.SetProperties;
import com.vmturbo.platform.sdk.common.MediationMessage.ValidationRequest;
import com.vmturbo.topology.processor.operation.IOperationMessageHandler;
import com.vmturbo.topology.processor.operation.Operation;
import com.vmturbo.topology.processor.operation.action.Action;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probes.ProbeException;

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
     * @param probeId probe to perform request on
     * @param targetId target to discover
     * @param discoveryRequest discovery request data
     * @param responseHandler handler to accept discovery responses.
     * @throws ProbeException if probe requested does not exist.
     * @throws CommunicationException if some communication error occurred.
     * @throws InterruptedException if thread is interrupted while sending request.
     */
    void sendDiscoveryRequest(long probeId, long targetId,
                              @Nonnull DiscoveryRequest discoveryRequest,
                    @Nonnull final IOperationMessageHandler<Discovery> responseHandler)
                    throws ProbeException, CommunicationException, InterruptedException;

    /**
     * Sends validation request. Method returns after request is sent. Result of the request
     * processing is reported to {@code responseHandler}.
     *
     * @param probeId probe to perform request on
     * @param validationRequest validation request data
     * @param responseHandler handler to accept validation responses.
     * @throws ProbeException if probe requested does not exist.
     * @throws CommunicationException if some communication error occurred
     * @throws InterruptedException if thread is interrupted while sending request.
     */
    void sendValidationRequest(final long probeId,
                    @Nonnull final ValidationRequest validationRequest,
                    @Nonnull final IOperationMessageHandler<Validation> responseHandler)
                    throws InterruptedException, ProbeException, CommunicationException;

    /**
     * Sends action request. Method returns after request is sent. Result of the request
     * processing is reported to {@code responseHandler}.
     *
     * @param probeId probe to perform request on
     * @param actionRequest action request data
     * @param actionMessageHandler handler to accept action responses.
     * @throws ProbeException if probe requested does not exist.
     * @throws CommunicationException if some communication error occurred
     * @throws InterruptedException if thread is interrupted while sending request.
     */
    void sendActionRequest(final long probeId,
                           @Nonnull final ActionRequest actionRequest,
                           @Nonnull final IOperationMessageHandler<Action> actionMessageHandler)
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
     * Remove message handlers matching the {@code shouldRemoveFilter}. Removing these handlers
     * will cause messages targeting these handlers to be discarded. Probes do not support
     * aborting an operation, so removing the handler can be used to discard the results
     * of those operations.
     *
     * @param shouldRemoveFilter A predicate method that checks whether a particular message
     *                          handler should be removed. Handlers passed to this method
     *                          that return true will be removed.
     */
    void removeMessageHandlers(@Nonnull final Predicate<Operation> shouldRemoveFilter);

    /**
     * Check for expired operation handlers, expiring and removing any that have
     * exceeded their timeout. Returns the number of message handlers retained after
     * expiring handlers.
     */
    int checkForExpiredHandlers();

    /**
     * Returns clock to use for message expiration detection.
     *
     * @return clock to use
     */
    Clock getMessageHandlerExpirationClock();
}

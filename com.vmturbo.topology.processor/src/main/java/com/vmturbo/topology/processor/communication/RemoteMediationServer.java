package com.vmturbo.topology.processor.communication;

import java.time.Clock;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.InitializationContent;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.SetProperties;
import com.vmturbo.platform.sdk.common.MediationMessage.ValidationRequest;
import com.vmturbo.sdk.server.common.SdkWebsocketServerTransportHandler.TransportRegistrar;
import com.vmturbo.topology.processor.communication.ExpiringMessageHandler.HandlerStatus;
import com.vmturbo.topology.processor.operation.IOperationMessageHandler;
import com.vmturbo.topology.processor.operation.Operation;
import com.vmturbo.topology.processor.operation.action.Action;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeRpcService;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * Remote mediation (SDK) server. This class provides routines to interact with remote probes.
 */
public class RemoteMediationServer implements TransportRegistrar, RemoteMediation {

    private final Logger logger = LogManager.getLogger();

    public RemoteMediationServer(@Nonnull final ProbeStore probeStore) {
        Objects.requireNonNull(probeStore);
        this.probeStore = probeStore;
        logger.info("Remote mediation server started");
        PassiveAdjustableExpiringMap<Integer, MessageAnticipator> expiringHandlerMap =
                        new PassiveAdjustableExpiringMap<>();
        messageHandlers = Collections.synchronizedMap(expiringHandlerMap);
        messageHandlerExpirationClock = expiringHandlerMap.getExpirationClock();
    }

    private final ProbeStore probeStore;

    // counter used to store the messageID that we need
    // to use when sending out a request
    // note: when the counter overflow, a negative number will be used
    private final AtomicInteger messageIDCounter = new AtomicInteger(0);

    /**
     * A map of MessageID -> MessageHandler.
     * Certain requests to remote probes may take a very long time to complete.
     * In order to differentiate when the lack of a response is due to a long-running request
     * as opposed to the client disappearing, clients periodically send a keep-alive
     * as they assemble their response. If a keep-alive is not sent in time, the handler will
     * expire and will be removed from the map the next time it is accessed.
     */
    private final Map<Integer, MessageAnticipator> messageHandlers;

    /**
     * The clock used by the map of messageHandlers to expire its entries.
     */
    private final Clock messageHandlerExpirationClock;

    /**
     * Get the next message id to be used. Message ID can be a negative value.
     * @return The next message id to use when sending a message.
     */
    private int nextMessageId() {
        return messageIDCounter.getAndIncrement();
    }

    @Override
    public void registerTransport(ContainerInfo containerInfo,
                    ITransport<MediationServerMessage, MediationClientMessage> serverEndpoint) {
        logger.info("Registration message received from " + serverEndpoint);

        // Register the transport handlers before registering the probes, so that
        // we still receive connection errors that happen while the probe store is saving
        // the probe info.
        //
        // Note: This should be safe, because we expect the probe to be resilient to receiving a
        // request to remove a probe while it's still processing that probe's addition.
        registerTransportHandlers(serverEndpoint);

        for (final ProbeInfo probeInfo : containerInfo.getProbesList()) {
            try {
                probeStore.registerNewProbe(probeInfo, serverEndpoint);
                logger.info("Transport has been registered");
            } catch (ProbeException e) {
                logger.error("Probe " + probeInfo.getProbeType() + " from " + serverEndpoint
                                + " failed to register", e);
            }
        }
    }

    /**
     * This method is implemented only for backward-compatibility with OpsManager.
     * Initialization of probe properties on a newly-registered mediation client is not done
     * using an initialization message anymore.  Instead, it is done by listener coded which
     * is found in {@link ProbeRpcService#ProbeRpcService}.
     *
     * @return empty {@link InitializationContent} message.
     */
    @Override
    public InitializationContent getInitializationContent() {
        return
            InitializationContent.newBuilder()
                .setProbeProperties(SetProperties.getDefaultInstance())
                .build();
    }

    private void registerTransportHandlers(
                    ITransport<MediationServerMessage, MediationClientMessage> serverEndpoint) {
        serverEndpoint.addEventHandler(new ITransport.EventHandler<MediationClientMessage>() {

            @Override
            public void onClose() {
                processContainerClose(serverEndpoint);
            }

            @Override
            public void onMessage(MediationClientMessage message) {
                onTransportMessage(serverEndpoint, message);
            }

        });
    }

    /**
     * Called when a transport message is received. Look up a handler and if one exists,
     * pass it the message. If none exists, the message is discarded. If the handler
     * has nothing else to do, it is discarded after the message is handled.
     *
     * @param message The message to handle
     * @param serverEndpoint endpoint the message arrived at
     */
    void onTransportMessage(
            @Nonnull final ITransport<MediationServerMessage, MediationClientMessage> serverEndpoint,
            @Nonnull MediationClientMessage message) {
        final int messageId = message.getMessageID();
        final MessageAnticipator holder = messageHandlers.get(message.getMessageID());
        if (holder != null) {
            HandlerStatus status = holder.getMessageHandler().onReceive(message);
            if (status == HandlerStatus.COMPLETE) {
                messageHandlers.remove(messageId);
            }
        } else {
            logger.info("No handler found for message with id {}. Aborting the task on {}",
                    messageId, serverEndpoint);
            try {
                serverEndpoint.send(MediationServerMessage.newBuilder()
                        .setMessageID(nextMessageId())
                        .setInterruptOperation(messageId)
                        .build());
            } catch (InterruptedException | CommunicationException e) {
                logger.warn("Could not send interruption message for operation " + messageId +
                        " through " + serverEndpoint, e);
            }
        }
    }

    @Override
    public Set<ProbeInfo> getConnectedProbes() {
        return ImmutableSet.copyOf(probeStore.getProbes().values().stream()
                .filter(probeInfo -> {
                    Optional<Long> probeId = probeStore.getProbeIdForType(probeInfo.getProbeType());
                    return probeId.isPresent() && probeStore.isProbeConnected(probeId.get());
                })
                .collect(Collectors.toSet()));
    }

    /**
     * When a container is closed, remove all probe types for that container from the probe type
     * map.
     *
     * @param endpoint endpoint, representing communication link with the closed container.
     */
    protected void processContainerClose(
                    ITransport<MediationServerMessage, MediationClientMessage> endpoint) {
        logger.info(() -> "container closed: " + endpoint
                        + ". Unregistering it from probe storage...");
        probeStore.removeTransport(endpoint);

        synchronized(messageHandlers) {
            final Iterator<MessageAnticipator> iter = messageHandlers.values().iterator();
            while (iter.hasNext()) {
                final MessageAnticipator holder = iter.next();
                if (holder.getTransport().equals(endpoint)) {
                    holder.getMessageHandler().onTransportClose();
                    iter.remove();
                }
            }
        }
        logger.info("container {} closed", endpoint);
    }

    private void sendMessageToProbe(long probeId,
                                    MediationServerMessage message,
                                    @Nullable IOperationMessageHandler<?> responseHandler)
            throws CommunicationException, InterruptedException, ProbeException {
        boolean success = false;
        try {
            // Use first available transport.
            final ITransport<MediationServerMessage, MediationClientMessage> transport =
                    probeStore.getTransport(probeId).iterator().next();
            // Register the handler before sending the message so there is no gap where there is
            // no registered handler for an outgoing message. Of course this means cleanup is
            // necessary!
            if (responseHandler != null) {
                messageHandlers.put(
                    message.getMessageID(),
                    new MessageAnticipator(transport, responseHandler));
            }
            transport.send(message);
            success = true;
        } finally {
            if (!success) {
                messageHandlers.remove(message.getMessageID());
            }
        }
    }

    @Override
    public void sendDiscoveryRequest(final long probeId,
                                     @Nonnull final DiscoveryRequest discoveryRequest,
                                     @Nonnull final IOperationMessageHandler<Discovery>
                                             responseHandler)
        throws ProbeException, CommunicationException, InterruptedException {

        final MediationServerMessage message = MediationServerMessage.newBuilder()
                .setMessageID(nextMessageId())
                .setDiscoveryRequest(discoveryRequest).build();

        sendMessageToProbe(probeId, message, responseHandler);
    }

    @Override
    public void sendValidationRequest(final long probeId,
            @Nonnull final ValidationRequest validationRequest,
            @Nonnull final IOperationMessageHandler<Validation> validationMessageHandler)
            throws InterruptedException, ProbeException, CommunicationException {
        final MediationServerMessage message = MediationServerMessage.newBuilder()
                .setMessageID(nextMessageId())
                .setValidationRequest(validationRequest).build();

        sendMessageToProbe(probeId, message, validationMessageHandler);
    }

    @Override
    public void sendActionRequest(final long probeId,
            @Nonnull final ActionRequest actionRequest,
            @Nonnull final IOperationMessageHandler<Action> actionMessageHandler)
            throws InterruptedException, ProbeException, CommunicationException {
        final MediationServerMessage message = MediationServerMessage.newBuilder()
                .setMessageID(nextMessageId())
                .setActionRequest(actionRequest).build();

        sendMessageToProbe(probeId, message, actionMessageHandler);
    }

    @Override
    public void sendSetPropertiesRequest(long probeId, @Nonnull SetProperties setProperties)
            throws InterruptedException, ProbeException, CommunicationException {
        final MediationServerMessage message =
            MediationServerMessage.newBuilder()
                .setMessageID(nextMessageId())
                .setProperties(setProperties)
                .build();

        sendMessageToProbe(probeId, message, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeMessageHandlers(@Nonnull final Predicate<Operation> shouldRemoveFilter) {
        synchronized (messageHandlers) {
            messageHandlers.entrySet().removeIf(entry -> {
                final Operation operation = entry.getValue().getMessageHandler().getOperation();
                return shouldRemoveFilter.test(operation);
            });
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int checkForExpiredHandlers() {
        // PassiveAdjustableExpiringMap will check for expiration on all its entries
        // whenever any operation is performed on it, including size()
        return messageHandlers.size();
    }

    /**
     * Get the clock used for message handler expiration.
     *
     * @return The clock used for message handler expiration.
     */
    @Override
    public Clock getMessageHandlerExpirationClock() {
        return messageHandlerExpirationClock;
    }

    public Logger getLogger() {
        return logger;
    }

    /**
     * Object, representing a structure, which will be used on any messages appear.
     */
    private class MessageAnticipator implements ExpiringValue {
        private final ITransport<?, ?> transport;
        private final IOperationMessageHandler<?> messageHandler;

        MessageAnticipator(ITransport<?, ?> transport, IOperationMessageHandler<?> messageHandler) {
            this.transport = transport;
            this.messageHandler = messageHandler;
        }

        public ITransport<?, ?> getTransport() {
            return transport;
        }

        public IOperationMessageHandler<?> getMessageHandler() {
            return messageHandler;
        }

        @Override
        public long expirationTime() {
            return messageHandler.expirationTime();
        }

        @Override
        public void onExpiration() {
            messageHandler.onExpiration();
        }
    }
}

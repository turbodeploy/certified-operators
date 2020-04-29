package com.vmturbo.topology.processor.communication;

import java.time.Clock;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import com.vmturbo.platform.sdk.common.MediationMessage.TargetUpdateRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ValidationRequest;
import com.vmturbo.sdk.server.common.SdkWebsocketServerTransportHandler.TransportRegistrar;
import com.vmturbo.topology.processor.communication.ExpiringMessageHandler.HandlerStatus;
import com.vmturbo.topology.processor.operation.IOperationMessageHandler;
import com.vmturbo.topology.processor.operation.Operation;
import com.vmturbo.topology.processor.operation.action.Action;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probeproperties.ProbePropertyStore;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStoreException;

/**
 * Remote mediation (SDK) server. This class provides routines to interact with remote probes.
 */
public class RemoteMediationServer implements TransportRegistrar, RemoteMediation {

    private final Logger logger = LogManager.getLogger();

    private final ProbePropertyStore probePropertyStore;

    private final ProbeStore probeStore;

    private final ProbeContainerChooser containerChooser;
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
     * Construct the instance.
     *
     * @param probeStore probes registry
     * @param probePropertyStore probe and target-specific properties registry
     * @param containerChooser it will route the requests to the right transport
     */
    public RemoteMediationServer(@Nonnull final ProbeStore probeStore,
                                 @Nonnull ProbePropertyStore probePropertyStore,
                                 @Nonnull ProbeContainerChooser containerChooser) {
        Objects.requireNonNull(probeStore);
        this.probeStore = probeStore;
        this.probePropertyStore = probePropertyStore;
        logger.info("Remote mediation server started");
        PassiveAdjustableExpiringMap<Integer, MessageAnticipator> expiringHandlerMap =
                        new PassiveAdjustableExpiringMap<>();
        messageHandlers = Collections.synchronizedMap(expiringHandlerMap);
        messageHandlerExpirationClock = expiringHandlerMap.getExpirationClock();
        this.containerChooser = Objects.requireNonNull(containerChooser);
    }

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
        containerInfo.getPersistentTargetIdsList()
            .forEach(targetId -> containerChooser.assignTargetToTransport(serverEndpoint, targetId));
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

    @Override
    @Nonnull
    public InitializationContent getInitializationContent(@Nonnull ContainerInfo containerInfo) {
        // a good deal of TP logic already relies on no more than 1 registered probe per type
        Set<Long> probeIds = containerInfo.getProbesList().stream().map(ProbeInfo::getProbeType)
                        .map(type -> probeStore.getProbeIdForType(type))
                        .filter(Optional::isPresent).map(Optional::get).collect(Collectors.toSet());
        try {
            SetProperties initContent = probePropertyStore.buildSetPropertiesMessageForProbe(probeIds);
            logger.debug("Initializing probes {} with properties:\n{}", probeIds::toString,
                         initContent::toString);
            return InitializationContent.newBuilder().setProbeProperties(initContent).build();
        } catch (ProbeException | TargetStoreException e) {
            logger.warn("Failed to construct probe properties for " + probeIds, e);
            return InitializationContent.getDefaultInstance();
        }
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

    private void broadcastMessageToProbeInstances(long probeId,
                                             MediationServerMessage message)
        throws CommunicationException, InterruptedException, ProbeException {
            for (ITransport<MediationServerMessage, MediationClientMessage> transport
                : probeStore.getTransport(probeId)) {
                transport.send(message);
            }
    }

    private void sendMessageToProbe(@Nonnull final Target target,
                                    MediationServerMessage message,
                                    @Nullable IOperationMessageHandler<?> responseHandler)
            throws CommunicationException, InterruptedException, ProbeException {
        boolean success = false;
        try {
            final ITransport<MediationServerMessage, MediationClientMessage> transport =
                containerChooser.choose(target.getProbeId(),
                    target.getSerializedIdentifyingFields(), message);
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
    public int sendDiscoveryRequest(final Target target,
                                     @Nonnull final DiscoveryRequest discoveryRequest,
                                     @Nonnull final IOperationMessageHandler<Discovery>
                                             responseHandler)
        throws ProbeException, CommunicationException, InterruptedException {

        final int messageId = nextMessageId();
        final MediationServerMessage message = MediationServerMessage.newBuilder()
                .setMessageID(messageId)
                .setDiscoveryRequest(discoveryRequest).build();

        sendMessageToProbe(target, message, responseHandler);
        return messageId;
    }

    @Override
    public void sendValidationRequest(@Nonnull final Target target,
            @Nonnull final ValidationRequest validationRequest,
            @Nonnull final IOperationMessageHandler<Validation> validationMessageHandler)
            throws InterruptedException, ProbeException, CommunicationException {
        final MediationServerMessage message = MediationServerMessage.newBuilder()
                .setMessageID(nextMessageId())
                .setValidationRequest(validationRequest).build();

        sendMessageToProbe(target, message, validationMessageHandler);
    }

    @Override
    public void sendActionRequest(@Nonnull final Target target,
            @Nonnull final ActionRequest actionRequest,
            @Nonnull final IOperationMessageHandler<Action> actionMessageHandler)
            throws InterruptedException, ProbeException, CommunicationException {
        final MediationServerMessage message = MediationServerMessage.newBuilder()
                .setMessageID(nextMessageId())
                .setActionRequest(actionRequest).build();

        sendMessageToProbe(target, message, actionMessageHandler);
    }

    @Override
    public void sendSetPropertiesRequest(long probeId, @Nonnull SetProperties setProperties)
            throws InterruptedException, ProbeException, CommunicationException {
        final MediationServerMessage message =
            MediationServerMessage.newBuilder()
                .setMessageID(nextMessageId())
                .setProperties(setProperties)
                .build();

        broadcastMessageToProbeInstances(probeId, message);
    }

    @Override
    public void handleTargetRemoval(long probeId, long targetId,
                                    @Nonnull TargetUpdateRequest request)
                    throws CommunicationException, InterruptedException, ProbeException {
        synchronized (messageHandlers) {
            messageHandlers.entrySet().removeIf(entry -> {
                final Operation operation = entry.getValue().getMessageHandler().getOperation();
                return operation.getTargetId() == targetId;
            });
        }
        MediationServerMessage message =
                        MediationServerMessage.newBuilder()
                            .setMessageID(nextMessageId())
                            .setTargetUpdateRequest(request)
                            .build();
        broadcastMessageToProbeInstances(probeId, message);
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

    public InitializationContent getInitializationContent() {
        return InitializationContent.getDefaultInstance();
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

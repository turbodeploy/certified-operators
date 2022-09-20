package com.vmturbo.topology.processor.communication;

import java.time.Clock;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
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
import com.vmturbo.communication.chunking.MessageChunker;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.identity.exceptions.IdentifierConflictException;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.kvstore.KeyValueStoreOperationException;
import com.vmturbo.platform.common.dto.PlanExport.PlanExportDTO;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionApprovalRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionAuditRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionListRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionUpdateStateRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.GetActionStateRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.InitializationContent;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.PlanExportRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeTargetInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.SetProperties;
import com.vmturbo.platform.sdk.common.MediationMessage.TargetUpdateRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.ValidationRequest;
import com.vmturbo.sdk.server.common.SdkWebsocketServerTransportHandler.TransportRegistrar;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue.PropertyValueList;
import com.vmturbo.topology.processor.communication.ExpiringMessageHandler.HandlerStatus;
import com.vmturbo.topology.processor.operation.IOperationMessageHandler;
import com.vmturbo.topology.processor.operation.Operation;
import com.vmturbo.topology.processor.operation.action.Action;
import com.vmturbo.topology.processor.operation.action.ActionList;
import com.vmturbo.topology.processor.operation.actionapproval.ActionApproval;
import com.vmturbo.topology.processor.operation.actionapproval.ActionUpdateState;
import com.vmturbo.topology.processor.operation.actionapproval.GetActionState;
import com.vmturbo.topology.processor.operation.actionaudit.ActionAudit;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.planexport.PlanExport;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probeproperties.ProbePropertyStore;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.DuplicateTargetException;
import com.vmturbo.topology.processor.targets.InvalidTargetException;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.TargetStoreException;

/**
 * Remote mediation (SDK) server. This class provides routines to interact with remote probes.
 */
public class RemoteMediationServer implements TransportRegistrar, RemoteMediation {

    private final Logger logger = LogManager.getLogger();

    private final ProbePropertyStore probePropertyStore;

    protected final ProbeStore probeStore;

    protected final TargetStore targetStore;

    protected final ProbeContainerChooser containerChooser;
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
    protected final Map<Integer, MessageAnticipator> messageHandlers;

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
     * @param targetStore target store for targets.
     */
    public RemoteMediationServer(@Nonnull final ProbeStore probeStore,
                                 @Nonnull ProbePropertyStore probePropertyStore,
                                 @Nonnull ProbeContainerChooser containerChooser,
                                 @Nonnull final TargetStore targetStore) {
        Objects.requireNonNull(probeStore);
        this.probeStore = probeStore;
        this.probePropertyStore = probePropertyStore;
        logger.info("Remote mediation server started");
        PassiveAdjustableExpiringMap<Integer, MessageAnticipator> expiringHandlerMap =
                        new PassiveAdjustableExpiringMap<>();
        messageHandlers = Collections.synchronizedMap(expiringHandlerMap);
        messageHandlerExpirationClock = expiringHandlerMap.getExpirationClock();
        this.containerChooser = Objects.requireNonNull(containerChooser);
        this.targetStore = targetStore;
    }

    /**
     * Get the next message id to be used. Message ID can be a negative value.
     * @return The next message id to use when sending a message.
     */
    protected int nextMessageId() {
        return messageIDCounter.getAndIncrement();
    }

    @Override
    public void registerTransport(ContainerInfo containerInfo,
                    ITransport<MediationServerMessage, MediationClientMessage> serverEndpoint) {
        logger.info("Registration message received from {} with probes {}", serverEndpoint,
                containerInfo.getProbesList()
                        .stream()
                        .map(ProbeInfo::getProbeType)
                        .collect(Collectors.toList()));
        containerChooser.onTransportRegistered(containerInfo, serverEndpoint);

        for (final ProbeInfo probeInfo : containerInfo.getProbesList()) {
            try {
                probeStore.registerNewProbe(probeInfo, containerInfo, serverEndpoint);
                logger.info("Transport has been registered");
            } catch (ProbeException e) {
                logger.error("Probe " + probeInfo.getProbeType() + " from " + serverEndpoint
                                + " failed to register", e);
            } catch (KeyValueStoreOperationException e) {
                // we've encountered a runtime exception accessing consul.
                // Clean up before returning.
                logger.error("Error while attempting to register probes for transport {}."
                        + " Closing transport.", serverEndpoint);
                serverEndpoint.close();
                processContainerClose(serverEndpoint);
                throw e;
            }
        }

        // Register the transport handlers after registering the probes to avoid a race
        // condition where the transport is closed before it is registered with the probe
        // store, but after the handler is notified of closure.  This leads to a closed
        // transport being listed with the RemoteProbeStore.
        //
        // Note: This is safe, because we expect the serverEndpoint to play back any
        // queued messages when the first handler is registered.
        registerTransportHandlers(serverEndpoint);

        for (final ProbeInfo probeInfo : containerInfo.getProbesList()) {
            if (probeInfo.hasProbeTargetInfo()) {
                if (!FeatureFlags.ENABLE_TP_PROBE_SECURITY.isEnabled() && serverEndpoint.isExternal()) {
                    logger.warn("Probe security must be enabled for external probe to add target {}",
                            probeInfo.getProbeType() + "-" + probeInfo.getDisplayName());
                    continue;
                }
                logger.info("Attempting to add target for probe: "
                    + probeInfo.getProbeType() + " - " + probeInfo.getDisplayName());
                addProbeTarget(probeInfo);
            }
        }
    }

    /**
     * Add a Target to the Probe.
     *
     * @param probeInfo the probe info.
     */
    private void addProbeTarget(ProbeInfo probeInfo) {
        try {
            final Optional<Long> probeId = probeStore.getProbeIdForType(probeInfo.getProbeType());
            if (!probeId.isPresent()) {
                logger.error("Could not find probe {} for adding target", probeInfo.getProbeType());
                return;
            }
            targetStore.createOrUpdateExistingTarget(buildTargetSpec(probeInfo.getProbeTargetInfo(),
                probeId.get(), probeInfo.getProbeType()), true);
        } catch (InvalidTargetException | DuplicateTargetException | IdentityStoreException
                    | TargetNotFoundException | IdentifierConflictException e) {
            logger.error(e);
        }
    }

    /**
     * Creates a TargetSpec for adding the probe target.
     *
     * @param ProbeTargetInfo the probe info.
     * @param probeId the probe id.
     * @param probeType the probe type.
     * @return TargetSpec.
     */
    private TargetSpec buildTargetSpec(ProbeTargetInfo probeTargetInfo, long probeId, String probeType) {
        TargetSpec.Builder targetSpecBuilder = TargetSpec.newBuilder();
        targetSpecBuilder.setProbeId(probeId);
        probeTargetInfo.getInputValuesList().forEach(discAccountValue -> {
            targetSpecBuilder.addAccountValue(accountValueConverter(discAccountValue));
        });
        targetSpecBuilder.setLastEditingUser(probeType);
        if (probeTargetInfo.hasCommunicationBindingChannel()) {
            targetSpecBuilder.setCommunicationBindingChannel(probeTargetInfo.getCommunicationBindingChannel());
        }
        // Setting to read only for targets added through probes so users are not able to modify it from the ui
        // as any modification should come from probe target config changes.
        targetSpecBuilder.setEditable(false);
        return targetSpecBuilder.build();
    }

    /**
     * We have 2 protobuf definitions for AccountValue. One is opsmgr Discovery and one in XL TopologyProcessorDTO.
     * We need to convert the discovery one to TP one for target addition.
     *
     * @param discoveryAccountValue AccountValue from discovery.
     * @return AccountValue for TP.
     */
    private AccountValue accountValueConverter(com.vmturbo.platform.common.dto.Discovery.AccountValue discoveryAccountValue) {
        AccountValue.Builder tpAccountValue = AccountValue.newBuilder();
        tpAccountValue.setKey(discoveryAccountValue.getKey());
        tpAccountValue.setStringValue(discoveryAccountValue.getStringValue());
        discoveryAccountValue.getGroupScopePropertyValuesList().forEach(propValList -> {
            tpAccountValue.addGroupScopePropertyValues(PropertyValueList.newBuilder()
                .addAllValue(propValList.getValueList()));
        });
        return tpAccountValue.build();
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
        try {
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
        } catch (IllegalStateException e) {
            // This can occur if the endpoint is closed immediately after it connects to the server.
            // It means the endpoint was already closed when we added the handler, so just process
            // it like any other close.
            processContainerClose(serverEndpoint);
        } catch (RuntimeException e) {
            logger.error("Exception while adding event handler to transport " + serverEndpoint,
                    e);
            processContainerClose(serverEndpoint);
            throw e;
        }
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
            logger.info("No handler found for message {} for operation {}. Aborting the task on {}",
                    message.getMediationClientMessageCase(), messageId, serverEndpoint);
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
        containerChooser.onTransportRemoved(endpoint);

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
                containerChooser.choose(target, message);
            // Register the handler before sending the message so there is no gap where there is
            // no registered handler for an outgoing message. Of course this means cleanup is
            // necessary!
            if (responseHandler != null) {
                messageHandlers.put(
                    message.getMessageID(),
                    new MessageAnticipator(transport, responseHandler));
            }
            getLogger().trace("Sending message to {} through {}", target::getNoSecretDto,
                () -> transport);
            transport.send(message);
            success = true;
        } finally {
            if (!success) {
                messageHandlers.remove(message.getMessageID());
            }
        }
    }

    private void sendChunkedMessageToProbe(@Nonnull final Target target,
                                           List<MediationServerMessage> chunks,
                                           @Nullable IOperationMessageHandler<?> responseHandler)
        throws CommunicationException, InterruptedException, ProbeException {
        if (chunks.size() < 1) {
            throw new CommunicationException("Attempted to send empty chunked message");
        }

        boolean success = false;
        final MediationServerMessage firstChunk = chunks.get(0);
        final int messageId = firstChunk.getMessageID();
        try {
            final ITransport<MediationServerMessage, MediationClientMessage> transport =
                containerChooser.choose(target, firstChunk);

            // Register the handler before sending the first message so there is no gap where there
            // is no registered handler for an outgoing message. Of course this means cleanup is
            // necessary!
            if (responseHandler != null) {
                messageHandlers.put(messageId, new MessageAnticipator(transport, responseHandler));
            }
            for (int chunkNumber = 0; chunkNumber < chunks.size(); chunkNumber++) {
                final int oneBasedChunkNumber = chunkNumber + 1;
                getLogger().trace("Sending chunk {}/{} to {} through {}",
                    () -> oneBasedChunkNumber, chunks::size, target::getNoSecretDto, () -> transport);
                transport.send(chunks.get(chunkNumber));
            }
            success = true;
        } finally {
            if (!success) {
                messageHandlers.remove(messageId);
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
    public void sendPlanExportRequest(@Nonnull final Target target,
                                      @Nonnull final PlanExportRequest exportRequest,
                                      @Nonnull final IOperationMessageHandler<PlanExport> planExportMessageHandler)
        throws InterruptedException, ProbeException, CommunicationException {
        int messageId = nextMessageId();

        // Note: optional fields are only sent in the first chunk, but messageId is optional
        // and we need it in every message sent. So, we can't chunk the MediationServerMessage.
        //
        // Further, the message chunker only looks one level deep, and what we need chunked due to
        // potential size is the planData. So, chunk that and build MediationServerMessages from
        // each chunk.

        final MessageChunker<PlanExportDTO> planDataChunker =
            new MessageChunker<>(PlanExportDTO.getDescriptor(), PlanExportDTO::newBuilder);

        final List<MediationServerMessage> chunks = planDataChunker.chunk(exportRequest.getPlanData())
            .stream()
            .map(planDataChunk -> MediationServerMessage.newBuilder()
                .setMessageID(messageId)
                .setPlanExportRequest(exportRequest.toBuilder().setPlanData(planDataChunk))
                .build())
            .collect(Collectors.toList());

        sendChunkedMessageToProbe(target, chunks, planExportMessageHandler);
    }

    @Override
    public void sendActionListRequest(
            @Nonnull final Target target,
            @Nonnull final ActionListRequest actionListRequest,
            @Nonnull final IOperationMessageHandler<ActionList> actionMessageHandler)
            throws InterruptedException, ProbeException, CommunicationException {
        final MediationServerMessage message = MediationServerMessage.newBuilder()
                .setMessageID(nextMessageId())
                .setActionListRequest(actionListRequest).build();

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
    public void sendActionApprovalsRequest(@Nonnull Target target,
            @Nonnull ActionApprovalRequest actionApprovalRequest,
            @Nonnull IOperationMessageHandler<ActionApproval> messageHandler)
            throws InterruptedException, ProbeException, CommunicationException {
        final int messageId = nextMessageId();
        final MediationServerMessage message = MediationServerMessage.newBuilder()
                .setMessageID(messageId)
                .setActionApproval(actionApprovalRequest).build();

        sendMessageToProbe(target, message, messageHandler);
    }

    @Override
    public void sendActionUpdateStateRequest(@Nonnull Target target,
            @Nonnull ActionUpdateStateRequest actionUpdateStateRequest,
            @Nonnull IOperationMessageHandler<ActionUpdateState> messageHandler)
            throws InterruptedException, ProbeException, CommunicationException {
        final int messageId = nextMessageId();
        final MediationServerMessage message = MediationServerMessage.newBuilder()
                .setMessageID(messageId)
                .setActionUpdateState(actionUpdateStateRequest).build();
        sendMessageToProbe(target, message, messageHandler);
    }

    @Override
    public void sendGetActionStatesRequest(@Nonnull Target target,
            @Nonnull GetActionStateRequest getActionStateRequest,
            @Nonnull IOperationMessageHandler<GetActionState> messageHandler)
            throws InterruptedException, ProbeException, CommunicationException {
        final int messageId = nextMessageId();
        final MediationServerMessage message = MediationServerMessage.newBuilder()
                .setMessageID(messageId)
                .setGetActionState(getActionStateRequest).build();
        sendMessageToProbe(target, message, messageHandler);
    }

    @Override
    public void sendActionAuditRequest(@Nonnull Target target,
            @Nonnull ActionAuditRequest actionAuditRequest,
            @Nonnull IOperationMessageHandler<ActionAudit> messageHandler)
            throws InterruptedException, ProbeException, CommunicationException {
        final int messageId = nextMessageId();
        final MediationServerMessage message = MediationServerMessage.newBuilder().setMessageID(
                messageId).setActionAudit(actionAuditRequest).build();
        sendMessageToProbe(target, message, messageHandler);
    }

    @Override
    public void handleTargetRemoval(@Nonnull Target target,
                                    @Nonnull TargetUpdateRequest request)
                    throws CommunicationException, InterruptedException, ProbeException {
        synchronized (messageHandlers) {
            messageHandlers.entrySet().removeIf(entry -> {
                final Operation operation = entry.getValue().getMessageHandler().getOperation();
                return operation.getTargetId() == target.getId();
            });
        }
        containerChooser.onTargetRemoved(target);
        MediationServerMessage message =
                        MediationServerMessage.newBuilder()
                            .setMessageID(nextMessageId())
                            .setTargetUpdateRequest(request)
                            .build();
        //todo: test delete target, multiple transports
        broadcastMessageToProbeInstances(target.getProbeId(), message);
    }

    @Override
    public void handleTargetAddition(Target target) throws ProbeException {
        containerChooser.onTargetAdded(target);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void checkForExpiredHandlers() {
        // PassiveAdjustableExpiringMap will check for expiration on all its entries
        // whenever any operation is performed on it, including size()
        messageHandlers.size();
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
    protected class MessageAnticipator implements ExpiringValue {
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

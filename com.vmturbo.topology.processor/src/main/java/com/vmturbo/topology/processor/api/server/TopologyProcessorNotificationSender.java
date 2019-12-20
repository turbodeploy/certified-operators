package com.vmturbo.topology.processor.api.server;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.Data;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.End;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.Start;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.MessageChunker;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.ActionsLost;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TopologyProcessorNotification;
import com.vmturbo.topology.processor.operation.Operation;
import com.vmturbo.topology.processor.operation.OperationListener;
import com.vmturbo.topology.processor.operation.action.Action;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probes.ProbeStoreListener;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStoreListener;

/**
 * Implementation of API controller. This will provide with listener instances and route the calls
 * to subscribers.
 */
public class TopologyProcessorNotificationSender
        extends ComponentNotificationSender<TopologyProcessorNotification>
        implements TopoBroadcastManager, TargetStoreListener, OperationListener, ProbeStoreListener{

    private final Map<Class<? extends Operation>, OperationNotifier> operationsListeners;
    private final IMessageSender<Topology> liveTopologySender;
    private final IMessageSender<Topology> userPlanTopologySender;
    private final IMessageSender<Topology> schedPlanTopologySender;
    private final IMessageSender<TopologyProcessorNotification> notificationSender;
    private final IMessageSender<TopologySummary> topologySummarySender;
    private final ExecutorService threadPool;
    private final Clock clock;


    public TopologyProcessorNotificationSender(@Nonnull final ExecutorService threadPool,
            @Nonnull final Clock clock,
            @Nonnull IMessageSender<Topology> liveTopologySender,
            @Nonnull IMessageSender<Topology> userPlanTopologySender,
            @Nonnull IMessageSender<Topology> schedPlanTopologySender,
            @Nonnull IMessageSender<TopologyProcessorNotification> notificationSender,
            @Nonnull IMessageSender<TopologySummary> topologySummarySender) {
        super();
        this.clock = Objects.requireNonNull(clock);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.liveTopologySender = Objects.requireNonNull(liveTopologySender);
        this.userPlanTopologySender = Objects.requireNonNull(userPlanTopologySender);
        this.schedPlanTopologySender = Objects.requireNonNull(schedPlanTopologySender);
        this.notificationSender = Objects.requireNonNull(notificationSender);
        this.topologySummarySender = Objects.requireNonNull(topologySummarySender);

        operationsListeners = new HashMap<>();
        operationsListeners.put(Validation.class,
                        operation -> notifyValidationState((Validation)operation));
        operationsListeners.put(Discovery.class,
                        operation -> notifyDiscoveryState((Discovery)operation));
        // TODO (roman, Aug 2016): Add notifications for actions.
        operationsListeners.put(Action.class, operation -> notifyActionState((Action)operation));
    }

    /**
     * Sends non-critical notifications. If sending is failed, failure is logged. All these
     * messages are not critical to be lost.
     *
     * @param message message to send.
     */
    private void sendMessageSilently(@Nonnull TopologyProcessorNotification message) {
        try {
            sendMessage(notificationSender, message);
        } catch (CommunicationException | InterruptedException e) {
            getLogger().error("Could not send notification message " + message.getTypeCase(), e);
        }
    }

    @Override
    public void onTargetAdded(@Nonnull final Target target) {
        getLogger().debug(() -> "Sending onTargetAdded notifications for target '"
                + target.getDisplayName() + "' (" + target.getId() + ")");
        final TopologyProcessorNotification message = createNewMessage()
            .setTargetAddedNotification(target.getNoSecretDto()).build();
        sendMessageSilently(message);
    }

    @Override
    public void onTargetUpdated(@Nonnull final Target target) {
        getLogger().debug(() -> "Sending onTargetChanged notifications for target '"
                + target.getDisplayName() + "' (" + target.getId() + ")");
        final TopologyProcessorNotification message =
                createNewMessage().setTargetChangedNotification(target.getNoSecretDto()).build();
        sendMessageSilently(message);
    }

    @Override
    public void onTargetRemoved(@Nonnull final Target target) {
        getLogger().debug(() -> "Sending onTargetRemoved notifications for target '"
                + target.getDisplayName() + "' (" + target.getId() + ")");
        final TopologyProcessorNotification message =
                createNewMessage().setTargetRemovedNotification(target.getId()).build();
        sendMessageSilently(message);
    }

    /**
     * Converts local date time into long type.
     *
     * @param date source date
     * @return date representation in milliseconds
     */
    private static long toEpochMillis(LocalDateTime date) {
        return date.toInstant(ZoneOffset.ofTotalSeconds(0)).toEpochMilli();
    }

    private void notifyValidationState(@Nonnull final Validation result)
            throws InterruptedException {
        getLogger().debug(() -> "Target " + result.getTargetId() + " validation reported with status "
                        + result.getStatus());
        final TopologyProcessorNotification message = createNewMessage()
            .setValidationNotification(convertOperationToDto(result))
            .build();
        sendMessageSilently(message);
    }

    private void notifyDiscoveryState(@Nonnull final Discovery result) {
        getLogger().debug(() -> "Target " + result.getTargetId() + " discovery reported with status "
                        + result.getStatus());
        final TopologyProcessorNotification message = createNewMessage()
                .setDiscoveryNotification(convertOperationToDto(result))
                .build();
        sendMessageSilently(message);
    }

    private void notifyActionState(@Nonnull final Action action) throws InterruptedException {
        final TopologyProcessorNotification.Builder messageBuilder = createNewMessage();
        switch (action.getStatus()) {
            case IN_PROGRESS:
                messageBuilder.setActionProgress(ActionProgress.newBuilder()
                    .setActionId(action.getActionId())
                    .setProgressPercentage(action.getProgress())
                    .setDescription(action.getDescription()));
                break;
            case SUCCESS:
                messageBuilder.setActionSuccess(ActionSuccess.newBuilder()
                    .setActionId(action.getActionId())
                    .setSuccessDescription(action.getDescription()));
                break;
            case FAILED:
                messageBuilder.setActionFailure(ActionFailure.newBuilder()
                    .setActionId(action.getActionId())
                    .setErrorDescription(action.getDescription()));
                break;
            default:
                getLogger().error("Action {}: unknown action status: {}",
                        action.getId(), action.getStatus());
                break;
        }

        sendMessageSilently(messageBuilder.build());
    }

    private OperationStatus convertOperationToDto(@Nonnull final Operation src) {
        final OperationStatus.Builder opResBuilder = OperationStatus.newBuilder();
        opResBuilder.setId(src.getId());
        opResBuilder.setStartTime(toEpochMillis(src.getStartTime()));
        if (src.getCompletionTime() != null) {
            opResBuilder.setEndTime(toEpochMillis(src.getCompletionTime()));
        }
        opResBuilder.setTargetId(src.getTargetId());
        opResBuilder.setStatus(OperationStatus.Status.valueOf(src.getStatus().name()));
        opResBuilder.addAllErrorMessages(src.getErrors());
        return opResBuilder.build();
    }

    private TopologyProcessorNotification.Builder createNewMessage() {
        return TopologyProcessorNotification.newBuilder().setBroadcastId(newMessageChainId());
    }

    @Nonnull
    @Override
    public TopologyBroadcast broadcastLiveTopology(@Nonnull final TopologyInfo topologyInfo) {
        return new TopologyBroadcastImpl(liveTopologySender, topologyInfo);
    }

    @Nonnull
    @Override
    public TopologyBroadcast broadcastUserPlanTopology(@Nonnull final TopologyInfo topologyInfo) {
        return new TopologyBroadcastImpl(userPlanTopologySender, topologyInfo);
    }

    @Nonnull
    @Override
    public TopologyBroadcast broadcastScheduledPlanTopology(
            @Nonnull final TopologyInfo topologyInfo) {
        return new TopologyBroadcastImpl(schedPlanTopologySender, topologyInfo);
    }

    /**
     * Broadcast a {@link TopologySummary} describing a successful or failed topology broadcast.
     *
     * @param topologySummary The {@link TopologySummary} message.
     */
    public void broadcastTopologySummary(@Nonnull final TopologySummary topologySummary) {
        try {
            topologySummarySender.sendMessage(topologySummary);
        } catch (CommunicationException|InterruptedException e) {
            getLogger().error("Could not send TopologySummary message", e);
        }
    }

    @Override
    public void notifyOperationState(@Nonnull Operation operation){
        try {
            getOperationListener(operation).notifyOperation(operation);
        } catch (InterruptedException e) {
            // TODO implement guaranteed delivery instead of RTE
            throw new RuntimeException("Thread interrupted", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyOperationsCleared() {
        getLogger().info("Sending notification that all in-progress actions got cleared.");
        final TopologyProcessorNotification message = createNewMessage()
            .setActionsLost(ActionsLost.newBuilder()
                .setBeforeTime(clock.millis()))
            .build();
        sendMessageSilently(message);
    }

    // TODO switch to IClassMap, after SDK is synched between MT and XL
    @SuppressWarnings("unchecked")
    private OperationNotifier getOperationListener(Operation operation) {
        Class<? extends Operation> clazz = operation.getClass();
        while (clazz != null) {
            final OperationNotifier processor = operationsListeners.get(clazz);
            if (processor != null) {
                return processor;
            }
            clazz = (Class<? extends Operation>)clazz.getSuperclass();
        }
        throw new IllegalArgumentException(
                        "Operation class " + operation.getClass() + " is not supported");
    }

    @Override
    public void onProbeRegistered(long probeId, ProbeInfo probe) {
        TopologyProcessorDTO.ProbeInfo infoDto = buildProbeInfoDto(probeId, probe);
        final TopologyProcessorNotification message = createNewMessage()
            .setProbeRegistrationNotification(infoDto)
            .build();
        sendMessageSilently(message);
    }

    private TopologyProcessorDTO.ProbeInfo buildProbeInfoDto(final long probeId, @Nonnull final ProbeInfo probeInfo) {
        return TopologyProcessorDTO.ProbeInfo.newBuilder()
            .setId(probeId)
            .setType(probeInfo.getProbeType())
            .setCategory(probeInfo.getProbeCategory())
            // TODO: (DavidBlinn 4/6/17) support for adding AccountDefEntries
            .addAllIdentifyingFields(probeInfo.getTargetIdentifierFieldList())
            .build();
    }

    @Override
    protected String describeMessage(
            @Nonnull TopologyProcessorNotification topologyProcessorNotification) {
        return topologyProcessorNotification.getTypeCase().name() + " broadcast #" +
                topologyProcessorNotification.getBroadcastId();
    }

    /**
     * Topology broadcast implementation, sending data, as the next chunk is full to send. Uses
     * {@link MessageChunker#CHUNK_SIZE} for chunk size.
     */
    private class TopologyBroadcastImpl implements TopologyBroadcast {

        private final TopologyInfo topologyInfo;

        /**
         * Lock for internal synchronization.
         */
        private final Object lock = new Object();

        /**
         * Task to await initial message sending.
         */
        private final Future<?> initialMessage;
        private final IMessageSender<Topology> messageSender;

        /**
         * An optional command to run
         */
        private final Consumer<TopologyInfo> postBroadcastCommand;

        /**
         * Collection to store chunk data.
         */
        @GuardedBy("lock")
        private final Collection<TopologyEntityDTO> chunk;

        /**
         * Collection to store extension chunk data.
         */
        @GuardedBy("lock")
        private final Collection<TopologyDTO.TopologyExtension> extensionChunk;

        /**
         * Sequential number of current chunk, wich will be sent later.
         */
        private long totalCount = 0;

        /**
         * Whether the broadcast is finished.
         */
        @GuardedBy("lock")
        private boolean finished = false;

        TopologyBroadcastImpl(@Nonnull IMessageSender<Topology> messageSender,
                              @Nonnull final TopologyInfo topologyInfo) {
            this(messageSender, topologyInfo, null);
        }

        TopologyBroadcastImpl(@Nonnull IMessageSender<Topology> messageSender,
                              @Nonnull final TopologyInfo topologyInfo,
                              @Nullable Consumer<TopologyInfo> postBroadcastCommand) {
            this.messageSender = Objects.requireNonNull(messageSender);
            Preconditions.checkArgument(topologyInfo.hasTopologyId());
            Preconditions.checkArgument(topologyInfo.hasTopologyContextId());
            Preconditions.checkArgument(topologyInfo.hasCreationTime());
            Preconditions.checkArgument(topologyInfo.hasTopologyType());
            this.topologyInfo = topologyInfo;
            this.chunk = new ArrayList<>(MessageChunker.CHUNK_SIZE);
            this.extensionChunk = new ArrayList<>(MessageChunker.CHUNK_SIZE);
            this.postBroadcastCommand = postBroadcastCommand;
            final Topology subMessage = Topology.newBuilder()
                    .setTopologyId(getTopologyId())
                    .setStart(Start.newBuilder()
                            .setTopologyInfo(topologyInfo))
                    .build();
            // As startup message holds no data, it's safe to return immediately. There is not
            // problem, if it will hang in memory at the same time, as the first chunk.
            initialMessage = threadPool.submit(() -> {
                sendTopologySegment(subMessage);
                return null;
            });
        }

        private void awaitInitialMessage() throws CommunicationException, InterruptedException {
            try {
                initialMessage.get();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof CommunicationException) {
                    throw (CommunicationException)e.getCause();
                } else {
                    throw new CommunicationException(
                            "Unexpected error occurred while sending " + "initial message of " +
                                    "broadcast " + topologyInfo.getTopologyId(), e);
                }
            }
        }

        @Override
        public long getTopologyId() {
            return topologyInfo.getTopologyId();
        }

        @Override
        public long getTopologyContextId() {
            return topologyInfo.getTopologyContextId();
        }

        @Override
        public TopologyType getTopologyType() {
            return topologyInfo.getTopologyType();
        }

        @Override
        public long getCreationTime() {
            return topologyInfo.getCreationTime();
        }

        @Override
        public long finish() throws CommunicationException, InterruptedException {
            awaitInitialMessage();
            synchronized (lock) {
                finished = true;
                sendChunk();
                sendExtensionsChunk();
                final Topology subMessage = Topology.newBuilder()
                        .setTopologyId(getTopologyId())
                        .setEnd(End.newBuilder().setTotalCount(totalCount))
                        .build();
                sendTopologySegment(subMessage);
                // if we have a post broadcast command, run it
                if (postBroadcastCommand != null) {
                    postBroadcastCommand.accept(topologyInfo);
                }
                return totalCount;
            }
        }

        @Override
        public void append(@Nonnull TopologyEntityDTO entity) throws CommunicationException,
                InterruptedException {
            awaitInitialMessage();
            synchronized (lock) {
                if (finished) {
                    throw new IllegalStateException("Broadcast " + getTopologyId() + " is already " +
                            "finished. It does not accept new entities");
                }
                chunk.add(entity);
                if (chunk.size() >= MessageChunker.CHUNK_SIZE) {
                    sendChunk();
                    chunk.clear();
                }
            }
        }

        /**
         * Appends the next topology extension entity to the notification.
         * This call may block until the next chunk is sent.
         *
         * @param extension to add to broadcast.
         * @throws InterruptedException   if thread has been interrupted
         * @throws NullPointerException   if {@code entity} is {@code null}
         * @throws IllegalStateException  if {@link #finish()} has been already called
         * @throws CommunicationException persistent communication exception
         */
        @Override
        public void appendExtension(@Nonnull TopologyDTO.TopologyExtension extension)
            throws CommunicationException, InterruptedException {
            awaitInitialMessage();
            synchronized (lock) {
                if (finished) {
                    throw new IllegalStateException(
                        "Broadcast " + getTopologyId() + " is already " +
                        "finished. It does not accept new extensions");
                }
                // Send the entities first.
                if (chunk.size() > 0) {
                    sendChunk();
                    chunk.clear();
                }
                extensionChunk.add(extension);
                if (extensionChunk.size() >= MessageChunker.CHUNK_SIZE) {
                    sendExtensionsChunk();
                    extensionChunk.clear();
                }
            }
        }

        private void sendChunk() throws CommunicationException, InterruptedException {
            Collection<Topology.DataSegment> segments = chunk.stream().map(dto -> {
                return Topology.DataSegment.newBuilder().setEntity(dto).build();
            }).collect(Collectors.toList());
            final Topology subMessage = Topology.newBuilder()
                    .setData(Data.newBuilder().addAllEntities(segments))
                    .setTopologyId(getTopologyId())
                    .build();
            sendTopologySegment(subMessage);
            totalCount += chunk.size();
        }

        private void sendExtensionsChunk() throws CommunicationException, InterruptedException {
            Collection<Topology.DataSegment> segments = extensionChunk.stream().map(ext -> {
                return Topology.DataSegment.newBuilder().setExtension(ext).build();
            }).collect(Collectors.toList());
            final Topology subMessage = Topology.newBuilder()
                                                .setData(Data.newBuilder().addAllEntities(segments))
                                                .setTopologyId(getTopologyId())
                                                .build();
            sendTopologySegment(subMessage);
            totalCount += extensionChunk.size();
        }

        private void sendTopologySegment(final @Nonnull Topology segment)
                throws CommunicationException, InterruptedException {
            getLogger().debug("Sending topology {} segment {}", segment::getTopologyId,
                    segment::getSegmentCase);
            messageSender.sendMessage(segment);
        }

    }

    private interface OperationNotifier {
        void notifyOperation(@Nonnull Operation operation) throws InterruptedException;
    }
}

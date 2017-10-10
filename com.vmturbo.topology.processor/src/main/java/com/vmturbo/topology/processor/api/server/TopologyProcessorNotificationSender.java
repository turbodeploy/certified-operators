package com.vmturbo.topology.processor.api.server;

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

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.Data;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.End;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.Start;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.chunking.MessageChunker;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
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
        implements TopoBroadcastManager, TargetStoreListener, OperationListener, ProbeStoreListener {

    private final Map<Class<? extends Operation>, OperationNotifier> operationsListeners;
    // TODO remove in future after switch off websockets
    private final long chunkSendDelayMs;
    private final IMessageSender<TopologyProcessorNotification> topologySender;
    private final IMessageSender<TopologyProcessorNotification> notificationSender;

    public TopologyProcessorNotificationSender(@Nonnull final ExecutorService threadPool,
            long chunkSendDelayMs,
            @Nonnull IMessageSender<TopologyProcessorNotification> topologySender,
            @Nonnull IMessageSender<TopologyProcessorNotification> notifiationSender) {
        super(threadPool);
        this.topologySender = Objects.requireNonNull(topologySender);
        this.notificationSender = Objects.requireNonNull(notifiationSender);
        operationsListeners = new HashMap<>();
        operationsListeners.put(Validation.class,
                        operation -> notifyValidationState((Validation)operation));
        operationsListeners.put(Discovery.class,
                        operation -> notifyDiscoveryState((Discovery)operation));
        // TODO (roman, Aug 2016): Add notifications for actions.
        operationsListeners.put(Action.class, operation -> notifyActionState((Action)operation));
        if (chunkSendDelayMs < 0) {
            throw new IllegalArgumentException("Chunk send delay must not be a negative value");
        } else {
            this.chunkSendDelayMs = chunkSendDelayMs;
        }
    }

    @Override
    public void onTargetAdded(@Nonnull final Target target) {
        getLogger().debug(() -> "Sending onTargetAdded notifications for target " + target);
        final TopologyProcessorNotification message = createNewMessage()
            .setTargetAddedNotification(target.getNoSecretDto()).build();
        sendMessage(notificationSender, message);
    }

    @Override
    public void onTargetUpdated(@Nonnull final Target target) {
        getLogger().debug(() -> "Sending onTargetChanged notifications for target " + target);
        final TopologyProcessorNotification message =
                createNewMessage().setTargetChangedNotification(target.getNoSecretDto()).build();
        sendMessage(notificationSender, message);
    }

    @Override
    public void onTargetRemoved(@Nonnull final Target target) {
        getLogger().debug(() -> "Sending onTargetRemoved notifications for target " + target);
        final TopologyProcessorNotification message =
                createNewMessage().setTargetRemovedNotification(target.getId()).build();
        sendMessage(notificationSender, message);
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
        sendMessage(notificationSender, message);
    }

    private void notifyDiscoveryState(@Nonnull final Discovery result) {
        getLogger().debug(() -> "Target " + result.getTargetId() + " discovery reported with status "
                        + result.getStatus());
        final TopologyProcessorNotification message = createNewMessage()
                .setDiscoveryNotification(convertOperationToDto(result))
                .build();
        sendMessage(notificationSender, message);
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

        sendMessage(notificationSender, messageBuilder.build());
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

    private void sendTopologySegment(final @Nonnull Topology segment) throws InterruptedException {
        final TopologyProcessorNotification message =
                createNewMessage().setTopologyNotification(segment).build();
        sendMessageSync(topologySender, message);
    }

    private TopologyProcessorNotification.Builder createNewMessage() {
        return TopologyProcessorNotification.newBuilder().setBroadcastId(newMessageChainId());
    }

    @Nonnull
    @Override
    public TopologyBroadcast broadcastTopology(final long topologyContextId, final long topologyId,
                    final TopologyType topologyType) {
        return new TopologyBroadcastImpl(topologyContextId, topologyId, topologyType);
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
        sendMessage(notificationSender, message);
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
        /**
         * Topology broadcast id.
         */
        private final long topologyId;

        /**
         * Topology created time.
         */
        private final long creationTime;

        /**
         * Topology context id.
         */
        private final long topologyContextId;

        /**
         * Topology type.
         */
        private final TopologyType topologyType;

        /**
         * Lock for internal synchronization.
         */
        private final Object lock = new Object();

        /**
         * Task to await initial message sending.
         */
        private final Future<?> initialMessage;
        /**
         * Collection to store chunk data.
         */

        @GuardedBy("lock")
        private final Collection<TopologyEntityDTO> chunk;
        /**
         * Sequential number of current chunk, wich will be sent later.
         */

        private long totalCount = 0;
        /**
         * Whether the broadcast is finished.
         */
        @GuardedBy("lock")

        private boolean finished = false;

        TopologyBroadcastImpl(final long topologyContextId, final long topologyId,
                    final TopologyType topologyType) {
            this.topologyId = topologyId;
            this.topologyContextId = topologyContextId;
            this.topologyType = topologyType;
            this.chunk = new ArrayList<>(MessageChunker.CHUNK_SIZE);
            this.creationTime = System.currentTimeMillis();
            final Topology subMessage = Topology.newBuilder()
                    .setTopologyId(getTopologyId())
                    .setStart(Start.newBuilder()
                            .setTopologyInfo(TopologyInfo.newBuilder()
                                    .setTopologyId(topologyId)
                                    .setTopologyContextId(topologyContextId)
                                    .setTopologyType(topologyType)
                                    .setCreationTime(creationTime)))
                    .build();
            // As startup message holds no data, it's safe to return immediately. There is not
            // problem, if it will hang in memory at the same time, as the first chunk.
            initialMessage = getExecutorService().submit(() -> {
                sendTopologySegment(subMessage);
                return null;
            });
        }

        private void awaitInitialMessage() throws InterruptedException {
            try {
                initialMessage.get();
            } catch (ExecutionException e) {
                getLogger().error("Unexpected error occurred while sending initial message of " +
                        "broadcast " + topologyId, e);
            }
        }

        @Override
        public long getTopologyId() {
            return topologyId;
        }

        @Override
        public long getTopologyContextId() {
            return topologyContextId;
        }

        @Override
        public TopologyType getTopologyType() {
            return topologyType;
        }

        @Override
        public long getCreationTime() {
            return creationTime;
        }

        @Override
        public long finish() throws InterruptedException {
            awaitInitialMessage();
            synchronized (lock) {
                finished = true;
                sendChunk();
                final Topology subMessage = Topology.newBuilder()
                        .setTopologyId(getTopologyId())
                        .setEnd(End.newBuilder().setTotalCount(totalCount))
                        .build();
                sendTopologySegment(subMessage);
                return totalCount;
            }
        }

        @Override
        public void append(@Nonnull TopologyEntityDTO entity) throws InterruptedException {
            awaitInitialMessage();
            synchronized (lock) {
                if (finished) {
                    throw new IllegalStateException("Broadcast " + topologyId + " is already " +
                            "finished. It does not accept new entities");
                }
                chunk.add(entity);
                if (chunk.size() >= MessageChunker.CHUNK_SIZE) {
                    sendChunk();
                    chunk.clear();
                }
            }
        }

        private void sendChunk() throws InterruptedException {
            final Topology subMessage = Topology.newBuilder()
                    .setData(Data.newBuilder().addAllEntities(chunk))
                    .setTopologyId(topologyId)
                    .build();
            Thread.sleep(chunkSendDelayMs);
            sendTopologySegment(subMessage);
            totalCount += chunk.size();
        }
    }

    private interface OperationNotifier {
        void notifyOperation(@Nonnull Operation operation) throws InterruptedException;
    }
}

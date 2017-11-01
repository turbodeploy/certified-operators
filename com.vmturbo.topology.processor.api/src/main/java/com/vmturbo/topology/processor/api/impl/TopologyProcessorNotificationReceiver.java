package com.vmturbo.topology.processor.api.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.ChunkingReceiver;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.client.ApiClientException;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.topology.processor.api.ActionExecutionListener;
import com.vmturbo.topology.processor.api.DiscoveryStatus;
import com.vmturbo.topology.processor.api.EntitiesListener;
import com.vmturbo.topology.processor.api.ProbeListener;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TargetListener;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TopologyProcessorNotification;
import com.vmturbo.topology.processor.api.TopologyProcessorException;
import com.vmturbo.topology.processor.api.ValidationStatus;

/**
 * The websocket client connecting to the Topology Processor.
 */
class TopologyProcessorNotificationReceiver extends ComponentNotificationReceiver<TopologyProcessorNotification> {

    private final Set<EntitiesListener> entitiesListeners =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final Set<TargetListener> targetListeners = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final Set<ActionExecutionListener> actionListeners = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final Set<ProbeListener> probeListeners = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ChunkingReceiver<TopologyEntityDTO> topologyChunkReceiver;

    private <LISTENER_TYPE> void doWithListeners(@Nonnull final Set<LISTENER_TYPE> listeners,
                                 final Consumer<LISTENER_TYPE> command) {
        for (final LISTENER_TYPE listener : listeners) {
            getExecutorService().submit(() -> {
                try {
                    command.accept(listener);
                } catch (RuntimeException e) {
                    getLogger().error("Error executing command for listener " + listener, e);
                }
            });
        }
    }

    public TopologyProcessorNotificationReceiver(
            @Nullable final IMessageReceiver<TopologyProcessorNotification> messageReceiver,
            @Nullable final IMessageReceiver<Topology> topologyReceiver,
            @Nonnull final ExecutorService executorService) {
        super(messageReceiver, executorService);
        this.topologyChunkReceiver = new ChunkingReceiver<>(executorService);
        topologyReceiver.addListener(this::onTopologyNotification);

    }

    private void onTargetAddedNotification(@Nonnull final TopologyProcessorNotification message) {
        final TopologyProcessorDTO.TargetInfo notification = message.getTargetAddedNotification();
        getLogger().debug("Target added notification received for target {}", notification.getId());
        final TargetInfo targetInfo = new TargetInfoProtobufWrapper(notification);
        doWithListeners(targetListeners, l -> l.onTargetAdded(targetInfo));
    }

    private void onTargetChangedNotification(@Nonnull final TopologyProcessorNotification message) {
        final TopologyProcessorDTO.TargetInfo notification = message.getTargetChangedNotification();
        getLogger().debug("Target updated notification received for target {}", notification.getId());
        final TargetInfo targetInfo = new TargetInfoProtobufWrapper(notification);
        doWithListeners(targetListeners, l -> l.onTargetChanged(targetInfo));
    }

    private void onTargetRemovedNotification(@Nonnull final TopologyProcessorNotification message) {
        final long targetId = message.getTargetRemovedNotification();
        getLogger().debug("Target removed notification received for target {}", targetId);
        doWithListeners(targetListeners, l -> l.onTargetRemoved(targetId));
    }

    private void onTargetValidatedNotification(@Nonnull final TopologyProcessorNotification message) {
        final OperationStatus notification = message.getValidationNotification();
        getLogger().debug("Validation notification received for target {}", notification.getTargetId());
        final ValidationStatus result = new OperationStatusWrapper(notification);
        doWithListeners(targetListeners, l -> l.onTargetValidated(result));
    }

    private void onTargetDiscoveredNotification(@Nonnull final TopologyProcessorNotification message) {
        final OperationStatus notification = message.getDiscoveryNotification();
        getLogger().debug("Discovery notification received for target {}", +notification.getTargetId());
        final DiscoveryStatus result = new OperationStatusWrapper(notification);
        doWithListeners(targetListeners, l -> l.onTargetDiscovered(result));
    }

    private void onTopologyNotification(@Nonnull final Topology topology,
            @Nonnull Runnable commitCommand) {
        final long topologyId = topology.getTopologyId();
        switch (topology.getSegmentCase()) {
            case START:
                topologyChunkReceiver.startTopologyBroadcast(topology.getTopologyId(),
                        createEntityConsumers(topology.getStart().getTopologyInfo()));
                break;
            case DATA:
                topologyChunkReceiver.processData(topology.getTopologyId(),
                        topology.getData().getEntitiesList());
                break;
            case END:
                topologyChunkReceiver.finishTopologyBroadcast(topology.getTopologyId(),
                        topology.getEnd().getTotalCount());
                commitCommand.run();
                break;
            default:
                getLogger().warn("Unknown broadcast data segment received: {}",
                        topology.getSegmentCase());
        }
    }

    private Collection<Consumer<RemoteIterator<TopologyEntityDTO>>> createEntityConsumers(
            @Nonnull final TopologyInfo topologyInfo) {
        getLogger().info("TopologyInfo : " + topologyInfo);
        return entitiesListeners.stream().map(listener -> {
            final Consumer<RemoteIterator<TopologyEntityDTO>> consumer =
                    iterator -> listener.onTopologyNotification(topologyInfo, iterator);
            return consumer;
        }).collect(Collectors.toList());
    }


    private void onActionProgressNotification(@Nonnull final TopologyProcessorNotification message) {
        final ActionProgress notification = message.getActionProgress();
        getLogger().debug("ActionProgress notification received for action {}", notification.getActionId());
        doWithListeners(actionListeners, l -> l.onActionProgress(notification));
    }

    private void onActionSuccessNotification(@Nonnull final TopologyProcessorNotification message) {
        final ActionSuccess notification = message.getActionSuccess();
        getLogger().debug("ActionSuccess notification received for action {}", notification.getActionId());
        doWithListeners(actionListeners, l -> l.onActionSuccess(notification));
    }

    private void onActionFailureNotification(@Nonnull final TopologyProcessorNotification message) {
        final ActionFailure notification = message.getActionFailure();
        getLogger().debug("ActionFailure notification received for action {}", notification.getActionId());
        doWithListeners(actionListeners, l -> l.onActionFailure(notification));
    }

    private void onProbeRegisteredNotification(@Nonnull final TopologyProcessorNotification message) {
        final TopologyProcessorDTO.ProbeInfo notification = message.getProbeRegistrationNotification();
        getLogger().debug("Probe registration notification received for probe {}", notification.getId());
        doWithListeners(probeListeners, l -> l.onProbeRegistered(notification));
    }

    @Override
    protected void processMessage(@Nonnull final TopologyProcessorNotification message) throws ApiClientException {
        getLogger().trace("Processing message {}", message.getBroadcastId());
        switch (message.getTypeCase()) {
            case TARGET_ADDED_NOTIFICATION:
                onTargetAddedNotification(message);
                break;
            case TARGET_CHANGED_NOTIFICATION:
                onTargetChangedNotification(message);
                break;
            case TARGET_REMOVED_NOTIFICATION:
                onTargetRemovedNotification(message);
                break;
            case VALIDATION_NOTIFICATION:
                onTargetValidatedNotification(message);
                break;
            case DISCOVERY_NOTIFICATION:
                onTargetDiscoveredNotification(message);
                break;
            case ACTION_PROGRESS:
                onActionProgressNotification(message);
                break;
            case ACTION_SUCCESS:
                onActionSuccessNotification(message);
                break;
            case ACTION_FAILURE:
                onActionFailureNotification(message);
                break;
            case PROBE_REGISTRATION_NOTIFICATION:
                onProbeRegisteredNotification(message);
                break;
            default:
                throw new TopologyProcessorException("Message type unrecognized: " + message);
        }
        getLogger().trace("Message {} processed successfully", message.getBroadcastId());
    }

    public void addTargetListener(@Nonnull final TargetListener listener) {
        this.targetListeners.add(listener);
    }

    public void addEntitiesListener(@Nonnull final EntitiesListener listener) {
        this.entitiesListeners.add(listener);
    }

    public void addActionListener(@Nonnull final ActionExecutionListener listener) {
        this.actionListeners.add(listener);
    }

    public void addProbeListener(@Nonnull final ProbeListener listener) {
        this.probeListeners.add(listener);
    }
}

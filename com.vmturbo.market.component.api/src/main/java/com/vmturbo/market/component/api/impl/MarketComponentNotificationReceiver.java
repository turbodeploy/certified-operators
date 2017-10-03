package com.vmturbo.market.component.api.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.protobuf.CodedInputStream;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology.Start;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.ChunkingReceiver;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.market.component.api.ActionsListener;
import com.vmturbo.market.component.api.ProjectedTopologyListener;
import com.vmturbo.market.component.dto.MarketMessages.MarketComponentNotification;

/**
 * The websocket client connecting to the Market Component.
 */
class MarketComponentNotificationReceiver extends
        ComponentNotificationReceiver<MarketComponentNotification> {

    private final Set<ActionsListener> actionsListenersSet =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final Set<ProjectedTopologyListener> projectedTopologyListenersSet =
                    Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ChunkingReceiver<TopologyEntityDTO> topologyChunkReceiver;

    MarketComponentNotificationReceiver(
            @Nonnull final IMessageReceiver<MarketComponentNotification> messageReceiver,
            @Nonnull final ExecutorService executorService) {
        super(messageReceiver, executorService);
        topologyChunkReceiver = new ChunkingReceiver<>(executorService);
    }

    void addActionsListener(@Nonnull final ActionsListener listener) {
        actionsListenersSet.add(Objects.requireNonNull(listener));
    }

    void addProjectedTopologyListener(@Nonnull final ProjectedTopologyListener listener) {
        projectedTopologyListenersSet.add(Objects.requireNonNull(listener));
    }

    @Override
    protected void processMessage(@Nonnull final MarketComponentNotification message)
            throws MarketComponentException {
        switch (message.getTypeCase()) {
            case ACTION_PLAN:
                processActions(message.getActionPlan());
                break;
            case PROJECTED_TOPOLOGY:
                processProjectedTopology(message.getProjectedTopology());
                break;
            default:
                throw new MarketComponentException("Message type unrecognized: " + message);
        }
    }

    private void processActions(@Nonnull final ActionPlan actions) {
        for (final ActionsListener listener : actionsListenersSet) {
            getExecutorService().submit(() -> {
                try {
                    listener.onActionsReceived(actions);
                } catch (RuntimeException e) {
                    getLogger().error("Error executing entities notification for listener " +
                            listener, e);
                }
            });
        }
    }

    private void processProjectedTopology(@Nonnull final ProjectedTopology topology) {
        final long topologyId = topology.getTopologyId();
        switch (topology.getSegmentCase()) {
            case START:
                final Start start = topology.getStart();
                topologyChunkReceiver.startTopologyBroadcast(topology.getTopologyId(),
                        createEntityConsumers(topologyId, start.getSourceTopologyInfo()));
                break;
            case DATA:
                topologyChunkReceiver.processData(topology.getTopologyId(),
                        topology.getData().getEntitiesList());
                break;
            case END:
                topologyChunkReceiver.finishTopologyBroadcast(topology.getTopologyId(),
                        topology.getEnd().getTotalCount());
                break;
            default:
                getLogger().warn("Unknown broadcast data segment received: {}",
                        topology.getSegmentCase());
        }
    }

    private Collection<Consumer<RemoteIterator<TopologyEntityDTO>>> createEntityConsumers(
            final long topologyId, final TopologyInfo topologyInfo) {
        return projectedTopologyListenersSet.stream().map(listener -> {
            final Consumer<RemoteIterator<TopologyEntityDTO>> consumer =
                    iterator -> listener.onProjectedTopologyReceived(
                            topologyId, topologyInfo, iterator);
            return consumer;
        }).collect(Collectors.toList());
    }
}

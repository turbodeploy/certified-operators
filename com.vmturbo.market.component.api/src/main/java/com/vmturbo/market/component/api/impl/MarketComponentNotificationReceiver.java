package com.vmturbo.market.component.api.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology.Start;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.ChunkingReceiver;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.client.ApiClientException;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.market.component.api.ActionsListener;
import com.vmturbo.market.component.api.ProjectedTopologyListener;

/**
 * The notification receiver connecting to the Market Component.
 */
public class MarketComponentNotificationReceiver extends
        ComponentNotificationReceiver<ActionPlan> {

    private final Set<ActionsListener> actionsListenersSet =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final Set<ProjectedTopologyListener> projectedTopologyListenersSet =
                    Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ChunkingReceiver<TopologyEntityDTO> topologyChunkReceiver;

    public MarketComponentNotificationReceiver(
            @Nonnull final IMessageReceiver<ProjectedTopology> projectedTopologyReceiver,
            @Nonnull final IMessageReceiver<ActionPlan> actionPlanReceiver,
            @Nonnull final ExecutorService executorService) {
        super(actionPlanReceiver, executorService);
        topologyChunkReceiver = new ChunkingReceiver<>(executorService);
        projectedTopologyReceiver.addListener(this::processProjectedTopology);
    }

    void addActionsListener(@Nonnull final ActionsListener listener) {
        actionsListenersSet.add(Objects.requireNonNull(listener));
    }

    void addProjectedTopologyListener(@Nonnull final ProjectedTopologyListener listener) {
        projectedTopologyListenersSet.add(Objects.requireNonNull(listener));
    }

    @Override
    protected void processMessage(@Nonnull final ActionPlan actions)
            throws ApiClientException, InterruptedException {
        for (final ActionsListener listener : actionsListenersSet) {
            listener.onActionsReceived(actions);
        }
    }

    private void processProjectedTopology(@Nonnull final ProjectedTopology topology, @Nonnull Runnable commitCommand) {
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
                commitCommand.run();
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

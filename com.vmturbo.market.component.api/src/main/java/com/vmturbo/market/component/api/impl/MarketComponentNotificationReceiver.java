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
import javax.annotation.Nullable;

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
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.PriceIndexListener;
import com.vmturbo.market.component.api.ProjectedTopologyListener;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;

/**
 * The notification receiver connecting to the Market Component.
 */
public class MarketComponentNotificationReceiver extends
        ComponentNotificationReceiver<ActionPlan> implements MarketComponent{

    /**
     * Projected topologies topic.
     */
    public static final String PROJECTED_TOPOLOGIES_TOPIC = "projected-topologies";
    /**
     * Action plans topic.
     */
    public static final String ACTION_PLANS_TOPIC = "action-plans";
    /**
     * Price index topic.
     */
    public static final String PRICE_INDICES_TOPIC = "price-indices";

    private final Set<ActionsListener> actionsListenersSet;

    private final Set<ProjectedTopologyListener> projectedTopologyListenersSet;
    private final Set<PriceIndexListener> priceIndexListenerSet;
    private final ChunkingReceiver<TopologyEntityDTO> topologyChunkReceiver;

    public MarketComponentNotificationReceiver(
            @Nullable final IMessageReceiver<ProjectedTopology> projectedTopologyReceiver,
            @Nullable final IMessageReceiver<ActionPlan> actionPlanReceiver,
            @Nullable final IMessageReceiver<PriceIndexMessage> priceIndexReceiver,
            @Nonnull final ExecutorService executorService) {
        super(actionPlanReceiver, executorService);
        if (projectedTopologyReceiver != null) {
            projectedTopologyListenersSet = Collections.newSetFromMap(new ConcurrentHashMap<>());
            projectedTopologyReceiver.addListener(this::processProjectedTopology);
        } else {
            projectedTopologyListenersSet = Collections.emptySet();
        }
        if (actionPlanReceiver != null) {
            actionsListenersSet = Collections.newSetFromMap(new ConcurrentHashMap<>());
        } else {
            actionsListenersSet = Collections.emptySet();
        }
        topologyChunkReceiver = new ChunkingReceiver<>(executorService);
        if (priceIndexReceiver != null) {
            priceIndexListenerSet = Collections.newSetFromMap(new ConcurrentHashMap<>());
            priceIndexReceiver.addListener(this::processPriceIndex);
        } else {
            priceIndexListenerSet = Collections.emptySet();
        }
    }

    @Override
    public void addActionsListener(@Nonnull final ActionsListener listener) {
        actionsListenersSet.add(Objects.requireNonNull(listener));
    }

    @Override
    public void addProjectedTopologyListener(@Nonnull final ProjectedTopologyListener listener) {
        projectedTopologyListenersSet.add(Objects.requireNonNull(listener));
    }

    @Override
    public void addPriceIndexListener(@Nonnull PriceIndexListener listener) {
        priceIndexListenerSet.add(Objects.requireNonNull(listener));
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

    private void processPriceIndex(@Nonnull final PriceIndexMessage topology,
            @Nonnull Runnable commitCommand) {
        for (final PriceIndexListener listener : priceIndexListenerSet) {
            listener.onPriceIndexReceived(topology);
        }
        commitCommand.run();
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

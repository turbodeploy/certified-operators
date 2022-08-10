package com.vmturbo.cost.api.impl;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Sets;

import io.opentracing.SpanContext;

import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.TopologyOnDemandCostChunk;
import com.vmturbo.communication.chunking.ChunkingReceiver;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.MulticastNotificationReceiver;
import com.vmturbo.cost.api.CostComponent;
import com.vmturbo.cost.api.CostNotificationListener;
import com.vmturbo.cost.api.TopologyCostListener;

/**
 * The notification receiver connecting to the cost component.
 */
public class CostComponentImpl extends
        MulticastNotificationReceiver<CostNotification, CostNotificationListener> implements CostComponent {

    /**
     * Kafka topic to receive cost notification messages (status updates).
     */
    public static final String COST_NOTIFICATIONS = "cost-notifications";

    /**
     * Kafka topic to receive on-demand prices for all vms in a topology.
     */
    public static final String TOPOLOGY_VM_ON_DEMAND_PRICES = "topology-vm-on-demand-cost";

    private final Set<TopologyCostListener> topologyCostListeners;
    private final ChunkingReceiver<TopologyOnDemandCostChunk> topologyCostChunkReceiver;
    /**
     * The constructor of the cost component message receiver
     *
     * @param costNotificationMessageReceiver Message receiver for cost status notifications
     * @param executorService Executor service to use for communication with the server.
     * @param kafkaReceiverTimeoutSeconds The max number of seconds to wait for all notification
     *                                    listeners to finish handling a notification.
     */
    public CostComponentImpl(
            @Nullable final IMessageReceiver<CostNotification> costNotificationMessageReceiver,
            @Nullable final IMessageReceiver<TopologyOnDemandCostChunk> topologyCostChunkReceiver,
            @Nonnull final ExecutorService executorService, int kafkaReceiverTimeoutSeconds) {
        super(costNotificationMessageReceiver, executorService, kafkaReceiverTimeoutSeconds,
                msg -> listener -> listener.onCostNotificationReceived(msg));
        this.topologyCostChunkReceiver = new ChunkingReceiver<>(executorService);
        if (topologyCostChunkReceiver == null) {
            topologyCostListeners = Collections.emptySet();
        } else {
            topologyCostListeners = Sets.newConcurrentHashSet();
            topologyCostChunkReceiver.addListener(this::processTopologyCostChunk);
        }
    }

    @Override
    public void addCostNotificationListener(@Nonnull final CostNotificationListener listener) {
        addListener(listener);
    }

    @Override
    public void addTopologyCostListener(@NotNull TopologyCostListener listener) {
        topologyCostListeners.add(listener);
    }

    private void processTopologyCostChunk(final TopologyOnDemandCostChunk chunk, Runnable runnable, SpanContext spanContext) {
        topologyCostListeners.forEach(listener -> listener.onTopologyCostReceived(chunk));
    }
}

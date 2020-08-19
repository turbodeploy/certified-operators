package com.vmturbo.topology.processor.api.impl;

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

import io.opentracing.SpanContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.ChunkingReceiver;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.topology.processor.api.EntitiesListener;

/**
 * Topology receiver is an object to receive topology broadcasts and pass them into listeners.
 */
public class TopologyReceiver {

    private final Set<EntitiesListener> listeners;
    private final boolean active;
    private final Logger logger = LogManager.getLogger(getClass());

    public TopologyReceiver(@Nullable IMessageReceiver<Topology> messageReceiver,
                            @Nonnull ExecutorService threadPool) {
        this.active = (messageReceiver != null);
        if (!active) {
            // Create immutable empty set to avoid adding new listeners if there is not
            // subscription
            listeners = Collections.emptySet();
        } else {
            listeners = Collections.newSetFromMap(new ConcurrentHashMap<>());
            final ChunkingReceiver<TopologyDTO.Topology.DataSegment> chunkingReceiver =
                new ChunkingReceiver<>(threadPool);
            messageReceiver.addListener(
                (topology, commitCmd, spanContext) -> this.onTopologyNotification(
                    chunkingReceiver, topology, commitCmd, spanContext));
        }
    }

    private void onTopologyNotification(
        @Nonnull ChunkingReceiver<TopologyDTO.Topology.DataSegment> receiver,
        @Nonnull final Topology topology,
        @Nonnull Runnable commitCommand,
        @Nonnull final SpanContext tracingContext) {
        logger.trace("Received {} segment for topology broadcast {}", topology::getSegmentCase,
                     topology::getTopologyId);
        switch (topology.getSegmentCase()) {
            case START:
                receiver.startTopologyBroadcast(topology.getTopologyId(),
                    createEntityConsumers(topology.getStart().getTopologyInfo(), tracingContext));
                break;
            case DATA:
                receiver.processData(topology.getTopologyId(),
                                     topology.getData().getEntitiesList());
                break;
            case END:
                receiver.finishTopologyBroadcast(topology.getTopologyId(),
                                                 topology.getEnd().getTotalCount());
                commitCommand.run();
                break;
            default:
                logger.warn("Unknown broadcast data segment received: {}",
                            topology.getSegmentCase());
        }
    }

    private Collection<Consumer<RemoteIterator<TopologyDTO.Topology.DataSegment>>>
    createEntityConsumers(@Nonnull final TopologyInfo topologyInfo,
                          @Nonnull final SpanContext tracingContext) {
        logger.info("TopologyInfo : " + topologyInfo);
        return listeners.stream().map(listener -> {
            final Consumer<RemoteIterator<TopologyDTO.Topology.DataSegment>> consumer =
                iterator -> listener.onTopologyNotification(topologyInfo, iterator, tracingContext);
            return consumer;
        }).collect(Collectors.toList());
    }

    /**
     * Adds a listener to receive topology entities.
     *
     * @param listener listener to add
     * @throws IllegalStateException if listeners are not supported for this receiver.
     */
    public void addListener(@Nonnull EntitiesListener listener) {
        if (!active) {
            throw new IllegalStateException(
                "There is no subscription to thr requested topic." + " Cannot add listeners.");
        }
        listeners.add(Objects.requireNonNull(listener));
    }
}

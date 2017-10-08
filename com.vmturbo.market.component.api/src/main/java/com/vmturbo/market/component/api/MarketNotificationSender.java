package com.vmturbo.market;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology.Data;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology.End;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology.Start;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.chunking.MessageChunker;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.market.component.dto.MarketMessages.MarketComponentNotification;

/**
 * Handles the websocket connections with clients using the
 * {@link com.vmturbo.market.component.api.MarketComponent} API.
 */
public class MarketNotificationSender extends
        ComponentNotificationSender<MarketComponentNotification> {

    private final IMessageSender<MarketComponentNotification> topologySender;
    private final IMessageSender<MarketComponentNotification> notificationSender;

    // TODO remove in future after switch off websockets
    private final long chunkSendDelayMs;

    public MarketNotificationSender(@Nonnull final ExecutorService threadPool,
            long chunkSendDelayMs,
            @Nonnull IMessageSender<MarketComponentNotification> topologySender,
            @Nonnull IMessageSender<MarketComponentNotification> notificationSender) {
        super(threadPool);
        this.topologySender = Objects.requireNonNull(topologySender);
        this.notificationSender = Objects.requireNonNull(notificationSender);
        if (chunkSendDelayMs < 0) {
            throw new IllegalArgumentException("Chunk send delay must not be a negative value");
        } else {
            this.chunkSendDelayMs = chunkSendDelayMs;
        }
    }

    /**
     * Notify currently connected clients about actions the market is recommending.
     *
     * <p>Sends the notifications asynchronously to all clients connected at the time of the method call.
     * If the sending of a notification fails for any reason the notification does not get re-sent.
     *
     * @param actionPlan The {@link ActionPlan} protobuf objects describing the actions to execute.
     */
    public void notifyActionsRecommended(@Nonnull final ActionPlan actionPlan) {
        final MarketComponentNotification serverMessage = createNewMessage().setActionPlan(actionPlan).build();
        sendMessage(notificationSender, serverMessage);
    }

    /**
     * Notify currently connected clients about the projected topology.
     *
     * <p>Sends the notifications asynchronously to all clients connected at the time of the method call.
     * If the sending of a notification fails for any reason the notification does not get re-sent.
     *
     * @param srcTopologyId source id to use for broadcast.
     * @param projectedTopologyId projected topology id to use for broadcast.
     * @param topologyContextId context id where analysis has been run.
     * @param creationTime the time of original topology created.
     * @param projectedTopo protobuf objects describing the traders after plan execution.
     */
    public void notifyProjectedTopology(final long srcTopologyId, final long projectedTopologyId,
            final long topologyContextId, final TopologyType topologyType, final long creationTime,
            @Nonnull final Collection<TopologyEntityDTO> projectedTopo) {
        getExecutorService().submit(() -> {
            try {
                notifyTopologyInternal(srcTopologyId, projectedTopologyId, topologyContextId,
                        topologyType, creationTime, projectedTopo);
            } catch (InterruptedException e) {
                getLogger().info("Thread interrupted while sending projected topology " +
                        projectedTopologyId, e);
            } catch (RuntimeException e) {
                getLogger().error("Error sending projected topology " + projectedTopologyId, e);
            }
        });
    }

    private void notifyTopologyInternal(final long srcTopologyId, final long projectedTopologyId,
            final long topologyContextId, final TopologyType topologyType, final long creationTime,
            @Nonnull final Collection<TopologyEntityDTO> projectedTopo)
            throws InterruptedException {
        sendTopologySegment(ProjectedTopology.newBuilder()
                .setStart(Start.newBuilder()
                        .setSourceTopologyInfo(TopologyInfo.newBuilder()
                                .setTopologyId(srcTopologyId)
                                .setTopologyContextId(topologyContextId)
                                .setTopologyType(topologyType)
                                .setCreationTime(creationTime))
                        .build())
                .setTopologyId(projectedTopologyId)
                .build());
        final Iterable<Collection<TopologyEntityDTO>> chunks = MessageChunker.chunk(projectedTopo);
        long totalCount = 0;
        for (Collection<TopologyEntityDTO> chunk : chunks) {
            totalCount += chunk.size();
            final ProjectedTopology topology = ProjectedTopology.newBuilder()
                    .setData(Data.newBuilder().addAllEntities(chunk))
                    .setTopologyId(projectedTopologyId)
                    .build();
            sendTopologySegment(topology);
        }
        sendTopologySegment(ProjectedTopology.newBuilder()
                .setTopologyId(projectedTopologyId)
                .setEnd(End.newBuilder().setTotalCount(totalCount))
                .build());
    }

    private void sendTopologySegment(@Nonnull final ProjectedTopology topology) throws
            InterruptedException {
        final MarketComponentNotification serverMessage =
                createNewMessage().setProjectedTopology(topology).build();
        Thread.sleep(chunkSendDelayMs);
        sendMessage(topologySender, serverMessage);
    }

    @Nonnull
    private MarketComponentNotification.Builder createNewMessage() {
        return MarketComponentNotification.newBuilder()
            .setBroadcastId(newMessageChainId());
    }

    @Override
    protected String describeMessage(
            @Nonnull MarketComponentNotification marketComponentNotification) {
        return MarketComponentNotification.class.getSimpleName() + "[" +
                marketComponentNotification.getBroadcastId() + "]";
    }
}

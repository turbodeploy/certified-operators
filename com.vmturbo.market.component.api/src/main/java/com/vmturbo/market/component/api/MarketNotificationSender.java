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

/**
 * Handles the websocket connections with clients using the
 * {@link com.vmturbo.market.component.api.MarketComponent} API.
 */
public class MarketNotificationSender extends
        ComponentNotificationSender<ActionPlan> {

    private final IMessageSender<ProjectedTopology> projectedTopologySender;
    private final IMessageSender<ActionPlan> actionPlanSender;

    public MarketNotificationSender(@Nonnull final ExecutorService threadPool,
            @Nonnull IMessageSender<ProjectedTopology> projectedTopologySender,
            @Nonnull IMessageSender<ActionPlan> actionPlanSender) {
        super(threadPool);
        this.projectedTopologySender = Objects.requireNonNull(projectedTopologySender);
        this.actionPlanSender = Objects.requireNonNull(actionPlanSender);
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
        sendMessage(actionPlanSender, actionPlan);
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

    private void sendTopologySegment(@Nonnull final ProjectedTopology segment) throws
            InterruptedException {
        getLogger().debug("Sending topology {} segment {}", segment::getTopologyId,
                segment::getSegmentCase);
        projectedTopologySender.sendMessage(segment);
    }

    @Override
    protected String describeMessage(
            @Nonnull ActionPlan actionPlan) {
        return ActionPlan.class.getSimpleName() + "[" +
                actionPlan.getTopologyId() + "]";
    }
}

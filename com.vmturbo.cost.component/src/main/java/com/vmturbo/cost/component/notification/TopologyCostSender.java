package com.vmturbo.cost.component.notification;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.TopologyOnDemandCostChunk;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * Enable the cost component to send TopologyOnDemandCostChunks to subscribers.
 */
public class TopologyCostSender extends
        ComponentNotificationSender<TopologyOnDemandCostChunk> {

    /**
     * The sender of the chunks.
     */
    private final IMessageSender<TopologyOnDemandCostChunk> chunkSender;

    /**
     * Construct the TopologyCostSender.
     *
     * @param chunkSender The sender of chunks
     */
    public TopologyCostSender(
            @Nonnull IMessageSender<TopologyOnDemandCostChunk> chunkSender) {
        this.chunkSender = Objects.requireNonNull(chunkSender);
    }

    /**
     * Send a TopologyOnDemandCostChunk.
     *
     * @param costChunk The TopologyOnDemandCostChunk to send
     * @throws InterruptedException   if sending thread has been interrupted (message may or may not be sent)
     * @throws CommunicationException if persistent communication error occurred
     */
    public void sendTopologyCostChunk(@Nonnull final TopologyOnDemandCostChunk costChunk)
            throws CommunicationException, InterruptedException {
        sendMessage(chunkSender, costChunk);
    }

    @Override
    protected String describeMessage(@Nonnull final TopologyOnDemandCostChunk
                                             chunkMessage) {
        final StringBuffer description = new StringBuffer(TopologyOnDemandCostChunk.class.getSimpleName())
                .append(" for topology ").append(chunkMessage.getTopologyOid());
        if (chunkMessage.hasStart()) {
            final TopologyInfo topologyInfo = chunkMessage.getStart().getTopologyInfo();
            description.append(" topology context ID ").append(topologyInfo.getTopologyContextId())
                    .append(" type ").append(topologyInfo.getTopologyType())
                    .append(" created at ").append(topologyInfo.getCreationTime()).append(" START");
        } else if (chunkMessage.hasData()) {
            description.append(" chunk containing ").append(chunkMessage.getData().getEntityCostsCount())
                    .append(" entity costs");
        } else if (chunkMessage.hasEnd()) {
            description.append(" END");
        }
        return description.toString();
    }
}

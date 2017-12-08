package com.vmturbo.market;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology.Data;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology.End;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology.Start;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.MessageChunker;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessagePayload;

/**
 * Handles the websocket connections with clients using the
 * {@link com.vmturbo.market.component.api.MarketComponent} API.
 */
public class MarketNotificationSender extends
        ComponentNotificationSender<ActionPlan> {

    private final IMessageSender<ProjectedTopology> projectedTopologySender;
    private final IMessageSender<Topology> planAnalysisTopologySender;
    private final IMessageSender<ActionPlan> actionPlanSender;
    private final IMessageSender<PriceIndexMessage> priceIndexSender;

    public MarketNotificationSender(
            @Nonnull IMessageSender<ProjectedTopology> projectedTopologySender,
            @Nonnull IMessageSender<Topology> planAnalysisTopologySender,
            @Nonnull IMessageSender<ActionPlan> actionPlanSender,
            @Nonnull IMessageSender<PriceIndexMessage> priceIndexSender) {
        this.projectedTopologySender = Objects.requireNonNull(projectedTopologySender);
        this.planAnalysisTopologySender = Objects.requireNonNull(planAnalysisTopologySender);
        this.actionPlanSender = Objects.requireNonNull(actionPlanSender);
        this.priceIndexSender = Objects.requireNonNull(priceIndexSender);
    }

    /**
     * Send actions the market is recommending notification synchronously.
     *
     * @param actionPlan The {@link ActionPlan} protobuf objects describing the actions to execute.
     * @throws CommunicationException if persistent communication error occurs
     * @throws InterruptedException if thread is interrupted
     */
    public void notifyActionsRecommended(@Nonnull final ActionPlan actionPlan)
            throws CommunicationException, InterruptedException {
        sendMessage(actionPlanSender, actionPlan);
    }

    /**
     * Send a plan analysis topology to any interested consumers.
     *
     * @param sourceTopologyInfo The topology info from the plan source topology
     * @param topologyDTOs The entities that are being analyzed in the Plan
     * @throws CommunicationException if persistent communication error occurred
     * @throws InterruptedException if thread has been interrupted
     */
    public void notifyPlanAnalysisTopology(@Nonnull final TopologyInfo sourceTopologyInfo,
                                           @Nonnull final Collection<TopologyEntityDTO> topologyDTOs)
            throws CommunicationException, InterruptedException {
        sendPlanAnalysisTopologySegment(Topology.newBuilder()
                .setStart(Topology.Start.newBuilder()
                        .setTopologyInfo(sourceTopologyInfo)
                        .build())
                .setTopologyId(sourceTopologyInfo.getTopologyId())
                .build());
        final Iterable<Collection<TopologyEntityDTO>> chunks = MessageChunker.chunk(topologyDTOs);
        long totalCount = 0;
        for (Collection<TopologyEntityDTO> chunk : chunks) {
            totalCount += chunk.size();
            final Topology topology = Topology.newBuilder()
                    .setData(Topology.Data.newBuilder().addAllEntities(chunk).build())
                    .setTopologyId(sourceTopologyInfo.getTopologyId())
                    .build();
            sendPlanAnalysisTopologySegment(topology);
        }
        sendPlanAnalysisTopologySegment(Topology.newBuilder()
                .setTopologyId(sourceTopologyInfo.getTopologyId())
                .setEnd(Topology.End.newBuilder().setTotalCount(totalCount).build())
                .build());
    }

    private void sendPlanAnalysisTopologySegment(@Nonnull final Topology segment)
            throws CommunicationException, InterruptedException {
        getLogger().debug("Sending plan analysis topology {} segment {}", segment::getTopologyId,
                segment::getSegmentCase);
        planAnalysisTopologySender.sendMessage(segment);
    }

    /**
     * Send projected topology notification synchronously.
     *
     * @param originalTopologyInfo The {@link TopologyInfo} describing the original topology.
     * @param projectedTopologyId The ID of the projected topology.
     * @param projectedTopo The protobuf objects describing the traders after plan execution.
     * @throws CommunicationException if persistent communication error occurs
     * @throws InterruptedException if thread interrupted
     */
    public void notifyProjectedTopology(@Nonnull final TopologyInfo originalTopologyInfo,
                                    final long projectedTopologyId,
                                        @Nonnull final Collection<TopologyEntityDTO> projectedTopo)
            throws CommunicationException, InterruptedException {
        sendProjectedTopologySegment(ProjectedTopology.newBuilder()
                .setStart(Start.newBuilder()
                        .setSourceTopologyInfo(originalTopologyInfo)
                        .build())
                .setTopologyId(projectedTopologyId)
                .build());
        final Iterable<Collection<TopologyEntityDTO>> chunks = MessageChunker.chunk(projectedTopo);
        long totalCount = 0;
        for (Collection<TopologyEntityDTO> chunk : chunks) {
            totalCount += chunk.size();
            final ProjectedTopology topology = ProjectedTopology.newBuilder()
                    .setData(Data.newBuilder().addAllEntities(chunk).build())
                    .setTopologyId(projectedTopologyId)
                    .build();
            sendProjectedTopologySegment(topology);
        }
        sendProjectedTopologySegment(ProjectedTopology.newBuilder()
                .setTopologyId(projectedTopologyId)
                .setEnd(End.newBuilder().setTotalCount(totalCount).build())
                .build());
    }

    private void sendProjectedTopologySegment(@Nonnull final ProjectedTopology segment)
            throws CommunicationException, InterruptedException {
        getLogger().debug("Sending topology {} segment {}", segment::getTopologyId,
                segment::getSegmentCase);
        projectedTopologySender.sendMessage(segment);
    }

    /**
     * Notify the counterpart about the PriceIndices for all the traders in the market.
     *
     * @param topologyInfo The {@link TopologyInfo} of the topology the price index describes.
     * @param priceIndexMessage The message to send.
     * @throws InterruptedException if thread has been interrupted
     * @throws CommunicationException if persistent communication error occurred
     */
    public void sendPriceIndex(@Nonnull final TopologyInfo topologyInfo,
            final PriceIndexMessage priceIndexMessage)
            throws CommunicationException, InterruptedException {
        PriceIndexMessage.Builder builder = PriceIndexMessage.newBuilder();
        final PriceIndexMessage serverMessage = builder.addAllPayload(
                priceIndexMessage.getPayloadList()
                        .stream()
                        .map(p -> createPayload(p.getOid(), (float)p.getPriceindexCurrent(),
                                (float)p.getPriceindexProjected()))
                        .collect(Collectors.toList()))
                .setMarketId(priceIndexMessage.getMarketId())
                .setTopologyContextId(priceIndexMessage.getTopologyContextId())
                .setTopologyId(topologyInfo.getTopologyId())
                .setSourceTopologyCreationTime(topologyInfo.getCreationTime())
                .build();
        priceIndexSender.sendMessage(serverMessage);
        getLogger().info("Successfully sent price index information for {}",
                topologyInfo.getTopologyId());
    }

    /**
     * Creates the payload.
     *
     * @param oid The OID.
     * @param piNow The current Price Index.
     * @param piProjected The projected Price Index.
     * @return The price index message payload.
     */
    @Nonnull
    private PriceIndexMessagePayload createPayload(final long oid, final float piNow,
            final float piProjected) {
        return PriceIndexMessagePayload.newBuilder()
                .setOid(oid)
                .setPriceindexCurrent(piNow)
                .setPriceindexProjected(piProjected)
                .build();
    }

    @Override
    protected String describeMessage(
            @Nonnull ActionPlan actionPlan) {
        return ActionPlan.class.getSimpleName() + "[" +
                actionPlan.getTopologyId() + "]";
    }
}

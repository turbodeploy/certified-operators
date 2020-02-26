package com.vmturbo.market;

import java.util.Collection;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ProjectedEntityCosts;
import com.vmturbo.common.protobuf.cost.Cost.ProjectedEntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ActionPlanSummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisSummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology.Data;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology.End;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology.Start;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.DataSegment;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.chunking.OversizedElementException;
import com.vmturbo.components.api.chunking.ProtobufChunkIterator;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * Handles the websocket connections with clients using the
 * {@link com.vmturbo.market.component.api.MarketComponent} API.
 */
public class MarketNotificationSender extends ComponentNotificationSender<ActionPlan> {

    private final IMessageSender<AnalysisSummary> analysisSummarySender;
    private final IMessageSender<ProjectedTopology> projectedTopologySender;
    private final IMessageSender<ProjectedEntityCosts> projectedEntityCostsSender;
    private final IMessageSender<ProjectedEntityReservedInstanceCoverage> projectedEntityRiCoverageSender;
    private final IMessageSender<Topology> planAnalysisTopologySender;
    private final IMessageSender<ActionPlan> actionPlanSender;
    private final IMessageSender<AnalysisStatusNotification> analysisStatusSender;

    public MarketNotificationSender(
            @Nonnull IMessageSender<ProjectedTopology> projectedTopologySender,
            @Nonnull IMessageSender<ProjectedEntityCosts> projectedEntityCostsSender,
            @Nonnull IMessageSender<ProjectedEntityReservedInstanceCoverage> projectedEntityRiCoverageSender,
            @Nonnull IMessageSender<Topology> planAnalysisTopologySender,
            @Nonnull IMessageSender<ActionPlan> actionPlanSender,
            @Nonnull IMessageSender<AnalysisSummary> analysisSummarySender,
            @Nonnull IMessageSender<AnalysisStatusNotification>  analysisStatusSender) {
        this.projectedTopologySender = Objects.requireNonNull(projectedTopologySender);
        this.projectedEntityCostsSender = Objects.requireNonNull(projectedEntityCostsSender);
        this.projectedEntityRiCoverageSender = Objects.requireNonNull(projectedEntityRiCoverageSender);
        this.planAnalysisTopologySender = Objects.requireNonNull(planAnalysisTopologySender);
        this.actionPlanSender = Objects.requireNonNull(actionPlanSender);
        this.analysisSummarySender = Objects.requireNonNull(analysisSummarySender);
        this.analysisStatusSender = Objects.requireNonNull(analysisStatusSender);
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

        final ProtobufChunkIterator<TopologyEntityDTO> iterator = ProtobufChunkIterator.partition(topologyDTOs,
            planAnalysisTopologySender.getRecommendedRequestSizeBytes(),
            planAnalysisTopologySender.getMaxRequestSizeBytes());
        long totalCount = 0;

        while (iterator.hasNext()) {
            final Collection<TopologyEntityDTO> nextChunk;
            try {
                nextChunk = iterator.next();
            } catch (OversizedElementException e) {
                // This MAY happen in some very unusual customer topologies. A single entity would have
                // to be larger than the 64MB limit, which is highly unlikely. We keep sending what
                // we can.
                logOversizedElement("plan analysis topology", e, sourceTopologyInfo);
                continue;
            }
            totalCount += nextChunk.size();
            final Topology.Builder topologyBldr = Topology.newBuilder()
                .setTopologyId(sourceTopologyInfo.getTopologyId());
            for (TopologyEntityDTO entity : nextChunk) {
                topologyBldr.getDataBuilder().addEntities(DataSegment.newBuilder()
                    .setEntity(entity));
            }
            sendPlanAnalysisTopologySegment(topologyBldr.build());
        }

        sendPlanAnalysisTopologySegment(Topology.newBuilder()
                .setTopologyId(sourceTopologyInfo.getTopologyId())
                .setEnd(Topology.End.newBuilder().setTotalCount(totalCount).build())
                .build());
    }

    private void logOversizedElement(@Nonnull final String elementType,
                                     @Nonnull final OversizedElementException e,
                                     @Nonnull final TopologyInfo topologyInfo) {
        getLogger().error("A chunk of the {} failed to be sent because an element is too " +
            "large (topology {}, context {}). Message: {}", elementType,
            topologyInfo.getTopologyId(), topologyInfo.getTopologyContextId(), e.getMessage());
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
                                    @Nonnull final Collection<ProjectedTopologyEntity> projectedTopo,
                                        final long actionPlanId)
            throws CommunicationException, InterruptedException {
        sendProjectedTopologySegment(ProjectedTopology.newBuilder()
                .setStart(Start.newBuilder()
                        .setSourceTopologyInfo(originalTopologyInfo)
                        .build())
                .setTopologyId(projectedTopologyId)
                .build());

        final ProtobufChunkIterator<ProjectedTopologyEntity> chunkIterator =
            ProtobufChunkIterator.partition(projectedTopo,
                projectedTopologySender.getRecommendedRequestSizeBytes(),
                projectedTopologySender.getMaxRequestSizeBytes());
        long totalCount = 0;
        while (chunkIterator.hasNext()) {
            Collection<ProjectedTopologyEntity> chunk = null;
            try {
                chunk = chunkIterator.next();
            } catch (OversizedElementException e) {
                // This MAY happen in some very unusual customer topologies. A single entity would have
                // to be larger than the 64MB limit, which is highly unlikely. We keep sending what
                // we can.
                logOversizedElement("projected topology", e, originalTopologyInfo);
                continue;
            }
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
        sendAnalysisSummary(projectedTopologyId, originalTopologyInfo, actionPlanId);
    }

    /**
     * Send projected entity costs notification synchronously. Synchronously means this method
     * will not return until all costs have been sent over to the message broker.
     *
     * @param originalTopologyInfo The {@link TopologyInfo} describing the original topology.
     * @param projectedTopologyId The ID of the projected topology.
     * @param entityCosts The entity costs of cloud entities in the projected topology.
     * @throws CommunicationException if persistent communication error occurs
     * @throws InterruptedException if thread interrupted
     */
    public void notifyProjectedEntityCosts(@Nonnull final TopologyInfo originalTopologyInfo,
                                           final long projectedTopologyId,
                                           @Nonnull final Collection<EntityCost> entityCosts)
            throws CommunicationException, InterruptedException {

        sendProjectedEntityCostSegment(ProjectedEntityCosts.newBuilder()
                .setStart(ProjectedEntityCosts.Start.newBuilder()
                        .setSourceTopologyInfo(originalTopologyInfo))
                .setProjectedTopologyId(projectedTopologyId)
                .build());
        final ProtobufChunkIterator<EntityCost> chunkIterator =
            ProtobufChunkIterator.partition(entityCosts,
                projectedEntityCostsSender.getRecommendedRequestSizeBytes(),
                projectedEntityCostsSender.getMaxRequestSizeBytes());
        long totalCount = 0;
        while (chunkIterator.hasNext()) {
            final Collection<EntityCost> costChunk;
            try {
                costChunk = chunkIterator.next();
            } catch (OversizedElementException e) {
                // This should never happen because costs are tiny (compared to the max request size)!
                logOversizedElement("projected entity costs", e, originalTopologyInfo);
                continue;
            }
            totalCount += costChunk.size();
            sendProjectedEntityCostSegment(ProjectedEntityCosts.newBuilder()
                .setProjectedTopologyId(projectedTopologyId)
                .setData(ProjectedEntityCosts.Data.newBuilder()
                        .addAllEntityCosts(costChunk))
                .build());
        }
        sendProjectedEntityCostSegment(ProjectedEntityCosts.newBuilder()
            .setProjectedTopologyId(projectedTopologyId)
            .setEnd(ProjectedEntityCosts.End.newBuilder()
                    .setTotalCount(totalCount))
            .build());
    }

    /**
     * Send projected entity reserved instance coverage notification synchronously.
     * Synchronously means this method will not return until all costs have been
     * sent over to the message broker.
     *
     * @param originalTopologyInfo
     *            The {@link TopologyInfo} describing the original topology.
     * @param projectedTopologyId
     *            The ID of the projected topology.
     * @param projectedCoverage
     *            The entity reserved instance coverage of cloud entities in the
     *            projected topology.
     * @throws CommunicationException
     *             if persistent communication error occurs
     * @throws InterruptedException
     *             if thread interrupted
     */
    public void notifyProjectedEntityRiCoverage(@Nonnull final TopologyInfo originalTopologyInfo,
                    final long projectedTopologyId,
                    @Nonnull final Collection<EntityReservedInstanceCoverage> projectedCoverage)
                    throws CommunicationException, InterruptedException {

        sendProjectedEntityRiCoverageSegment(ProjectedEntityReservedInstanceCoverage.newBuilder()
                        .setStart(ProjectedEntityReservedInstanceCoverage.Start.newBuilder()
                                        .setSourceTopologyInfo(originalTopologyInfo))
                        .setProjectedTopologyId(projectedTopologyId).build());
        final ProtobufChunkIterator<EntityReservedInstanceCoverage> chunkIterator =
            ProtobufChunkIterator.partition(projectedCoverage,
                projectedEntityRiCoverageSender.getRecommendedRequestSizeBytes(),
                projectedEntityRiCoverageSender.getMaxRequestSizeBytes());
        long totalCount = 0;
        while (chunkIterator.hasNext()) {
            final Collection<EntityReservedInstanceCoverage> coverageChunk;
            try {
                coverageChunk = chunkIterator.next();
            } catch (OversizedElementException e) {
                // This should never happen because RI coverage messages are tiny
                // (compared to the max request size)!
                logOversizedElement("projected entity RI", e, originalTopologyInfo);
                continue;
            }
            totalCount += coverageChunk.size();
            sendProjectedEntityRiCoverageSegment(ProjectedEntityReservedInstanceCoverage
                            .newBuilder().setProjectedTopologyId(projectedTopologyId)
                            .setData(ProjectedEntityReservedInstanceCoverage.Data.newBuilder()
                                            .addAllProjectedRisCoverage(coverageChunk))
                            .build());
        }
        sendProjectedEntityRiCoverageSegment(
                        ProjectedEntityReservedInstanceCoverage.newBuilder()
                                        .setProjectedTopologyId(projectedTopologyId)
                                        .setEnd(ProjectedEntityReservedInstanceCoverage.End
                                                        .newBuilder().setTotalCount(totalCount))
                                        .build());
    }

    private void sendProjectedEntityRiCoverageSegment(
                    @Nonnull final ProjectedEntityReservedInstanceCoverage entityRiCoverageSegment)
                    throws CommunicationException, InterruptedException {
        getLogger().debug("Sending projected entity cost segment {} for topology {}",
                        entityRiCoverageSegment::getSegmentCase,
                        entityRiCoverageSegment::getProjectedTopologyId);
        projectedEntityRiCoverageSender.sendMessage(entityRiCoverageSegment);

    }

    private void sendProjectedEntityCostSegment(@Nonnull final ProjectedEntityCosts entityCostSegment)
            throws CommunicationException, InterruptedException {
        getLogger().debug("Sending projected entity cost segment {} for topology {}",
                entityCostSegment::getSegmentCase, entityCostSegment::getProjectedTopologyId);
        projectedEntityCostsSender.sendMessage(entityCostSegment);

    }

    private void sendProjectedTopologySegment(@Nonnull final ProjectedTopology segment)
            throws CommunicationException, InterruptedException {
        getLogger().debug("Sending topology {} segment {}", segment::getTopologyId,
                segment::getSegmentCase);
        projectedTopologySender.sendMessage(segment);
    }

    private void sendAnalysisSummary(@Nonnull final long projectdTopologyId,
                                     @Nonnull final TopologyInfo sourceTopologyInfo,
                                     @Nonnull final long actionPlanId) {
        try {
            analysisSummarySender.sendMessage(
                AnalysisSummary.newBuilder()
                    .setProjectedTopologyInfo(ProjectedTopologyInfo.newBuilder().setProjectedTopologyId(projectdTopologyId))
                    .setSourceTopologyInfo(sourceTopologyInfo)
                    .setActionPlanSummary(ActionPlanSummary.newBuilder().setActionPlanId(actionPlanId))
                    .build());
            getLogger().debug("Sending analysys results for projected topology with id {}",
                projectdTopologyId);
        } catch (CommunicationException|InterruptedException e) {
            getLogger().error("Could not send TopologySummary message", e);
        }
    }

    /**
     * Send notification about the status of a market analysis run.
     *
     * @param sourceTopologyInfo Source topology id associated with the market tun.
     * @param analysisState The state of the market run (FAILED/SUCCEEDED).
     */
   public void sendAnalysisStatus(@Nonnull final TopologyInfo sourceTopologyInfo,
                                     final int analysisState) {
        try {
            final AnalysisStatusNotification.Builder statusBuilder = AnalysisStatusNotification.newBuilder();
            statusBuilder.setTopologyContextId(sourceTopologyInfo.getTopologyContextId());
            statusBuilder.setTopologyId(sourceTopologyInfo.getTopologyId());
            statusBuilder.setStatus(analysisState);
            getLogger().debug("Sending analysis status src topology with id {}",
                              sourceTopologyInfo.getTopologyId());
            analysisStatusSender.sendMessage(statusBuilder.build());
        } catch (CommunicationException | InterruptedException e) {
            getLogger().error("Could not send AnalysisStatusNotification message", e);
        }
    }

    @Override
    protected String describeMessage(
            @Nonnull ActionPlan actionPlan) {
        return ActionPlan.class.getSimpleName() + "[" +
                actionPlan.getInfo() + "]";
    }
}

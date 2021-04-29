package com.vmturbo.cost.component.topology;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.opentracing.SpanContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdate;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdate.Builder;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdateType;
import com.vmturbo.common.protobuf.plan.PlanProgressStatusEnum.Status;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.entity.cost.EntityCostStore;
import com.vmturbo.cost.component.notification.CostNotificationSender;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsWriter;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceCoverageUpdate;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisInvoker;
import com.vmturbo.cost.component.util.BusinessAccountHelper;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.topology.processor.api.EntitiesListener;

public class PlanTopologyEntitiesListener implements EntitiesListener {

    private final Logger logger = LogManager.getLogger();

    private final long realtimeTopologyContextId;
    private final ComputeTierDemandStatsWriter computeTierDemandStatsWriter;
    private final TopologyCostCalculatorFactory topologyCostCalculatorFactory;
    private final EntityCostStore planEntityCostStore;
    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory;
    private final ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate;
    private final BusinessAccountHelper businessAccountHelper;
    private final ReservedInstanceAnalysisInvoker invoker;
    private final CostNotificationSender costNotificationSender;

    public PlanTopologyEntitiesListener(final long realtimeTopologyContextId,
                                        @Nonnull final ComputeTierDemandStatsWriter computeTierDemandStatsWriter,
                                        @Nonnull final TopologyEntityCloudTopologyFactory cloudTopologyFactory,
                                        @Nonnull final TopologyCostCalculatorFactory topologyCostCalculatorFactory,
                                        @Nonnull final EntityCostStore planEntityCostStore,
                                        @Nonnull final ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate,
                                        @Nonnull final BusinessAccountHelper businessAccountHelper,
                                        @Nonnull final ReservedInstanceAnalysisInvoker invoker,
                                        @Nonnull final CostNotificationSender costNotificationSender) {
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.computeTierDemandStatsWriter = Objects.requireNonNull(computeTierDemandStatsWriter);
        this.cloudTopologyFactory = cloudTopologyFactory;
        this.topologyCostCalculatorFactory = Objects.requireNonNull(topologyCostCalculatorFactory);
        this.planEntityCostStore = Objects.requireNonNull(planEntityCostStore);
        this.reservedInstanceCoverageUpdate = Objects.requireNonNull(reservedInstanceCoverageUpdate);
        this.businessAccountHelper = Objects.requireNonNull(businessAccountHelper);
        this.invoker = Objects.requireNonNull(invoker);
        this.costNotificationSender = Objects.requireNonNull(costNotificationSender);
    }

    @Override
    public void onTopologyNotification(
                @Nonnull final TopologyInfo topologyInfo,
                @Nonnull final RemoteIterator<TopologyDTO.Topology.DataSegment> entityIterator,
                @Nonnull final SpanContext tracingContext) {
        final long topologyContextId = topologyInfo.getTopologyContextId();
        try (TracingScope tracingScope = Tracing.trace("cost_plan_topology", tracingContext)) {
            if (topologyContextId == realtimeTopologyContextId) {
                logger.error("Received plan topology with wrong topologyContextId."
                    + "Expected:{}, Received:{}", topologyContextId, realtimeTopologyContextId);
                return;
            }
            logger.info("Received plan topology with topologyId: {}", topologyInfo.getTopologyId());
            final CloudTopology<TopologyEntityDTO> cloudTopology =
                cloudTopologyFactory.newCloudTopology(topologyContextId, entityIterator);

            // if no Cloud entity, skip further processing
            if (cloudTopology.size() > 0) {
                final TopologyCostCalculator topologyCostCalculator =
                    topologyCostCalculatorFactory.newCalculator(topologyInfo, cloudTopology);
                final Map<Long, CostJournal<TopologyEntityDTO>> costs =
                    topologyCostCalculator.calculateCosts(cloudTopology);
                try {
                    // Persist plan entity cost data
                    planEntityCostStore.persistEntityCost(costs, cloudTopology, topologyContextId, true);
                    // Send the plan entity cost status notification - SUCCESS.
                    sendPlanEntityCostNotification(buildPlanEntityCostNotification(topologyInfo,
                            Status.SUCCESS, null));
                } catch (DbException e) {
                    logger.error("Failed to persist plan entity costs.", e);
                    // Send the plan entity cost status notification - FAIL.
                    sendPlanEntityCostNotification(buildPlanEntityCostNotification(topologyInfo,
                            Status.FAIL, "Failed to persist plan entity costs."));
                }
            } else {
                logger.debug("Plan topology with topologyId: {}  doesn't have Cloud entity, skip processing",
                    topologyInfo.getTopologyId());
                // Send the plan entity cost status notification.
                // Send a success message because nothing need to be done.
                sendPlanEntityCostNotification(buildPlanEntityCostNotification(topologyInfo,
                        Status.SUCCESS, null));
            }
        }
    }

    /**
     * Sends notification after plan topology costs have been persisted.
     *
     * @param planEntityCostNotification The cost notification
     */
    private void sendPlanEntityCostNotification(@Nonnull final CostNotification planEntityCostNotification) {
        final StatusUpdate planEntityCostUpdate = planEntityCostNotification.getStatusUpdate();
        try {
            costNotificationSender.sendStatusNotification(planEntityCostNotification);
            logger.info("The plan entity cost notification has been sent successfully. Topology "
                            + "ID: {} topology context ID: {} status: {}",
                    planEntityCostUpdate.getTopologyId(),
                    planEntityCostUpdate.getTopologyContextId(),
                    planEntityCostUpdate.getStatus());
        } catch (CommunicationException | InterruptedException e) {
            logger.error("An error happened in sending the plan entity cost notification."
                    + "Topology ID: {} Topology context ID: {}", planEntityCostUpdate.getTopologyId(),
                    planEntityCostUpdate.getTopologyContextId(), e);
        }
    }

    /**
     * Builds a plan entity cost notification.
     *
     * @param originalTopologyInfo The topology info
     * @param status               The status of the plan entity cost processing
     * @param description          The description of the error if any
     * @return The plan entity cost notification
     */
    private CostNotification buildPlanEntityCostNotification(
            @Nonnull final TopologyInfo originalTopologyInfo,
            @Nonnull final Status status,
            @Nullable final String description) {
        final Builder planEntityCostNotificationBuilder = StatusUpdate.newBuilder()
                .setType(StatusUpdateType.PLAN_ENTITY_COST_UPDATE)
                .setTopologyId(originalTopologyInfo.getTopologyId())
                .setTopologyContextId(originalTopologyInfo.getTopologyContextId())
                .setStatus(status);
        // It adds the error description to the notification.
        if (description != null) {
            planEntityCostNotificationBuilder.setStatusDescription(description);
        }
        return CostNotification.newBuilder()
                .setStatusUpdate(planEntityCostNotificationBuilder).build();
    }
}

package com.vmturbo.cost.component.entity.cost;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.Status;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdate;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdate.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.cost.component.notification.CostNotificationSender;
import com.vmturbo.market.component.api.ProjectedEntityCostsListener;

/**
 * Listener that receives the projected entity costs from the market and forwards them to
 * the classes in the cost component that store them and make them available for queries.
 */
public class CostComponentProjectedEntityCostListener implements ProjectedEntityCostsListener {

    private static final Logger logger = LogManager.getLogger();

    private final ProjectedEntityCostStore projectedEntityCostStore;

    private final PlanProjectedEntityCostStore planProjectedEntityCostStore;

    private final CostNotificationSender costNotificationSender;

    CostComponentProjectedEntityCostListener(
            @Nonnull final ProjectedEntityCostStore projectedEntityCostStore,
            @Nonnull final PlanProjectedEntityCostStore planProjectedEntityCostStore,
            @Nonnull final CostNotificationSender costNotificationSender) {
        this.projectedEntityCostStore = Objects.requireNonNull(projectedEntityCostStore);
        this.planProjectedEntityCostStore = Objects.requireNonNull(planProjectedEntityCostStore);
        this.costNotificationSender = Objects.requireNonNull(costNotificationSender);
    }

    @Override
    public void onProjectedEntityCostsReceived(final long projectedTopologyId,
                                               @Nonnull final TopologyInfo originalTopologyInfo,
                                               @Nonnull final RemoteIterator<EntityCost> entityCosts) {
        logger.debug("Receiving projected entity costs for topology {}", projectedTopologyId);
        final List<EntityCost> costList = new ArrayList<>();
        long costCount = 0;
        int chunkCount = 0;
        while (entityCosts.hasNext()) {
            try {
                final Collection<EntityCost> nextChunk = entityCosts.nextChunk();
                for (EntityCost cost : nextChunk) {
                    costCount++;
                    costList.add(cost);
                }
                chunkCount++;
                // TODO (OM-51227): Handle the error properly. If one of the chunks fails.
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for processing projected entity costs chunk." +
                        "Processed " + chunkCount + " chunks so far.", e);
            } catch (TimeoutException e) {
                logger.error("Timed out waiting for next entity costs chunk." +
                        " Processed " + chunkCount + " chunks so far.", e);
            } catch (CommunicationException e) {
                logger.error("Connection error when waiting for next entity costs chunk." +
                        " Processed " + chunkCount + " chunks so far.", e);
            }
        }
        try {
            if (TopologyType.PLAN.equals(originalTopologyInfo.getTopologyType())) {
                planProjectedEntityCostStore.updatePlanProjectedEntityCostsTableForPlan(
                        originalTopologyInfo, costList);
            } else {
                projectedEntityCostStore.updateProjectedEntityCosts(costList);
            }
            sendCostNotification(buildCostNotification(originalTopologyInfo, Status.SUCCESS));
        } catch (Exception e) {
            logger.error(e);
            sendCostNotification(buildCostNotification(originalTopologyInfo, Status.FAIL, e.getMessage()));
        }
        logger.debug("Finished processing projected entity costs. Got costs for {} entities, " +
                "delivered in {} chunks.", costCount, chunkCount);
    }

    /**
     * Sends the cost notification.
     *
     * @param costNotification The cost notification
     * @throws InterruptedException   if sending thread has been interrupted. This does not
     *                                guarantee, that message has ben sent nor it has not been sent
     * @throws CommunicationException if persistent communication error occurred (message could
     *                                not be sent in future).
     */
    private void sendCostNotification(@Nonnull final CostNotification costNotification) {
        try {
            costNotificationSender.sendNotification(costNotification);
        } catch (CommunicationException | InterruptedException e) {
            logger.error("An error happened in sending the cost notification.", e);
        }
    }

    /**
     * Builds a cost notification object based on the input status. This method is useful if the
     * notification is success.
     *
     * @param originalTopologyInfo The original topology info
     * @param status               The status of the cost processing
     * @return The cost notification object based on the status
     */
    private CostNotification buildCostNotification(@Nonnull final TopologyInfo originalTopologyInfo,
                                                   @Nonnull final Status status) {
        return buildCostNotification(originalTopologyInfo, status, null);
    }

    /**
     * Builds a cost notification object based on the input status. This method is useful if an
     * error happens.
     *
     * @param originalTopologyInfo The original topology info
     * @param status               The status of the cost processing
     * @param description          The description of the error
     * @return The cost notification object based on the status
     */
    private CostNotification buildCostNotification(@Nonnull final TopologyInfo originalTopologyInfo,
                                                   @Nonnull final Status status,
                                                   @Nullable final String description) {
        final Builder projectedNotificationBuilder = StatusUpdate.newBuilder()
                .setTopologyId(originalTopologyInfo.getTopologyId())
                .setTopologyContextId(originalTopologyInfo.getTopologyContextId())
                .setStatus(status);
        // It adds the error description to the notification.
        if (description != null) {
            projectedNotificationBuilder.setStatusDescription(description);
        }
        return CostNotification.newBuilder()
                .setProjectedCostUpdate(projectedNotificationBuilder.build())
                .build();
    }
}

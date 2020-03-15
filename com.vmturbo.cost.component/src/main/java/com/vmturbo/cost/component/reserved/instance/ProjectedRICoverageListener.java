package com.vmturbo.cost.component.reserved.instance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdate;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdate.Builder;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdateType;
import com.vmturbo.common.protobuf.plan.PlanProgressStatusEnum.Status;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.cost.component.notification.CostNotificationSender;
import com.vmturbo.market.component.api.ProjectedReservedInstanceCoverageListener;

/**
 * Listener that receives the projected entity RI coverage from the market and forwards them to
 * the classes in the cost component that store them and make them available for queries.
 */
public class ProjectedRICoverageListener implements ProjectedReservedInstanceCoverageListener {

    private static final Logger logger = LogManager.getLogger();

    private final ProjectedRICoverageAndUtilStore projectedRICoverageAndUtilStore;

    private final PlanProjectedRICoverageAndUtilStore planProjectedRICoverageAndUtilStore;

    private final CostNotificationSender costNotificationSender;

    ProjectedRICoverageListener(@Nonnull final ProjectedRICoverageAndUtilStore projectedRICoverageStore,
                                @Nonnull final PlanProjectedRICoverageAndUtilStore planProjectedRICoverageAndUtilStore,
                                @Nonnull final CostNotificationSender costNotificationSender) {
        this.projectedRICoverageAndUtilStore = Objects.requireNonNull(projectedRICoverageStore);
        this.planProjectedRICoverageAndUtilStore = Objects.requireNonNull(planProjectedRICoverageAndUtilStore);
        this.costNotificationSender = Objects.requireNonNull(costNotificationSender);
    }

    @Override
    public void onProjectedEntityRiCoverageReceived(final long projectedTopologyId,
                                                    @Nonnull final TopologyInfo originalTopologyInfo,
                                                    @Nonnull final RemoteIterator<EntityReservedInstanceCoverage>
                                                    riCoverageIterator) {
        logger.debug("Receiving projected RI coverage information for topology {}", projectedTopologyId);
        final List<EntityReservedInstanceCoverage> riCoverageList = new ArrayList<>();
        long coverageCount = 0;
        int chunkCount = 0;
        try {
            while (riCoverageIterator.hasNext()) {
                try {
                    final Collection<EntityReservedInstanceCoverage> nextChunk = riCoverageIterator.nextChunk();
                    for (EntityReservedInstanceCoverage riCoverage : nextChunk) {
                        coverageCount++;
                        riCoverageList.add(riCoverage);
                    }
                    chunkCount++;
                } catch (InterruptedException e) {
                    logger.error("Interrupted while waiting for processing projected RI coverage chunk." +
                            "Processed " + chunkCount + " chunks so far.", e);
                } catch (TimeoutException e) {
                    logger.error("Timed out waiting for next entity RI coverage chunk." +
                            " Processed " + chunkCount + " chunks so far.", e);
                } catch (CommunicationException e) {
                    logger.error("Connection error when waiting for next entity RI coverage chunk." +
                            " Processed " + chunkCount + " chunks so far.", e);
                }
            }
            if (originalTopologyInfo.getTopologyType() == TopologyType.PLAN) {
                // Update DB tables with the RI coverage results from the Actions:
                // PLAN_PROJECTED_ENTITY_TO_RESERVED_INSTANCE_MAPPING
                planProjectedRICoverageAndUtilStore.updateProjectedEntityToRIMappingTableForPlan(originalTopologyInfo,
                        riCoverageList);
                // PLAN_PROJECTED_RESERVED_INSTANCE_COVERAGE
                planProjectedRICoverageAndUtilStore.updateProjectedRICoverageTableForPlan(projectedTopologyId,
                        originalTopologyInfo,
                        riCoverageList);
                // PLAN_PROJECTED_RESERVED_INSTANCE_UTILIZATION
                planProjectedRICoverageAndUtilStore.updateProjectedRIUtilTableForPlan(originalTopologyInfo,
                        riCoverageList);
            } else {
                projectedRICoverageAndUtilStore.updateProjectedRICoverage(originalTopologyInfo,
                        riCoverageList);
            }
            // Send the projected RI coverage status notification.
            sendProjectedRiCoverageNotification(buildProjectedRiCoverageNotification(
                    originalTopologyInfo, Status.SUCCESS));
            logger.debug("Finished processing projected RI coverage info. Got RI coverage for {} entities, " +
                    "delivered in {} chunks.", coverageCount, chunkCount);
        } catch (Exception e) {
            logger.error("Error in processing the projected RI coverage. Processed " +
                    chunkCount + " chunks so far.", e);
            sendProjectedRiCoverageNotification(buildProjectedRiCoverageNotification(
                    originalTopologyInfo, Status.FAIL));
        }
    }

    /**
     * Sends the projected RI coverage notification.
     *
     * @param projectedRiCoverageNotification The projected RI coverage notification
     */
    private void sendProjectedRiCoverageNotification(
            @Nonnull final CostNotification projectedRiCoverageNotification) {
        try {
            costNotificationSender.sendStatusNotification(projectedRiCoverageNotification);
            final StatusUpdate projectedRiCoverageUpdate = projectedRiCoverageNotification.getStatusUpdate();
            logger.debug("The projected RI coverage notification has been sent successfully. " +
                            "topology ID: {} topology context ID: {} status: {}",
                    projectedRiCoverageUpdate.getTopologyId(),
                    projectedRiCoverageUpdate.getTopologyContextId(),
                    projectedRiCoverageUpdate.getStatus());
        } catch (CommunicationException | InterruptedException e) {
            logger.error("An error happened in sending the projected RI coverage notification.",
                    e);
        }
    }

    /**
     * Builds a projected RI coverage notification based on the input status. This method is
     * useful if the notification is success.
     *
     * @param originalTopologyInfo The original topology info
     * @param status               The status of the cost processing
     * @return The projected RI coverage notification
     */
    private CostNotification buildProjectedRiCoverageNotification(
            @Nonnull final TopologyInfo originalTopologyInfo,
            @Nonnull final Status status) {
        return buildProjectedRiCoverageNotification(originalTopologyInfo, status, null);
    }

    /**
     * Builds a projected RI coverage notification based on the input status. This method is
     * useful if an error happens.
     *
     * @param originalTopologyInfo The original topology info
     * @param status               The status of the cost processing
     * @param description          The description of the error
     * @return The projected RI coverage notification
     */
    private CostNotification buildProjectedRiCoverageNotification(
            @Nonnull final TopologyInfo originalTopologyInfo,
            @Nonnull final Status status,
            @Nullable final String description) {
        final Builder projectedRiCoverageNotificationBuilder = StatusUpdate.newBuilder()
                .setType(StatusUpdateType.PROJECTED_RI_COVERAGE_UPDATE)
                .setTopologyId(originalTopologyInfo.getTopologyId())
                .setTopologyContextId(originalTopologyInfo.getTopologyContextId())
                .setStatus(status);
        // It adds the error description to the notification.
        if (description != null) {
            projectedRiCoverageNotificationBuilder.setStatusDescription(description);
        }
        return CostNotification.newBuilder()
                .setStatusUpdate(projectedRiCoverageNotificationBuilder).build();
    }

}

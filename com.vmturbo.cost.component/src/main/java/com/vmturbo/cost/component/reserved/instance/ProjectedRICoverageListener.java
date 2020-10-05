package com.vmturbo.cost.component.reserved.instance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import java.util.Objects;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Maps;

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
import com.vmturbo.commons.Pair;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.cost.component.notification.CostNotificationSender;
import com.vmturbo.cost.component.reserved.instance.PlanProjectedRICoverageAndUtilStore.TopologyRiCoverageContainer;
import com.vmturbo.market.component.api.ProjectedReservedInstanceCoverageListener;
import com.vmturbo.repository.api.RepositoryListener;

/**
 * Listener that receives the projected entity RI coverage from the market and forwards them to
 * the classes in the cost component that store them and make them available for queries.
 */
public class ProjectedRICoverageListener implements RepositoryListener, ProjectedReservedInstanceCoverageListener {

    private static final Logger logger = LogManager.getLogger();

    private final ProjectedRICoverageAndUtilStore projectedRICoverageAndUtilStore;

    private final PlanProjectedRICoverageAndUtilStore planProjectedRICoverageAndUtilStore;

    private final CostNotificationSender costNotificationSender;

    private final long realtimeTopologyContextId;

    /**
     * Tracks artifacts associated with projected topology availability, and RI coverage
     * notification receipt corresponding to given projectedTopologyIds. Both artifacts must be
     * received before the RI coverage notification should be sent.
     */
    private final Map<Long, Pair<CostNotification, TopologyRiCoverageContainer>>
            projectedTopologyIdToCoverageAndTopologyReceived = Maps.newHashMap();

    ProjectedRICoverageListener(@Nonnull final ProjectedRICoverageAndUtilStore projectedRICoverageStore,
                                @Nonnull final PlanProjectedRICoverageAndUtilStore planProjectedRICoverageAndUtilStore,
                                @Nonnull final CostNotificationSender costNotificationSender,
                                @Nonnull final long realtimeTopologyContextId) {
        this.projectedRICoverageAndUtilStore = Objects.requireNonNull(projectedRICoverageStore);
        this.planProjectedRICoverageAndUtilStore = Objects.requireNonNull(planProjectedRICoverageAndUtilStore);
        this.costNotificationSender = Objects.requireNonNull(costNotificationSender);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * Caches projected topology broadcast results. Ensures that both projected RI coverage results
     * and projected topology broadcast results have been received by the cost component before any
     * post processing is initiated.
     *
     * @param projectedTopologyId The projectedTopologyId of the topology broadcast received
     * @param planId The topologyContextId of the topology broadcast
     */
    @Override
    public void onProjectedTopologyAvailable(long projectedTopologyId, long planId) {
        if (realtimeTopologyContextId == planId) {
            return;
        }
        TopologyRiCoverageContainer topologyRiCoverageContainer =
                planProjectedRICoverageAndUtilStore.projectedTopologyAvailableHandler(
                        projectedTopologyId, planId);
        final Pair<CostNotification, TopologyRiCoverageContainer> value =
                projectedTopologyIdToCoverageAndTopologyReceived.get(projectedTopologyId);
        if (Objects.isNull(value)) {
            projectedTopologyIdToCoverageAndTopologyReceived.put(
                    projectedTopologyId,
                    new Pair<>(
                            null, topologyRiCoverageContainer));
        } else {
            // We have both projectedTopology and projectedRiCoverage
            projectedTopologyIdToCoverageAndTopologyReceived.remove(projectedTopologyId);
            sendProjectedRiCoverageNotification(value.first);
        }
    }

    /**
     * Caches projected RI coverage results. Ensures that both projected RI coverage results
     * and projected topology broadcast results have been received by the cost component before any
     * post processing is initiated.
     *
     * @param projectedTopologyId The projectedTopologyId of the projected RI coverage results received
     * @param costNotification The RI coverage notification built
     */
    public void updateRiCoverageMapWithCostNotification(long projectedTopologyId, CostNotification costNotification) {
        final Pair<CostNotification, TopologyRiCoverageContainer> value =
                projectedTopologyIdToCoverageAndTopologyReceived.get(projectedTopologyId);
        if (Objects.isNull(value)) {
            projectedTopologyIdToCoverageAndTopologyReceived.put(
                    projectedTopologyId,
                    new Pair<>(
                            costNotification,
                            null));
        } else {
            // We have both projectedTopology and projectedRiCoverage
            projectedTopologyIdToCoverageAndTopologyReceived.remove(projectedTopologyId);
            sendProjectedRiCoverageNotification(costNotification);
        }

    }

    /**
     * Calls into the failure handler in {@link PlanProjectedRICoverageAndUtilStore}.
     *
     * @param projectedTopologyId projected topology id
     * @param topologyContextId context id of the available topology
     * @param failureDescription description wording of the failure cause
     */
    @Override
    public void onProjectedTopologyFailure(long projectedTopologyId, long topologyContextId,
            @Nonnull String failureDescription) {
        planProjectedRICoverageAndUtilStore.projectedTopologyFailureHandler(
                projectedTopologyId, topologyContextId, failureDescription);
    }

    /**
     * No-op.
     *
     * @param topologyId topology id
     * @param topologyContextId context id of the available topology
     */
    @Override
    public void onSourceTopologyAvailable(long topologyId, long topologyContextId) { }

    /**
     * No-op.
     *
     * @param topologyId topology id
     * @param topologyContextId context id of the available topology
     * @param failureDescription description wording of the failure cause
     */
    @Override
    public void onSourceTopologyFailure(long topologyId, long topologyContextId,
            @Nonnull String failureDescription) {}

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
            final CostNotification costNotification = buildProjectedRiCoverageNotification(
                    originalTopologyInfo, Status.SUCCESS);
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
                planProjectedRICoverageAndUtilStore.updateProjectedRIUtilTableForPlan(
                        originalTopologyInfo,
                        riCoverageList,
                        projectedRICoverageAndUtilStore.resolveBuyRIsInScope(originalTopologyInfo.getTopologyContextId()));
                updateRiCoverageMapWithCostNotification(projectedTopologyId, costNotification);
            } else {
                projectedRICoverageAndUtilStore.updateProjectedRICoverage(originalTopologyInfo,
                        riCoverageList);
                sendProjectedRiCoverageNotification(costNotification);
            }
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
    protected void sendProjectedRiCoverageNotification(
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
    protected CostNotification buildProjectedRiCoverageNotification(
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

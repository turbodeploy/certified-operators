package com.vmturbo.plan.orchestrator.plan;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionsUpdated;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdate;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdateType;
import com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification;
import com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification.AnalysisState;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.Builder;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanProgressStatusEnum.Status;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.cost.api.CostNotificationListener;
import com.vmturbo.history.component.api.HistoryComponentNotifications.StatsAvailable;
import com.vmturbo.history.component.api.StatsListener;
import com.vmturbo.market.component.api.AnalysisStatusNotificationListener;
import com.vmturbo.plan.orchestrator.reservation.ReservationPlacementHandler;
import com.vmturbo.repository.api.RepositoryListener;
import com.vmturbo.topology.processor.api.TopologySummaryListener;

/**
 * Listener for action orchestrator's notifications.
 */
public class PlanProgressListener implements ActionsListener, RepositoryListener, StatsListener,
        CostNotificationListener, TopologySummaryListener, AnalysisStatusNotificationListener {

    private final PlanDao planDao;

    private final ReservationPlacementHandler reservationPlacementHandler;

    private final long realtimeTopologyContextId;

    private static final Logger logger = LogManager.getLogger(PlanProgressListener.class);

    private final PlanRpcService planService;

    public PlanProgressListener(@Nonnull final PlanDao planDao,
                                @Nonnull final PlanRpcService planService,
                                @Nonnull final ReservationPlacementHandler reservationPlacementHandler,
                                final long realtimeTopologyContextId) {
        this.planDao = Objects.requireNonNull(planDao);
        this.planService = planService;
        this.reservationPlacementHandler = Objects.requireNonNull(reservationPlacementHandler);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    @Override
    public void onTopologySummary(@Nonnull TopologySummary topologySummary) {
        final long planId = topologySummary.getTopologyInfo().getTopologyContextId();
        // Ignore realtime notifications.
        if (planId == realtimeTopologyContextId) {
            logger.debug("Dropping realtime broadcast success notification: {}", topologySummary);
            return;
        }

        try {
            final PlanInstance plan = planDao.updatePlanInstance(planId, planBuilder -> {
                planBuilder.getPlanProgressBuilder()
                    .setSourceTopologySummary(topologySummary);
                // If the topology failed to broadcast, we won't get any other results for the plan.
                // Mark it as failed.
                if (topologySummary.hasFailure()) {
                    logger.error("Plan {} - failed due to topology broadcast failure: {} ", planId,
                        topologySummary.getFailure().getErrorDescription());
                    planBuilder.setStatus(PlanStatus.FAILED);
                    planBuilder.setStatusMessage(topologySummary.getFailure().getErrorDescription());
                } else {
                    if (planBuilder.getStatus() == PlanStatus.CONSTRUCTING_TOPOLOGY) {
                        planBuilder.setStatus(PlanStatus.RUNNING_ANALYSIS);
                    }
                }
            });

            logger.info("Received broadcast success notification for plan {}. New status: {}",
                planId, plan.getStatus());
        } catch (IntegrityException e) {
            logger.error("Could not change plan's {} state after broadcast of topology ",
                topologySummary.getTopologyInfo().getTopologyId());
        } catch (NoSuchObjectException e) {
            logger.error("Could not find plan by topology context id {}", planId, e);
        }
    }

    /**
     * Callback receiving the actions the market computed.
     *
     * @param actionPlan The actions recommended by the market.
     */
    @Override
    public void onActionsReceived(@Nonnull final ActionPlan actionPlan) {
        onActionsUpdated(ActionsUpdated.newBuilder()
            .setActionPlanId(actionPlan.getId())
            .setActionPlanInfo(actionPlan.getInfo())
            .build());
    }

    /**
     * Callback when the actions stored in the ActionOrchestrator have been updated. Replaces the
     * "onActionsReceived" event.
     *
     * @param actionsUpdated The actions recommended by the market.
     */
    @Override
    public void onActionsUpdated(@Nonnull final ActionsUpdated actionsUpdated) {
        // The context of the action plan is the plan that the actions apply to.
        final long planId = actionsUpdated.getActionPlanInfo().hasMarket() ?
                actionsUpdated.getActionPlanInfo().getMarket().getSourceTopologyInfo().getTopologyContextId()
                : actionsUpdated.getActionPlanInfo().getBuyRi().getTopologyContextId();
        logger.debug("Received action plan with {} for topology context ID {}", actionsUpdated.getActionPlanId(), planId);
        if (planId != realtimeTopologyContextId) {
            try {
                Optional<PlanInstance> planInstance = planDao.getPlanInstance(planId);
                if (!planInstance.isPresent()) {
                    throw new NoSuchObjectException(null);
                } else {
                    if (actionsUpdated.hasUpdateFailure()) {
                        // If the action update failed.
                        logger.error("Plan {}- {}", planId,
                                actionsUpdated.getUpdateFailure().getErrorMessage());
                        planDao.updatePlanInstance(planId, planBuilder -> {
                            planBuilder.setStatus(PlanStatus.FAILED);
                            planBuilder.setStatusMessage(actionsUpdated.getUpdateFailure().getErrorMessage());
                        });
                    } else {
                        // If the action update was successful.
                        if (actionsUpdated.getActionPlanInfo().hasBuyRi()) {
                            final long actionPlanId = actionsUpdated.getActionPlanId();
                            if (PlanRpcServiceUtil.isScalingEnabled(planInstance.get().getScenario().getScenarioInfo())) {
                                // buy ri and optimize workload will have VM resize enabled
                                // AO now has received actionplan generated by buy RI
                                // trigger market analysis now
                                planDao.updatePlanInstance(planId, oldInstance ->
                                        oldInstance.setStatus(PlanStatus.BUY_RI_COMPLETED));
                                planDao.updatePlanInstance(planId, oldInstance ->
                                        oldInstance.addActionPlanId(actionPlanId));
                            } else {
                                // buy ri only plan will have VM resize disabled
                                planDao.updatePlanInstance(planId, oldInstance ->
                                        oldInstance.addActionPlanId(actionPlanId));
                                planDao.updatePlanInstance(planId, oldInstance ->
                                        oldInstance.setStatus(PlanStatus.WAITING_FOR_RESULT));
                            }
                            // Trigger topology broadcast from TP and possibly market analysis.
                            // If resize is enabled, market analysis will run and and broadcast
                            // source and projected topology to history/repository
                            // If resize is not enabled, market analysis will not run due to
                            // disabled setting.
                            // In both cases, source and projected topology will be broadcast
                            // from Market to Repository.
                            planService.triggerAnalysis(planInstance.get());
                        } else {
                            planDao.updatePlanInstance(planId, plan -> processActionsUpdated(plan, actionsUpdated));
                        }
                        logger.info("Plan {} is assigned action plan id {}", planId, actionsUpdated.getActionPlanId());
                    }
                }
            } catch (IntegrityException e) {
                logger.error(
                        "Could not change plan's {} state according to action " +
                                "plan {}", planId, actionsUpdated.getActionPlanId(), e);
            } catch (NoSuchObjectException e) {
                logger.warn("Could not find plan by topology context id {}",
                        planId, e);
            }
        } else {
            logger.debug("Dropping real-time action plan notification.");
        }
    }

    /**
     * Notifies, that new projected topology is available for operations in the repository.
     *
     * @param projectedTopologyId projected topology id
     * @param topologyContextId context id of the available topology
     */
    @Override
    public void onProjectedTopologyAvailable(long projectedTopologyId, long topologyContextId) {
        logger.debug("Received projected topology {} available notification for topology context ID {}",
                projectedTopologyId, topologyContextId);
        if (topologyContextId != realtimeTopologyContextId) {
            try {
                logger.info("Plan {} is assigned projected topology id {}. Updating plan instance...",
                        topologyContextId, projectedTopologyId);
                final PlanInstance updatedPlan = planDao.updatePlanInstance(topologyContextId, plan ->
                        processProjectedTopology(plan, projectedTopologyId));
                logger.info("Finished updating plan instance for plan {}", topologyContextId);
                if (updatedPlan.getProjectType() == PlanProjectType.RESERVATION_PLAN) {
                    reservationPlacementHandler.updateReservations(topologyContextId,
                            projectedTopologyId, true);
                }
            } catch (IntegrityException e) {
                logger.error("Could not change plan's {} state according to  " +
                        "available projected topology {}", topologyContextId, projectedTopologyId, e);
            } catch (NoSuchObjectException e) {
                logger.warn("Could not find plan by topology context id {}", topologyContextId, e);
            }
        } else {
            logger.debug("Updating reservation based on real-time projected topology notification.");
            reservationPlacementHandler.updateReservations(topologyContextId,
                    projectedTopologyId, false);
            logger.debug("Finished update reservation based on real-time projected topology notification.");
        }
    }

    /**
     * Notifies, that projected topology is failed to be stored in the repository.
     *
     * @param projectedTopologyId projected topology id
     * @param topologyContextId context id of the available topology
     * @param failureDescription description wording of the failure cause
     */
    @Override
    public void onProjectedTopologyFailure(long projectedTopologyId, long topologyContextId,
            @Nonnull String failureDescription) {
        logger.warn("Projected topology {}, generated by market failed to save in repository",
                projectedTopologyId);
        if (topologyContextId != realtimeTopologyContextId) {
            try {
                final PlanInstance plan = planDao.updatePlanInstance(topologyContextId, planBuilder -> {
                    planBuilder.setStatus(PlanStatus.FAILED);
                    planBuilder.setStatusMessage(
                            "Failed to save projected topology in the repository: " +
                                    failureDescription);
                });
                logger.warn("Marked plan {} as failed because of: {}", plan.getPlanId(),
                        plan.getStatusMessage());
            } catch (IntegrityException e) {
                logger.error("Could not change plan's {} state according to  " +
                        "available projected topology {}", topologyContextId, projectedTopologyId, e);
            } catch (NoSuchObjectException e) {
                logger.warn("Could not find plan by topology context id {}", topologyContextId, e);
            }
        } else {
            logger.debug("Dropping real-time projected topology notification.");
        }
    }

    /**
     * Notifies, that a new source topology (raw topology sent for analysis)
     * is available for operations in the repository.
     *
     * @param topologyId topology id
     * @param topologyContextId context id of the available topology
     */
    @Override
    public void onSourceTopologyAvailable(long topologyId, long topologyContextId) {
        logger.debug("Received source topology {} available notification for topology context ID {}",
                topologyId, topologyContextId);
        if (topologyContextId != realtimeTopologyContextId) {
            try {
                logger.debug("Plan {} is assigned source topology id {}. Updating plan instance...",
                        topologyContextId, topologyId);
                planDao.updatePlanInstance(topologyContextId, plan ->
                        processSourceTopology(plan, topologyId));
                logger.debug("Finished updating plan instance for plan {}", topologyContextId);
            } catch (IntegrityException e) {
                logger.error("Could not change plan's {} state according to  " +
                        "available source topology {}", topologyContextId, topologyId, e);
            } catch (NoSuchObjectException e) {
                logger.warn("Could not find plan by topology context id {}", topologyContextId, e);
            }
        }
    }

    /**
     * Notifies, that a new source topology (raw topology sent for analysis)
     * is failed to be stored in the repository.
     *
     * @param topologyId topology id
     * @param topologyContextId context id of the available topology
     * @param failureDescription description wording of the failure cause
     */
    @Override
    public void onSourceTopologyFailure(long topologyId, long topologyContextId, @Nonnull String failureDescription) {
        logger.warn("Source topology context {} topologyId {}, failed to save in repository",
                topologyContextId, topologyId);
        if (topologyContextId != realtimeTopologyContextId) {
            try {
                final PlanInstance plan = planDao.updatePlanInstance(topologyContextId, planBuilder -> {
                    // TODO: (OM-51227) Integrate the OCP plan failure logic
                    planBuilder.setStatus(PlanStatus.FAILED);
                    planBuilder.setStatusMessage(
                            "Failed to save source topology in the repository: " +
                                    failureDescription);
                });
                logger.warn("Plan {} as failed because of: {}", plan.getPlanId(),
                        plan.getStatusMessage());
            } catch (IntegrityException e) {
                logger.error("Could not change plan's {} state according to  " +
                        "available source topology {}", topologyContextId, topologyId, e);
            } catch (NoSuchObjectException e) {
                logger.warn("Could not find plan by topology context id {}", topologyContextId, e);
            }
        } else {
            logger.debug("Dropping real-time source topology notification topologyId " + topologyId);
        }
    }

    /**
     * Indicates that the history component has new statistics available for a plan topology.
     * Use the topology context id in the message to determine if the stats are for a plan
     * or live topology.
     *
     * @param statsAvailable A message describing the plan for which stats are available.
     */
    @Override
    public void onStatsAvailable(@Nonnull final StatsAvailable statsAvailable) {
        // The context of the action plan is the plan that the actions apply to.
        final long planId = statsAvailable.getTopologyContextId();
        if (planId != realtimeTopologyContextId) {
            logger.info("Plan {} has stats available", planId);
            try {
                if (statsAvailable.hasUpdateFailure()) {
                    logger.error("Stats failed for plan {}. {}.", planId,
                            statsAvailable.getUpdateFailure().getErrorMessage());
                    planDao.updatePlanInstance(planId, plan -> {
                        plan.setStatus(PlanStatus.FAILED);
                        plan.setStatusMessage(statsAvailable.getUpdateFailure().getErrorMessage());
                    });
                } else {
                    planDao.updatePlanInstance(planId, PlanProgressListener::processStatsAvailable);
                }
            } catch (IntegrityException e) {
                logger.error("Could not change plan's "
                        + planId + " state according to stats available.", e);
            } catch (NoSuchObjectException e) {
                logger.warn("Could not find plan by topology context id {}",
                        planId, e);
            }
        } else {
            logger.debug("Dropping real-time stats available notification.");
        }
    }

    private static void processStatsAvailable(@Nonnull final PlanInstance.Builder plan) {
        plan.setStatsAvailable(true);
        plan.setStatus(getPlanStatusBasedOnPlanType(plan));
    }

    private static void processSourceTopology(@Nonnull final PlanInstance.Builder plan,
                                              final long topologyId) {
        plan.setSourceTopologyId(topologyId);
        plan.setStatus(getPlanStatusBasedOnPlanType(plan));
    }

    private void processActionsUpdated(@Nonnull final PlanInstance.Builder plan,
            @Nonnull final ActionsUpdated actionsUpdated) {
        plan.addActionPlanId(actionsUpdated.getActionPlanId());
        plan.setStatus(getPlanStatusBasedOnPlanType(plan));
    }

    private static void processProjectedTopology(@Nonnull final PlanInstance.Builder plan,
            final long projectedTopologyId) {
        plan.setProjectedTopologyId(projectedTopologyId);
        plan.setStatus(getPlanStatusBasedOnPlanType(plan));
    }

    @Override
    public void onCostNotificationReceived(@Nonnull final CostNotification costNotification) {

        if (costNotification.hasStatusUpdate()) {

            final StatusUpdate statusUpdate = costNotification.getStatusUpdate();
            // The context of the action plan is the plan that the actions apply to.
            final long planId = costNotification.getStatusUpdate().getTopologyContextId();
            if (planId != realtimeTopologyContextId) {
                try {
                    if (statusUpdate.getType() == StatusUpdateType.PROJECTED_COST_UPDATE) {

                        planDao.updatePlanInstance(planId, p -> {
                            processProjectedCostUpdate(p, costNotification);
                        });

                        logger.info("Projected cost notification has been received from " +
                                        "cost component- status: {} topology ID: {} topology " +
                                        "context ID: {}",
                                statusUpdate.getStatus(),
                                statusUpdate.getTopologyId(),
                                statusUpdate.getTopologyContextId());
                    } else if (statusUpdate.getType() == StatusUpdateType.PROJECTED_RI_COVERAGE_UPDATE) {
                        planDao.updatePlanInstance(planId, p -> {
                            processProjectedRiUpdate(p, costNotification);
                        });
                        logger.info("Projected RI coverage notification has been received " +
                                        "from cost component- status:{} topology ID: {} topology " +
                                        "context ID: {}",
                                statusUpdate.getStatus(),
                                statusUpdate.getTopologyId(),
                                statusUpdate.getTopologyContextId());
                    } else {
                        logger.debug("Ignoring cost notification status update (Type={})",
                                statusUpdate.getType());
                    }
                } catch (IntegrityException e) {
                    logger.error("Could not change plan's "
                            + planId + " state according to cost notification.", e);
                } catch (NoSuchObjectException e) {
                    logger.warn("Could not find plan by topology context id {}",
                            planId, e);
                }
            } else {
                logger.debug("Dropping real-time cost notification.");
            }
        }
    }

    /**
     * Notifies, the status of a market analysis run.
     * For Plans, it should correspond to PlanStatus and in realtime to AnalysisStatus.
     *
     * @param analysisStatus status of the analysis run (e.g. SUCCEEDED/FAILED).
     */
    @Override
    public void
           onAnalysisStatusNotification(@Nonnull com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification
                                        analysisStatus) {
        Long topologyId = analysisStatus.getTopologyId();
        Long topologyContextId = analysisStatus.getTopologyContextId();
        logger.debug("Received analysis run status notification for topology context ID {}",
                     topologyId, topologyContextId);
        if (topologyContextId != realtimeTopologyContextId) {
            try {
                logger.debug("Plan {} has a status message from market {}. Updating plan instance...",
                             topologyContextId, topologyId);
                    planDao.updatePlanInstance(topologyContextId,
                                           plan -> processAnalysisStatusNotification(plan,
                                                                             analysisStatus));
                logger.debug("Finished updating plan instance for plan {}", topologyContextId);
            } catch (IntegrityException e) {
                logger.error("Could not change plan's {} state according to  " +
                             "available source topology {}", topologyContextId, topologyId, e);
            } catch (NoSuchObjectException e) {
                logger.warn("Could not find plan by topology context id {}", topologyContextId, e);
            }
        }
    }

    /**
     * Processes the projected cost notifications related to a plan.
     *
     * @param plan                      The plan
     * @param analysisStatus The notification regarding the status of the market analysis run for the plan.
     */
    private static void processAnalysisStatusNotification(@Nonnull final PlanInstance.Builder plan,
                                      @Nonnull final AnalysisStatusNotification analysisStatus) {
        final int analysisRunStatus = analysisStatus.getStatus();
        if (analysisRunStatus == AnalysisState.SUCCEEDED.ordinal()) {
            plan.setPlanProgress(plan.getPlanProgress().toBuilder()
                .setAnalysisStatus(Status.SUCCESS));
            plan.setStatus(getPlanStatusBasedOnPlanType(plan));
        } else if (analysisRunStatus == AnalysisState.FAILED.ordinal()) {
            plan.setPlanProgress(plan.getPlanProgress().toBuilder()
                 .setAnalysisStatus(Status.FAIL));
            plan.setStatus(PlanStatus.FAILED);
            // TODO (roman, Feb 5 2020): Propagate a better error message here.
            plan.setStatusMessage("Analysis failed.");
        }
    }

    /**
     * Processes the projected cost notifications related to a plan.
     *
     * @param plan                      The plan
     * @param projectedCostNotification The projected cost notification
     */
    private static void processProjectedCostUpdate(@Nonnull final PlanInstance.Builder plan,
                                                   @Nonnull final CostNotification
                                                           projectedCostNotification) {
        final Status projectedCostUpdateStatus = projectedCostNotification.getStatusUpdate()
                .getStatus();
        plan.setPlanProgress(plan.getPlanProgress().toBuilder()
                .setProjectedCostStatus(projectedCostUpdateStatus));
        plan.setStatus(getPlanStatusBasedOnPlanType(plan));
    }

    /**
     * Processes the projected RI coverage notifications related to a plan.
     *
     * @param plan                            The plan
     * @param projectedRiCoverageNotification The projected RI coverage notification
     */
    private static void processProjectedRiUpdate(@Nonnull final PlanInstance.Builder plan,
                                                 @Nonnull final CostNotification
                                                         projectedRiCoverageNotification) {
        final Status projectedRiCoverageUpdateStatus = projectedRiCoverageNotification
                .getStatusUpdate().getStatus();
        plan.setPlanProgress(plan.getPlanProgress().toBuilder()
                .setProjectedRiCoverageStatus(projectedRiCoverageUpdateStatus));
        plan.setStatus(getPlanStatusBasedOnPlanType(plan));
    }

    /**
     * Returns the status of the plan based on the plan type. For example, optimize cloud plan
     * with buy RI or optimize cloud plan without buy RI.
     *
     * @param plan The plan
     * @return The plan new status
     */
    @Nonnull
    @VisibleForTesting
    static PlanStatus getPlanStatusBasedOnPlanType(@Nonnull final Builder plan) {
        final PlanStatus existingStatus = plan.getStatus();
        if (isPlanDone(existingStatus)) {
            if (plan.getEndTime() == 0) {
                updatePlanEndTime(plan, existingStatus);
            }
            return existingStatus;
        }
        PlanStatus newStatus = existingStatus;
        if (isOCP(plan)) {
            // The optimize cloud plan (OCP)
            if (isOCPOptimizeAndBuyRI(plan)) {
                // OCP buy RI and optimize services (OCP type 1).
                newStatus = getOCPWithBuyRIPlanStatus(plan, true);
            } else if (isOCPOptimizeServices(plan) || isOCPBuyRIOnly(plan)) {
                // OCP buy RI only or optimize services (OCP type 2 and 3).
                newStatus = getOCPWithBuyRIPlanStatus(plan, false);
            } else {
                logger.warn("This OCP wasn't OCP type 1, 2 or 3. Plan ID: " + plan.getPlanId());
            }
            if (!newStatus.equals(existingStatus)) {
                updatePlanEndTime(plan, newStatus);
                logger.debug("The plan status has been changed from {} to {}- plan ID: " +
                        "{}", existingStatus, newStatus, plan.getPlanId());
            }
            return newStatus;
        }
        // Plans other than OCPs.
        newStatus = (plan.getStatsAvailable() && !plan.getActionPlanIdList().isEmpty() &&
                plan.hasProjectedTopologyId()) && plan.hasSourceTopologyId() ?
                PlanStatus.SUCCEEDED : PlanStatus.WAITING_FOR_RESULT;
        if (!newStatus.equals(existingStatus)) {
            updatePlanEndTime(plan, newStatus);
        }
        return newStatus;
    }

    private static boolean isPlanDone(final PlanStatus existingStatus) {
        return PlanStatus.FAILED.equals(existingStatus) ||
                PlanStatus.SUCCEEDED.equals(existingStatus);
    }

    private static void updatePlanEndTime(Builder plan, PlanStatus newStatus) {
        if (isPlanDone(newStatus)) {
            plan.setEndTime(System.currentTimeMillis());
        }
    }

    /**
     * Defines the OCP plan with buy RI status based on the notifications from stats, topology and
     * cost.
     * TODO: (OM-51279) Add the timeout logic to this method.
     *
     * @param plan The plan
     * @param isOCPOptimizeAndBuyRI If the plan is OCP type 1 or not
     * @return The status of the plan
     */
    @Nonnull
    @VisibleForTesting
    static PlanStatus getOCPWithBuyRIPlanStatus(@Nonnull final PlanInstance.Builder plan,
                                                final boolean isOCPOptimizeAndBuyRI) {
        final Status planProgressProjectedCostStatus = plan.getPlanProgress()
                .getProjectedCostStatus();
        final Status planProgressProjectedRiCoverageStatus = plan.getPlanProgress()
                .getProjectedRiCoverageStatus();
        if (Status.FAIL.equals(planProgressProjectedCostStatus) ||
                Status.FAIL.equals(planProgressProjectedRiCoverageStatus)) {
            return PlanStatus.FAILED;
        }
        // Optimize cloud plan (OCP)
        if (plan.getStatsAvailable() && !plan.getActionPlanIdList().isEmpty() &&
                plan.hasProjectedTopologyId() && plan.hasSourceTopologyId()) {
            if (isOCPOptimizeAndBuyRI) {
                // OCP buy RI and optimize services
                return Status.SUCCESS.equals(planProgressProjectedCostStatus) &&
                        Status.SUCCESS.equals(planProgressProjectedRiCoverageStatus) ?
                        PlanStatus.SUCCEEDED : PlanStatus.WAITING_FOR_RESULT;
            }
            // OCP buy RI only
            return PlanStatus.SUCCEEDED;
        }
        return PlanStatus.WAITING_FOR_RESULT;
    }

    /**
     * If the plan is OCP or not.
     *
     * @param plan The plan
     * @return If the plan is OCP or not
     */
    @VisibleForTesting
    static boolean isOCP(@Nonnull final Builder plan) {
        @Nullable ScenarioInfo scenarioInfo = plan.hasScenario() ? plan.getScenario()
                .getScenarioInfo() : null;
        if (scenarioInfo == null) {
            return false;
        }
        return StringConstants.OPTIMIZE_CLOUD_PLAN.equals(scenarioInfo.getType());
    }

    /**
     * If the plan is optimize services and buy RI or not.
     *
     * @param plan the OCP plan
     * @return If the plan is optimize services and buy RI or not
     */
    @VisibleForTesting
    static boolean isOCPOptimizeAndBuyRI(@Nonnull final PlanInstance.Builder plan) {
        String planSubType = PlanRpcServiceUtil.getPlanSubType(plan.getScenario()
                .getScenarioInfo());
        return StringConstants.OPTIMIZE_CLOUD_PLAN__RIBUY_AND_OPTIMIZE_SERVICES.equals(planSubType);
    }

    /**
     * If the plan is OCP optimize services or not.
     *
     * @param plan The OCP plan
     * @return If the plan is OCP optimize services or not
     */
    @VisibleForTesting
    static boolean isOCPOptimizeServices(@Nonnull final PlanInstance.Builder plan) {
        String planSubType = PlanRpcServiceUtil.getPlanSubType(plan.getScenario()
                .getScenarioInfo());
        return StringConstants.OPTIMIZE_CLOUD_PLAN__OPTIMIZE_SERVICES.equals(planSubType);
    }

    /**
     * If the plan is OCP buy RI only or not.
     *
     * @param plan The OCP plan
     * @return If the plan is OCP buy RI only or not
     */
    @VisibleForTesting
    static boolean isOCPBuyRIOnly(@Nonnull final PlanInstance.Builder plan) {
        String planSubType = PlanRpcServiceUtil.getPlanSubType(plan.getScenario()
                .getScenarioInfo());
        return StringConstants.OPTIMIZE_CLOUD_PLAN__RIBUY_ONLY.equals(planSubType);
    }

}

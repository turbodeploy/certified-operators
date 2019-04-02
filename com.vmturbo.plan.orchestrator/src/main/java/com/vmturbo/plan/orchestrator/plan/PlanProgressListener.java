package com.vmturbo.plan.orchestrator.plan;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionsUpdated;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.component.api.HistoryComponentNotifications.StatsAvailable;
import com.vmturbo.history.component.api.StatsListener;
import com.vmturbo.plan.orchestrator.reservation.ReservationPlacementHandler;
import com.vmturbo.repository.api.RepositoryListener;

/**
 * Listener for action orchestrator's notifications.
 */
public class PlanProgressListener implements ActionsListener, RepositoryListener, StatsListener {

    private final PlanDao planDao;

    private final ReservationPlacementHandler reservationPlacementHandler;

    private final long realtimeTopologyContextId;

    private final Logger logger = LogManager.getLogger(getClass());

    public PlanProgressListener(@Nonnull final PlanDao planDao,
                                @Nonnull final ReservationPlacementHandler reservationPlacementHandler,
                                final long realtimeTopologyContextId) {
        this.planDao = Objects.requireNonNull(planDao);
        this.reservationPlacementHandler = Objects.requireNonNull(reservationPlacementHandler);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    @Override
    public void onActionsReceived(@Nonnull final ActionPlan actionPlan) {
        onActionsUpdated(ActionsUpdated.newBuilder()
            .setActionPlanId(actionPlan.getId())
            .setActionPlanInfo(actionPlan.getInfo())
            .build());
    }

    @Override
    public void onActionsUpdated(@Nonnull final ActionsUpdated actionsUpdated) {
        // The context of the action plan is the plan that the actions apply to.
        final TopologyInfo sourceTopologyInfo = actionsUpdated.getActionPlanInfo()
            .getMarket().getSourceTopologyInfo();
        final long planId = sourceTopologyInfo.getTopologyContextId();
        logger.debug("Received action plan with {} for plan {}", actionsUpdated.getActionPlanId(), planId);
        if (planId != realtimeTopologyContextId) {
            try {
                planDao.updatePlanInstance(planId, plan -> processActionsUpdated(plan, actionsUpdated));
                logger.info("Plan {} is assigned action plan id {}", planId, actionsUpdated.getActionPlanId());
            } catch (IntegrityException e) {
                logger.error(
                        "Could not change plan's " + planId + " state according to action " +
                                "plan " + actionsUpdated.getActionPlanId(), e);
            } catch (NoSuchObjectException e) {
                logger.warn("Could not find plan by topology context id {}",
                        planId, e);
            }
        } else {
            logger.debug("Dropping real-time action plan notification.");
        }
    }

    @Override
    public void onProjectedTopologyAvailable(long projectedTopologyId, long topologyContextId) {
        logger.debug("Received projected topology {} available notification for plan {}",
                projectedTopologyId, topologyContextId);
        if (topologyContextId != realtimeTopologyContextId) {
            try {
                logger.info("Plan {} is assigned projected topology id {}. Updating plan instance...",
                        topologyContextId, projectedTopologyId);
                planDao.updatePlanInstance(topologyContextId, plan ->
                        processProjectedTopology(plan, projectedTopologyId));
                logger.info("Finished updating plan instance for plan {}", topologyContextId);
            } catch (IntegrityException e) {
                logger.error("Could not change plan's " + topologyContextId + " state according to  " +
                        "available projected topology {}" + projectedTopologyId, e);
            } catch (NoSuchObjectException e) {
                logger.warn("Could not find plan by topology context id {}", topologyContextId, e);
            }
        } else {
            logger.debug("Updating reservation based on real-time projected topology notification.");
            reservationPlacementHandler.updateReservations(topologyContextId, projectedTopologyId);
            logger.debug("Finished update reservation based on real-time projected topology notification.");
        }
    }

    @Override
    public void onProjectedTopologyFailure(long projectedTopologyId, long topologyContextId,
            @Nonnull String failureDescription) {
        logger.debug("Projected topology {}, generated by market failed to safe in repository",
                projectedTopologyId);
        if (topologyContextId != realtimeTopologyContextId) {
            try {
                final PlanInstance plan = planDao.updatePlanInstance(topologyContextId, planBuilder -> {
                    planBuilder.setStatus(PlanStatus.FAILED);
                    planBuilder.setStatusMessage(
                            "Failed to save projected topology in the repository: " +
                                    failureDescription);
                });
                logger.info("Marked plan {} as failed because of: {}", plan.getPlanId(),
                        plan.getStatusMessage());
            } catch (IntegrityException e) {
                logger.error("Could not change plan's " + topologyContextId + " state according to  " +
                        "available projected topology {}" + projectedTopologyId, e);
            } catch (NoSuchObjectException e) {
                logger.warn("Could not find plan by topology context id {}", topologyContextId, e);
            }
        } else {
            logger.debug("Dropping real-time projected topology notification.");
        }
    }

    @Override
    public void onSourceTopologyAvailable(long topologyId, long topologyContextId) {
        // Nothing to do
    }

    @Override
    public void onSourceTopologyFailure(long topologyId, long topologyContextId, @Nonnull String failureDescription) {
        // Nothing to do
    }

    @Override
    public void onStatsAvailable(@Nonnull final StatsAvailable statsAvailable) {
        // The context of the action plan is the plan that the actions apply to.
        final long planId = statsAvailable.getTopologyContextId();
        if (planId != realtimeTopologyContextId) {
            logger.info("Plan {} has stats available", planId);
            try {
                planDao.updatePlanInstance(planId, PlanProgressListener::processStatsAvailable);
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
        if (plan.hasProjectedTopologyId() && plan.hasActionPlanId()) {
            plan.setStatus(PlanStatus.SUCCEEDED);
        } else {
            plan.setStatus(PlanStatus.WAITING_FOR_RESULT);
        }
    }

    private static void processActionsUpdated(@Nonnull final PlanInstance.Builder plan,
            @Nonnull final ActionsUpdated actionsUpdated) {
        plan.setActionPlanId(actionsUpdated.getActionPlanId());
        if (plan.hasProjectedTopologyId() && plan.hasStatsAvailable()) {
            plan.setStatus(PlanStatus.SUCCEEDED);
        } else {
            plan.setStatus(PlanStatus.WAITING_FOR_RESULT);
        }
    }

    private static void processProjectedTopology(@Nonnull final PlanInstance.Builder plan,
            final long projectedTopologyId) {
        plan.setProjectedTopologyId(projectedTopologyId);
        if (plan.hasActionPlanId() && plan.hasStatsAvailable()) {
            plan.setStatus(PlanStatus.SUCCEEDED);
        } else {
            plan.setStatus(PlanStatus.WAITING_FOR_RESULT);
        }
    }
}

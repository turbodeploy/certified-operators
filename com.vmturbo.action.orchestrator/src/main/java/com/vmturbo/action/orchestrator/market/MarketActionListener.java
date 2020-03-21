package com.vmturbo.action.orchestrator.market;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.api.ActionOrchestratorNotificationSender;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.common.protobuf.PlanDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisSummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.market.component.api.ActionsListener;
import com.vmturbo.market.component.api.AnalysisSummaryListener;

/**
 * Listens to action recommendations from the market.
 */
public class MarketActionListener implements ActionsListener, AnalysisSummaryListener {

    private static final Logger logger = LogManager.getLogger();

    private final ActionOrchestratorNotificationSender notificationSender;

    private final ActionStorehouse actionStorehouse;

    private final ActionPlanAssessor actionPlanAssessor;

    private final Object latestKnownActionPlanLock = new Object();

    /**
     * The ID of the latest known action plan (received on the analysis-summary topic).
     */
    private long latestMarketActionPlanId = -1;

    /**
     * Constructs new instance of {@code MarketActionListener}.
     *
     * @param notificationSender Notification sender.
     * @param actionStorehouse Action store house.
     * @param actionPlanAssessor Action plan assessor.
     */
    public MarketActionListener(@Nonnull final ActionOrchestratorNotificationSender notificationSender,
                                @Nonnull final ActionStorehouse actionStorehouse,
                                @Nonnull final ActionPlanAssessor actionPlanAssessor) {
        this.notificationSender = Objects.requireNonNull(notificationSender);
        this.actionStorehouse = Objects.requireNonNull(actionStorehouse);
        this.actionPlanAssessor = Objects.requireNonNull(actionPlanAssessor);
    }

    @Override
    public void onAnalysisSummary(@Nonnull final AnalysisSummary analysisSummary) {
        if (analysisSummary.getSourceTopologyInfo().getTopologyType() == TopologyType.REALTIME) {
            synchronized (latestKnownActionPlanLock) {
                latestMarketActionPlanId = analysisSummary.getActionPlanSummary().getActionPlanId();
            }
        }
    }

    /**
     * Return whether or not we should skip processing the action plan.
     *
     * @param actionPlan The action plan.
     * @return True if we shouldn't use this action plan to populate actions.
     */
    private boolean shouldSkip(@Nonnull final ActionPlan actionPlan) {
        // We never skip plan action plans.
        if (!isLiveMktActionPlan(actionPlan)) {
            return false;
        }

        // We skip expired live action plans no matter what.
        //
        // This means that when we first start up, if we get an old action plan before getting
        // all the analysis summaries, we still skip it.
        if (actionPlanAssessor.isActionPlanExpired(actionPlan)) {
            return true;
        }

        synchronized (latestKnownActionPlanLock) {
            // We rely on the fact that (right now) action plan ids are increasing.
            // If we already got an analysis summary indicating there is a "newer" action plan
            // available, we skip the current action plan.
            return actionPlan.getId() < latestMarketActionPlanId;
        }
    }

    @Override
    public void onActionsReceived(@Nonnull final ActionPlan orderedActions) {
        if (logger.isDebugEnabled()) {
            orderedActions.getActionList().forEach(action -> logger.debug("Received action: " + action));
        }

        if (shouldSkip(orderedActions)) {
            logger.warn("Dropping action plan {} (info: {}) " +
                            "with {} actions (analysis start [{}] completion [{}])",
                    orderedActions.getId(),
                    orderedActions.getInfo(),
                    orderedActions.getActionCount(),
                    localDateTimeFromSystemTime(orderedActions.getAnalysisStartTimestamp()),
                    localDateTimeFromSystemTime(orderedActions.getAnalysisCompleteTimestamp()));
            return;
        }

        // Populate the store with the new recommendations and refresh the cache.
        try {
            // We don't store "transient" plan actions.
            // However, for transient plans we still want to send the actions update notification
            // so the plan lifecycle can continue as normal.
            if (PlanDTOUtil.isTransientPlan(orderedActions.getInfo().getMarket().getSourceTopologyInfo())) {
                logger.info("Skipping persistence of {} actions for transient plan {}",
                    orderedActions.getActionCount(), orderedActions.getInfo().getMarket().getSourceTopologyInfo().getTopologyContextId());
            } else {
                final ActionStore actionStore = actionStorehouse.storeActions(orderedActions);
                logger.info("Received {} actions in action plan {} (info: {})" +
                        " (analysis start [{}] completion [{}])" +
                        " (store population: {}).",
                    orderedActions.getActionCount(),
                    orderedActions.getId(),
                    orderedActions.getInfo(),
                    localDateTimeFromSystemTime(orderedActions.getAnalysisStartTimestamp()),
                    localDateTimeFromSystemTime(orderedActions.getAnalysisCompleteTimestamp()),
                    actionStore.size());
            }
            // Notify listeners that actions are ready for retrieval.
            try {
                notificationSender.notifyActionsUpdated(orderedActions);
            } catch (CommunicationException | InterruptedException e) {
                logger.error("Could not send actions recommended notification for "
                        + orderedActions.getId(), e);
            }
        } catch (Exception e) {
            logger.error("An error happened while populating the actions.", e);
            notificationSender.notifyActionsUpdateFailure(orderedActions);
        }
    }

    private LocalDateTime localDateTimeFromSystemTime(final long systemTimeMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(systemTimeMillis), ZoneOffset.UTC);
    }

    /**
     * Decide if an action plan is a live action plan.
     *
     * @param orderedActions The action plan to assess.
     * @return True if the action plan is due to a live analysis, false if it is not.
     */
    private boolean isLiveMktActionPlan(@Nonnull final ActionPlan orderedActions) {
        return orderedActions.getInfo().hasMarket() &&
            orderedActions.getInfo().getMarket().getSourceTopologyInfo().getTopologyType() == TopologyType.REALTIME;
    }
}

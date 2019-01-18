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
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.market.component.api.ActionsListener;

/**
 * Listens to action recommendations from the market.
 */
public class MarketActionListener implements ActionsListener {

    private static final Logger logger = LogManager.getLogger();

    private final ActionOrchestratorNotificationSender notificationSender;

    private final ActionStorehouse actionStorehouse;

    private final ActionPlanAssessor actionPlanAssessor;

    public MarketActionListener(@Nonnull final ActionOrchestratorNotificationSender notifiationSender,
                                @Nonnull final ActionStorehouse actionStorehouse,
                                @Nonnull final ActionPlanAssessor actionPlanAssessor) {
        this.notificationSender = Objects.requireNonNull(notifiationSender);
        this.actionStorehouse = Objects.requireNonNull(actionStorehouse);
        this.actionPlanAssessor = Objects.requireNonNull(actionPlanAssessor);
    }

    @Override
    public void onActionsReceived(@Nonnull final ActionPlan orderedActions) {
        if (logger.isDebugEnabled()) {
            orderedActions.getActionList().forEach(action -> logger.debug("Received action: " + action));
        }

        if (actionPlanAssessor.isActionPlanExpired(orderedActions)) {
            logger.warn("Dropping action plan {} for topology {} and topologyContext {} " +
                    "with {} actions (analysis start [{}] completion [{}])",
                orderedActions.getId(),
                orderedActions.getTopologyId(),
                orderedActions.getTopologyContextId(),
                orderedActions.getActionCount(),
                localDateTimeFromSystemTime(orderedActions.getAnalysisStartTimestamp()),
                localDateTimeFromSystemTime(orderedActions.getAnalysisCompleteTimestamp()));
            return;
        }

        // Populate the store with the new recommendations and refresh the cache.
        final ActionStore actionStore = actionStorehouse.storeActions(orderedActions);
        logger.info("Received {} actions in action plan {} for topology {} and context {}" +
                " (analysis start [{}] completion [{}])" +
                " (store population: {}).",
            orderedActions.getActionCount(),
            orderedActions.getId(),
            orderedActions.getTopologyId(),
            orderedActions.getTopologyContextId(),
            localDateTimeFromSystemTime(orderedActions.getAnalysisStartTimestamp()),
            localDateTimeFromSystemTime(orderedActions.getAnalysisCompleteTimestamp()),
            actionStore.size());

        // Notify listeners that actions are ready for retrieval.
        try {
            notificationSender.notifyActionsUpdated(orderedActions);
        } catch (CommunicationException | InterruptedException e) {
            logger.error(
                    "Could not send actions recommended notification for " + orderedActions.getId(),
                    e);
        }
    }

    private LocalDateTime localDateTimeFromSystemTime(final long systemTimeMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(systemTimeMillis), ZoneOffset.UTC);
    }
}

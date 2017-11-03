package com.vmturbo.action.orchestrator.market;

import java.util.Objects;
import java.util.Optional;

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

    public MarketActionListener(@Nonnull final ActionOrchestratorNotificationSender notifiationSender,
                                @Nonnull final ActionStorehouse actionStorehouse) {
        this.notificationSender = Objects.requireNonNull(notifiationSender);
        this.actionStorehouse = Objects.requireNonNull(actionStorehouse);
    }

    @Override
    public void onActionsReceived(@Nonnull final ActionPlan orderedActions) {
        if (logger.isDebugEnabled()) {
            orderedActions.getActionList().forEach(action -> logger.debug("Received action: " + action));
        }

        // Populate the store with the new recommendations and refresh the cache.
        final ActionStore actionStore = actionStorehouse.storeActions(orderedActions);
        logger.info("Received {} actions for plan {} and context {} (store population: {}).",
            orderedActions.getActionCount(),
            orderedActions.getId(),
            orderedActions.getTopologyContextId(),
            actionStore.size());

        // Notify listeners that actions are ready for retrieval.
        try {
            notificationSender.notifyActionsRecommended(orderedActions);
        } catch (CommunicationException | InterruptedException e) {
            logger.error(
                    "Could not send actions recommended notification for " + orderedActions.getId(),
                    e);
        }
    }
}

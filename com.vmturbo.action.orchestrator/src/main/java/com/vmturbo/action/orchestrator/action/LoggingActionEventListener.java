package com.vmturbo.action.orchestrator.action;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;

/**
 * Logs all action events to the logs.
 */
public class LoggingActionEventListener implements ActionStateMachine.ActionEventListener {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void onActionEvent(
            ActionView actionView,
            @Nonnull final ActionState preState,
            @Nonnull final ActionState postState,
            @Nonnull final ActionEvent event,
            boolean performedTransition) {
        if (event instanceof ActionEvent.NotRecommendedEvent) {
            logger.trace("Action {} {} event {} ({} -> {})",
                    actionView.getId(),
                    performedTransition ? "handled" : "dropped",
                    event,
                    preState,
                    postState
            );
        } else {
            logger.info("Action {} {} event {} ({} -> {})",
                    actionView.getId(),
                    performedTransition ? "handled" : "dropped",
                    event,
                    preState,
                    postState
            );
        }
    }
}

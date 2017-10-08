package com.vmturbo.action.orchestrator.execution.notifications;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ProgressEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.SuccessEvent;
import com.vmturbo.action.orchestrator.api.ActionOrchestratorNotificationSender;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.topology.processor.api.ActionExecutionListener;

/**
 * The {@link ActionStateUpdater} listens for Action Execution notifications
 * as actions are being executed. When they are received, the corresponding
 * actions are updated so that their state reflects the notification received.
 */
public class ActionStateUpdater implements ActionExecutionListener {

    private static final Logger logger = LogManager.getLogger();

    private final ActionStorehouse actionStorehouse;

    private final ActionOrchestratorNotificationSender notificationSender;

    /**
     * The ID of the topology context for realtime market analysis (as opposed to a plan market analysis).
     * It currently makes no sense to execute plan actions, so only actions in the realtime context are
     * examined. If this design changes, this value should be parameterized.
     */
    private final long realtimeTopologyContextId;

    /**
     * Create a new {@link ActionStateUpdater}.
     *
     * @param actionStorehouse The storehouse in which to look up actions as notifications are received.
     * @param notificationSender The API backend to send notifications to.
     * @param realtimeTopologyContextId The ID of the topology context for realtime market analysis
     *                                  (as opposed to a plan market analysis).
     */
    public ActionStateUpdater(@Nonnull final ActionStorehouse actionStorehouse,
                              @Nonnull final ActionOrchestratorNotificationSender notificationSender,
                              final long realtimeTopologyContextId) {
        this.actionStorehouse = Objects.requireNonNull(actionStorehouse);
        this.notificationSender = notificationSender;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }


    /**
     * Update the progress of an action.
     *
     * @param actionProgress The progress notification for an action.
     */
    @Override
    public void onActionProgress(@Nonnull final ActionProgress actionProgress) {
        final Optional<Action> storedAction = actionStorehouse
            .getStore(realtimeTopologyContextId)
            .flatMap(store -> store.getAction(actionProgress.getActionId()));
        if (storedAction.isPresent()) {
            Action action = storedAction.get();
            action.receive(new ProgressEvent(actionProgress.getProgressPercentage(),
                    actionProgress.getDescription()));
            notificationSender.notifyActionProgress(actionProgress);
        } else {
            logger.error("Unable to update progress for " + actionProgress);
        }
    }

    /**
     * Mark an action as succeeded.
     *
     * @param actionSuccess The progress notification for an action.
     */
    @Override
    public void onActionSuccess(@Nonnull final ActionSuccess actionSuccess) {
        final Optional<Action> storedAction = actionStorehouse
            .getStore(realtimeTopologyContextId)
            .flatMap(store -> store.getAction(actionSuccess.getActionId()));
        if (storedAction.isPresent()) {
            Action action = storedAction.get();
            action.receive(new SuccessEvent());
            notificationSender.notifyActionSuccess(actionSuccess);
        } else {
            logger.error("Unable to mark success for " + actionSuccess);
        }
    }

    /**
     * Mark an action as failed.
     *
     * @param actionFailure The progress notification for an action.
     */
    @Override
    public void onActionFailure(@Nonnull final ActionFailure actionFailure) {
        final Optional<Action> storedAction = actionStorehouse
            .getStore(realtimeTopologyContextId)
            .flatMap(store -> store.getAction(actionFailure.getActionId()));
        if (storedAction.isPresent()) {
            Action action = storedAction.get();
            action.receive(new FailureEvent(actionFailure.getErrorDescription()));
            notificationSender.notifyActionFailure(actionFailure);
        } else {
            logger.error("Unable to mark failure for " + actionFailure);
        }
    }
}

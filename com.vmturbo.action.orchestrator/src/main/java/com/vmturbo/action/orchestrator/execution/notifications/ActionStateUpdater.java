package com.vmturbo.action.orchestrator.execution.notifications;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.Action.SerializationState;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ProgressEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.SuccessEvent;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.api.ActionOrchestratorNotificationSender;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLogEntry;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.communication.CommunicationException;
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

    private final ActionHistoryDao actionHistoryDao;

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
                              @Nonnull final ActionHistoryDao actionHistoryDao,
                              final long realtimeTopologyContextId) {
        this.actionStorehouse = Objects.requireNonNull(actionStorehouse);
        this.actionHistoryDao = Objects.requireNonNull(actionHistoryDao);
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
            try {
                notificationSender.notifyActionProgress(actionProgress);
            } catch (CommunicationException | InterruptedException e) {
                logger.error("Unable to send notification for progress of " + actionProgress, e);
            }
        } else {
            logger.error("Unable to update progress for " + actionProgress);
        }
    }

    /**
     * Mark an action as succeeded. It also persists the action to DB and sends execution result to
     * remote audit log.
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
            try {
                notificationSender.notifyActionSuccess(actionSuccess);
            } catch (CommunicationException | InterruptedException e) {
                logger.error("Unable to send notification for success of " + actionSuccess, e);
            }
            saveToDb(action);
            writeToAudit(action, true);

        } else {
            logger.error("Unable to mark success for " + actionSuccess);
        }
    }

    /**
     * Mark an action as failed. It also persists the action to DB and sends execution result to
     * remote audit log.
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
            try {
                notificationSender.notifyActionFailure(actionFailure);
            } catch (CommunicationException | InterruptedException e) {
                logger.error("Unable to send notification for failure of " + actionFailure, e);
            }
            saveToDb(action);
            writeToAudit(action, false);

        } else {
            logger.error("Unable to mark failure for " + actionFailure);
        }
    }

    /**
     * Send the action execution result to remote audit log.
     *
     * @param action the executed action.
     * @param isSuccessful is the execution successful.
     */
    private void writeToAudit(final Action action, final boolean isSuccessful) {
        try {
            AuditLogEntry entry = new AuditLogEntry.Builder(AuditAction.EXECUTE_ACTION, action.toString(), isSuccessful)
                    .targetName(String.valueOf(action.getId()))
                    .build();
            AuditLogUtils.audit(entry);
        } catch (RuntimeException e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * Persist executed action to DB.
     *
     * @param action the executed action.
     */
    private void saveToDb(final Action action) {
        try {
            SerializationState serializedAction = new SerializationState(action);
            actionHistoryDao.persistActionHistory(
                    serializedAction.getRecommendation().getId(),
                    serializedAction.getRecommendation(),
                    realtimeTopologyContextId,
                    serializedAction.getRecommendationTime(),
                    serializedAction.getActionDecision(),
                    serializedAction.getExecutionStep(),
                    serializedAction.getCurrentState().getNumber()
            );
        } catch (RuntimeException e) {
            logger.error(e.getMessage());
        }
    }
}

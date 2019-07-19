package com.vmturbo.action.orchestrator.execution.notifications;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.Action.SerializationState;
import com.vmturbo.action.orchestrator.action.ActionEvent.AfterFailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.AfterSuccessEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ProgressEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.SuccessEvent;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.api.ActionOrchestratorNotificationSender;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.ExecutionStartException;
import com.vmturbo.action.orchestrator.execution.FailedCloudVMGroupProcessor;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLogEntry;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.topology.processor.api.ActionExecutionListener;

/**
 * The {@link ActionStateUpdater} listens for Action Execution notifications
 * as actions are being executed. When they are received, the corresponding
 * actions are updated so that their state reflects the notification received.
 */
public class ActionStateUpdater implements ActionExecutionListener {

    private static final Logger logger = LogManager.getLogger();

    private static final String PREVIOUS_FAILURE_MESSAGE =
        "Failing due to a failure in a previous execution phase.";

    private final ActionStorehouse actionStorehouse;

    private final ActionOrchestratorNotificationSender notificationSender;

    private final ActionHistoryDao actionHistoryDao;

    /**
     * Used to execute actions (by sending them to Topology Processor).
     */
    private final ActionExecutor actionExecutor;

    /**
     * The store for all the known {@link WorkflowDTO.Workflow} items.
     */
    private final WorkflowStore workflowStore;

    /**
     * The ID of the topology context for realtime market analysis (as opposed to a plan market analysis).
     * It currently makes no sense to execute plan actions, so only actions in the realtime context are
     * examined. If this design changes, this value should be parameterized.
     */
    private final long realtimeTopologyContextId;

    private final FailedCloudVMGroupProcessor failedCloudVMGroupProcessor;

    /**
     * Create a new {@link ActionStateUpdater}.
     * @param actionStorehouse The storehouse in which to look up actions as notifications are received.
     * @param notificationSender The API backend to send notifications to.
     * @param actionExecutor to execute actions (by sending them to Topology Processor)
     * @param workflowStore the store for all the known {@link WorkflowDTO.Workflow} items
     * @param realtimeTopologyContextId The ID of the topology context for realtime market analysis
     * @param failedCloudVMGroupProcessor to process failed actions and add VM entities to a group.
     */
    public ActionStateUpdater(@Nonnull final ActionStorehouse actionStorehouse,
                              @Nonnull final ActionOrchestratorNotificationSender notificationSender,
                              @Nonnull final ActionHistoryDao actionHistoryDao,
                              @Nonnull final ActionExecutor actionExecutor,
                              @Nonnull final WorkflowStore workflowStore,
                              final long realtimeTopologyContextId, final FailedCloudVMGroupProcessor failedCloudVMGroupProcessor) {
        this.actionStorehouse = Objects.requireNonNull(actionStorehouse);
        this.actionHistoryDao = Objects.requireNonNull(actionHistoryDao);
        this.notificationSender = Objects.requireNonNull(notificationSender);
        this.actionExecutor = Objects.requireNonNull(actionExecutor);
        this.workflowStore = Objects.requireNonNull(workflowStore);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.failedCloudVMGroupProcessor = failedCloudVMGroupProcessor;
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
            ActionState previousState = action.getState();

            // Special case: if a POST workflow just completed for an action with previous failures,
            // then we want to move the action to FAILED state.
            if (ActionState.POST_IN_PROGRESS == action.getState() && action.hasFailures()) {
                action.receive(new FailureEvent(PREVIOUS_FAILURE_MESSAGE));
            } else {
                // Notify the action of the successful completion, possibly triggering a transition
                // within the action's state machine.
                action.receive(new SuccessEvent());

                if (ActionState.POST_IN_PROGRESS == action.getState()) {
                    // Allow the action to immediately transition from POST_IN_PROGRESS to SUCCEEDED,
                    // if no post-execution workflow is defined
                    action.receive(new AfterSuccessEvent());
                }
            }
            failedCloudVMGroupProcessor.handleActionSuccess(action);

            // Store the updated action and update the audit log
            saveToDb(action);
            writeToAudit(action, true);

            // Retrieve the current action state, now that the action has received all events
            final ActionState actionState = action.getState();
            // Multiple SuccessEvents can also be triggered for a single action that has PRE
            // and POST states. If the action does not transition to the succeeded state, then
            // this is a partial completion, which does not indicate that the overall action has
            // completed yet.
            switch (actionState) {
                case IN_PROGRESS:
                case POST_IN_PROGRESS:
                    logger.debug("Action {} completed state {} successfully and transitioned to state {}.",
                        action.getId(), previousState, actionState);
                    if (action.hasPendingExecution()) {
                        continueActionExecution(action);
                    }
                    break;
                case SUCCEEDED:
                    // If the action transitions to the SUCCEEDED state, notify the rest of the system
                    logger.info("Action executed successfully: {}", action);
                    try {
                        notificationSender.notifyActionSuccess(actionSuccess);
                    } catch (CommunicationException | InterruptedException e) {
                        logger.error("Unable to send notification for success of " + actionSuccess, e);
                    }
                    break;
                default:
                    logger.warn("Unexpected action state following ActionSuccess event. Action {}, "
                            + "State {}.", action.getId(), actionState);
            }
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
            final String errorDescription = actionFailure.getErrorDescription();
            // Notify the action of the failure, possibly triggering a transition
            // within the action's state machine.
            action.receive(new FailureEvent(errorDescription));

            if (ActionState.POST_IN_PROGRESS == action.getState()) {
                // Allow the action to immediately transition from POST_IN_PROGRESS to FAILED,
                // if no post-execution workflow is defined
                action.receive(new AfterFailureEvent(errorDescription));
            }

            logger.info("Action execution failed for action: {}", action);

            failedCloudVMGroupProcessor.handleActionFailure(action);
            saveToDb(action);
            writeToAudit(action, false);

            // Allow the action to initate a post-execution workflow, if one is defined
            if (ActionState.POST_IN_PROGRESS == action.getState() && action.hasPendingExecution()) {
                // There is a POST workflow defined, which needs to be run even after a failure
                continueActionExecution(action);
            } else {
                try {
                    notificationSender.notifyActionFailure(actionFailure);
                } catch (CommunicationException | InterruptedException e) {
                    logger.error("Unable to send notification for failure of " + actionFailure, e);
                }
            }

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
                    serializedAction.getCurrentState().getNumber(),
                    serializedAction.getActionDetailData()
            );
        } catch (RuntimeException e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * For actions with multiple steps (e.g. PRE and POST), kick off the next stage of execution.
     *
     * @param action to continue executing
     */
    private void continueActionExecution(final Action action) {
        final Optional<ActionDTO.Action> translatedRecommendation =
            action.getActionTranslation().getTranslatedRecommendation();
        if (translatedRecommendation.isPresent() && action.getCurrentExecutableStep().isPresent()) {
            long targetId = action.getCurrentExecutableStep().get().getTargetId();
            // execute the action, passing the workflow override (if any)
            try {
                actionExecutor.execute(targetId, translatedRecommendation.get(),
                    action.getWorkflow(workflowStore));
            } catch (ExecutionStartException e) {
                logger.error("Failed to start next executable step of action " + action.getId()
                    + " due to error: " + e.getMessage(), e);
                action.receive(new FailureEvent(e.getMessage()));
            }
        } else {
            logger.error("Failed to start next executable step of action {}.",
                action.getId());
            action.receive(new FailureEvent("Failed to start next execution step of action."));
        }
    }
}

package com.vmturbo.action.orchestrator.execution.notifications;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.Action.SerializationState;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ProgressEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.SuccessEvent;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionSchedule;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.api.ActionOrchestratorNotificationSender;
import com.vmturbo.action.orchestrator.audit.ActionAuditSender;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.ExecutionStartException;
import com.vmturbo.action.orchestrator.execution.FailedCloudVMGroupProcessor;
import com.vmturbo.action.orchestrator.state.machine.Transition.TransitionResult;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStoreException;
import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLog;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResponse;
import com.vmturbo.topology.processor.api.ActionExecutionListener;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.ActionsLost;

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

    private final AcceptedActionsDAO acceptedActionsStore;

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

    private final ActionAuditSender auditSender;

    private final IMessageSender<ActionResponse> actionUpdatesSender;

    /**
     * Create a new {@link ActionStateUpdater}.
     *
     * @param actionStorehouse The storehouse in which to look up actions as notifications are
     * received.
     * @param notificationSender The API backend to send notifications to.
     * @param actionHistoryDao dao layer persists information about executed actions
     * @param acceptedActionsStore dao layer works with acceptances for actions
     * @param actionExecutor to execute actions (by sending them to Topology Processor)
     * @param workflowStore the store for all the known {@link WorkflowDTO.Workflow} items
     * @param realtimeTopologyContextId The ID of the topology context for realtime market analysis
     * @param failedCloudVMGroupProcessor to process failed actions and add VM entities to a group.
     * @param auditSender audit events message sender to report for finished actions
     * @param actionStateUpdatesSender action state updates sender
     */
    public ActionStateUpdater(@Nonnull final ActionStorehouse actionStorehouse,
            @Nonnull final ActionOrchestratorNotificationSender notificationSender,
            @Nonnull final ActionHistoryDao actionHistoryDao,
            @Nonnull final AcceptedActionsDAO acceptedActionsStore,
            @Nonnull final ActionExecutor actionExecutor,
            @Nonnull final WorkflowStore workflowStore, final long realtimeTopologyContextId,
            final FailedCloudVMGroupProcessor failedCloudVMGroupProcessor,
            @Nonnull final ActionAuditSender auditSender,
            @Nonnull final IMessageSender<ActionResponse> actionStateUpdatesSender) {
        this.actionStorehouse = Objects.requireNonNull(actionStorehouse);
        this.actionHistoryDao = Objects.requireNonNull(actionHistoryDao);
        this.acceptedActionsStore = acceptedActionsStore;
        this.notificationSender = Objects.requireNonNull(notificationSender);
        this.actionExecutor = Objects.requireNonNull(actionExecutor);
        this.workflowStore = Objects.requireNonNull(workflowStore);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.failedCloudVMGroupProcessor = failedCloudVMGroupProcessor;
        this.auditSender = Objects.requireNonNull(auditSender);
        this.actionUpdatesSender = Objects.requireNonNull(actionStateUpdatesSender);
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
                sendStateUpdateIfNeeded(action,
                        getActionStateUpdateDescription(actionProgress.getDescription(),
                                "in progress", action.getRecommendationOid()),
                        actionProgress.getProgressPercentage());
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
            // Notify the action of the successful completion, possibly triggering a transition
            // within the action's state machine.
            final TransitionResult<ActionState> transitionResult = action.receive(new SuccessEvent());

            failedCloudVMGroupProcessor.handleActionSuccess(action);
            writeSuccessActionToAudit(action);
            processActionState(action, transitionResult, true);

            // Multiple SuccessEvents can also be triggered for a single action that has PRE
            // and POST states. If the action does not transition to the succeeded state, then
            // this is a partial completion, which does not indicate that the overall action has
            // completed yet.
            final ActionState actionStateAfterTransition = transitionResult.getAfterState();
            if (actionStateAfterTransition == ActionState.SUCCEEDED) {
                // If the action transitions to the SUCCEEDED state, notify the rest of the system
                logger.info("Action executed successfully: {}", action);
                removeAcceptanceForSuccessfullyExecutedAction(action);
                notifySystemAboutSuccessfulActionExecution(actionSuccess, action);
            }
        } else {
            logger.error("Unable to mark success for " + actionSuccess);
        }
    }

    private void notifySystemAboutSuccessfulActionExecution(@Nonnull ActionSuccess actionSuccess,
            @Nonnull Action action) {
        try {
            auditSender.sendActionEvents(Collections.singleton(action));
            notificationSender.notifyActionSuccess(actionSuccess);
            sendStateUpdateIfNeeded(action,
                    getActionStateUpdateDescription(actionSuccess.getSuccessDescription(),
                            "executed successfully", action.getRecommendationOid()), 100);
        } catch (CommunicationException | InterruptedException e) {
            logger.error("Unable to send notification for success of " + actionSuccess, e);
        }
    }

    /**
     * Remove acceptance for action if it was manually accepted and had execution scheduled window.
     *
     * @param action executed action
     */
    private void removeAcceptanceForSuccessfullyExecutedAction(Action action) {
        final Optional<ActionSchedule> actionSchedule = action.getSchedule();
        if (actionSchedule.isPresent() && actionSchedule.get().getAcceptingUser() != null) {
            acceptedActionsStore.deleteAcceptedAction(action.getRecommendationOid());
            logger.debug("Acceptance was removed for successfully executed action - {}.",
                    action.toString());
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
            failAction(action, actionFailure);
        } else {
            logger.error("Unable to mark failure for " + actionFailure);
        }
    }

    @Override
    public void onActionsLost(@Nonnull final ActionsLost actionsLost) {
        Optional<ActionStore> storeOpt = actionStorehouse.getStore(realtimeTopologyContextId);
        if (!storeOpt.isPresent()) {
            // No realtime action store - can't have in-progress actions!
            return;
        }

        final ActionStore liveActionStore = storeOpt.get();
        final Stream<ActionView> targetActions;
        if (actionsLost.getBeforeTime() > 0) {
            targetActions = liveActionStore.getActionViews().get(ActionQueryFilter.newBuilder()
                    .addStates(ActionState.IN_PROGRESS)
                    .addStates(ActionState.PRE_IN_PROGRESS)
                    .addStates(ActionState.POST_IN_PROGRESS)
                    .build())
                .filter(actionView -> actionView.getDecision()
                    // Only include actions that HAD a decision (should be all in progress
                    // actions) before the "before time".
                    //
                    // The time check is mainly a safeguard against long delays in message
                    // reception (e.g. if Kafka is down or overloaded/unresponsibe). By using
                    // timestamps we are introducing the possibility of some errors if the clocks
                    // between components are out of sync. However, it should be safe to assume
                    // that they will be loosely in sync and that's all we need: this is mainly to
                    // prevent messages that are delivered much later (e.g. an hour later) from
                    // clearing all in-progress actions indiscriminately.
                    .map(decision -> decision.getDecisionTime() < actionsLost.getBeforeTime())
                    .orElse(false));
        } else if (!actionsLost.getLostActionId().getActionIdsList().isEmpty()) {
            targetActions = liveActionStore.getActionViews()
                .get(actionsLost.getLostActionId().getActionIdsList());
        } else {
            targetActions = Stream.empty();
        }

        targetActions.forEach(actionView -> {
            liveActionStore.getAction(actionView.getId())
                .ifPresent(action -> failAction(action, ActionFailure.newBuilder()
                    .setErrorDescription("Topology Processor lost action state.")
                    .setActionId(actionView.getId())
                    .build()));
        });
    }

    private void failAction(@Nonnull final Action action,
                            @Nonnull final ActionFailure actionFailure) {
        final String errorDescription =
                getActionStateUpdateDescription(actionFailure.getErrorDescription(),
                        "failed execution", action.getRecommendationOid());
        // Notify the action of the failure, possibly triggering a transition
        // within the action's state machine.
        final TransitionResult<ActionState> transitionResult =
                action.receive(new FailureEvent(errorDescription));
        logger.info("Action execution failed for action: {}", action);

        if (action.getMode() == ActionMode.EXTERNAL_APPROVAL) {
            removeAcceptanceForExternalAcceptedAction(action);
        }

        failedCloudVMGroupProcessor.handleActionFailure(action);
        writeFailActionToAudit(action, actionFailure);
        processActionState(action, transitionResult, false);

        final ActionState actionStateAfterTransition = transitionResult.getAfterState();
        // if IN_PROGRESS execution was failed for action then general execution result will
        // be FAILED for action regardless of results of POST_IN_PROGRESS execution
        if (actionStateAfterTransition == ActionState.FAILED || actionStateAfterTransition == ActionState.POST_IN_PROGRESS) {
            notifySystemAboutFailedActionExecution(action, actionFailure, errorDescription);
        }
    }

    private void notifySystemAboutFailedActionExecution(@Nonnull Action action,
            @Nonnull ActionFailure actionFailure, @Nonnull String errorDescription) {
        try {
            auditSender.sendActionEvents(Collections.singleton(action));
            notificationSender.notifyActionFailure(actionFailure);
            sendStateUpdateIfNeeded(action, errorDescription, 100);
        } catch (CommunicationException | InterruptedException e) {
            logger.error("Unable to send notification for failure of " + actionFailure, e);
        }
    }

    /**
     * If action in terminal state then store execution results in DB otherwise continue
     * execution (i.g. executing POST workflow).
     *
     * @param action the action
     * @param transitionResult the transition results for this action
     * @param isSuccessful if true then action completed state successfully otherwise
     * unsuccessfully
     */
    private void processActionState(@Nonnull final Action action,
            @Nonnull final TransitionResult<ActionState> transitionResult, boolean isSuccessful) {
        if (action.isFinished()) {
            saveToDb(action);
        } else {
            final String stateCompleteResult = isSuccessful ? "successfully" : "failed";
            logger.debug("Action {} {} completed state {} and transitioned to state {}.",
                    action::getId, () -> stateCompleteResult, transitionResult::getBeforeState,
                    action::getState);
            if (action.hasPendingExecution()) {
                continueActionExecution(action);
            }
        }
    }

    private void sendStateUpdateIfNeeded(@Nonnull Action action, @Nonnull String description,
            int progressPercentage) throws CommunicationException, InterruptedException {
        if (action.getMode() == ActionMode.EXTERNAL_APPROVAL) {
            final ActionResponseState actionResponseState =
                    mapActionStateToActionResponseState(action.getState());
            final ActionResponse actionResponse = ActionResponse.newBuilder()
                    .setActionOid(action.getRecommendationOid())
                    .setActionResponseState(actionResponseState)
                    .setProgress(progressPercentage)
                    .setResponseDescription(description)
                    .build();
            actionUpdatesSender.sendMessage(actionResponse);
        }
    }

    @Nonnull
    private static ActionResponseState mapActionStateToActionResponseState(
            final ActionState stateStr) {
        switch (stateStr) {
            case READY:
                return ActionResponseState.PENDING_ACCEPT;
            case ACCEPTED:
                return ActionResponseState.ACCEPTED;
            case REJECTED:
                return ActionResponseState.REJECTED;
            case QUEUED:
                return ActionResponseState.QUEUED;
            case SUCCEEDED:
                return ActionResponseState.SUCCEEDED;
            case IN_PROGRESS:
            case PRE_IN_PROGRESS:
            case POST_IN_PROGRESS:
                return ActionResponseState.IN_PROGRESS;
            case FAILED:
                return ActionResponseState.FAILED;
            case CLEARED:
                return ActionResponseState.CLEARED;
            default:
                throw new IllegalArgumentException("Unsupported action state " + stateStr);
        }
    }

    /**
     * Remove acceptance for external accepted action with associated execution window.
     *
     * @param action previously accepted action
     */
    private void removeAcceptanceForExternalAcceptedAction(@Nonnull Action action) {
        final Optional<ActionSchedule> actionSchedule = action.getSchedule();
        if (actionSchedule.isPresent() && actionSchedule.get().getAcceptingUser() != null) {
            acceptedActionsStore.deleteAcceptedAction(action.getRecommendationOid());
            logger.debug(
                    "Acceptance was removed for failed execution of externally accepted action - {}.",
                    action.toString());
        }
    }

    /**
     * Send success action execution result to remote audit log.
     *
     * @param action the executed action.
     */
    private void writeSuccessActionToAudit(@Nonnull final Action action) {
        try {
            AuditLog.newEntry(AuditAction.EXECUTE_ACTION, Objects.requireNonNull(action).getDescription(), true)
                .targetName(String.valueOf(action.getId()))
                .audit();
        } catch (RuntimeException e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * Send failed action execution result to remote audit log.
     *
     * @param action the executed action.
     * @param actionFailure {@link ActionFailure} action failure object.
     */
    private void writeFailActionToAudit(@Nonnull final Action action,
                                        @Nonnull final ActionFailure actionFailure) {
        try {
            String actionDescription = Objects.requireNonNull(action).getDescription();
            String errorDescription = Objects.requireNonNull(actionFailure).getErrorDescription();
            final String descriptionWithFailureReason = String.format("Action: %s.  Failure: %s",
                actionDescription,
                errorDescription);
            AuditLog.newEntry(AuditAction.EXECUTE_ACTION, descriptionWithFailureReason, false)
                .targetName(String.valueOf(action.getId()))
                .audit();
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
                    serializedAction.getActionDetailData(),
                    serializedAction.getAssociatedAccountId(),
                    serializedAction.getAssociatedResourceGroupId(),
                    serializedAction.getRecommendationOid()
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
            } catch (ExecutionStartException | WorkflowStoreException e) {
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

    private String getActionStateUpdateDescription(@Nonnull String stateUpdateDescription,
            @Nonnull String defaultStateUpdateDescription, long recommendationID) {
        if (StringUtils.isNotBlank(stateUpdateDescription)) {
            return stateUpdateDescription;
        } else {
            return String.format("Action with %s OID %s", recommendationID,
                    defaultStateUpdateDescription);
        }
    }
}

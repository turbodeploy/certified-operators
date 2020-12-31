package com.vmturbo.action.orchestrator.audit;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionEvent;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStoreException;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Audit events sender for internal action states.
 */
public class ActionAuditSender {

    private final WorkflowStore workflowStore;
    private final IMessageSender<ActionEvent> messageSender;
    private final Logger logger = LogManager.getLogger(getClass());
    private final ThinTargetCache thinTargetCache;
    /**
     * Persist actions which were already sent for audit (raw - actionOID, column - workflowId,
     * value ActionView).
     * Action can be send for audit to several targets (defines by discovered workflow).
     * We also persist information about {@link ActionView}, because we need to know information
     * about action when action won't be recommended by market but we need to send CLEARED
     * event to external audit.
     */
    // TODO OM-64028: clean actions if target discovered workflow was removed
    private final Table<Long, Long, ActionView> sentActionsToWorkflowCache =
            HashBasedTable.create();
    private final ActionTranslator actionTranslator;


    /**
     * Constructs action audit sender.
     *
     * @param workflowStore workflow store
     * @param messageSender notifications sender
     * @param thinTargetCache target cache
     * @param actionTranslator action translator
     */
    public ActionAuditSender(@Nonnull WorkflowStore workflowStore,
            @Nonnull IMessageSender<ActionEvent> messageSender,
            @Nonnull ThinTargetCache thinTargetCache,
            @Nonnull ActionTranslator actionTranslator) {
        this.workflowStore = Objects.requireNonNull(workflowStore);
        this.messageSender = Objects.requireNonNull(messageSender);
        this.thinTargetCache = Objects.requireNonNull(thinTargetCache);
        this.actionTranslator = Objects.requireNonNull(actionTranslator);
    }

    /**
     * Receives new actions.
     *
     * @param actions new actions received.
     * @throws CommunicationException if communication error occurred while sending
     *         notifications
     * @throws InterruptedException if current thread has been interrupted
     */
    public void sendActionEvents(@Nonnull Collection<? extends ActionView> actions)
            throws CommunicationException, InterruptedException {
        final MutableInt auditedActions = new MutableInt(0);
        for (ActionView action : actions) {
            Optional<WorkflowDTO.Workflow> workflowOptional;
            try {
                workflowOptional = action.getWorkflow(workflowStore, action.getState());
            } catch (WorkflowStoreException e) {
                logger.warn("Failed to fetch workflow for action {}. Audit will be skipped.",
                        action.getId());
                workflowOptional = Optional.empty();
            }
            if (workflowOptional.isPresent()) {
                processWorkflow(action, workflowOptional.get(), auditedActions);
            } else {
                logger.trace(
                        "Action {} does not have a currently associated workflow. Skip the audit",
                        action::getId);
            }
        }
        processCanceledActions(actions);
        if (auditedActions.getValue() > 0) {
            logger.info("Sent {} of {} actions for external audit", auditedActions, actions.size());
        }
    }

    private void processWorkflow(@Nonnull ActionView action, @Nonnull Workflow workflow,
            @Nonnull MutableInt auditedActions)
            throws CommunicationException, InterruptedException {
        final Optional<ThinTargetInfo> auditTarget = getAuditTarget(workflow);
        if (auditTarget.isPresent()) {
            final String auditTargetType = auditTarget.get().probeInfo().type();
            // TODO OM-64606 remove this ServiceNow specific logic after changing the ServiceNow app
            //  and having generic audit logic for orchestration targets
            if (auditTargetType.equals(SDKProbeType.SERVICENOW.getProbeType())) {
                processServiceNowWorkflow(action, workflow, auditedActions);
            } else {
                processAuditWorkflow(action, workflow, auditedActions);
            }
        } else {
            logger.error("Failed to get target discovered workflow {}. Audit was skipped.",
                    workflow.getId());
        }
    }

    private void processCanceledActions(@Nonnull Collection<? extends ActionView> actions)
            throws CommunicationException, InterruptedException {
        final Set<Long> currentActions =
                actions.stream().map(ActionView::getRecommendationOid).collect(Collectors.toSet());
        final Map<Long, Long> canceledActions = new HashMap<>();
        for (Cell<Long, Long, ActionView> cell : sentActionsToWorkflowCache.cellSet()) {
            final Long actionOID = cell.getRowKey();
            final Long workflowId = cell.getColumnKey();
            final ActionView actionView = cell.getValue();
            if (!currentActions.contains(actionOID) && sendCanceledAction(actionView)) {
                canceledActions.put(actionOID, workflowId);
            }
        }
        if (canceledActions.size() > 0) {
            logger.info("Sent {} CANCELED actions for external audit", canceledActions.size());
            // remove CANCELED actions from cache
            canceledActions.forEach(sentActionsToWorkflowCache::remove);
        }
    }

    private boolean sendCanceledAction(@Nullable ActionView action)
            throws CommunicationException, InterruptedException {
        if (action != null) {
            Optional<WorkflowDTO.Workflow> workflowOptional;
            try {
                // we need to check absence of workflow, because it can be missed in the
                // environment for action which is not recommended in latest market cycle
                workflowOptional = action.getWorkflow(workflowStore, ActionState.READY);
            } catch (WorkflowStoreException e) {
                logger.warn("Failed to fetch workflow for action {}. CANCELED event won't be send"
                        + " for audit.", action.getId());
                workflowOptional = Optional.empty();
            }
            if (workflowOptional.isPresent()) {
                final Workflow workflow = workflowOptional.get();
                final ActionEvent event =
                        getActionEvent(action, workflow, ActionResponseState.PENDING_ACCEPT,
                                ActionResponseState.CLEARED);
                logger.debug("Sending CANCELED event for action {} to external audit",
                        action.getRecommendationOid());
                messageSender.sendMessage(event);
                return true;
            }
        }
        return false;
    }

    private void processServiceNowWorkflow(@Nonnull ActionView action,
            @Nonnull Workflow workflow, @Nonnull MutableInt auditedActions)
            throws CommunicationException, InterruptedException {
        final ActionResponseState oldState;
        final ActionResponseState newState;
        switch (workflow.getWorkflowInfo().getActionPhase()) {
            case ON_GENERATION: {
                logger.debug("Found action event for {} to have and onGeneration action phase {}",
                        action::getId, workflow::getId);
                oldState = ActionResponseState.PENDING_ACCEPT;
                newState = ActionResponseState.PENDING_ACCEPT;
                break;
            }
            case AFTER_EXECUTION: {
                logger.debug("Found action event for {} to have and afterExecution action phase {}",
                        action::getId, workflow::getId);
                oldState = ActionResponseState.IN_PROGRESS;
                newState = action.getState() == ActionState.SUCCEEDED
                        ? ActionResponseState.SUCCEEDED
                        : ActionResponseState.FAILED;
                break;
            }
            default:
                logger.trace("Action {}'s workflow {} ({}) is for phase {}. Skipping audit",
                        action::getId, workflow::getId, () -> workflow.getWorkflowInfo().getName(),
                        () -> workflow.getWorkflowInfo().getActionPhase());
                return;
        }

        final ActionEvent event = getActionEvent(action, workflow, oldState, newState);
        logger.debug("Sending action {} audit event to external audit", action.getId());
        messageSender.sendMessage(event);
        auditedActions.increment();
    }

    private void processAuditWorkflow(@Nonnull ActionView action, @Nonnull Workflow workflow,
            @Nonnull MutableInt auditedActions)
            throws CommunicationException, InterruptedException {
        if (!isAlreadySent(action.getRecommendationOid(), workflow.getId())) {
            final ActionResponseState oldState;
            final ActionResponseState newState;
            switch (workflow.getWorkflowInfo().getActionPhase()) {
                case ON_GENERATION: {
                    logger.debug(
                            "Found action event for {} to have and onGeneration action phase {}",
                            action::getId, workflow::getId);
                    oldState = ActionResponseState.PENDING_ACCEPT;
                    newState = ActionResponseState.PENDING_ACCEPT;
                    break;
                }
                // TODO OM-64830 implement sending action execution results using AFTER_EXECUTION phase
                default:
                    logger.trace("Action {}'s workflow {} ({}) is for phase {}. Skipping audit",
                            action::getId, workflow::getId,
                            () -> workflow.getWorkflowInfo().getName(),
                            () -> workflow.getWorkflowInfo().getActionPhase());
                    return;
            }

            final ActionEvent event = getActionEvent(action, workflow, oldState, newState);
            sentActionsToWorkflowCache.put(action.getRecommendationOid(), workflow.getId(),
                    action);
            logger.debug("Sending action {} audit event to external audit", action.getId());
            messageSender.sendMessage(event);
            auditedActions.increment();
        } else {
            logger.trace("Action {} was already sent for audit.", action::getId);
        }
    }

    private ActionEvent getActionEvent(@Nonnull ActionView action, @Nonnull Workflow workflow,
            @Nonnull ActionResponseState oldState, @Nonnull ActionResponseState newState) {
        final ExecuteActionRequest request =
                ActionExecutor.createRequest(workflow.getWorkflowInfo().getTargetId(),
                    actionTranslator.translateToSpec(action), Optional.of(workflow),
                    action.getDescription(), action.getRecommendationOid());
        return ActionEvent.newBuilder()
                .setOldState(oldState)
                .setNewState(newState)
                .setTimestamp(System.currentTimeMillis())
                .setActionRequest(request)
                .build();
    }

    private Optional<ThinTargetInfo> getAuditTarget(Workflow workflow) {
        long workflowTargetId = workflow.getWorkflowInfo().getTargetId();
        return thinTargetCache.getTargetInfo(workflowTargetId);
    }

    private boolean isAlreadySent(long actionOid, long workflowId) {
        return sentActionsToWorkflowCache.contains(actionOid, workflowId);
    }
}

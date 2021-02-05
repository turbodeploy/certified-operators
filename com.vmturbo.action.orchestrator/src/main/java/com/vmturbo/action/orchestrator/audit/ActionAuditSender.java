package com.vmturbo.action.orchestrator.audit;

import java.time.Clock;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Table.Cell;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.AuditedActionInfo;
import com.vmturbo.action.orchestrator.action.AuditedActionsManager;
import com.vmturbo.action.orchestrator.action.AuditedActionsManager.AuditedActionsUpdate;
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
    private final ActionTranslator actionTranslator;

    private final AuditedActionsManager auditedActionsManager;

    private final long minsClearedActionsCriteria;

    private final Clock clock;


    /**
     * Constructs action audit sender.
     *
     * @param workflowStore workflow store
     * @param messageSender notifications sender
     * @param thinTargetCache target cache
     * @param actionTranslator action translator
     * @param auditedActionsManager object responsible for maintaining the book keeping.
     * @param minsClearedActionsCriteria the amount of time that needs to pass before concluding an
     *                                   action is cleared.
     * @param clock the object used to get the current time.
     */
    public ActionAuditSender(
            @Nonnull WorkflowStore workflowStore,
            @Nonnull IMessageSender<ActionEvent> messageSender,
            @Nonnull ThinTargetCache thinTargetCache,
            @Nonnull ActionTranslator actionTranslator,
            @Nonnull AuditedActionsManager auditedActionsManager,
            final long minsClearedActionsCriteria,
            @Nonnull Clock clock) {
        this.workflowStore = Objects.requireNonNull(workflowStore);
        this.messageSender = Objects.requireNonNull(messageSender);
        this.thinTargetCache = Objects.requireNonNull(thinTargetCache);
        this.actionTranslator = Objects.requireNonNull(actionTranslator);
        this.auditedActionsManager = Objects.requireNonNull(auditedActionsManager);
        this.minsClearedActionsCriteria = minsClearedActionsCriteria;
        this.clock = Objects.requireNonNull(clock);
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
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        final MutableInt auditedActions = new MutableInt(0);
        final AuditedActionsUpdate auditedActionsUpdates = new AuditedActionsUpdate();
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
                processWorkflow(action, workflowOptional.get(), auditedActions, auditedActionsUpdates);
            } else {
                logger.trace(
                        "Action {} does not have a currently associated workflow. Skip the audit",
                        action::getId);
            }
        }
        processCanceledActions(actions, auditedActionsUpdates);
        // persist all updates of audited actions
        auditedActionsManager.persistAuditedActionsUpdates(auditedActionsUpdates);
        stopWatch.stop();
        logger.info("Took {} to send {} of {} actions for external audit",
            stopWatch, auditedActions, actions.size());
    }

    private void processWorkflow(@Nonnull ActionView action, @Nonnull Workflow workflow,
            @Nonnull MutableInt auditedActions,
            @Nonnull AuditedActionsUpdate auditedActionsUpdates)
            throws CommunicationException, InterruptedException {
        final Optional<ThinTargetInfo> auditTarget = getAuditTarget(workflow);
        if (auditTarget.isPresent()) {
            final String auditTargetType = auditTarget.get().probeInfo().type();
            // TODO OM-64606 remove this ServiceNow specific logic after changing the ServiceNow app
            //  and having generic audit logic for orchestration targets
            if (auditTargetType.equals(SDKProbeType.SERVICENOW.getProbeType())) {
                processServiceNowWorkflow(action, workflow, auditedActions);
            } else {
                processAuditWorkflow(action, workflow, auditedActions, auditedActionsUpdates);
            }
        } else {
            logger.error("Failed to get target discovered workflow {}. Audit was skipped.",
                    workflow.getId());
        }
    }

    private void processCanceledActions(@Nonnull Collection<? extends ActionView> actions,
            @Nonnull AuditedActionsUpdate auditedActionsUpdates)
            throws CommunicationException, InterruptedException {
        final Set<Long> currentActions =
                actions.stream().map(ActionView::getRecommendationOid).collect(Collectors.toSet());
        final Collection<Cell<Long, Long, Optional<Long>>> alreadySentActions =
                auditedActionsManager.getAlreadySentActions();
        for (Cell<Long, Long, Optional<Long>> cell : alreadySentActions) {
            final Long actionOID = cell.getRowKey();
            final Long workflowId = cell.getColumnKey();
            final Optional<Long> clearedTimestamp = cell.getValue();
            if (!currentActions.contains(actionOID)) {
                final Long clearedTime;
                if (!clearedTimestamp.isPresent()) {
                    // persist timestamp when action first time was not recommended after sending
                    // this action earlier for ON_GEN audit
                    clearedTime = clock.millis();
                    auditedActionsUpdates.addRecentlyClearedAction(
                            new AuditedActionInfo(actionOID, workflowId, clearedTime));
                } else {
                    clearedTime = clearedTimestamp.get();
                }
                if (isClearedCriteriaMet(clearedTime)) {
                    // if earlier audited action is not recommended during certain time and met
                    // cleared criteria, then we send it as CLEARED
                    sendCanceledAction(actionOID, workflowId);
                    auditedActionsUpdates.addExpiredClearedAction(
                            new AuditedActionInfo(actionOID, workflowId, null));
                }
            } else if (clearedTimestamp != null && clearedTimestamp.isPresent()) {
                // reset cleared_timestamp, because action which wasn't recommended during certain time is came back
                auditedActionsUpdates.addAuditedAction(
                        new AuditedActionInfo(actionOID, workflowId, null));
            }
        }
    }

    private boolean isClearedCriteriaMet(@Nonnull Long clearedTimestamp) {
        final Long currentTime = clock.millis();
        long clearedDurationMsec = currentTime - clearedTimestamp;
        return clearedDurationMsec > TimeUnit.MINUTES.toMillis(minsClearedActionsCriteria);
    }

    private boolean sendCanceledAction(@Nonnull Long recommendationId, @Nonnull Long workflowId)
            throws CommunicationException, InterruptedException {
        Optional<Workflow> workflowOptional;
        try {
            // we need to check absence of workflow, because it can be missed in the
            // environment for action which is not recommended in latest market cycle
            workflowOptional = workflowStore.fetchWorkflow(workflowId);
        } catch (WorkflowStoreException e) {
            logger.warn("Failed to fetch workflow with {} id  for action {}. CANCELED event won't "
                    + "be send for audit.", workflowId, recommendationId);
            workflowOptional = Optional.empty();
        }
        if (workflowOptional.isPresent()) {
            final Workflow workflow = workflowOptional.get();
            final ActionEvent event = getCanceledActionEvent(recommendationId, workflow,
                    ActionResponseState.PENDING_ACCEPT, ActionResponseState.CLEARED);
            logger.debug("Sending CANCELED event for action {} to external audit",
                    recommendationId);
            messageSender.sendMessage(event);
            return true;
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
            @Nonnull MutableInt auditedActions,
            @Nonnull AuditedActionsUpdate auditedActionsUpdates)
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
            auditedActionsUpdates.addAuditedAction(
                    new AuditedActionInfo(action.getRecommendationOid(), workflow.getId(), null));
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
                .setTimestamp(clock.millis())
                .setActionRequest(request)
                .build();
    }

    private ActionEvent getCanceledActionEvent(@Nonnull Long recommendationId, @Nonnull Workflow workflow,
            @Nonnull ActionResponseState oldState, @Nonnull ActionResponseState newState) {
        final ExecuteActionRequest request = ExecuteActionRequest.newBuilder()
                .setActionId(recommendationId)
                .setTargetId(workflow.getWorkflowInfo().getTargetId())
                .build();
        return ActionEvent.newBuilder()
                .setOldState(oldState)
                .setNewState(newState)
                .setTimestamp(clock.millis())
                .setActionRequest(request)
                .build();
    }

    private Optional<ThinTargetInfo> getAuditTarget(Workflow workflow) {
        long workflowTargetId = workflow.getWorkflowInfo().getTargetId();
        return thinTargetCache.getTargetInfo(workflowTargetId);
    }

    private boolean isAlreadySent(long actionOid, long workflowId) {
        return auditedActionsManager.isAlreadySent(actionOid, workflowId);
    }
}

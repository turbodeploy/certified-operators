package com.vmturbo.action.orchestrator.audit;

import java.time.Clock;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
     * Sends audits for completed actions to their appropriate destination. Clears any book keeping
     * for that action if there is any. This is separate from sendOnGenerationEvents because the
     * book keeping there is different.
     *
     * @param action the action that completes.
     * @throws CommunicationException if communication error occurred while sending
     *         notifications
     * @throws InterruptedException if current thread has been interrupted while blocking.
     */
    public void sendAfterExecutionEvents(@Nonnull ActionView action)
        throws CommunicationException, InterruptedException {
        Optional<WorkflowDTO.Workflow> workflowOptional;
        try {
            workflowOptional = action.getWorkflow(workflowStore, action.getState());
        } catch (WorkflowStoreException e) {
            logger.warn("Failed to fetch workflow for action {}. Audit AFTER_EXEC will be skipped.",
                action.getId());
            workflowOptional = Optional.empty();
        }
        if (workflowOptional.isPresent()) {
            final Workflow workflow = workflowOptional.get();
            switch (workflow.getWorkflowInfo().getActionPhase()) {
                case AFTER_EXECUTION: {
                    logger.debug("Found action event for action {} with AFTER_EXEC workflow {}",
                        action::getId, workflow::getId);

                    final ActionResponseState newState;
                    if (action.getState() == ActionState.SUCCEEDED) {
                        newState = ActionResponseState.SUCCEEDED;
                    } else if (action.getState() == ActionState.FAILED) {
                        newState = ActionResponseState.FAILED;
                    } else {
                        logger.error(
                            "For action {}: unsupported action with state {} for after exec ",
                            action, action.getState());
                        break;
                    }

                    final ActionEvent event = getActionEvent(action, workflow, ActionResponseState.IN_PROGRESS, newState);
                    logger.debug("Sending action {} audit AFTER_EXEC event to external audit", action.getId());
                    messageSender.sendMessage(event);

                    // TODO OM-64606 remove this ServiceNow specific logic after changing the
                    //      ServiceNow app and having generic audit logic for orchestration targets
                    if (!Optional.of(SDKProbeType.SERVICENOW.getProbeType()).equals(getProbeType(workflow))) {
                        final AuditedActionsUpdate update = new AuditedActionsUpdate();
                        update.addRemovedActionRecommendationOid(action.getRecommendationOid());
                        auditedActionsManager.persistAuditedActionsUpdates(update);
                    }
                    break;
                }
                default:
                    logger.trace("Action {}'s workflow {} ({}) is for phase {}. Skipping audit AFTER_EXEC",
                        action::getId, workflow::getId, () -> workflow.getWorkflowInfo().getName(),
                        () -> workflow.getWorkflowInfo().getActionPhase());
                    return;
            }
        } else {
            logger.trace(
                "Action {} does not have a currently associated workflow. Skip the audit AFTER_EXEC",
                action::getId);
        }
    }

    /**
     * Receives new actions.
     *
     * @param actions new actions received.
     * @throws CommunicationException if communication error occurred while sending
     *         notifications
     * @throws InterruptedException if current thread has been interrupted
     */
    public void sendOnGenerationEvents(@Nonnull Collection<? extends ActionView> actions)
            throws CommunicationException, InterruptedException {
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        final AuditedActionsUpdate auditedActionsUpdates = new AuditedActionsUpdate();
        final int auditedActionsCount =
                processActionsForAudit(actions, auditedActionsUpdates, false);
        processCanceledActions(actions, auditedActionsUpdates);
        // persist all updates of audited actions
        auditedActionsManager.persistAuditedActionsUpdates(auditedActionsUpdates);
        stopWatch.stop();
        logger.info("Took {} to send {} of {} actions for external audit", stopWatch,
                auditedActionsCount, actions.size());
    }

    /**
     * Resends the actions which were audited earlier.
     *
     * @param actions list of actions which will be resend for audit
     * @return number of action which were resent for audit
     * @throws CommunicationException if communication error occurred while sending
     * notifications
     * @throws InterruptedException if current thread has been interrupted
     */
    public int resendActionEvents(@Nonnull Collection<? extends ActionView> actions)
            throws CommunicationException, InterruptedException {
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        final int reAuditedActionsCount = processActionsForAudit(actions, null, true);
        logger.info("Took {} ms to resend {} actions for external audit",
                stopWatch.getTime(TimeUnit.MILLISECONDS), reAuditedActionsCount);
        return reAuditedActionsCount;
    }

    private int processActionsForAudit(@Nonnull Collection<? extends ActionView> actions,
            @Nullable AuditedActionsUpdate auditedActionsUpdates, boolean allowRepeatedAudit)
            throws CommunicationException, InterruptedException {
        int auditedActionsCount = 0;
        for (ActionView action : actions) {
            Optional<WorkflowDTO.Workflow> workflowOptional;
            try {
                workflowOptional = action.getWorkflow(workflowStore, action.getState());
            } catch (WorkflowStoreException e) {
                logger.warn("Failed to fetch workflow for action {}. Audit ON_GEN will be skipped.",
                        action.getId());
                workflowOptional = Optional.empty();
            }
            if (workflowOptional.isPresent()) {
                boolean wasActionAudited =
                        processWorkflow(action, workflowOptional.get(), auditedActionsUpdates,
                                allowRepeatedAudit);
                if (wasActionAudited) {
                    auditedActionsCount++;
                }
            } else {
                logger.trace(
                        "Action {} does not have a currently associated workflow. Skip the audit",
                        action::getId);
            }
        }
        return auditedActionsCount;
    }

    private boolean processWorkflow(@Nonnull ActionView action, @Nonnull Workflow workflow,
            @Nullable AuditedActionsUpdate auditedActionsUpdates,
            boolean allowRepeatedAudit)
            throws CommunicationException, InterruptedException {
        Optional<String> probeTypeOpt = getProbeType(workflow);
        if (probeTypeOpt.isPresent()) {
            // TODO OM-64606 remove this ServiceNow specific logic after changing the ServiceNow app
            //  and having generic audit logic for orchestration targets
            if (SDKProbeType.SERVICENOW.getProbeType().equals(probeTypeOpt.get())) {
                return processServiceNowWorkflow(action, workflow);
            } else {
                return processAuditWorkflow(action, workflow, auditedActionsUpdates,
                        allowRepeatedAudit);
            }
        } else {
            logger.error("Failed to get target discovered workflow {}. Audit was skipped.",
                    workflow.getId());
            return false;
        }
    }

    private Optional<String> getProbeType(Workflow workflow) {
        final Optional<ThinTargetInfo> auditTarget = getAuditTarget(workflow);
        return auditTarget.map(thinTargetInfo -> thinTargetInfo.probeInfo().type());
    }

    private void processCanceledActions(@Nonnull Collection<? extends ActionView> actions,
            @Nonnull AuditedActionsUpdate auditedActionsUpdates)
            throws CommunicationException, InterruptedException {
        final Set<Long> currentActions =
                actions.stream().map(ActionView::getRecommendationOid).collect(Collectors.toSet());
        final Collection<AuditedActionInfo> alreadySentActions =
                auditedActionsManager.getAlreadySentActions();
        for (AuditedActionInfo auditedAction : alreadySentActions) {
            final long recommendationId = auditedAction.getRecommendationId();
            final long workflowId = auditedAction.getWorkflowId();
            final Optional<Long> clearedTimestamp = auditedAction.getClearedTimestamp();
            if (!currentActions.contains(recommendationId)) {
                final Long clearedTime;
                if (!clearedTimestamp.isPresent()) {
                    // persist timestamp when action first time was not recommended after sending
                    // this action earlier for ON_GEN audit
                    clearedTime = clock.millis();
                    auditedActionsUpdates.addRecentlyClearedAction(
                            new AuditedActionInfo(recommendationId, workflowId, Optional.of(clearedTime)));
                } else {
                    clearedTime = clearedTimestamp.get();
                }
                if (isClearedCriteriaMet(clearedTime)) {
                    // if earlier audited action is not recommended during certain time and met
                    // cleared criteria, then we send it as CLEARED
                    sendCanceledAction(recommendationId, workflowId);
                    auditedActionsUpdates.addExpiredClearedAction(
                            new AuditedActionInfo(recommendationId, workflowId, Optional.empty()));
                }
            } else if (clearedTimestamp.isPresent()) {
                // reset cleared_timestamp, because action which wasn't recommended during certain time is came back
                auditedActionsUpdates.addAuditedAction(
                        new AuditedActionInfo(recommendationId, workflowId, Optional.empty()));
            }
        }
    }

    private boolean isClearedCriteriaMet(@Nonnull Long clearedTimestamp) {
        final Long currentTime = clock.millis();
        long clearedDurationMs = currentTime - clearedTimestamp;
        return clearedDurationMs > TimeUnit.MINUTES.toMillis(minsClearedActionsCriteria);
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

    private boolean processServiceNowWorkflow(@Nonnull ActionView action,
            @Nonnull Workflow workflow) throws CommunicationException, InterruptedException {
        final ActionResponseState oldState;
        final ActionResponseState newState;
        switch (workflow.getWorkflowInfo().getActionPhase()) {
            case ON_GENERATION: {
                logger.debug("Found action event for action {} with ON_GEN workflow {}",
                        action::getId, workflow::getId);
                oldState = ActionResponseState.PENDING_ACCEPT;
                newState = ActionResponseState.PENDING_ACCEPT;
                break;
            }
            default:
                logger.trace("Action {}'s workflow {} ({}) is for phase {}. Skipping audit",
                        action::getId, workflow::getId, () -> workflow.getWorkflowInfo().getName(),
                        () -> workflow.getWorkflowInfo().getActionPhase());
                return false;
        }

        final ActionEvent event = getActionEvent(action, workflow, oldState, newState);
        logger.debug("Sending action {} audit event to external audit", action.getId());
        messageSender.sendMessage(event);
        return true;
    }

    private boolean processAuditWorkflow(@Nonnull ActionView action, @Nonnull Workflow workflow,
            @Nullable AuditedActionsUpdate auditedActionsUpdates, boolean allowRepeatedAudit)
            throws CommunicationException, InterruptedException {
        if (!isAlreadySent(action.getRecommendationOid(), workflow.getId()) || allowRepeatedAudit) {
            final ActionResponseState oldState;
            final ActionResponseState newState;
            switch (workflow.getWorkflowInfo().getActionPhase()) {
                case ON_GENERATION: {
                    logger.debug(
                        "Found action event for action {} with ON_GEN workflow {}",
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
                    return false;
            }

            final ActionEvent event = getActionEvent(action, workflow, oldState, newState);
            // when we resend earlier audited actions we don't need to keep track of audited
            // action and persist them in bookkeeping cache
            if (auditedActionsUpdates != null) {
                auditedActionsUpdates.addAuditedAction(
                        new AuditedActionInfo(action.getRecommendationOid(), workflow.getId(),
                                Optional.empty()));
            }
            logger.debug("Sending action {} audit event to external audit", action.getId());
            messageSender.sendMessage(event);
            return true;
        } else {
            logger.trace("Action {} was already sent for audit.", action::getId);
            return false;
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

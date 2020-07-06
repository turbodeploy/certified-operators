package com.vmturbo.action.orchestrator.audit;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionEvent;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;

/**
 * Audit events sender for internal action states.
 */
public class ActionAuditSender {

    private final WorkflowStore workflowStore;
    private final IMessageSender<ActionEvent> messageSender;
    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Constructs action audit sender.
     *
     * @param workflowStore workflow store
     * @param messageSender notifications sender
     */
    public ActionAuditSender(@Nonnull WorkflowStore workflowStore,
            @Nonnull IMessageSender<ActionEvent> messageSender) {
        this.workflowStore = Objects.requireNonNull(workflowStore);
        this.messageSender = Objects.requireNonNull(messageSender);
    }

    /**
     * Receives new actions.
     *
     * @param actions new actions received.
     * @throws CommunicationException if communicatino error occurred while sending
     *         notifications
     * @throws InterruptedException if current thread has been interrupted
     */
    public void sendActionEvents(@Nonnull Collection<ActionView> actions)
            throws CommunicationException, InterruptedException {
        int counter = 0;
        for (ActionView action : actions) {
            final Optional<WorkflowDTO.Workflow> workflowOptional = action.getWorkflow(
                    workflowStore);
            if (workflowOptional.isPresent()) {
                if (processWorkflow(action, workflowOptional.get())) {
                    counter++;
                }
            } else {
                logger.trace(
                        "Action {} does not have a currently associated workflow. Skip the audit",
                        action::getId);
            }
        }
        if (counter > 0) {
            logger.info("Sent {} of {} actions for external audit", counter, actions.size());
        }
    }

    private boolean processWorkflow(@Nonnull ActionView action, @Nonnull WorkflowDTO.Workflow workflow)
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
                return false;
        }

        final ExecuteActionRequest request = ActionExecutor.createRequest(
                workflow.getWorkflowInfo().getTargetId(), action.getRecommendation(),
                Optional.of(workflow), action.getDescription(), action.getRecommendationOid());
        final ActionEvent event = ActionEvent.newBuilder()
                .setOldState(oldState)
                .setNewState(newState)
                .setTimestamp(System.currentTimeMillis())
                .setActionRequest(request)
                .build();
        logger.debug("Sending action {} audit event to external audit", action.getId());
        messageSender.sendMessage(event);
        return true;
    }
}

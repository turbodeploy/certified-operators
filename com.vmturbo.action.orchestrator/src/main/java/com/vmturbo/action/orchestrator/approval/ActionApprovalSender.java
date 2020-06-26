package com.vmturbo.action.orchestrator.approval;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionApprovalRequests;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * Action approval requester is responsible for sending approval requests for action reported by a
 * market.
 */
public class ActionApprovalSender {

    private final Logger logger = LogManager.getLogger(getClass());

    private final WorkflowStore workflowStore;
    private final IMessageSender<ActionApprovalRequests> messageSender;

    /**
     * Constructs action approval sender.
     *
     * @param workflowStore workflow store
     * @param messageSender message sender
     */
    public ActionApprovalSender(@Nonnull WorkflowStore workflowStore,
            @Nonnull IMessageSender<ActionApprovalRequests> messageSender) {
        this.workflowStore = Objects.requireNonNull(workflowStore);
        this.messageSender = Objects.requireNonNull(messageSender);
    }

    /**
     * Sends approval requests for actions in the specified store.
     *
     * @param store store to get actions ready for approval from
     * @throws InterruptedException if current thread has been interrupted
     */
    public void sendApprovalRequests(@Nonnull ActionStore store) throws InterruptedException {
        if (!store.allowsExecution()) {
            return;
        }
        final ActionApprovalRequests.Builder builder = ActionApprovalRequests.newBuilder();
        for (Action action : store.getActions()
                .values()) {
            if (action.getMode() == ActionMode.EXTERNAL_APPROVAL) {
                final Optional<ActionDTO.Action> recommendationOptional =
                        action.getActionTranslation()
                                .getTranslatedRecommendation();
                if (recommendationOptional.isPresent()) {
                    final ActionDTO.Action recommendation = recommendationOptional.get();
                    final Optional<WorkflowDTO.Workflow> workflowOpt = action.getWorkflow(
                            workflowStore);

                    // It does not matter which target this action is expected to face.
                    final ExecuteActionRequest request =
                            ActionExecutor.createRequest(-1, recommendation, workflowOpt,
                                    action.getDescription(), action.getRecommendationOid());
                    builder.addActions(request);
                }
            }
        }
        if (builder.getActionsCount() > 0) {
            try {
                messageSender.sendMessage(builder.build());
            } catch (CommunicationException e) {
                logger.warn("Failed sending action approval request for actions "
                        + builder.getActionsList()
                        .stream()
                        .map(ExecuteActionRequest::getActionId)
                        .collect(Collectors.toList()), e);
            }
        }
    }
}

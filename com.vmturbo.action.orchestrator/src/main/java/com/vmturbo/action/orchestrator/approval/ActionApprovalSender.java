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
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.store.PlanActionStore;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStoreException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
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
    private final ActionTargetSelector actionTargetSelector;
    private final EntitiesAndSettingsSnapshotFactory entitySettingsCache;
    private final ActionTranslator actionTranslator;

    /**
     * Constructs action approval sender.
     *
     * @param workflowStore workflow store
     * @param messageSender message sender
     * @param actionTargetSelector selects which target/probe to execute each action against
     * @param entitySettingsCache cache of entity settings
     * @param actionTranslator the action translator
     */
    public ActionApprovalSender(@Nonnull WorkflowStore workflowStore,
            @Nonnull IMessageSender<ActionApprovalRequests> messageSender,
            @Nonnull ActionTargetSelector actionTargetSelector,
            @Nonnull EntitiesAndSettingsSnapshotFactory entitySettingsCache,
            @Nonnull ActionTranslator actionTranslator) {
        this.workflowStore = Objects.requireNonNull(workflowStore);
        this.messageSender = Objects.requireNonNull(messageSender);
        this.actionTargetSelector = Objects.requireNonNull(actionTargetSelector);
        this.entitySettingsCache = Objects.requireNonNull(entitySettingsCache);
        this.actionTranslator = Objects.requireNonNull(actionTranslator);
    }

    /**
     * Sends approval requests for actions in the specified store.
     *
     * @param store store to get actions ready for approval from
     * @throws InterruptedException if current thread has been interrupted
     */
    public void sendApprovalRequests(@Nonnull ActionStore store) throws InterruptedException {
        if (store.getStoreTypeName().equals(PlanActionStore.STORE_TYPE_NAME)) {
            return;
        }

        final ActionApprovalRequests.Builder builder = ActionApprovalRequests.newBuilder();
        for (Action action : store.getActions()
                .values()) {
            // It does not make sense to send any actions except of READY. All the other actions
            // are already executing or failed or rejected (it's late to approve/reject them)
            if (action.getMode() == ActionMode.EXTERNAL_APPROVAL
                    && action.getState() == ActionState.READY) {
                final Optional<ActionDTO.Action> recommendationOptional =
                        action.getActionTranslation()
                                .getTranslatedRecommendation();
                if (recommendationOptional.isPresent()) {
                    final ActionDTO.Action recommendation = recommendationOptional.get();
                    try {
                        final Optional<WorkflowDTO.Workflow> workflowOpt =
                                action.getWorkflow(workflowStore, action.getState());

                        final Optional<Long> targetForAction = getTargetForAction(recommendation,
                                action.getWorkflowExecutionTarget(workflowStore));
                        if (targetForAction.isPresent()) {
                            final ExecuteActionRequest request =
                                    ActionExecutor.createRequest(targetForAction.get(),
                                    actionTranslator.translateToSpec(action), workflowOpt,
                                            action.getDescription(), action.getRecommendationOid());
                            builder.addActions(request);
                        } else {
                            logger.warn(
                                    "Action with {} OID wasn't send for external approval, because "
                                            + "target for executing the action wasn't found",
                                    action.getId());
                        }
                    } catch (WorkflowStoreException e) {
                        logger.warn("Could not get workflow for action " + action.getId()
                                + ". Skip approval request for it");
                    }
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

    /**
     * Get target which should execute the action.
     *
     * @param recommendation the recommendation
     * @param workflowExecutionTarget execution target for action with REPLACE workflow
     * @return target id or {@link Optional#empty()} if there is no target where action can be
     * executed
     */
    private Optional<Long> getTargetForAction(@Nonnull ActionDTO.Action recommendation,
            @Nonnull Optional<Long> workflowExecutionTarget) {
        final ActionTargetInfo actionTargetInfo =
                actionTargetSelector.getTargetForAction(recommendation, entitySettingsCache,
                        workflowExecutionTarget);
        return actionTargetInfo.targetId();
    }
}

package com.vmturbo.action.orchestrator.execution;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent;
import com.vmturbo.action.orchestrator.action.ActionSchedule;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.topology.processor.api.ActionExecutionListener;

/**
 * The task for executing an action.
 */
public abstract class ConditionalActionTask implements ConditionalSubmitter.ConditionalTask {
    @VisibleForTesting
    static final String FAILED_TRANSFORM_MSG = "Failed to translate action %d for execution.";
    @VisibleForTesting
    static final String EXECUTION_START_MSG = "Failed to start action %d due to error.";

    private static final Logger logger = LogManager.getLogger();

    private final List<Action> actionList;
    private final long targetId;

    private final WorkflowStore workflowStore;
    private final ActionTranslator actionTranslator;
    private final ActionExecutor actionExecutor;
    private final ActionExecutionListener actionExecutionListener;

    /**
     * Creates an instance of {@link ConditionalActionTask}.
     *
     * @param targetId the id for the target that executes the action.
     * @param actionList the list of actions to execute.
     * @param workflowStore the store for accessing workflow.
     * @param actionTranslator the translator object.
     * @param actionExecutor the executor for actions.
     * @param actionExecutionListener the listener for action states.
     */
    public ConditionalActionTask(long targetId, @Nonnull List<Action> actionList,
                                 @Nonnull WorkflowStore workflowStore,
                                 @Nonnull ActionTranslator actionTranslator,
                                 @Nonnull ActionExecutor actionExecutor,
                                 @Nonnull ActionExecutionListener actionExecutionListener) {
        this.actionList = Objects.requireNonNull(actionList);
        this.targetId = targetId;
        this.workflowStore = Objects.requireNonNull(workflowStore);
        this.actionTranslator = Objects.requireNonNull(actionTranslator);
        this.actionExecutor = Objects.requireNonNull(actionExecutor);
        this.actionExecutionListener = Objects.requireNonNull(actionExecutionListener);
    }

    /**
     * Compare tasks. No two tasks with the same condition can be executed
     * at the same time.
     *
     * @param o the task to be compared.
     * @return zero if the condition is the same
     */
    @Override
    public int compareTo(@Nonnull ConditionalSubmitter.ConditionalTask o) {

        final Set<Long> targetIds1 = getMoveOrResizeActionTargetIds();

        if (!(o instanceof ConditionalActionTask)) {
            // if the task is not an action task it can continue
            return 1;
        }

        final Set<Long> targetIds2 = ((ConditionalActionTask)o).getMoveOrResizeActionTargetIds();

        if (!targetIds1.isEmpty() && !targetIds2.isEmpty()
                && !Sets.intersection(targetIds1, targetIds2).isEmpty()) {
            logger.info("Matched condition for tasks: {} {}", this, o);
            return 0;
        }

        return 1;
    }

    @Nonnull
    private Set<Long> getMoveOrResizeActionTargetIds() {
        final Set<Long> actionTargetIds = new HashSet<>();

        for (final Action action : actionList) {
            final ActionDTO.Action actionDto = action.getTranslationResultOrOriginal();
            final ActionDTO.ActionInfo info = actionDto.getInfo();
            if (info.hasMove()) {
                actionTargetIds.add(info.getMove().getTarget().getId());
            }

            if (info.hasResize()) {
                actionTargetIds.add(info.getResize().getTarget().getId());
            }
        }

        return actionTargetIds;
    }

    /**
     * Business logic executed by the task.
     */
    @Override
    @Nonnull
    public ConditionalActionTask call() throws Exception {
        final List<Action> filteredActionList = new ArrayList<>(actionList.size());
        for (final Action action : actionList) {
            if (checkIfEligibilityForExecutionExpired(action)) {
                logger.info("Action {}:{} is no longer eligible for execution.",
                        action.getId(), action.getRecommendationOid());
                continue;
            }

            // A prepare event prepares the action for execution, and initiates
            // a PRE workflow if one is associated with this action.
            action.receive(new ActionEvent.BeginExecutionEvent());
            Optional<ActionDTO.Action> translated = action.getActionTranslation()
                    .getTranslatedRecommendation();
            if (!translated.isPresent()) {
                final String errorMsg = String.format(FAILED_TRANSFORM_MSG, action.getId());
                logger.error(errorMsg);
                action.receive(new ActionEvent.FailureEvent(errorMsg));
                continue;
            }

            filteredActionList.add(action);
        }

        if (filteredActionList.isEmpty()) {
            return this;
        }

        final String actionIdsString = filteredActionList.stream()
                .map(Action::getId)
                .map(String::valueOf)
                .collect(Collectors.joining(", "));
        try {
            logger.info("Attempting to execute the actions: {}", actionIdsString);

            final List<ActionWithWorkflow> actionWithWorkflowList = new ArrayList<>(
                    filteredActionList.size());
            for (final Action action : filteredActionList) {
                // Fetch the Workflow, if any, that controls this Action
                final Optional<WorkflowDTO.Workflow> workflowOpt = action.getWorkflow(
                        workflowStore, action.getState());
                final ActionDTO.ActionSpec actionSpec = actionTranslator.translateToSpec(action);
                actionWithWorkflowList.add(new ActionWithWorkflow(actionSpec, workflowOpt));
            }

            actionExecutor.executeSynchronously(targetId, actionWithWorkflowList,
                    actionExecutionListener);

            logger.info("Completed executing actions: {}", actionIdsString);
        } catch (ExecutionStartException e) {
            logger.error("Failed to start actions: " + actionIdsString, e);
            for (final Action action : filteredActionList) {
                final String errorMsg = String.format(EXECUTION_START_MSG, action.getId());
                actionExecutionListener.onActionFailure(ActionNotificationDTO.ActionFailure.newBuilder().setActionId(
                        action.getId()).setErrorDescription(errorMsg).build());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Automated action execution interrupted", e);
            // We don't need failing the action here because we don't know if
            // it actually failed or not. ActionStateUpdater will still
            // change the action state if and when the action completes.
        }

        return this;
    }

    /**
     * Checks if the action is still eligible for execution. This is needed because it may
     * take some time from the time we submit the executor for execution till we actually
     * start executing action.
     *
     * @param action the action to check its eligibility.
     * @return true if the action is still eligible for execution.
     */
    public abstract boolean checkIfEligibilityForExecutionExpired(Action action);

    @Override
    @Nonnull
    public List<Action> getActionList() {
        return actionList;
    }

    public long getTargetId() {
        return targetId;
    }

    boolean isExecutionWindowActive(@Nonnull Action action) {
        Optional<ActionSchedule> actionSchedule = action.getSchedule();
        return actionSchedule.map(ActionSchedule::isActiveScheduleNow).orElse(true);
    }

    @Override
    public String toString() {
        final String actionsString = actionList.stream()
                .map(action -> String.format("[actionId=%s, targetId=%s, description='%s']",
                        action.getId(), targetId, action.getDescription()))
                .collect(Collectors.joining(", "));
        return String.format("%s (%s)", getClass().getSimpleName(), actionsString);
    }
}

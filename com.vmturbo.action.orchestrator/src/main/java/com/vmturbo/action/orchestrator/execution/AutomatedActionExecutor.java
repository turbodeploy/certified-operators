package com.vmturbo.action.orchestrator.execution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.AutomaticAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.PrepareExecutionEvent;
import com.vmturbo.action.orchestrator.execution.ActionExecutor.SynchronousExecutionException;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.state.machine.UnexpectedEventException;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;

public class AutomatedActionExecutor {

    private static final Logger logger = LogManager.getLogger();

    @VisibleForTesting
    static final String TARGET_RESOLUTION_MSG = "Action %d has no resolvable target " +
            "and cannot be executed.";
    @VisibleForTesting
    static final String FAILED_TRANSFORM_MSG = "Failed to translate action %d for execution.";
    @VisibleForTesting
    static final String EXECUTION_START_MSG = "Failed to start action %d due to error.";

    /**
     * To execute actions (by sending them to Topology Processor)
     */
    private final ActionExecutor actionExecutor;

    /**
     * To translate an action from the market's domain-agnostic form to the domain-specific form
     * relevant for execution and display in the real world.
     */
    private final ActionTranslator actionTranslator;

    /**
     * To schedule actions for asynchronous execution
     */
    private final ExecutorService executionService;

    /**
     * For selecting which target/probe to execute each action against
     */
    private final ActionTargetSelector actionTargetSelector;

    /**
     * the store for all the known {@link WorkflowDTO.Workflow} items
     */
    private final WorkflowStore workflowStore;

    /**
     * @param actionExecutor to execute actions (by sending them to Topology Processor)
     * @param executorService to schedule actions for asynchronous execution
     * @param translator to translate an action from the market's domain-agnostic form to the
     *                   domain-specific form relevant for execution and display in the real world.
     * @param workflowStore to determine if any workflows should be used to execute actions
     * @param actionTargetSelector to select which target/probe to execute each action against
     */
    public AutomatedActionExecutor(@Nonnull final ActionExecutor actionExecutor,
                                   @Nonnull final ExecutorService executorService,
                                   @Nonnull final ActionTranslator translator,
                                   @Nonnull final WorkflowStore workflowStore,
                                   @Nonnull final ActionTargetSelector actionTargetSelector) {
        this.actionExecutor = Objects.requireNonNull(actionExecutor);
        this.executionService = Objects.requireNonNull(executorService);
        this.actionTranslator = Objects.requireNonNull(translator);
        this.workflowStore = Objects.requireNonNull(workflowStore);
        this.actionTargetSelector = Objects.requireNonNull(actionTargetSelector);
    }

    /**
     * Finds the target associated with all entities involved in each action.
     * Does not include actions with no resolvable target
     *
     * @param allActions action objects
     * @return map of target id to the set of action ids directed at the target
     */
    private Map<Long, Set<Long>> mapActionsToTarget(Map<Long, Action> allActions) {
        final Map<Long, Set<Long>> result = new HashMap<>();
        final Map<Long, ActionTargetInfo> targetIdsForActions =
                actionTargetSelector.getTargetsForActions(allActions.values().stream()
                    .map(Action::getRecommendation),
                    Optional.empty());
        for(Entry<Long, ActionTargetInfo> targetIdForActionEntry : targetIdsForActions.entrySet()) {
            final Long actionId = targetIdForActionEntry.getKey();
            final ActionTargetInfo targetInfo = targetIdForActionEntry.getValue();
            if (targetInfo.targetId().isPresent()) {
                final Set<Long> targetActions = result.computeIfAbsent(targetInfo.targetId().get(), tgt -> new HashSet<>());
                targetActions.add(actionId);
                result.put(targetInfo.targetId().get(), targetActions);
            }
        }
        return result;
    }

    /**
     * Execute all actions in store that are in Automatic mode.
     * subject to queueing and/or throttling
     *
     * @param store ActionStore containing all actions
     */
    public List<ActionExecutionTask> executeAutomatedFromStore(ActionStore store) {
        if (!store.allowsExecution()) {
            return Collections.emptyList();
        }
        Map<Long, Action> autoActions = store.getActions().entrySet().stream()
                .filter(entry -> entry.getValue().getMode().equals(ActionMode.AUTOMATIC)
                            && entry.getValue().determineExecutability())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<Long, Set<Long>> actionsByTarget = mapActionsToTarget(autoActions);

        //remove any actions for which target retrieval failed
        List<Long> toRemove = new ArrayList<>();
        Set<Long> validActions = actionsByTarget.values().stream()
                .flatMap(Set::stream).collect(Collectors.toSet());
        autoActions.entrySet().stream()
                .filter(entry -> !validActions.contains(entry.getKey()))
                .map(Entry::getValue)
                .forEach(failed -> {
                    String errorMsg = String.format(TARGET_RESOLUTION_MSG, failed.getId());
                    failed.receive(new FailureEvent(errorMsg));
                    toRemove.add(failed.getId());
                });
        toRemove.forEach(id -> {
            autoActions.remove(id);
        });

        List<ActionExecutionTask> actionsToBeExecuted = new ArrayList<>();
        final String userNameAndUuid = AuditLogUtils.getUserNameAndUuidFromGrpcSecurityContext();
        actionsByTarget.forEach((targetId, actionSet) -> {
            actionSet.forEach(actionId -> {
                Action action = autoActions.get(actionId);
                try {
                    action.receive(new AutomaticAcceptanceEvent(userNameAndUuid, targetId));
                } catch (UnexpectedEventException ex) {
                    // log the error and continue with the execution of next action.
                    logger.error("Illegal state transition for action {}", action, ex);
                    return;
                }
                // We don't need to refresh severity cache because we will refresh it
                // in the ActionStorehouse after calling this method.
                try {
                    Future<Action> actionFuture = executionService.submit(() -> {
                        // A prepare event prepares the action for execution, and initiates a PRE
                        // workflow if one is associated with this action.
                        action.receive(new PrepareExecutionEvent());
                        // Allows the action to begin execution, if a PRE workflow is not running
                        action.receive(new BeginExecutionEvent());
                        actionTranslator.translate(action);
                        Optional<ActionDTO.Action> translated =
                                action.getActionTranslation().getTranslatedRecommendation();
                        if (translated.isPresent()) {
                            try {
                                logger.info("Attempting to execute action {}", action);
                                // Fetch the Workflow, if any, that controls this Action
                                Optional<WorkflowDTO.Workflow> workflowOpt =
                                        action.getWorkflow(workflowStore);
                                // Execute the Action on the given target, or the Workflow target
                                // if a Workflow is specified.
                                actionExecutor.executeSynchronously(targetId, translated.get(),
                                        workflowOpt);
                            } catch (ExecutionStartException e) {
                                final String errorMsg = String.format(EXECUTION_START_MSG, actionId);
                                logger.error(errorMsg, e);
                                action.receive(new FailureEvent(errorMsg));
                            } catch (SynchronousExecutionException e) {
                                logger.error(e.getFailure().getErrorDescription(), e);
                                // We don't need fail the action here because ActionStateUpdater will
                                // do it for us.
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                logger.error("Automated action execution interrupted", e);
                                // We don't need fail the action here because we don't know if it
                                // actually failed or not. ActionStateUpdater will still change the
                                // action state if and when the action completes.
                            }
                        } else {
                            final String errorMsg = String.format(FAILED_TRANSFORM_MSG, actionId);
                            logger.error(errorMsg);
                            action.receive(new FailureEvent(errorMsg));
                        }
                        return action;
                    });
                    actionsToBeExecuted.add(new ActionExecutionTask(action, actionFuture));
                } catch (RejectedExecutionException ex) {
                    logger.error("Failed to submit action {} to executor.", actionId, ex);
                }
            });
        });
        logger.info("TotalExecutableActions={}, SubmittedActionsCount={}",
                    autoActions.size(), actionsToBeExecuted.size());
        return actionsToBeExecuted;
    }

    public static class ActionExecutionTask {

        private final Action action;
        private final Future<Action> future;

        public ActionExecutionTask(Action action, Future<Action> future) {
            this.action = action;
            this.future = future;
        }

        public Action getAction() {
            return action;
        }

        public Future<Action> getFuture() {
            return future;
        }
    }
}

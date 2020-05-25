package com.vmturbo.action.orchestrator.execution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.AutomaticAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ManualAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.PrepareExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionSchedule;
import com.vmturbo.action.orchestrator.execution.ActionExecutor.SynchronousExecutionException;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.state.machine.UnexpectedEventException;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
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
     * To schedule actions for asynchronous execution
     */
    private final ExecutorService executionService;

    /**
     * For selecting which target/probe to execute each action against
     */
    private final ActionTargetSelector actionTargetSelector;

    private final EntitiesAndSettingsSnapshotFactory entitySettingsCache;

    /**
     * the store for all the known {@link WorkflowDTO.Workflow} items
     */
    private final WorkflowStore workflowStore;

    /**
     * @param actionExecutor to execute actions (by sending them to Topology Processor)
     * @param executorService to schedule actions for asynchronous execution
     * @param workflowStore to determine if any workflows should be used to execute actions
     * @param actionTargetSelector to select which target/probe to execute each action against
     * @param entitySettingsCache an entity snapshot factory used for creating entity snapshot.
     *                            It is now only used for creating empty entity snapshot.
     */
    public AutomatedActionExecutor(@Nonnull final ActionExecutor actionExecutor,
                                   @Nonnull final ExecutorService executorService,
                                   @Nonnull final WorkflowStore workflowStore,
                                   @Nonnull final ActionTargetSelector actionTargetSelector,
                                   @Nonnull final EntitiesAndSettingsSnapshotFactory entitySettingsCache) {
        this.actionExecutor = Objects.requireNonNull(actionExecutor);
        this.executionService = Objects.requireNonNull(executorService);
        this.workflowStore = Objects.requireNonNull(workflowStore);
        this.actionTargetSelector = Objects.requireNonNull(actionTargetSelector);
        this.entitySettingsCache = Objects.requireNonNull(entitySettingsCache);
    }

    /**
     * Finds the target associated with all entities involved in each action.
     * Does not include actions with no resolvable target
     *
     * @param allActions action objects
     * @return map of target id to the set of action ids directed at the target
     */
    private Map<Long, Set<Long>> mapActionsToTarget(
            Map<Long, ActionExecutionReadinessDetails> allActions) {
        final Map<Long, Set<Long>> result = new HashMap<>();
        final Map<Long, ActionTargetInfo> targetIdsForActions =
                actionTargetSelector.getTargetsForActions(allActions.values().stream()
                    .map(el -> el.getAction().getRecommendation()), entitySettingsCache.emptySnapshot());
        for (Entry<Long, ActionTargetInfo> targetIdForActionEntry : targetIdsForActions.entrySet()) {
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
     * Execute all actions in store that are in Automatic mode or manually accepted (MANUAL mode)
     * with activated execution schedule.
     * Subject to queueing and/or throttling.
     *
     * @param store ActionStore containing all actions
     */
    public List<ActionExecutionTask> executeAutomatedFromStore(ActionStore store) {
        if (!store.allowsExecution()) {
            return Collections.emptyList();
        }
        final Map<Long, ActionExecutionReadinessDetails> autoActions = store.getActions().values().stream()
                .map(this::getActionExecutionReadinessDetails)
                .filter(ActionExecutionReadinessDetails::isReadyForExecution)
                .collect(Collectors.toMap(k -> k.getAction().getId(), v -> v));

        final Map<Long, Set<Long>> actionsByTarget = mapActionsToTarget(autoActions);

        //remove any actions for which target retrieval failed
        List<Long> toRemove = new ArrayList<>();
        Set<Long> validActions = actionsByTarget.values().stream()
                .flatMap(Set::stream).collect(Collectors.toSet());
        autoActions.entrySet().stream()
                .filter(entry -> !validActions.contains(entry.getKey()))
                .map(Entry::getValue)
                .forEach(failed -> {
                    final Action action = failed.getAction();
                    String errorMsg = String.format(TARGET_RESOLUTION_MSG, action.getId());
                    action.receive(new FailureEvent(errorMsg));
                    toRemove.add(action.getId());
                });
        toRemove.forEach(autoActions::remove);

        List<ActionExecutionTask> actionsToBeExecuted = new ArrayList<>();
        final String userNameAndUuid = AuditLogUtils.getUserNameAndUuidFromGrpcSecurityContext();


        // We want to distribute actions across targets. If we have 5 targets, 10 actions each,
        // and a threadpool of size 10, we don't want to queue 10 actions for target 1.
        // It's better to queue 2 actions for targets 1-5 - so that we can minimize the impact
        // of execution on any particular probe/backend service.
        //
        // So what we do is:
        //   1) Build a map from (target id) -> (iterator over actions to execute for the target)
        //   2) Do a breadth-first traversal over the map (i.e. looping over the target ids
        //      multiple times, taking one action id every time) and queue the actions.

        // Build up a map from (target id) -> (iterator over actions to execute for the target).
        final Map<Long, Iterator<Long>> actionItsByTargetId = new HashMap<>();
        actionsByTarget.forEach((targetId, actionSet) -> {
            actionItsByTargetId.put(targetId, actionSet.iterator());
        });

        // Breadth-first traversal over the (target id) -> (iterator over action ids) map.
        while (!actionItsByTargetId.isEmpty()) {
            final Iterator<Entry<Long, Iterator<Long>>> actionItsByTargetIdIt = actionItsByTargetId.entrySet().iterator();
            while (actionItsByTargetIdIt.hasNext()) {
                final Entry<Long, Iterator<Long>> targetToActionIt = actionItsByTargetIdIt.next();
                final Long targetId = targetToActionIt.getKey();
                final Iterator<Long> actionIt = targetToActionIt.getValue();
                if (!actionIt.hasNext()) {
                    // We're done processing all actions associated with this target.
                    // Remove the (target, iterator) entry from the outer map.
                    actionItsByTargetIdIt.remove();
                    continue;
                }

                // Process the action.
                final Long actionId = actionIt.next();
                final ActionExecutionReadinessDetails actionExecutionReadinessDetails = autoActions.get(actionId);
                final Action action = actionExecutionReadinessDetails.getAction();
                try {
                    if (actionExecutionReadinessDetails.isAutomaticallyAccepted()) {
                        action.receive(new AutomaticAcceptanceEvent(userNameAndUuid, targetId));
                    } else {
                        action.receive(new ManualAcceptanceEvent(userNameAndUuid, targetId));
                    }
                } catch (UnexpectedEventException ex) {
                    // log the error and continue with the execution of next action.
                    logger.error("Illegal state transition for action {}", action, ex);
                    continue;
                }
                // We don't need to refresh severity cache because we will refresh it
                // in the ActionStorehouse after calling this method.
                //
                // TODO (roman, July 30 2019): OM-49079 - Submit an asynchronous callable to
                // the threadpool, and use a different mechanism than the size of the threadpool
                // to limit concurrent actions.
                try {
                    Future<Action> actionFuture = executionService.submit(() -> {
                        // A prepare event prepares the action for execution, and initiates a PRE
                        // workflow if one is associated with this action.
                        action.receive(new PrepareExecutionEvent());
                        // Allows the action to begin execution, if a PRE workflow is not running
                        action.receive(new BeginExecutionEvent());
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
            }
        }

        logger.info("TotalExecutableActions={}, SubmittedActionsCount={}",
                    autoActions.size(), actionsToBeExecuted.size());
        return actionsToBeExecuted;
    }

    /**
     * Handle is action ready for execution or not and if it's ready than how action was accepted
     * (automatically or manually).
     *
     * @param action action
     * @return wrapper for action with information about execution readiness detail.
     */
    private ActionExecutionReadinessDetails getActionExecutionReadinessDetails(
            @Nonnull Action action) {
        boolean isReadyForExecution = false;
        boolean isAutomaticallyAccepted = false;
        final ActionMode actionMode = action.getMode();
        final Optional<ActionSchedule> scheduleOpt = action.getSchedule();
        if (actionMode == ActionMode.AUTOMATIC) {
            isReadyForExecution = true;
            isAutomaticallyAccepted = true;
        }
        if (scheduleOpt.isPresent() && actionMode == ActionMode.MANUAL && scheduleOpt.get()
                .isActiveSchedule() && scheduleOpt.get().getAcceptingUser() != null) {
            isReadyForExecution = true;
        }
        if (isReadyForExecution && !action.determineExecutability()) {
            isReadyForExecution = false;
        }
        return new ActionExecutionReadinessDetails(action, isReadyForExecution,
                isAutomaticallyAccepted);
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

    /**
     * Helper class contains information about action, execution readiness status and acceptor
     * mode (automatically accepted by system or manually by user).
     */
    public static class ActionExecutionReadinessDetails {
        private Action action;
        private boolean isReadyForExecution;
        private boolean isAutomaticallyAccepted;

        /**
         * Constructor of {@link ActionExecutionReadinessDetails}.
         *
         * @param action the action representation
         * @param isReadyForExecution status of readiness for execution
         * @param isAutomaticallyAccepted defines action accepted automatically or manual by user
         */
        public ActionExecutionReadinessDetails(@Nonnull Action action, boolean isReadyForExecution,
                boolean isAutomaticallyAccepted) {
            this.action = action;
            this.isReadyForExecution = isReadyForExecution;
            this.isAutomaticallyAccepted = isAutomaticallyAccepted;
        }

        /**
         * Return action.
         *
         * @return {@link Action}
         */
        @Nonnull
        public Action getAction() {
            return action;
        }

        /**
         * Return execution readiness status for action.
         *
         * @return true if action is ready for execution, otherwise false.
         */
        public boolean isReadyForExecution() {
            return isReadyForExecution;
        }

        /**
         * Return information about action acceptance mode.
         *
         * @return true if action accepted automatically, otherwise false (accepted manually)
         */
        public boolean isAutomaticallyAccepted() {
            return isAutomaticallyAccepted;
        }
    }
}

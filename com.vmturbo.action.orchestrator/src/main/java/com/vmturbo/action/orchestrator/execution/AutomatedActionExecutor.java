package com.vmturbo.action.orchestrator.execution;

import java.util.ArrayList;
import java.util.Collection;
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
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.AutomaticAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.QueuedEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.RollBackToAcceptedEvent;
import com.vmturbo.action.orchestrator.action.ActionSchedule;
import com.vmturbo.action.orchestrator.execution.ActionExecutor.SynchronousExecutionException;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.ConditionalSubmitter.ConditionalFuture;
import com.vmturbo.action.orchestrator.execution.ConditionalSubmitter.ConditionalTask;
import com.vmturbo.action.orchestrator.state.machine.UnexpectedEventException;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;

/**
 * Manages initiating the execution of actions.
 */
public class AutomatedActionExecutor {

    private static final Logger logger = LogManager.getLogger();

    @VisibleForTesting
    static final String TARGET_RESOLUTION_MSG = "Action %d has no resolvable target "
        + "and cannot be executed.";
    @VisibleForTesting
    static final String FAILED_TRANSFORM_MSG = "Failed to translate action %d for execution.";
    @VisibleForTesting
    static final String EXECUTION_START_MSG = "Failed to start action %d due to error.";

    /**
     * To execute actions (by sending them to Topology Processor).
     */
    private final ActionExecutor actionExecutor;

    /**
     * To schedule actions for asynchronous execution.
     */
    private final Executor submitter;

    /**
     * For selecting which target/probe to execute each action against.
     */
    private final ActionTargetSelector actionTargetSelector;

    private final EntitiesAndSettingsSnapshotFactory entitySettingsCache;

    /**
     * The store for all the known {@link WorkflowDTO.Workflow} items.
     */
    private final WorkflowStore workflowStore;
    private final ActionTranslator actionTranslator;
    private final ActionCombiner actionCombiner;

    /**
     * Creates a AutomatedActionExecutor that talks will all the provided services.
     *
     * @param actionExecutor to execute actions (by sending them to Topology Processor)
     * @param submitter to schedule actions for asynchronous execution
     * @param workflowStore to determine if any workflows should be used to execute actions
     * @param actionTargetSelector to select which target/probe to execute each action against
     * @param entitySettingsCache an entity snapshot factory used for creating entity snapshot.
     * @param actionTranslator the action translator.
     * @param actionCombiner the bean to combine related actions for a single execution.
     */
    public AutomatedActionExecutor(@Nonnull final ActionExecutor actionExecutor,
            @Nonnull final Executor submitter,
            @Nonnull final WorkflowStore workflowStore,
            @Nonnull final ActionTargetSelector actionTargetSelector,
            @Nonnull final EntitiesAndSettingsSnapshotFactory entitySettingsCache,
            @Nonnull final ActionTranslator actionTranslator,
            @Nonnull final ActionCombiner actionCombiner) {
        this.actionExecutor = Objects.requireNonNull(actionExecutor);
        this.submitter = Objects.requireNonNull(submitter);
        this.workflowStore = Objects.requireNonNull(workflowStore);
        this.actionTargetSelector = Objects.requireNonNull(actionTargetSelector);
        this.entitySettingsCache = Objects.requireNonNull(entitySettingsCache);
        this.actionTranslator = Objects.requireNonNull(actionTranslator);
        this.actionCombiner = Objects.requireNonNull(actionCombiner);
    }

    /**
     * Finds the target associated with all entities involved in each action.
     * Does not include actions with no resolvable target
     *
     * @param allActions action objects
     * @return map of target id to the set of action ids directed at the target
     */
    private Map<Long, Set<ActionExecutionReadinessDetails>> mapActionsToTarget(
            Map<Long, ActionExecutionReadinessDetails> allActions) {
        final Map<Long, Set<ActionExecutionReadinessDetails>> result = new HashMap<>();
        final Map<Long, Long> workflowExecutionTargetsForActions =
                getWorkflowExecutionTargetsForActions(allActions.values()
                        .stream()
                        .map(ActionExecutionReadinessDetails::getAction)
                        .collect(Collectors.toList()));
        final Map<Long, ActionTargetInfo> targetIdsForActions =
                actionTargetSelector.getTargetsForActions(
                        allActions.values().stream().map(el -> el.getAction().getRecommendation()),
                        entitySettingsCache.emptySnapshot(), workflowExecutionTargetsForActions);
        for (Entry<Long, ActionTargetInfo> targetIdForActionEntry : targetIdsForActions.entrySet()) {
            final Long actionId = targetIdForActionEntry.getKey();
            final ActionTargetInfo targetInfo = targetIdForActionEntry.getValue();
            if (targetInfo.targetId().isPresent()) {
                final Long targetId = targetInfo.targetId().get();
                final Set<ActionExecutionReadinessDetails> targetActions = result.computeIfAbsent(
                        targetId, tgt -> new HashSet<>());
                targetActions.add(allActions.get(actionId));
                result.put(targetId, targetActions);
            }
        }
        return result;
    }

    private Map<Long, Long> getWorkflowExecutionTargetsForActions(
            @Nonnull Collection<Action> actions) {
        final Map<Long, Long> workflowExecutionTargetsForActions = new HashMap<>();
        for (Action action : actions) {
            final Optional<Long> workflowExecutionTarget =
                    action.getWorkflowExecutionTarget(workflowStore);
            workflowExecutionTarget.ifPresent(
                    discoveredTarget -> workflowExecutionTargetsForActions.put(action.getId(),
                            discoveredTarget));
        }
        return workflowExecutionTargetsForActions;
    }

    /**
     * Execute all actions in store that are in Automatic mode or manually
     * accepted (MANUAL mode) with activated execution schedule. Subject to
     * queueing and/or throttling.
     *
     * @param store ActionStore containing all actions
     * @return list of futures sent for execution
     */
    public List<ConditionalFuture> executeAutomatedFromStore(ActionStore store) {
        printSubmitterState();

        if (!store.allowsExecution()) {
            return Collections.emptyList();
        }
        final Map<Long, ActionExecutionReadinessDetails> autoActions = store.getActions().values().stream()
                .map(this::getActionExecutionReadinessDetails)
                .filter(ActionExecutionReadinessDetails::isReadyForExecution)
                .collect(Collectors.toMap(k -> k.getAction().getId(), v -> v));

        final Map<Long, Set<ActionExecutionReadinessDetails>> actionsByTarget = mapActionsToTarget(autoActions);

        //remove any actions for which target retrieval failed
        List<Long> toRemove = new ArrayList<>();
        Set<Long> validActions = actionsByTarget.values().stream()
                .flatMap(Set::stream)
                .map(a -> a.getAction().getId())
                .collect(Collectors.toSet());
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

        List<ConditionalFuture> futures = new ArrayList<>();
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
        final Map<Long, Iterator<Set<ActionExecutionReadinessDetails>>> actionItsByTargetId = new HashMap<>();
        actionsByTarget.forEach((targetId, actionSet) ->
                actionItsByTargetId.put(targetId, actionCombiner.combineActions(actionSet).iterator()));

        // Breadth-first traversal over the (target id) -> (iterator over action ids) map.
        while (!actionItsByTargetId.isEmpty()) {
            final Iterator<Entry<Long, Iterator<Set<ActionExecutionReadinessDetails>>>> actionItsByTargetIdIt = actionItsByTargetId.entrySet().iterator();
            while (actionItsByTargetIdIt.hasNext()) {
                final Entry<Long, Iterator<Set<ActionExecutionReadinessDetails>>> targetToActionIt = actionItsByTargetIdIt.next();
                final Long targetId = targetToActionIt.getKey();
                final Iterator<Set<ActionExecutionReadinessDetails>> actionIt = targetToActionIt.getValue();
                if (!actionIt.hasNext()) {
                    // We're done processing all actions associated with this target.
                    // Remove the (target, iterator) entry from the outer map.
                    actionItsByTargetIdIt.remove();
                    continue;
                }

                final List<Action> actionList = new ArrayList<>();
                for (final ActionExecutionReadinessDetails actionDetails : actionIt.next()) {
                    final Long actionId = actionDetails.getAction().getId();
                    final ActionExecutionReadinessDetails actionExecutionReadinessDetails = autoActions.get(actionId);
                    final Action action = actionExecutionReadinessDetails.getAction();
                    try {
                        if (actionExecutionReadinessDetails.isAutomaticallyAccepted()) {
                            action.receive(new AutomaticAcceptanceEvent(userNameAndUuid, targetId));
                        }
                        action.receive(new QueuedEvent());
                        actionList.add(action);
                    } catch (UnexpectedEventException ex) {
                        // log the error and continue with the execution of next action.
                        logger.error("Illegal state transition for action {}", action, ex);
                    }
                }

                // We don't need to refresh severity cache because we will refresh it
                // in the ActionStorehouse after calling this method.
                //
                // TODO (roman, July 30 2019): OM-49079 - Submit an asynchronous callable to
                // the threadpool, and use a different mechanism than the size of the threadpool
                // to limit concurrent actions.
                try {
                    ConditionalFuture future = new ConditionalFuture(
                            new AutomatedActionTask(targetId, actionList));
                    submitter.execute(future);
                    futures.add(future);
                } catch (RejectedExecutionException ex) {
                    final String actionIdsString = actionList.stream()
                            .map(Action::getId)
                            .map(String::valueOf)
                            .collect(Collectors.joining(", "));
                    logger.error("Failed to submit actions {} to executor.", actionIdsString, ex);
                }
            }
        }

        logger.info("TotalExecutableActions={}, SubmittedActionsCount={}",
                autoActions.size(), futures.size());
        return futures;
    }

    private void printSubmitterState() {
        if (!(submitter instanceof ConditionalSubmitter)) {
            return;
        }

        ConditionalSubmitter conditionalSubmitter = (ConditionalSubmitter)submitter;

        int queuedFuturesCount = conditionalSubmitter.getQueuedFuturesCount();
        if (queuedFuturesCount != 0) {
            logger.warn("Submitter queue is not empty: {}", queuedFuturesCount);
        }

        int runningFuturesCount = conditionalSubmitter.getRunningFuturesCount();
        if (runningFuturesCount != 0) {
            logger.warn("Submitter has running futures: {}", runningFuturesCount);
        }
    }

    /**
     * Execution task with the condition defined in
     * {@link #compareTo(ConditionalTask)}. No two tasks with the same condition
     * can be executed at the same time.
     */
    public class AutomatedActionTask implements ConditionalTask {

        private final Long targetId;
        private final List<Action> actionList;

        /**
         * Action task.
         *
         * @param targetId target ID
         * @param actionList action list
         */
        public AutomatedActionTask(@Nonnull Long targetId, @Nonnull List<Action> actionList) {
            this.targetId = targetId;
            this.actionList = actionList;
        }

        /**
         * Compare tasks. No two tasks with the same condition can be executed
         * at the same time.
         *
         * @param o the task to be compared.
         * @return zero if the condition is the same
         */
        @Override
        public int compareTo(ConditionalTask o) {
            final Set<Long> targetIds1 = getMoveOrResizeActionTargetIds();

            final AutomatedActionTask otherTask = (AutomatedActionTask)o;
            final Set<Long> targetIds2 = otherTask.getMoveOrResizeActionTargetIds();

            if (!targetIds1.isEmpty() && !targetIds2.isEmpty()
                    && !Sets.intersection(targetIds1, targetIds2).isEmpty()) {
                logger.info("Matched condition for tasks: {} {}", this, otherTask);
                return 0;
            }

            return 1;
        }

        /**
         * Business logic executed by the task.
         */
        @Override
        @Nonnull
        public AutomatedActionTask call() throws Exception {
            final List<Action> filteredActionList = new ArrayList<>(actionList.size());
            for (final Action action : actionList) {
                if (!isExecutionWindowActive(action)) {
                    // rollback action from QUEUED to ACCEPTED state because of
                    // a missing execution window
                    logger.info("Action {} wasn't send for execution because "
                            + "associated execution window is not active", action.getId());
                    action.receive(new RollBackToAcceptedEvent());
                    continue;
                }

                // A prepare event prepares the action for execution, and initiates
                // a PRE workflow if one is associated with this action.
                action.receive(new BeginExecutionEvent());
                Optional<ActionDTO.Action> translated = action.getActionTranslation()
                        .getTranslatedRecommendation();
                if (!translated.isPresent()) {
                    final String errorMsg = String.format(FAILED_TRANSFORM_MSG, action.getId());
                    logger.error(errorMsg);
                    action.receive(new FailureEvent(errorMsg));
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
                logger.info("Attempting to execute the automated actions: {}", actionIdsString);

                final List<ActionWithWorkflow> actionWithWorkflowList = new ArrayList<>(
                        filteredActionList.size());
                for (final Action action : filteredActionList) {
                    // Fetch the Workflow, if any, that controls this Action
                    final Optional<WorkflowDTO.Workflow> workflowOpt = action.getWorkflow(
                            workflowStore, action.getState());
                    final ActionSpec actionSpec = actionTranslator.translateToSpec(action);
                    actionWithWorkflowList.add(new ActionWithWorkflow(actionSpec, workflowOpt));
                }

                actionExecutor.executeSynchronously(targetId, actionWithWorkflowList);

                logger.info("Completed executing automated actions: {}", actionIdsString);
            } catch (ExecutionStartException e) {
                logger.error("Failed to start actions: " + actionIdsString, e);
                for (final Action action : filteredActionList) {
                    final String errorMsg = String.format(EXECUTION_START_MSG, action.getId());
                    action.receive(new FailureEvent(errorMsg));
                }
            } catch (SynchronousExecutionException e) {
                logger.error(e.getMessage(), e);
                // We don't need failing the action here because ActionStateUpdater
                // will do it for us.
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Automated action execution interrupted", e);
                // We don't need failing the action here because we don't know if
                // it actually failed or not. ActionStateUpdater will still
                // change the action state if and when the action completes.
            }

            return this;
        }

        private boolean isExecutionWindowActive(@Nonnull Action action) {
            Optional<ActionSchedule> actionSchedule = action.getSchedule();
            return actionSchedule.map(ActionSchedule::isActiveScheduleNow).orElse(true);
        }

        @Nonnull
        private Set<Long> getMoveOrResizeActionTargetIds() {
            final Set<Long> actionTargetIds = new HashSet<>();

            for (final Action action : actionList) {
                final ActionDTO.Action actionDto = action.getTranslationResultOrOriginal();
                if (actionDto == null) {
                    logger.warn("Action translation is missing for action: {}", action.getId());
                    continue;
                }

                final ActionInfo info = actionDto.getInfo();
                if (info == null) {
                    logger.warn("Action info is missing for action: {}", actionDto);
                    continue;
                }

                if (info.hasMove()) {
                    actionTargetIds.add(info.getMove().getTarget().getId());
                }

                if (info.hasResize()) {
                    actionTargetIds.add(info.getResize().getTarget().getId());
                }
            }

            return actionTargetIds;
        }

        @Override
        @Nonnull
        public List<Action> getActionList() {
            return actionList;
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
        if (scheduleOpt.isPresent() && (actionMode == ActionMode.MANUAL
                || actionMode == ActionMode.EXTERNAL_APPROVAL) && scheduleOpt.get()
                .isActiveSchedule() && scheduleOpt.get().getAcceptingUser() != null) {
            isReadyForExecution = true;
        }
        return new ActionExecutionReadinessDetails(action, isReadyForExecution,
                isAutomaticallyAccepted);
    }

    /**
     * Helper class contains information about action, execution readiness status and acceptor
     * mode (automatically accepted by system or manually by user).
     */
    public static class ActionExecutionReadinessDetails {

        private final Action action;
        private final boolean isReadyForExecution;
        private final boolean isAutomaticallyAccepted;

        /**
         * Constructor of {@link ActionExecutionReadinessDetails}.
         *
         * @param action the action representation
         * @param isReadyForExecution status of readiness for execution
         * @param isAutomaticallyAccepted defines action accepted automatically or manual by user
         */
        public ActionExecutionReadinessDetails(@Nonnull Action action, boolean isReadyForExecution,
                boolean isAutomaticallyAccepted) {
            this.action = Objects.requireNonNull(action);
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

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ActionExecutionReadinessDetails that = (ActionExecutionReadinessDetails)o;
            return action.getId() == that.action.getId();
        }

        @Override
        public int hashCode() {
            return Objects.hash(action.getId());
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + " [action=" + action + ", isReadyForExecution="
                    + isReadyForExecution + ", isAutomaticallyAccepted=" + isAutomaticallyAccepted
                    + "]";
        }
    }
}

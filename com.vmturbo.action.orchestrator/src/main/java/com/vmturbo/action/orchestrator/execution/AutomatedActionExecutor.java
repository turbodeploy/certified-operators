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
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.AutomaticAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.execution.ActionExecutor.SynchronousExecutionException;
import com.vmturbo.action.orchestrator.state.machine.UnexpectedEventException;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.EntityInfo;

public class AutomatedActionExecutor {

    @VisibleForTesting
    static final String UNSUPPORTED_MSG = "Action %d is of unsupported type %s " +
            "and cannot be executed.";
    @VisibleForTesting
    static final String TARGET_RESOLUTION_MSG = "Action %d has no resolvable target " +
            "and cannot be executed.";
    @VisibleForTesting
    static final String FAILED_TRANSFORM_MSG = "Failed to translate action %d for execution.";
    @VisibleForTesting
    static final String EXECUTION_START_MSG = "Failed to start action %d due to error.";

    private final ActionExecutor actionExecutor;

    private final ActionTranslator actionTranslator;

    private final ExecutorService executionService;

    private final Logger logger = LogManager.getLogger();

    public AutomatedActionExecutor(@Nonnull final ActionExecutor executor,
                                   @Nonnull final ExecutorService executorService,
                                   @Nonnull final ActionTranslator translator) {
        this.actionExecutor = Objects.requireNonNull(executor);
        this.executionService = Objects.requireNonNull(executorService);
        this.actionTranslator = Objects.requireNonNull(translator);
    }

    /**
     * Retrieves all entity ids relevant to all actions of supported type.
     * Does not include actions of unsupported type.
     * @param actions map of action id to action
     * @return map of action id to set of involved entity ids
     */
    private Map<Long, Set<Long>> mapActionsToInvolvedEntities(Map<Long, Action> actions) {
        Map<Long, Set<Long>> result = new HashMap<>();
        for (final Action action : actions.values()) {
            try {
                result.put(action.getId(),
                        ActionDTOUtil.getInvolvedEntities(action.getRecommendation()));
            } catch (UnsupportedActionException e) {
                final String errorMessage = String.format(UNSUPPORTED_MSG, e.getActionId(),
                        e.getActionType());
                logger.error(errorMessage, e);
            }
        }
        return result;
    }

    /**
     * Retrieves map of entity info for each action by combining map of all entity info with
     * map of action ids to involved entity ids
     * @param allEntityInfos map of entity id to entity info for all entities in the action set
     * @param actionEntityIds map of action id to set of involved entity ids
     * @return map of action id to entity info map for that action
     */
    private Map<Long, Map<Long, EntityInfo>> mapActionsToEntityInfoMap(
            Map<Long, EntityInfo> allEntityInfos, Map<Long, Set<Long>> actionEntityIds) {
        Map<Long, Map<Long, EntityInfo>> result = new HashMap<>();
        for (final Entry<Long, Set<Long>> actionEntityEntry : actionEntityIds.entrySet()) {
            Map<Long, EntityInfo> relevantEntities = actionEntityEntry.getValue().stream()
                    .collect(Collectors.toMap(Function.identity(), allEntityInfos::get));
            result.put(actionEntityEntry.getKey(), relevantEntities);
        }
        return result;
    }

    /**
     * Finds the target associated with all entities involved in each action.
     * Does not include actions with no resolvable target
     * @param allActions action objects
     * @param actionEntityMapMap map of action id to the entity info map of entities involved
     * @return map of target id to the set of action ids directed at the target
     */
    private Map<Long, Set<Long>> mapActionsToTarget(Map<Long, Action> allActions,
                                        Map<Long, Map<Long, EntityInfo>> actionEntityMapMap) {
        final Map<Long, Set<Long>> result = new HashMap<>();
        for (final Entry<Long, Map<Long, EntityInfo>> actionEntry : actionEntityMapMap.entrySet()) {
            final Optional<Long> targetId = actionExecutor.getEntitiesTarget(
                    allActions.get(actionEntry.getKey()).getRecommendation(),
                    actionEntry.getValue());
            if (targetId.isPresent()) {
                final Set<Long> targetActions =
                        result.computeIfAbsent(targetId.get(), tgt -> new HashSet<>());
                targetActions.add(actionEntry.getKey());
                result.put(targetId.get(), targetActions);
            } else {
                final String message = String.format(TARGET_RESOLUTION_MSG, actionEntry.getKey());
                logger.debug(message);
            }
        }
        return result;
    }

    /**
     * Execute all^* actions in store that are in Automatic mode.
     * ^* subject to queueing and/or throttling
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

        Map<Long, Set<Long>> actionEntityIdMap = mapActionsToInvolvedEntities(autoActions);

        //remove any actions for which entity retrieval failed, and send failure events to them
        List<Long> toRemove = new ArrayList<>();
        autoActions.entrySet().stream()
                .filter(entry -> !actionEntityIdMap.containsKey(entry.getKey()))
                .map(Entry::getValue)
                .forEach(failed -> {
                    toRemove.add(failed.getId());
                    String errorMsg = String.format(UNSUPPORTED_MSG, failed.getId(),
                            failed.getRecommendation().getInfo().getActionTypeCase().toString());
                    failed.receive(new FailureEvent(errorMsg));
                });
        toRemove.forEach(autoActions::remove);

        Set<Long> allEntities = actionEntityIdMap.values().stream().flatMap(Set::stream)
                .collect(Collectors.toSet());
        Map<Long, EntityInfo> allEntityInfos = actionExecutor.getEntityInfo(allEntities);

        Map<Long, Map<Long, EntityInfo>> actionEntityMapMap =
                mapActionsToEntityInfoMap(allEntityInfos, actionEntityIdMap);

        Map<Long, Set<Long>> actionsByTarget = mapActionsToTarget(autoActions, actionEntityMapMap);

        //remove any actions for which target retrieval failed
        toRemove.clear();
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
            //these two intermediate maps aren't used anymore, but keep them up to date just in case?
            actionEntityIdMap.remove(id);
            actionEntityMapMap.remove(id);
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

                        action.receive(new BeginExecutionEvent());
                        actionTranslator.translate(action);
                        Optional<ActionDTO.Action> translated =
                                action.getActionTranslation().getTranslatedRecommendation();
                        if (translated.isPresent()) {
                            try {
                                logger.info("Attempting to execute action {}", actionId);
                                actionExecutor.executeSynchronously(targetId, translated.get());
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
                    autoActions, actionsToBeExecuted);
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

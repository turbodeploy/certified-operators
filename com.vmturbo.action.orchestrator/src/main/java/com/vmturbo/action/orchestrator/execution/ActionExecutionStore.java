package com.vmturbo.action.orchestrator.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionExecution;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionExecution.SkippedAction;

/**
 * In-memory store for action executions.
 */
@ThreadSafe
public class ActionExecutionStore {

    private final Map<Long, Execution> executionsById = new HashMap<>();
    private final Map<Long, Execution> executionsByActionId = new HashMap<>();

    private long currentId = 0;

    /**
     * Create and store new {@code ActionExecution}.
     *
     * @param actionIds List of action OIDs.
     * @param skippedActions List of actions that were skipped by Action Orchestrator.
     * @return Created {@code ActionExecution} instance.
     */
    public synchronized ActionExecution createExecution(
            @Nonnull final List<Long> actionIds,
            @Nonnull final List<SkippedAction> skippedActions) {
        final long id = currentId++;
        final Execution execution = new Execution(id, actionIds, skippedActions);
        executionsById.put(id, execution);
        actionIds.forEach(actionId -> executionsByActionId.put(actionId, execution));
        return execution.toActionExecution();
    }

    /**
     * Remove completed action from the store and remove related execution if all actions are
     * completed.
     *
     * @param actionId OID of action that was completed.
     */
    public synchronized void removeCompletedAction(final long actionId) {
        final Execution execution = executionsByActionId.remove(actionId);
        if (execution != null) {
            // If all actions in the execution are completed then remove it
            if (execution.removeActionAndCheckIfEmpty(actionId)) {
                executionsById.remove(execution.getId());
            }
        }
    }

    /**
     * Get action execution by its ID.
     *
     * @param id Action execution ID.
     * @return {@link ActionExecution} instance.
     */
    @Nonnull
    public synchronized Optional<ActionExecution> getActionExecution(final long id) {
        return Optional.ofNullable(executionsById.get(id))
                .map(Execution::toActionExecution);
    }

    /**
     * Get all action executions.
     *
     * @return List of all active action executions.
     */
    @Nonnull
    public synchronized List<ActionExecution> getAllActionExecutions() {
        return executionsById.values().stream()
                .map(Execution::toActionExecution)
                .collect(Collectors.toList());
    }

    /**
     * Internal class to keep an action execution.
     */
    private static class Execution {
        private final long id;
        private final List<Long> actionIds;
        private final List<SkippedAction> skippedActions;
        private final long acceptedTimestamp;

        private Execution(
                final long id,
                @Nonnull final List<Long> actionIds,
                @Nonnull final List<SkippedAction> skippedActions) {
            this.id = id;
            this.actionIds = new ArrayList<>(actionIds);
            this.skippedActions = skippedActions;
            acceptedTimestamp = System.currentTimeMillis();
        }

        private long getId() {
            return id;
        }

        private boolean removeActionAndCheckIfEmpty(final long actionId) {
            actionIds.remove(actionId);
            return actionIds.isEmpty();
        }

        private ActionExecution toActionExecution() {
            return ActionExecution.newBuilder()
                    .setId(id)
                    .addAllActionId(actionIds)
                    .addAllSkippedAction(skippedActions)
                    .setAcceptedTimestamp(acceptedTimestamp)
                    .build();
        }
    }
}

package com.vmturbo.action.orchestrator.execution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor.ActionExecutionReadinessDetails;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.topology.graph.util.BaseGraphEntity;

/**
 * This class combines related actions together to execute them in one request to mediation probe.
 */
public class ActionCombiner {

    private final ActionTopologyStore actionTopologyStore;

    /**
     * Create new instance of {@code ActionCombiner}.
     *
     * @param actionTopologyStore Action topology store.
     */
    public ActionCombiner(ActionTopologyStore actionTopologyStore) {
        this.actionTopologyStore = actionTopologyStore;
    }

    /**
     * Combine actions.
     *
     * @param actionSet Set of actions to combine.
     * @return Set of action sets. Each action set represents a group of actions that should be
     * executed together in one request.
     */
    @Nonnull
    public Set<Set<ActionExecutionReadinessDetails>> combineActions(
            @Nonnull final Set<ActionExecutionReadinessDetails> actionSet) {
        final Set<Set<ActionExecutionReadinessDetails>> result = new HashSet<>();

        final Map<Long, List<ActionExecutionReadinessDetails>> disruptiveActionsByConsumerId = new HashMap<>();
        for (ActionExecutionReadinessDetails action : actionSet) {
            final ActionEntity actionEntity = getActionEntity(action);
            final boolean isDisruptive = action.getAction().getRecommendation().getDisruptive();
            if (actionEntity != null && isDisruptive) {
                final List<Long> consumerIds = getConsumerIds(actionEntity);
                if (!consumerIds.isEmpty()) {
                    for (Long consumerId : consumerIds) {
                        disruptiveActionsByConsumerId
                                .computeIfAbsent(consumerId, k -> new ArrayList<>())
                                .add(action);
                    }
                    continue;
                }
            }
            result.add(Collections.singleton(action));
        }

        final List<List<ActionExecutionReadinessDetails>> disruptiveActionLists =
                disruptiveActionsByConsumerId.values().stream()
                        .sorted(Comparator.<List<ActionExecutionReadinessDetails>, Integer>comparing(List::size)
                                .reversed())
                        .collect(Collectors.toList());

        final Set<ActionExecutionReadinessDetails> addedActions = new HashSet<>();
        for (final List<ActionExecutionReadinessDetails> actionList : disruptiveActionLists) {
            final Set<ActionExecutionReadinessDetails> actionSetToAdd = actionList.stream()
                    .filter(action -> !addedActions.contains(action))
                    .collect(Collectors.toSet());
            if (!actionSetToAdd.isEmpty()) {
                result.add(actionSetToAdd);
                addedActions.addAll(actionSetToAdd);
            }
        }

        return result;
    }

    /**
     * Combine actions.
     *
     * @param actionList List of actions to combine.
     * @return List of action lists. Each action list represents a group of actions that should be
     * executed together in one request.
     */
    @Nonnull
    public List<List<Action>> combineActions(@Nonnull final List<Action> actionList) {
        final Set<ActionExecutionReadinessDetails> actionSet = actionList.stream()
                .map(ActionCombiner::toActionDetails)
                .collect(Collectors.toSet());
        return combineActions(actionSet).stream()
                .map(ActionCombiner::toActionList)
                .collect(Collectors.toList());
    }

    private static ActionExecutionReadinessDetails toActionDetails(@Nonnull final Action action) {
        return new ActionExecutionReadinessDetails(action, true, false);
    }

    private static List<Action> toActionList(
            @Nonnull final Set<ActionExecutionReadinessDetails> actionSet) {
        return actionSet.stream()
                .map(ActionExecutionReadinessDetails::getAction)
                .collect(Collectors.toList());
    }

    @Nonnull
    private List<Long> getConsumerIds(@Nonnull final ActionEntity actionEntity) {
        if (!actionTopologyStore.getSourceTopology().isPresent()) {
            return Collections.emptyList();
        }
        return actionTopologyStore.getSourceTopology().get().entityGraph()
                .getConsumers(actionEntity.getId())
                .map(BaseGraphEntity::getOid)
                .distinct()
                .collect(Collectors.toList());
    }

    @Nullable
    private static ActionEntity getActionEntity(
            @Nonnull final ActionExecutionReadinessDetails action) {
        final ActionDTO.Action actionDto = action.getAction().getRecommendation();
        // For now, we only support combining Scale actions. It can be extended if necessary.
        if (actionDto.getInfo().hasScale()) {
            return actionDto.getInfo().getScale().getTarget();
        }
        return null;
    }
}

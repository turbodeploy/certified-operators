package com.vmturbo.market.runner.wasted;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;

/**
 * Store wasted entity results from wasted analyses. This is used for holding actions for entities
 * that are candidates for deletion.
 */
public abstract class WastedEntityResults {
    private final Collection<Action> actions;
    private final Map<Integer, Set<Action>> actionsByEntityType =  new HashMap<>();

    /**
     * Util method to extract target entity id from a delete action.
     *
     * @param action {@link Action}.
     * @return target entity id.
     */

    public static long getEntityIdFromAction(Action action) {
        return getTargetFromAction(action).getId();
    }

    /**
     * Create new WastedResults.
     *
     * @param actions delete actions for wasted entities.
     */
    public WastedEntityResults(List<Action> actions) {
        this.actions = actions;
        // initialize actionMapByEntityType.
        actions.forEach(action -> {
            int entityType = getTargetFromAction(action).getType();
            actionsByEntityType.compute(entityType, (k, currentActions) -> {
                currentActions = currentActions == null ? new HashSet<>() : currentActions;
                currentActions.add(action);
                return currentActions;
            });
        });
    }

    /**
     * Get actions for wasted results.
     * @return actions delete actions for wasted entities.
     */
    public Collection<Action> getAllActions() {
        return actions;
    }

    /**
     * Utility to get the IDs of the entities referenced in the generated delete actions.
     *
     * @return set of the IDs of entites that the wasted analysis resulted in.
     */
    public Set<Long> getEntityIds() {
        return actions.stream()
                .map(WastedEntityResults::getEntityIdFromAction)
                .collect(Collectors.toSet());
    }

    /**
     * Get list of actions from {@link #actionsByEntityType} for a given entityType.
     *
     * @param entityType entityType number.
     * @return list of actions {@link Action}.
     */
    public Set<Action> getActionsByEntityType(int entityType) {
        return actionsByEntityType.getOrDefault(entityType, Collections.emptySet());
    }

    private static ActionEntity getTargetFromAction(Action action) {
        return action.getInfo().getDelete().getTarget();
    }
}

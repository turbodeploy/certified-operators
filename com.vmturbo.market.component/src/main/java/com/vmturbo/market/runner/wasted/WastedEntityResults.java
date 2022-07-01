package com.vmturbo.market.runner.wasted;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;

/**
 * Store wasted entity results from wasted analyses. This is used for holding actions for entities
 * that are candidates for deletion.
 */
public abstract class WastedEntityResults {
    private final Collection<Action> actions;

    /**
     * Create new WastedResults.
     * @param actions delete actions for wasted entities.
     */
    public WastedEntityResults(List<Action> actions) {
        this.actions = actions;
    }

    /**
     * Get actions for wasted results.
     * @return actions delete actions for wasted entities.
     */
    public Collection<Action> getActions() {
        return actions;
    }

    /**
     * Utility to get the IDs of the entities referenced in the generated delete actions.
     *
     * @return set of the IDs of entites that the wasted analysis resulted in.
     */
    public Set<Long> getEntityIds() {
        return actions.stream()
                .map(action -> action.getInfo().getDelete().getTarget().getId())
                .collect(Collectors.toSet());
    }
}

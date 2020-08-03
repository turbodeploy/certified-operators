package com.vmturbo.action.orchestrator.store;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;

/**
 * Interface to create Atomic action DTOs from a group of actions.
 */
public interface AtomicActionMerger {

    /** Checks whether {@code AtomicActionMerger} should be applied to the given action.
     *
     * @param action Action to check.
     * @return True if {@code AtomicActionMerger} should be applied.
     */
    boolean appliesTo(@Nonnull ActionDTO.Action action);

    /**
     * Create a new atomic ActionDTO from a group of actions.
     *
     * @param actionsToMerge    actions that will be merged to create a new Atomic action
     * @return Map of atomic actions and the list of actions that were merged for that atomic action
     */
    Map<Action.Builder, List<Action>> merge(@Nonnull List<ActionDTO.Action> actionsToMerge);
}

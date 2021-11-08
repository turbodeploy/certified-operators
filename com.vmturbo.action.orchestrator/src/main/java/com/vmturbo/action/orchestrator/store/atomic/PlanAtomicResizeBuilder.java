package com.vmturbo.action.orchestrator.store.atomic;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

/**
 * Builder class to build Atomic Resize actions for Plan actions.
 */
public class PlanAtomicResizeBuilder extends AtomicResizeBuilder {
    /**
     * Constructor for a new builder.
     *
     * @param aggregatedAction  AggregatedAction containing the relevant information
     *                              for creating the atomic action
     */
    PlanAtomicResizeBuilder(@Nonnull AggregatedAction aggregatedAction) {
        super();
        this.aggregatedAction = aggregatedAction;
        executableActions = getExecutableActionIds();
        nonExecutableActions = getNonExecutableActionIds();
    }

    /**
     * Return the set of action OIDs of the original resizes that are executable.
     *
     * <p>For real time topologies, this is will be based on the action policy setting on the entity.
     *
     * <p>For plan topologies, all resize actions are non-executable.
     *
     * @return the set of action OIDs of the executable resize actions, which is an empty set for plan topologies
     */
    @Override
    protected Set<Long> getExecutableActionIds() {
        // Set of action ID of individual container resizes not in recommend mode.
        return new HashSet<>();
    }

    /**
     * Return the set of action OIDs of the original resizes that are non-executable.
     *
     * <p>For real time topologies, this is will be based on the action policy setting on the entity.
     * Some resize actions can be non-executable if they are blocked by a related provider action.
     *
     * <p>For plan topologies, all resize actions are non-executable.
     *
     * @return the set of action IDs of the non-executable resize actions that corresponds to all the actions
     */
    @Override
    protected Set<Long> getNonExecutableActionIds() {
        // Set of action ID of individual container resizes in recommend mode.
        return aggregatedAction.getAllActions().stream()
                .map(a -> a.getId())
                .collect(Collectors.toSet());
    }
}

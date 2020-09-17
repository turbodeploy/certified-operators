package com.vmturbo.action.orchestrator.store;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO;

/**
 * Interface to de-duplicate and merge the market actions to {@link AggregatedAction} that will
 * be used to create the action DTOs for the atomic actions.
 * AtomicActionMerger will create AggregatedAction objects when a new action plan is received
 * by the Action Orchestrator.
 */
public interface AtomicActionMerger {

    /** Checks whether {@code AtomicActionMerger} should be applied to the given action.
     *
     * @param action Action to check.
     * @return True if {@code AtomicActionMerger} should be applied.
     */
    boolean appliesTo(@Nonnull ActionDTO.Action action);

    /**
     * Create {@link AggregatedAction}'s from a group of market recommended actions.
     * Each AggregatedAction will be executed by a single aggregation entity that controls
     * the target entities of the original market actions.
     *
     * @param actionsToMerge  actions that will be merged to create a new Atomic action
     * @return Map of OID of the aggregation target and the AggregatedAction.
     *          AggregatedAction will be used to create the atomic action that
     *          will be executed by the aggregation target.
     */
    Map<Long, AggregatedAction> mergeActions(@Nonnull List<ActionDTO.Action> actionsToMerge);
}

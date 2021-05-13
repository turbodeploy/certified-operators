package com.vmturbo.action.orchestrator.store.atomic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionSpec;

/**
 * Builds new ActionDTOs by merging ActionDTOs for different entities
 * based on the {@link AtomicActionSpec}.
 * When a new action plan is received by the Action Orchestrator, {@link LiveActionStore} will
 * invoke the AtomicActionFactory to create atomic action DTOs by merging a group of actions
 * for entities controlled by the same execution target.
 *
 * <p>The atomic action process looks as follows:
 *
 * Resize Container       Resize Container             Resize Container
 *  Instance Foo1          Instance Foo2                  Instance Bar1
 *              \         /                              /
 *               \       /        Deduplication         /
 *                \     /             Step             /
 *                 \   /                              /
 *         Deduplicated resize              Deduplicated resize
 *         on ContainerSpc Foo              on ContainerSpec Bar
 *                      \                     /
 *                       \    Aggregation    /
 *                        \      Step       /
 *                         \               /
 *                           Atomic Resize
 *                        on WorkloadController
 *
 * </p>
 */
public class AtomicActionFactory {

    private static final Logger logger = LogManager.getLogger();

    private final AtomicActionSpecsCache atomicActionSpecsCache;

    // Second step - to create Atomic action DTOs
    private final AtomicActionBuilderFactory atomicActionBuilderFactory;

    /**
     * Constructor.
     * @param atomicActionSpecsCache ActionMergeSpecsCache
     */
    public AtomicActionFactory(@Nonnull final AtomicActionSpecsCache atomicActionSpecsCache) {
        this.atomicActionSpecsCache = atomicActionSpecsCache;
        atomicActionBuilderFactory = new AtomicActionBuilderFactory();
    }

    /**
     * Executor for merging actions based on action type.
     */
    public interface ActionMergeExecutor {
        /**
         * Create {@link AggregatedAction} from a group of market actions.
         *
         * @param actionsToMerge  actions that will be merged to create a new Atomic action
         * @return Map of OID of the aggregation target and the AggregatedAction.
         *          AggregatedAction which will be used to create the atomic action that
         *          will be executed by the aggregation target.
         */
        Map<Long, AggregatedAction> executeAggregation(@Nonnull List<ActionDTO.Action> actionsToMerge);
    }

    /**
     * Factory to create {@link AtomicActionBuilder}.
     */
    private static class AtomicActionBuilderFactory {

        AtomicActionBuilderFactory() { }

        /**
         * AtomicActionBuilder that will create the {@link Action} for the given AggregatedAction.
         *
         * @param aggregatedAction the {@link AggregatedAction}
         * @return AtomicActionBuilder to build {@link Action} for the aggregated action
         */
        @Nullable
        AtomicActionBuilder getActionBuilder(@Nonnull final AggregatedAction aggregatedAction) {
            ActionTypeCase actionTypeCase = aggregatedAction.getActionTypeCase();
            switch (actionTypeCase) {
                case ATOMICRESIZE:
                    return new AtomicResizeBuilder(aggregatedAction);
                default:
                    return null;
            }
        }
    }

    /**
     * Result of the action merge process.
     */
    @Value.Immutable
    public interface AtomicActionResult {
        /**
         * The new primary ActionDTO for the action that will execute the aggregated and de-duplicated
         * market actions. Aggregated atomic action will not be created if the original actions are
         * in RECOMMEND mode.
         *
         * @return The new primary ActionDTO for the action.
         */
        Optional<ActionDTO.Action> atomicAction();

        /**
         * Map of the non-executable atomic action that de-duplicated actions for entities in the
         * scaling/deployment group to the list of original actions. Atomic actions for de-duplicated
         * targets will be created even if the original actions are in RECOMMEND mode.
         *
         * @return Map of the non-executable atomic actions.
         */
        Map<ActionDTO.Action, List<ActionDTO.Action>> deDuplicatedActions();

        /**
         * Get the list of actions that were merged without de-duplication.
         *
         * @return List of actions that were merged without de-duplication
         */
        List<ActionDTO.Action> mergedActions();
    }

    /**
     * Returns boolean indicating if the atomic actions can be created.
     *
     * @return true if the cache containing the atomic action specs is not empty, else false
     */
    public boolean canMerge() {
        return !atomicActionSpecsCache.isEmpty();
    }

    /**
     * Iterate and aggregate the market actions.
     *
     * @param actionsToMerge  actions that will be merged to create a new Atomic action
     * @return Map of OID of the aggregation target and the AggregatedAction.
     *          AggregatedAction will be used to create the atomic action that
     *          will be executed by the aggregation target.
     */
    public Map<Long, AggregatedAction> aggregate(@Nonnull List<ActionDTO.Action> actionsToMerge) {
        // create new action merge executor at the start of merging a new set of market actions
        ActionMergeExecutor actionMergeExecutor = new AtomicActionMergeExecutor(atomicActionSpecsCache);

        return actionMergeExecutor.executeAggregation(actionsToMerge);
    }

    /**
     * Create the {@link AtomicActionResult}s consisting the new {@link Action} for each of the
     * {@link AggregatedAction}. The new action will atomically
     * execute the de-duplicated and merged actions that are part of the aggregated action.
     *
     * @param aggregatedActionMap Map of OID of the aggregation target and the AggregatedAction.
     * @return  list of new atomic actions
     */
    public List<AtomicActionResult> atomicActions(@Nonnull final Map<Long, AggregatedAction> aggregatedActionMap) {
        List<AtomicActionResult> mergeResult = new ArrayList<>();

        // Create atomic action per AggregationAction
        for (Long aggregateTargetOid : aggregatedActionMap.keySet()) {
            AggregatedAction aggregatedAction = aggregatedActionMap.get(aggregateTargetOid);

            AtomicActionBuilder atomicActionBuilder = atomicActionBuilderFactory.getActionBuilder(aggregatedAction);

            if (atomicActionBuilder == null) {
                continue;
            }

            Optional<AtomicActionResult> atomicActionResult = atomicActionBuilder.build();
            atomicActionResult.ifPresent(mergeResult::add);
        }

        return mergeResult;
    }

    /**
     *  Executor for all the atomic action translations.
     */
    private static class AtomicActionMergeExecutor implements ActionMergeExecutor {
        private final Map<ActionTypeCase, AtomicActionMerger> actionMergerMap;

        /**
         * Constructor.
         * Creates an instance of {@link AtomicActionMerger} for supported action types.
         *
         * @param actionMergeSpecsCache cache with the atomic action merge specs for all action types
         */
        private AtomicActionMergeExecutor(AtomicActionSpecsCache actionMergeSpecsCache) {
            Map<Long, AtomicActionSpec> resizeSpecsMap
                        = actionMergeSpecsCache.getAtomicActionsSpec(ActionType.RESIZE);

            actionMergerMap = ImmutableMap.<ActionTypeCase, AtomicActionMerger>builder()
                                .put(ActionTypeCase.RESIZE, new AtomicResizeMerger(resizeSpecsMap))
                                .build();
        }

        @Override
        public Map<Long, AggregatedAction> executeAggregation(@Nonnull final List<Action> actionsToMerge) {
            // group the actions that need to be merged with the appropriate  AtomicActionMerger
            Map<AtomicActionMerger, List<Action>> actionsByMergeType = new HashMap<>();
            for (Action action : actionsToMerge) {
                // Get the action merger for this action type
                AtomicActionMerger actionMerger = getActionMerger(action);
                if (actionMerger != null && actionMerger.appliesTo(action)) {
                    actionsByMergeType.computeIfAbsent(actionMerger, v -> new ArrayList<>())
                            .add(action);
                }
            }

            // execute the merge for all the action mergers and combine result to create the AtomicActionResult
            Map<Long, AggregatedAction> aggregatedActions =
                    actionsByMergeType.entrySet().stream()
                            .map(entry -> {
                                AtomicActionMerger actionMerger = entry.getKey();
                                Map<Long, AggregatedAction> result;
                                try {
                                   result = actionMerger.mergeActions(entry.getValue());
                                }  catch (RuntimeException e) {
                                    logger.error("Error applying " + actionMerger.getClass().getSimpleName(), e);
                                    result = Collections.emptyMap();
                                }
                                return result;
                            })
                            .flatMap(map -> map.entrySet().stream())
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    Map.Entry::getValue));

            return aggregatedActions;
        }

        /**
         * Find the {@link AtomicActionMerger} that will merge this action with other actions.
         *
         * @param action action received from the market
         * @return   AtomicActionMerger if available for the action type, else null
         */
        @Nullable
        private AtomicActionMerger getActionMerger(@Nonnull final Action action) {
            ActionTypeCase actionTypeCase = action.getInfo().getActionTypeCase();
            switch (actionTypeCase) {
                case RESIZE:
                    return actionMergerMap.get(actionTypeCase);
                default:
                    return null;
            }
        }
    }
}

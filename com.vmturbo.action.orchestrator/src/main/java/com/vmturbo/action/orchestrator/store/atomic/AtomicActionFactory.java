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

import com.vmturbo.action.orchestrator.store.pipeline.LiveActionPipelineFactory;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionSpec;

/**
 * Builds new ActionDTOs by merging ActionDTOs for different entities
 * based on the {@link AtomicActionSpec}.
 * When a new action plan is received by the Action Orchestrator, {@link LiveActionPipelineFactory} will
 * invoke the AtomicActionFactory to create atomic action DTOs by merging a group of actions
 * for entities controlled by the same execution target.
 *
 * <p>The atomic action process looks as follows:
 *
 * Resize Container       Resize Container             Resize Container     Non-Executable Resize Container
 * Foo1::Commodity 1     Foo2::Commodity 1            Bar1::Commodity 1        Bar1::Commodity 2
 *              \         /                              /                                /
 *               \       /        Deduplication         /          Deduplication         /
 *                \     /             Step             /             Step               /
 *                 \   /                              /                                /
 *  Deduplicated Commodity 1 resize     Deduplicated Commodity 1 resize     Deduplicated Commodity 2 resize
 *          on ContainerSpc Foo            on ContainerSpec Bar                 on ContainerSpec Bar
 *                      \                      /                                     /
 *                       \    Aggregation     /                     Aggregation     /
 *                        \      Step        /                         Step        /
 *                         \                /                                     /
 *                    Executable Atomic Resize                      Non-Executable Atomic Resize
 *                     on WorkloadController                            on WorkloadController
 *
 * </p>
 */
public class AtomicActionFactory {

    private static final Logger logger = LogManager.getLogger();

    private final AtomicActionSpecsCache atomicActionSpecsCache;

    // Second step - to create Atomic action DTOs
    private final IAtomicActionBuilderFactory atomicActionBuilderFactory;

    /**
     * Constructor.
     * @param atomicActionSpecsCache ActionMergeSpecsCache
     */
    public AtomicActionFactory(@Nonnull final AtomicActionSpecsCache atomicActionSpecsCache) {
        this.atomicActionSpecsCache = atomicActionSpecsCache;
        this.atomicActionBuilderFactory = newAtomicActionBuilderFactory();
    }

    protected IAtomicActionBuilderFactory newAtomicActionBuilderFactory() {
        return new AtomicActionBuilderFactory();
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
     * Factory interface to create {@link AtomicActionBuilder}.
     */
    public interface IAtomicActionBuilderFactory {
        /**
         * AtomicActionBuilder that will create the {@link Action} for the given AggregatedAction.
         *
         * @param aggregatedAction the {@link AggregatedAction}
         * @return AtomicActionBuilder to build {@link Action} for the aggregated action
         */
        @Nullable
        AtomicActionBuilder getActionBuilder(@Nonnull AggregatedAction aggregatedAction);
    }

    /**
     * Factory to create {@link AtomicActionBuilder}.
     */
    private static class AtomicActionBuilderFactory implements IAtomicActionBuilderFactory {

        AtomicActionBuilderFactory() { }

        /**
         * AtomicActionBuilder that will create the {@link Action} for the given AggregatedAction.
         *
         * @param aggregatedAction the {@link AggregatedAction}
         * @return AtomicActionBuilder to build {@link Action} for the aggregated action
         */
        @Nullable
        public AtomicActionBuilder getActionBuilder(@Nonnull final AggregatedAction aggregatedAction) {
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
     *
     * <p>AtomicActionResult contains the atomic actions created after de-duplication and merge process.
     * The executable actions will be de-duplicated and merged to an executable atomic action.
     * The non-executable actions will be merged to a non-executable atomic action.
     * It is possible to have both an executable and non-executable atomic action present in an AtomicActionResult
     * or just one of them.
     */
    @Value.Immutable
    public interface AtomicActionResult {
        /**
         * The new primary ActionDTO for the action that will execute the aggregated and de-duplicated
         * market actions. Aggregated atomic action will not be created if none of the original actions
         * are in RECOMMEND mode or are non-executable.
         *
         * @return The new ActionDTO for the executable action.
         */
        Optional<ActionDTO.Action> atomicAction();

        /**
         * The new ActionDTO for the action that will de-duplicate and aggregate market actions
         * but cannot be executed. Non-executable aggregated atomic action will be created
         * when some original actions are in RECOMMEND mode or non-executable.
         *
         * @return The new ActionDTO for the non-executable action
         */
        Optional<ActionDTO.Action> nonExecutableAtomicAction();

        /**
         * Aggregation Target Entity.
         *
         * @return Aggregation target entity
         */
        ActionDTO.ActionEntity aggregationTarget();

        /**
         * Map of the de-duplication target entity to the actions for entities in the
         * scaling/deployment group to the list of original actions.
         *
         * @return Map of the non-executable atomic actions.
         */
        Map<ActionDTO.ActionEntity, List<ActionDTO.Action>> deDuplicatedActions();

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

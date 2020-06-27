package com.vmturbo.action.orchestrator.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.action.orchestrator.action.AtomicActionSpecsCache;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionSpec;

/**
 * Builds new ActionDTOs by merging ActionDTOs for different entities
 * based on the {@link AtomicActionSpec}.
 */
public class AtomicActionFactory {

    private static final Logger logger = LogManager.getLogger();

    private final AtomicActionSpecsCache atomicActionSpecsCache;
    private final ActionMergeExecutor actionMergeExecutor;

    /**
     * Constructor.
     * @param atomicActionSpecsCache ActionMergeSpecsCache
     */
    public AtomicActionFactory(@Nonnull final AtomicActionSpecsCache atomicActionSpecsCache) {
        this.atomicActionSpecsCache = atomicActionSpecsCache;
        actionMergeExecutor = new AtomicActionMergeExecutor(atomicActionSpecsCache);
    }

    /**
     * Executor for merging actions based on action type.
     */
    public interface ActionMergeExecutor {
        /**
         * Create a list of atomic actions using the actions recommended from the market.
         *
         * @param actionsToMerge    List of action DTOs from the market
         * @return List of Atomic actions created
         */
        List<AtomicActionResult> executeMerge(@Nonnull List<ActionDTO.Action> actionsToMerge);
    }

    /**
     * Result of the action merge process.
     */
    @Value.Immutable
    interface AtomicActionResult {
        // The new ActionDTO
        ActionDTO.Action atomicAction();

        // The list of market actions that were merged
        List<ActionDTO.Action> marketActions();
    }

    /**
     * Returns boolean indicating if the atomic actions can be created.
     *
     * @return true if the cache containing the atomic action specs is not empty, else false
     */
    boolean canMerge() {
        return !atomicActionSpecsCache.isEmpty();
    }

    /**
     * Merge the actions.
     *
     * @param actionList    list of actions
     * @return  list of new atomic actions
     */
    List<AtomicActionResult> merge(@Nonnull List<ActionDTO.Action> actionList) {

        return actionMergeExecutor.executeMerge(actionList);
    }

    /**
     *  Executor for all the atomic action translations.
     */
    private static class AtomicActionMergeExecutor implements ActionMergeExecutor {
        private final Map<ActionTypeCase, AtomicActionMerger> actionMergerMap;

        private AtomicActionMergeExecutor(AtomicActionSpecsCache actionMergeSpecsCache) {
            actionMergerMap = ImmutableMap.of(
                                    ActionTypeCase.RESIZE, new AtomicResizeMerger(actionMergeSpecsCache)
                             );
        }

        @Override
        public List<AtomicActionResult> executeMerge(@Nonnull final List<Action> actionsToMerge) {
            // group the  merge action translators with actions that need to be merged
            Map<AtomicActionMerger, List<Action>> actionsByAtomicTranslationMethod = new HashMap<>();
            for (Action action : actionsToMerge) {
                AtomicActionMerger actionTranslator = getActionMerger(action);
                if (actionTranslator != null) {
                    actionsByAtomicTranslationMethod.computeIfAbsent(actionTranslator, v -> new ArrayList<>())
                            .add(action);
                }
            }

            // execute the merge for all the action mergers and combine result to create the AtomicActionResult
            List<AtomicActionResult> atomicActions =
            actionsByAtomicTranslationMethod.entrySet().stream()
                    .map(entry -> {
                        List<AtomicActionResult> result = merge(entry.getKey(), entry.getValue());
                        return result;
                     })
                    .flatMap(List::stream)
                    .collect(Collectors.toList());

            return atomicActions;
        }

        /**
         * Execute the translation of the actions to atomic actions.
         *
         * @param actionMerger   AtomicActionMerger that will merge the actions
         * @param actionsToMerge    list of actions
         * @return      List of AtomicActionResult containing the atomic action
         *                              and the list of actions that were merged
         */
        private  List<AtomicActionResult> merge( @Nonnull final AtomicActionMerger actionMerger,
                        @Nonnull final List<Action> actionsToMerge) {
            try {
                Map<Action.Builder, List<Action>> mergeResult = actionMerger.merge(actionsToMerge);
                List<AtomicActionResult> atomicActions = mergeResult.entrySet().stream().map(resultEntry -> {
                    AtomicActionResult atomicAction =
                            ImmutableAtomicActionResult.builder()
                                    .atomicAction(resultEntry.getKey().build())
                                    .marketActions(resultEntry.getValue())
                                    .build();

                    logger.debug("{}: merged {} actions to {} resize items",
                            atomicAction.atomicAction().getId(), atomicAction.marketActions().size(),
                            atomicAction.atomicAction().getInfo().getAtomicResize().getResizesCount());
                    return atomicAction;
                }).collect(Collectors.toList());

                return atomicActions;
            } catch (RuntimeException e) {
                logger.error("Error applying " + actionMerger.getClass().getSimpleName(), e);
                return Collections.emptyList();
            }
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

package com.vmturbo.platform.analysis.actions;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

import com.google.common.collect.Lists;

import com.vmturbo.platform.analysis.economy.Trader;

/**
 * A class holding all utility method used for collapsing actions.
 * @author weiduan
 *
 */
public class ActionCollapse {

    private static final Logger logger = LogManager.getLogger(ActionCollapse.class);
    // the function to get economy index of a given trader
    private static final Function<Trader, String> ECONOMY_INDEX =
                    t -> String.valueOf(t.getEconomyIndex());
    // Reorder actions in following order.
    private static ActionType[] ACTIONS_REORDER_SEQUENCE = {
                        ActionType.PROVISION_BY_SUPPLY,
                        ActionType.PROVISION_BY_DEMAND,
                        ActionType.ACTIVATE,
                        ActionType.MOVE,
                        ActionType.COMPOUND_MOVE,
                        ActionType.RESIZE,
                        ActionType.DEACTIVATE,
                        ActionType.RECONFIGURE,
                    };

    /**
     * Collapses a list of {@link Action}s by combining "combinable" actions.
     * Two actions are said to be combinable if they can be replaced by one action that,
     * when {@link #take}n, achieves the same result as taking the pair of actions in order.
     * @param actions a list of actions to collapse. Assume the list is consistent, e.g. a Move
     * TO trader A cannot be followed by a Move FROM a different trader B.
     * @return a list of actions that represents the same outcome as the argument list.
     * @see #combine
     */
    @Pure
    public static @NonNull List<@NonNull Action>
                    collapsed(@NonNull @ReadOnly List<@NonNull @ReadOnly Action> actions) {
        Map<Object, @NonNull Action> combined = new LinkedHashMap<>();
        Map<Trader, List<Action>> perTargetMap = new LinkedHashMap<>();
        // Insert to map, combine actions
        for (Action action : actions) {
            Trader target = action.getActionTarget();
            // get or create list entry
            List<Action> perTargetList = perTargetMap.compute(target,
                            (t, l) -> l == null ? new LinkedList<>() : l);
            int perTargetLastIndex = perTargetList.size() - 1;
            if (action.getType() ==  ActionType.ACTIVATE) {
                Action lastAction = perTargetList.isEmpty() ? null
                                : perTargetList.get(perTargetLastIndex);
                if (lastAction != null && lastAction.getType() == ActionType.DEACTIVATE) {
                    // Activate after Deactivate cancels it
                    perTargetList.remove(perTargetLastIndex);
                    combined.remove(lastAction);
                    if (logger.isTraceEnabled()) {
                        logger.trace("Remove Deactivate/Activate for trader "
                                        + target.getEconomyIndex());
                    }
                    continue;
                }
            }
            Object key = action.getCombineKey();
            Action previousAction = combined.get(key);
            if (previousAction == null) {
                combined.put(key, action);
                perTargetList.add(action);
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace("1. " + previousAction.serialize(ECONOMY_INDEX));
                    logger.trace("2. " + action.serialize(ECONOMY_INDEX));
                }
                Action collapsedAction = previousAction.combine(action);
                if (collapsedAction == null) {
                    // The actions cancel each other
                    if (logger.isTraceEnabled()) {
                        logger.trace("    Cancel each other");
                    }
                    combined.remove(key);
                    perTargetList.remove(previousAction);
                } else {
                    if (logger.isTraceEnabled()) {
                        logger.trace("   Merged to " + collapsedAction.serialize(ECONOMY_INDEX));
                    }
                    combined.put(key, collapsedAction);
                    perTargetList.remove(previousAction);
                    perTargetList.add(collapsedAction);
                }
            }
        }
        List<Action> result = Lists.newArrayList(combined.values());
        for (List<Action> list : perTargetMap.values()) {
            // If the list ends with a Deactivate then
            if (!list.isEmpty() && list.get(list.size() - 1) instanceof Deactivate) {
                Action firstAction = list.get(0);
                List<Action> remove = (firstAction instanceof Activate
                                || firstAction instanceof ProvisionBySupply
                                || firstAction instanceof ProvisionByDemand)
                                                // If the list starts with an Activate, ProvisionByDemand or
                                                // ProvisionBySupply remove all the actions,
                                                // including the Activate and the Deactivate
                                                ? Lists.newArrayList(list)
                                                // If the list doesn't start with an Activate ProvisionByDemand or
                                                // ProvisionBySupply then remove everything
                                                // but keep the Deactivate
                                                : Lists.newArrayList(
                                                                list.subList(0, list.size() - 1));
                result.removeAll(remove);
                list.removeAll(remove);
            }
        }
        return result;
    }

    /**
     * Group actions of same type together, and the sequence for actions should follow the order:
     * provision-> move-> resize-> suspension -> reconfigure.
     *
     * @param collapsedActions The {@link Action} that has gone through collapsing and needs to
     * be grouped by type.
     *
     * @return The list of <b>actions</b> grouped and reordered
     * The action order should be provision-> move-> resize-> suspension -> reconfigure.
     */
    @Deterministic
    public static @NonNull List<@NonNull Action> groupActionsByTypeAndReorderBeforeSending(
                    @NonNull @ReadOnly List<@NonNull @ReadOnly Action> collapsedActions) {
        Map<ActionType, List<Action>> groupByActionTypeMap =
                        collapsedActions.stream().collect(Collectors.groupingBy(Action::getType));
        @NonNull
        List<@NonNull Action> reorderedActions = new ArrayList<@NonNull Action>();
        for (ActionType actionType : ACTIONS_REORDER_SEQUENCE) {
            List<Action> actionList = groupByActionTypeMap.get(actionType);
            if (actionList != null) {
                reorderedActions.addAll(actionList);
            }
        }
        return reorderedActions;
    }
}

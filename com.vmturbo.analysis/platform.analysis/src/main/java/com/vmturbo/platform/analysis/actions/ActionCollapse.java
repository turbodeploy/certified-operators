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
            if (action instanceof Activate) {
                Action lastAction = perTargetList.isEmpty() ? null
                                : perTargetList.get(perTargetLastIndex);
                if (lastAction instanceof Deactivate) {
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
        @NonNull
        List<@NonNull Action> reorderedActions = new ArrayList<@NonNull Action>();
        List<Action> provisionBySupply = new ArrayList<>();
        List<Action> provisionByDemand = new ArrayList<>();
        List<Action> activate = new ArrayList<>();
        List<Action> move = new ArrayList<>();
        List<Action> compoundMove = new ArrayList<>();
        List<Action> resize = new ArrayList<>();
        List<Action> deactivate = new ArrayList<>();
        List<Action> reconfigure = new ArrayList<>();
        for (Action action : collapsedActions) {
            if (action instanceof ProvisionBySupply) {
                provisionBySupply.add(action);
            } else if (action instanceof ProvisionByDemand) {
                provisionByDemand.add(action);
            } else if (action instanceof Activate) {
                activate.add(action);
            } else if (action instanceof Move) {
                move.add(action);
            } else if (action instanceof CompoundMove) {
                compoundMove.add(action);
            } else if (action instanceof Resize) {
                resize.add(action);
            } else if (action instanceof Deactivate) {
                deactivate.add(action);
            } else if (action instanceof Reconfigure) {
                reconfigure.add(action);
            }
        }
        reorderedActions.addAll(provisionBySupply);
        reorderedActions.addAll(provisionByDemand);
        reorderedActions.addAll(activate);
        reorderedActions.addAll(move);
        reorderedActions.addAll(compoundMove);
        reorderedActions.addAll(resize);
        reorderedActions.addAll(deactivate);
        reorderedActions.addAll(reconfigure);

        return reorderedActions;
    }

    /**
     * Filter {@link Move} actions that set the initial placement for a trader and
     * remove them from the {@link Action} list which will go through collapsing.
     *
     * <p>
     * The state for the argument being passed in could change.
     * </p>
     *
     * @param actions The {@link Action} list that will be processed to filter out initial moves
     *
     * @return The {@link Move} that sets the initial placement for a trader
     */
    public static @NonNull List<@NonNull Action>
                    preProcessBeforeCollapse(@NonNull List<@NonNull Action> actions) {
        // initial move actions are special move actions with null as source trader
        // they are equivalent as Start in legacy market and needs to be sent
        // before resize
        List<Action> initialMove = new ArrayList<@NonNull Action>();
        initialMove = actions.stream()
                        .filter(m -> m instanceof Move && ((Move)m).getSource() == null)
                        .collect(Collectors.toList());
        // remove all initial moves from the action list because otherwise collapsing may
        // merge initial move with regular move if both have same action target
        actions.removeAll(initialMove);
        return initialMove;
    }
}

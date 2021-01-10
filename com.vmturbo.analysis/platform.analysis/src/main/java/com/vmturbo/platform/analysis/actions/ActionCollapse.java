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

import com.vmturbo.platform.analysis.economy.ShoppingList;
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
                        ActionType.RECONFIGURE_PROVIDER_ADDITION,
                        ActionType.MOVE,
                        ActionType.COMPOUND_MOVE,
                        ActionType.RESIZE,
                        ActionType.DEACTIVATE,
                        ActionType.RECONFIGURE_PROVIDER_REMOVAL,
                        ActionType.RECONFIGURE_CONSUMER,
                    };

    /**
     * This inner class is used to help return variables that need to be updated
     * by {@link ActionCollapse#collapseMovesAndCompoundMove}.
     */
    static class ActionCollapser {
        Object key;
        boolean removePreviousAction;
        Action previousAction;
        Action action;
        ActionCollapser(Object key, boolean removePreviousAction, Action previousAction, Action action) {
            this.key = key;
            this.removePreviousAction = removePreviousAction;
            this.previousAction = previousAction;
            this.action = action;
        }
    }

    /**
     * Collapses a list of {@link Action}s by combining "combinable" actions.
     * Two actions are said to be combinable if they can be replaced by one action that,
     * when {@link Action#take}n, achieves the same result as taking the pair of actions in order.
     * @param actions a list of actions to collapse. Assume the list is consistent, e.g. a Move
     * TO trader A cannot be followed by a Move FROM a different trader B.
     * @return a list of actions that represents the same outcome as the argument list.
     * @see Action#combine
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
                        logger.trace("Remove Deactivate/Activate for trader {}, economy index = {}",
                            target.getDebugInfoNeverUseInCode(), target.getEconomyIndex());
                    }
                    continue;
                }
            }

            Object key = action.getCombineKey();
            Action previousAction = combined.get(key);
            boolean removePreviousAction = true;
            if (previousAction == null) {
                ActionCollapser actionCollapser =
                    collapseMovesAndCompoundMove(combined, perTargetList, action, target);
                previousAction = actionCollapser.previousAction;
                key = actionCollapser.key;
                removePreviousAction = actionCollapser.removePreviousAction;
                action = actionCollapser.action;
            }

            if (previousAction == null) {
                combined.put(key, action);
                perTargetList.add(action);
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace("1. " + previousAction.serialize(ECONOMY_INDEX));
                    logger.trace("2. " + action.serialize(ECONOMY_INDEX));
                }
                Action collapsedAction = previousAction.combine(action);
                if (removePreviousAction) {
                    perTargetList.remove(previousAction);
                }
                if (collapsedAction == null) {
                    // The actions cancel each other
                    if (logger.isTraceEnabled()) {
                        logger.trace("    Cancel each other");
                    }
                    combined.remove(key);
                } else {
                    if (logger.isTraceEnabled()) {
                        logger.trace("   Merged to " + collapsedAction.serialize(ECONOMY_INDEX));
                    }
                    combined.put(key, collapsedAction);
                    perTargetList.add(collapsedAction);
                }
            }
        }

        List<Action> result = Lists.newArrayList(combined.values());
        for (List<Action> list : perTargetMap.values()) {
            // If the list ends with a Deactivate then
            if (!list.isEmpty() && list.get(list.size() - 1).getType() == ActionType.DEACTIVATE) {
                Action firstAction = list.get(0);
                List<Action> remove = (firstAction instanceof Activate
                                || firstAction instanceof ProvisionBase)
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

            // Attempt to collapse opposite ReconfigureProvider actions with the same commodity
            // being added and removed from the trader.
            List<ReconfigureProviderAddition> additions = Lists.newArrayList();
            List<ReconfigureProviderRemoval> removals = Lists.newArrayList();
            for (Action a : list) {
                if (a instanceof ReconfigureProviderAddition) {
                    additions.add((ReconfigureProviderAddition)a);
                } else if (a instanceof ReconfigureProviderRemoval) {
                    removals.add((ReconfigureProviderRemoval)a);
                }
            }
            if (!additions.isEmpty() && !removals.isEmpty()) {
                for (ReconfigureProviderAddition addition : additions) {
                    for (ReconfigureProviderRemoval removal : removals) {
                        if (addition.getReconfiguredCommodities().keySet()
                            .equals(removal.getReconfiguredCommodities().keySet())) {
                            result.remove(addition);
                            result.remove(removal);
                            list.remove(addition);
                            list.remove(removal);
                        }
                    }
                }
            }
        }
        return result;
    }

    /**
     * This method is used to combine Move action with previous CompoundMove action or
     * combine CompoundMove action with previous Move actions.
     *
     * @param combined the map which maps an action combined key to an action
     * @param perTargetList the list of actions of the trader
     * @param action the current action needs to be processed
     * @param target the trader the action belongs to
     * @return an instance of {@link ActionCollapser} which stores variables need to be updated
     */
    private static ActionCollapser collapseMovesAndCompoundMove(
            Map<Object, @NonNull Action> combined, List<Action> perTargetList,
            Action action, Trader target) {
        Object key = action.getCombineKey();
        Action previousAction = combined.get(key);
        boolean removePreviousAction = true;

        if (action.getType() == ActionType.MOVE) {
            Move move = (Move) action;
            Object newKey = Lists.newArrayList(CompoundMove.class, action.getActionTarget());
            previousAction = combined.get(newKey);
            // If there exists a compoundMove for the same trader before the current move,
            // combine them. This will happen when a trader is first moved from unplaced
            // or one clique to a new clique, and then moved inside the new clique.
            if (previousAction != null) {
                key = newKey;
                // compoundMove will not be null because trader is moved from one clique
                // to another, which means source and destination will not cancel each other.
                CompoundMove compoundMove =
                    CompoundMove.createAndCheckCompoundMoveWithExplicitSources(
                        move.getEconomy(),
                        Lists.newArrayList(move.getTarget()),
                        Lists.newArrayList(move.getSource()),
                        Lists.newArrayList(move.getDestination()));
                action = compoundMove;
                if (logger.isTraceEnabled()) {
                    logger.trace("Merge Move with previous CompoundMove for trader {}," +
                            "economy index = {}",
                        target.getDebugInfoNeverUseInCode(), target.getEconomyIndex());
                }
            }
        } else if (action.getType() == ActionType.COMPOUND_MOVE) {
            CompoundMove compoundMove = (CompoundMove) action;
            int size = compoundMove.getConstituentMoves().size();
            List<ShoppingList> shoppingLists = new ArrayList<>(size);
            List<Trader> sources = new ArrayList<>(size);
            List<Trader> destinations = new ArrayList<>(size);
            for (Move move : compoundMove.getConstituentMoves()) {
                Object newKey = Lists.newArrayList(Move.class, move.getTarget());
                previousAction = combined.get(newKey);
                if (previousAction != null) {
                    perTargetList.remove(previousAction);
                    combined.remove(newKey);
                    Move previousMove = (Move) previousAction;
                    shoppingLists.add(move.getTarget());
                    sources.add(previousMove.getSource());
                    destinations.add(previousMove.getDestination());
                }
            }
            // If there exists any moves for the same trader before the current compoundMove,
            // find all moves and combine them. This will happen when a trader is first moved
            // inside the old clique, and then moved to a new clique.
            if (!shoppingLists.isEmpty()) {
                // previousCompoundMove will not be null because trader is moved from one clique
                // to another, which means source and destination will not cancel each other.
                CompoundMove previousCompoundMove =
                    CompoundMove.createAndCheckCompoundMoveWithExplicitSources(
                        compoundMove.getConstituentMoves().get(0).getEconomy(),
                        shoppingLists, sources, destinations);
                previousAction = previousCompoundMove;
                removePreviousAction = false;
                if (logger.isTraceEnabled()) {
                    logger.trace("Merge CompoundMove with previous Moves for trader {}," +
                            "economy index = {}",
                        target.getDebugInfoNeverUseInCode(), target.getEconomyIndex());
                }
            }
        }
        return new ActionCollapser(key, removePreviousAction, previousAction, action);
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

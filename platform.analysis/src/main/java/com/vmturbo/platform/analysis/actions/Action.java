package com.vmturbo.platform.analysis.actions;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;
import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.Lists;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;

/**
 * The root of the action hierarchy.
 */
// TODO (Vaptistis): Do we need actions to be applicable to other economies than those used to
// generate them?
public interface Action {

    static final Logger logger = Logger.getLogger(Action.class);
    static final Function<Trader, String> ECONOMY_INDEX = t -> String.valueOf(t.getEconomyIndex());
   // Methods

    /**
     * Returns a String representation of {@code this} action suitable for transmission to the
     * "all-the-rest" server.
     *
     * @param oid A function that maps {@link Trader traders} to OIDs.
     * @return A String representation of {@code this} action where all object references have been
     *         substituted with OIDs.
     */
    // TODO: hoist most work for serializing the action here. Subclasses may not even need to know
    // if the format is JSON or XML e.g..
    @Pure
    @NonNull String serialize(@NonNull Function<@NonNull Trader, @NonNull String> oid);

    /**
     * Returns the {@link Trader} that this action operates on. If the action has a getTrader
     * method then usually it will return the value of that method.
     * @return the {@link Trader} that this action operates on
     */
    @Pure
    @NonNull Trader getActionTarget();

    /**
     * Takes {@code this} action on a specified {@link Economy}.
     *
     * <p>
     *  An action can be taken only once, unless it is rolled back in which case it can be taken
     *  again.
     * </p>
     *
     * @return {@code this}
     */
    @NonNull Action take();

    /**
     * Rolls back {@code this} action on a specified {@link Economy}.
     *
     * <p>
     *  An action can only be rolled back after it is taken and only once for each time it is taken.
     * </p>
     *
     * @return {@code this}
     */
    @NonNull Action rollback();

    /**
     * Returns a human-readable description of {@code this} action for debugging purposes.
     *
     * <p>
     *  The returned message may or may not be similar to what the user will see.
     * <p>
     *
     * @param uuid A function from trader to its UUID.
     * @param name A function from trader to its human-readable name.
     * @param commodityType A function from numeric to human-readable commodity type.
     * @param traderType A function from numeric to human-readable trader type.
     * @return A human-readable description of {@code this} action.
     */
    @Pure
    @NonNull String debugDescription(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                     @NonNull Function<@NonNull Trader, @NonNull String> name,
                                     @NonNull IntFunction<@NonNull String> commodityType,
                                     @NonNull IntFunction<@NonNull String> traderType);
    /**
     * Returns a human-readable reason for {@code this} action for debugging purposes.
     *
     * <p>
     *  The returned message may or may not be similar to what the user will see.
     * <p>
     *
     * @param uuid A function from trader to its UUID.
     * @param name A function from trader to its human-readable name.
     * @param commodityType A function from numeric to human-readable commodity type.
     * @param traderType A function from numeric to human-readable trader type.
     * @return A human-readable reason for {@code this} action.
     */
    @Pure
    @NonNull String debugReason(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                @NonNull Function<@NonNull Trader, @NonNull String> name,
                                @NonNull IntFunction<@NonNull String> commodityType,
                                @NonNull IntFunction<@NonNull String> traderType);

    /**
     * A key to look up "combinable" actions - actions that can be {@link #combine}d
     * @return the key identifying actions that can be combined
     */
    @Pure
    default @NonNull Object getCombineKey() {
        return this;
    }

    /**
     * Computes an {@link Action} that, when {@link #take}n, achieves the same result as
     * taking {@code this} action and then the argument action. The default behavior is
     * to return {@code action}, i.e. the next action cancels the previous one.
     * @param action an {@link Action} to combine with {@code this}
     * @return the combined {@link Action}. Null means the actions cancel each other.
     * @see #collapsed
     */
    @Pure
    default @Nullable Action combine(@NonNull @ReadOnly Action action) {
        checkArgument(getCombineKey().equals(action.getCombineKey()));
        return action;
    }

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
    static @NonNull List<@NonNull Action> collapsed(@NonNull @ReadOnly List<@NonNull @ReadOnly Action> actions) {
        Map<Object, @NonNull Action> combined = new LinkedHashMap<>();
        Map<Trader, List<Action>> perTargetMap = new LinkedHashMap<>();
        // Insert to map, combine actions
        for (Action action : actions) {
            Trader target = action.getActionTarget();
            // get or create list entry
            List<Action> perTargetList = perTargetMap.compute(target, (t, l) -> l == null ? new LinkedList<>() : l);
            int perTargetLastIndex = perTargetList.size() - 1;
            if (action instanceof Activate) {
                Action lastAction = perTargetList.isEmpty() ? null : perTargetList.get(perTargetLastIndex);
                if (lastAction instanceof Deactivate) {
                    // Activate after Deactivate cancels it
                    perTargetList.remove(perTargetLastIndex);
                    combined.remove(lastAction);
                    if (logger.isTraceEnabled()) {
                        logger.trace("Remove Deactivate/Activate for trader " + target.getEconomyIndex());
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
            if (!list.isEmpty() && list.get(list.size() -1 ) instanceof Deactivate) {
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
                        : Lists.newArrayList(list.subList(0, list.size() - 1));
                result.removeAll(remove);
                list.removeAll(remove);
            }
        }
        return result;
    }

    /**
     * Group actions of same type together, and the sequence for actions should follow the order:
     * provision-> move(initial move) -> resize-> move-> suspension.
     *
     * @param initialMoves The {@link Move} actions that are the first placement for any trader
     * @param collapsedActions The {@link Action} that has gone through collapsing and needs to
     * be grouped by type.
     *
     * @return The list of <b>actions</b> grouped and reordered
     * The action order should be provision-> move(initial move) -> resize-> move-> suspension.
     */
    @Deterministic
    public static @NonNull List<@NonNull Action> groupActionsByTypeAndReorderBeforeSending(
                    @NonNull @ReadOnly List<@NonNull @ReadOnly Action> initialMoves,
                    @NonNull @ReadOnly List<@NonNull @ReadOnly Action> collapsedActions) {
        @NonNull
        List<@NonNull Action> reorderedActions = new ArrayList<@NonNull Action>();
        // group the collapsed actions by type
        Map<Class<? extends Action>, List<Action>> collapsedActionsPerType =
                        collapsedActions.stream().collect(Collectors.groupingBy(Action::getClass));

        if (collapsedActionsPerType.containsKey(ProvisionBySupply.class)) {
            reorderedActions.addAll(collapsedActionsPerType.get(ProvisionBySupply.class));
        }
        if (collapsedActionsPerType.containsKey(ProvisionByDemand.class)) {
            reorderedActions.addAll(collapsedActionsPerType.get(ProvisionByDemand.class));
        }
        // put the initial move actions before any resize actions
        reorderedActions.addAll(initialMoves);

        if (collapsedActionsPerType.containsKey(Resize.class)) {
            reorderedActions.addAll(collapsedActionsPerType.get(Resize.class));
        }
        if (collapsedActionsPerType.containsKey(Move.class)) {
            reorderedActions.addAll(collapsedActionsPerType.get(Move.class));
        }
        if (collapsedActionsPerType.containsKey(Deactivate.class)) {
            reorderedActions.addAll(collapsedActionsPerType.get(Deactivate.class));
        }
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
} // end Action interface

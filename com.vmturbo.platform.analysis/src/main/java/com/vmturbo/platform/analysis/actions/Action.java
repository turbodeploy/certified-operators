package com.vmturbo.platform.analysis.actions;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.google.common.collect.Lists;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.topology.OID;

/**
 * The root of the action hierarchy.
 */
// TODO (Vaptistis): Do we need actions to be applicable to other economies than those used to
// generate them?
public interface Action {
    // Methods

    /**
     * Returns a String representation of {@code this} action suitable for transmission to the
     * "all-the-rest" server.
     *
     * @param oid A function that maps {@link Trader traders} to {@link OID OIDs}.
     * @return A String representation of {@code this} action where all object references have been
     *         substituted with {@link OID OIDs}.
     */
    // TODO: hoist most work for serializing the action here. Subclasses may not even need to know
    // if the format is JSON or XML e.g..
    @Pure
    @NonNull String serialize(@NonNull Function<@NonNull Trader, @NonNull String> oid);

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
    default @NonNull Object getCombineKey() {
        return this;
    }

    /**
     * Computes an {@link Action} that, when {@link #take}n, achieves the same result as
     * taking {@code this} action and then the argument action. The default behavior is
     * to return {@code action}, i.e. the next action cancels the previous one.
     * @param action an {@link Action} to combine with {@code this}
     * @return the combined {@link Action}. Null means the actions cancel each other.
     * @see {@link #collapse}
     */
    default Action combine(Action action) {
        return action;
    }

    /**
     * Collapses a list of {@link Action}s by combining "combinable" actions.
     * Two actions are said to be combinable if they can be replaced by one action that,
     * when {@link #take}n, achieves the same result as taking the pair of actions in order.
     * @param actions a list of actions to collapse. Assume the list is consistent, e.g. a Move
     * TO trader A cannot be followed by a Move FROM a different trader B.
     * @return a list of actions that represents the same outcome as the argument list.
     * @see {@link #combine}
     */
    public static List<Action> collapse(List<Action> actions) {
        Map<Object, Action> combined = new LinkedHashMap<Object, @NonNull Action>();
        // Insert to map, combine actions
        for (Action action : actions) {
            Object key = action.getCombineKey();
            Action previousAction = combined.get(key);
            if (previousAction == null) {
                combined.put(key, action);
            } else {
                Action collapsedAction = previousAction.combine(action);
                if (collapsedAction == null) {
                    // The actions cancel each other
                    combined.remove(key);
                } else {
                    combined.put(key, collapsedAction);
                }
            }
        }
        return Lists.newArrayList(combined.values());
    }
} // end Action interface

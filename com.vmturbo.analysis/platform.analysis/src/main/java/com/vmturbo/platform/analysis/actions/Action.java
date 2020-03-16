package com.vmturbo.platform.analysis.actions;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.function.Function;
import java.util.function.IntFunction;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;

/**
 * The root of the action hierarchy.
 */
// TODO (Vaptistis): Do we need actions to be applicable to other economies than those used to
// generate them?
public interface Action {

   // Methods
    boolean setExecutable(boolean executable);
    boolean isExecutable();
    // Extract some Action that isn't normally extracted from the provision analysis round of
    // main market.
    // For example a Resize action in a ResizeThroughSupplier flow.
    boolean isExtractAction();
    void enableExtractAction();


    ActionType getType();

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
     * Returns the economy of {@code this} action. i.e. the economy containing the action target.
     *
     * @see #getActionTarget()
     */
    @Pure
    @NonNull Economy getEconomy(@ReadOnly Action this);

    /**
     * Returns the {@link Trader} that this action operates on. If the action has a getTrader
     * method then usually it will return the value of that method.
     *
     * @return the {@link Trader} that this action operates on
     */
    @Pure
    @NonNull Trader getActionTarget(@ReadOnly Action this);

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
     *
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
     *
     * @param action an {@link Action} to combine with {@code this}
     * @return the combined {@link Action}. Null means the actions cancel each other.
     * @see ActionCollapse#collapsed
     */
    @Pure
    default @Nullable Action combine(@NonNull @ReadOnly Action action) {
        checkArgument(getCombineKey().equals(action.getCombineKey()));
        return action;
    }

    /**
     * Returns the 'reason' commodity for this action.
     *
     * <p>For example : commodity specification that led to activation/provision.</p>
     */
    // TODO : Generalize this to return "Reason" for all actions.
    @Pure
    default @Nullable CommoditySpecification getReason() {
        return null;
    }

    @NonNull List<Action> getSubsequentActions();
} // end Action interface

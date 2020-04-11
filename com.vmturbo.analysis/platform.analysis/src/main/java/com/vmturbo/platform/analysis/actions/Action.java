package com.vmturbo.platform.analysis.actions;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
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

    // Design Notes: Analyzing existing code and considering requirements from current effort to
    // replay provision actions, it seems that there are 3 fundamental questions that need to be
    // answered regarding the replay of actions:
    //
    // 1. Can the action be physically applied to a different economy than the one it was generated
    //    for?
    // 2. Would this action respect settings and policies if it were to be taken on a given economy?
    // 3. Would this action bring a given economy closer to its desired state if it was to be taken?
    //
    // Past experience shows that not all of these questions need to be answered in all contexts and
    // that the answer remains more stable, while new features are added, for some than for others.
    // As a result I decided to implement them as separate methods. Since these are now virtual
    // methods, the long if-elseif-else statements to select the appropriate implementation aren't
    // needed and the client code can comply with the open-closed principle.
    //
    // The 3rd question is really only asked for Deactivate for now, and there were some existing
    // methods covering that, so no new methods are created for it for the time being.
    //
    // For 'port', which answers the 1st question while also creating the new action object that can
    // be applied to the new economy, a dependency injection technique was used so that action
    // classes can remain agnostic to concepts like topologies and OIDs and to provide more
    // flexibility while testing.
    //
    // Note that 'isValid', which answers the 2nd question, is intended to do relatively cheap tests
    // and in any case not make calls to the analysis algorithms.
    //
    // Another difference between 'isValid' and a method that would test if an action is towards the
    // desired state is that it never makes sense for an analysis algorithm to produce an invalid
    // action but it does make sense to produce actions that are not towards the desired state,
    // either as a prerequisite to other actions or in order to escape a local optimum and find a
    // better one. As a result 'isValid' can be used in testing of the analysis algorithms in
    // addition to replaying actions.

    /**
     * Ports {@code this} action from the current {@link Economy} to a new one, iff possible.
     *
     * <p>Actions are generated by algorithms running on an economy and make sense for that economy.
     * It is possible that they also make sense for another economy that doesn't differ too much.
     * </p>
     *
     * <p>This method will do the necessary checks and transformations to produce a new action that,
     * when applied, will effect the new economy in a way that is analogous to the way that the old
     * action would effect the old economy. e.g. The action target could be mapped to the trader
     * with the same OID in the new economy.</p>
     *
     * <p>It is possible that the new action doesn't make sense for the new economy. e.g. A trader
     * with the same OID as the action's target might not exist there. In this case, a
     * {@link NoSuchElementException} should be thrown.</p>
     *
     * <p>It is also possible that the new action doesn't bring the target economy closer to its
     * desired state, but in this case the new action will be returned to be further processed by a
     * higher-level method.</p>
     *
     * @param destinationEconomy The economy {@code this} action should be ported to.
     * @param destinationTrader The function that should be used to map a trader in the source
     *                          economy to one in the destination economy. If such a mapping doesn't
     *                          exist, the function should throw a {@link NoSuchElementException}.
     * @param destinationShoppingList The function that should be used to map a shopping list in the
     *                                source economy to one in the destination economy. If such a
     *                                mapping doesn't exist, the function should throw a
     *                                {@link NoSuchElementException}.
     * @return The ported action iff {@code this} action can be ported to <b>destinationEconomy</b>.
     * @throws NoSuchElementException Iff the action can't be ported to <b>destinationEconomy</b>
     *                                using the specified mapping.
     */
    @Pure
    @NonNull Action port(@NonNull Economy destinationEconomy,
         @NonNull Function<@NonNull Trader, @NonNull Trader> destinationTrader,
         @NonNull Function<@NonNull ShoppingList, @NonNull ShoppingList> destinationShoppingList);

    /**
     * Returns whether {@code this} action respects constraints and can be taken.
     *
     * <p>It does not concern itself with whether the economy will be closer to the desired state
     * after taking {@code this} action. It only considers settings like whether its target is
     * resizable, movable, etc. and current state like whether target is active or placed on source.
     * </p>
     *
     * <p>The exact set of constraints considered differs by implementation and is documented there.
     * </p>
     *
     * @return Whether {@code this} action respects constraints and can be taken.
     */
    @Pure
    boolean isValid();

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

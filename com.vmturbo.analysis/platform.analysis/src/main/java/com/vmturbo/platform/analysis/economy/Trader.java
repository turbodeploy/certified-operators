package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;


/**
 * An entity that trades goods in a {@link Market}.
 *
 * <p>
 *  It can participate in multiple markets either as a seller or a buyer and even participate in a
 *  single market multiple times (though never as a buyer and seller simultaneously). The latter can
 *  happen e.g. if a buyer buys multiple storage commodities. It is also possible that a Trader
 *  buys from another trader that is not in the current market, if a policy is created that is not
 *  enforced initially.
 * </p>
 */
public abstract class Trader {
    // Fields
    private int economyIndex_;
    private final int type_; // this should never change once the object is created.
    private @NonNull TraderState state_;
    private @NonNull Basket basketSold_;
    private final @NonNull List<@NonNull CommoditySold> commoditiesSold_ = new ArrayList<>();
    private final @NonNull List<@NonNull Integer> cliques_ = new ArrayList<>();
    private final @NonNull List<@NonNull ShoppingList> customers_ = new ArrayList<>();
    // TODO: (Jun 22, 2016) This field is intended to be temporarily used for debugging in the initial stages of M2. To avoid making drastic change in
    // market2, we use a setter and getter to set and get this field instead of putting it part of the constructor even though it should
    // only be set once and never changed.
    // If in future we want to keep it, we should make it part of the constructor, remove the setter, also modify all places that call it
    // and the corresponding tests.
    private String debugInfoNeverUseInCode_; // a field keeps information about the eclass and uuid of the trader.
    // Cached data

    // Cached unmodifiable view of the commoditiesSold_ list.
    private final @NonNull List<@NonNull CommoditySold> unmodifiableCommoditiesSold_ = Collections.unmodifiableList(commoditiesSold_);
    // Cached unmodifiable view of the cliques_ list.
    private final @NonNull List<@NonNull Integer> unmodifiableCliques_ = Collections.unmodifiableList(cliques_);
    // Cached unmodifiable view of the customers_ list.
    private final @NonNull List<@NonNull ShoppingList> unmodifiableCustomers_ = Collections.unmodifiableList(customers_);

    // Constructors

    /**
     * Constructs a new TraderWithSettings instance with the specified attributes.
     *
     * @param economyIndex see {@link #setEconomyIndex(int)}.
     * @param type see {@link #getType()}.
     * @param state see {@link #setState(TraderState)}.
     * @param basketSold see {@link #getBasketSold()}.
     */
    public Trader(int economyIndex, int type, @NonNull TraderState state, @NonNull Basket basketSold) {
        checkArgument(type >= 0, "type = " + type);

        type_ = type;
        state_ = state;
        basketSold_ = basketSold;
        setEconomyIndex(economyIndex);

        for(int i = 0 ; i < basketSold.size() ; ++i) {
            commoditiesSold_.add(new CommoditySoldWithSettings());
        }
    }

    // Methods

    /**
     * Returns the basket sold by {@code this} seller.
     */
    @Pure
    public @NonNull @ReadOnly Basket getBasketSold(@ReadOnly Trader this) {
        return basketSold_;
    }

    /**
     * Returns an unmodifiable list of the commodities {@code this} trader is selling.
     */
    @Pure
    public @NonNull @ReadOnly List<@NonNull @ReadOnly CommoditySold> getCommoditiesSold(@ReadOnly Trader this) {
        return unmodifiableCommoditiesSold_;
    }

    /**
     * Returns the commodity sold by {@code this} trader that corresponds to the given
     * {@link CommoditySpecification}.
     *
     * @param specification The commodity specification specifying the commodity sold that should be
     *         returned.
     * @return The commodity sold by {@code this} trader that corresponds to the given commodity
     *         specification, or {@code null} iff !{@link #getBasketSold()}.
     *         {@link Basket#contains(CommoditySpecification) contains(specification)}
     */
    @Pure
    public @PolyRead CommoditySold getCommoditySold(@PolyRead Trader this, @NonNull @ReadOnly CommoditySpecification specification) {
        int index = getBasketSold().indexOf(specification);

        return index != -1 ? commoditiesSold_.get(index) : null;
    }

    // TODO: consider making addCommoditySold and removeCommoditySold throw in cases they now return
    // null. Same for the corresponding Basket methods.

    /**
     * Adds a new commodity to the list of commodities sold by {@code this} seller and returns it.
     *
     * @param newSpecification The type of the new commodity. It will be added to {@code this}
     *          seller's basket, or ignored if it already exists.
     * @return The new commodity that was created and added, or {@code null} if it already existed.
     */
    @Deterministic
    public CommoditySold addCommoditySold(@NonNull @ReadOnly CommoditySpecification newSpecification) {
        basketSold_ = basketSold_.add(newSpecification);

        if (commoditiesSold_.size() < basketSold_.size()) {
            CommoditySoldWithSettings newCommoditySold = new CommoditySoldWithSettings();
            commoditiesSold_.add(basketSold_.indexOf(newSpecification), newCommoditySold);

            return newCommoditySold;
        }

        return null;
    }

    /**
     * Removes an existing commodity from the list of commodities sold by {@code this} seller.
     *
     * <p>
     *  A commodity sold by a single trader is uniquely identified by its type.
     *  Both the list of commodities sold and the basket sold are updated.
     * </p>
     *
     * @param specificationToRemove The specification of the commodity that needs to be removed.
     *              It will be removed from {@code this} seller's basket, or ignored if it was never
     *              in the basket.
     * @return The removed commodity sold, or {@code null} if it wasn't in the basket.
     */
    @Deterministic // in the sense that for the same referents of this and typeToRemove the result will
    // be the same. Calling this two times on the same topology will produce different results
    // because the topology is modified.
    public CommoditySold removeCommoditySold(@NonNull @ReadOnly CommoditySpecification specificationToRemove) {
        int index = basketSold_.indexOf(specificationToRemove);

        if (index == -1) {
            return null;
        } else {
            CommoditySold removed = commoditiesSold_.remove(index);
            basketSold_ = basketSold_.remove(specificationToRemove);

            return removed;
        }
    }

    /**
     * Returns an unmodifiable list of the k-partite cliques {@code this} trader is a member of.
     *
     * <p>
     *  There are situations when a buyer needs to buy a number of {@link ShoppingList}s from
     *  different {@link Market}s and that not all combinations of sellers from those markets are
     *  valid. In these situations the k-partite graph of valid placements is covered by k-partite
     *  cliques so that any combination of sellers from different parts of the graph and in the same
     *  k-partite clique are valid. This clique cover is constructed before the topology is sent to
     *  the analysis engine. Refer to the VMTurbo wiki for more details.
     * </p>
     *
     * <p>
     *  Each of those cliques is represented as an integer.
     * </p>
     *
     * <p>
     *  {@link Market}s organize their sellers based on k-partite clique membership.
     * </p>
     */
    @Pure
    public @NonNull @ReadOnly List<@NonNull Integer> getCliques(@ReadOnly Trader this) {
        return unmodifiableCliques_;
    }

    /**
     * Returns a modifiable list of the k-partite cliques {@code this} trader is a member of.
     *
     * <p>
     *  This is a modifiable version of the list returned by {@link #getCliques()}.
     * </p>
     *
     * @see #getCliques()
     */
    @Pure
    @NonNull @PolyRead List<@NonNull @PolyRead Integer> getModifiableCliques(@PolyRead Trader this) {
        return cliques_;
    }

    /**
     * Returns an unmodifiable list of {@code this} trader's customers.
     *
     * <p>
     *  A trader is a customer of another trader, iff the former is active and currently buying at
     *  least one commodity the latter is selling.
     * </p>
     *
     * <p>
     *  This method really returns shopping lists instead of discrete traders, so if a trader
     *  buys the same commodity specification more than once, the list will contain more than one
     *  shopping list belonging to the same trader. For a method returning a list of unique
     *  traders that are customers of {@code this} trader, see {@link #getUniqueCustomers()}.
     * </p>
     *
     * @see #getUniqueCustomers()
     */
    @Pure
    public @NonNull @ReadOnly List<@NonNull ShoppingList> getCustomers(@ReadOnly Trader this) {
        return unmodifiableCustomers_;
    }

    /**
     * Returns a modifiable list of {@code this} trader's customers.
     *
     * <p>
     *  This is a modifiable version of the list returned by {@link #getCustomers()}.
     * </p>
     *
     * @see #getCustomers()
     * @see #getUniqueCustomers()
     */
    @Pure
    @NonNull @PolyRead List<@NonNull @PolyRead ShoppingList> getModifiableCustomers(@PolyRead Trader this) {
        return customers_;
    }

    /**
     * Returns an unmodifiable set of {@code this} trader's customers.
     *
     * <p>
     *  A trader is a customer of another trader iff the former is active and currently buys any
     *  subset of the commodities the latter is selling.
     * </p>
     *
     * <p>
     *  This method returns a set of unique traders. For a list of all the shopping lists
     *  buying from {@code this} trader, see {@link #getCustomers()}.
     * </p>
     *
     * @see #getCustomers()
     */
    @Pure
    public @NonNull @ReadOnly Set<@NonNull @ReadOnly Trader> getUniqueCustomers(@ReadOnly Trader this) {
        @NonNull Set<@NonNull @ReadOnly Trader> customers = new HashSet<>();

        for (@NonNull ShoppingList shoppingList : getCustomers()) {
            customers.add(shoppingList.getBuyer());
        }

        return Collections.unmodifiableSet(customers);
    }

    // May need to add some reference to the associated reservation later...

    /**
     * The {@link TraderSettings settings} controlling {@code this} trader's behavior.
     */
    @Pure
    public abstract @NonNull TraderSettings getSettings();

    /**
     * Returns the <em>economy index</em> of {@code this} trader.
     *
     * <p>
     *  The economy index of a trader is its position in the {@link Economy#getTraders() traders
     *  list} and it's non-negative and non-increasing. It will be decreased iff a trader with lower
     *  economy index is removed from the economy.
     * </p>
     *
     * <p>
     *  This is an O(1) operation.
     * </p>
     */
    @Pure
    public int getEconomyIndex(@ReadOnly Trader this) {
        return economyIndex_;
    }

    /**
     * Sets the value of the <b>economy index</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param economyIndex the new value for the field. Must be non-negative.
     * @return {@code this}
     *
     * @see #getEconomyIndex()
     */
    @Deterministic
    @NonNull Trader setEconomyIndex(int economyIndex) {
        checkArgument(economyIndex >= 0, "economyIndex = " + economyIndex);
        economyIndex_ = economyIndex;
        return this;
    }

    /**
     * Returns the type of the trader.
     *
     * <p>
     *  Its a numerical representation of the type. An ID of sorts that may e.g. correspond to
     *  "physical machine" or "storage", but the correspondence is not important to the economy and
     *  kept (potentially) outside the economy. It is non-negative.
     * </p>
     */
    @Pure
    public int getType(@ReadOnly Trader this) {
        return type_;
    }

    /**
     * Returns the current {@link TraderState state} of {@code this} trader.
     */
    @Pure
    public @NonNull TraderState getState(@ReadOnly Trader this) {
        return state_;
    }

    /**
     * Sets the value of the <b>state</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param state the new value for the field.
     * @return {@code this}
     */
    @Deterministic
    @NonNull Trader setState(@NonNull @ReadOnly TraderState state) {
        state_ = state;
        return this;
    }

    /**
     * Changes the state of {@code this} trader, updating the corresponding markets he participates
     * in as a buyer or seller to reflect the change.
     *
     * @param newState The new state for the trader.
     * @return The old state of trader.
     */
    @Deterministic
    public abstract @NonNull TraderState changeState(@NonNull TraderState newState);

    /**
     * Sets the debugInfo field. It contains information about the eclass and uuid of the trader.
     * @param debugInfo a string contains eclass|uuid of the trader
     */
    public @NonNull Trader setDebugInfoNeverUseInCode(@NonNull String debugInfo) {
        debugInfoNeverUseInCode_ = debugInfo;
        return this;
    }

    /**
     * Returns the debugInfo field.
     */
    public String getDebugInfoNeverUseInCode(@ReadOnly Trader this) {
        return debugInfoNeverUseInCode_;
    }
} // end interface Trader

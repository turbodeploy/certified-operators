package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    // Cached data

    // Cached unmodifiable view of the commoditiesSold_ list.
    private final @NonNull List<@NonNull CommoditySold> unmodifiableCommoditiesSold_ = Collections.unmodifiableList(commoditiesSold_);

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
        checkArgument(type >= 0);

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
        checkArgument(economyIndex >= 0);
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

} // end interface Trader

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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

final class TraderWithSettings implements Trader, TraderSettings {
    // Internal fields
    private int economyIndex_;
    private final @NonNull ListMultimap<@NonNull Market, @NonNull BuyerParticipation> marketsAsBuyer_ = ArrayListMultimap.create();
    private final @NonNull List<Market> marketsAsSeller_ = new ArrayList<>();
    private final @NonNull ArrayList<@NonNull BuyerParticipation> customers_ = new ArrayList<>();

    // Fields for Trader
    private final int type_; // this should never change once the object is created.
    private @NonNull TraderState state_;
    private @NonNull Basket basketSold_;
    private final @NonNull List<@NonNull CommoditySold> commoditiesSold_ = new ArrayList<>();

    // Fields for TraderSettings
    private boolean suspendable_ = false;
    private boolean cloneable_ = false;
    private boolean movable_ = false;
    private double maxDesiredUtilization_ = 1.0;
    private double minDesiredUtilization_ = 0.0;

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
    public TraderWithSettings(int economyIndex, int type, @NonNull TraderState state, @NonNull Basket basketSold) {
        checkArgument(type >= 0);

        type_ = type;
        state_ = state;
        basketSold_ = basketSold;
        setEconomyIndex(economyIndex);

        for(int i = 0 ; i < basketSold.size() ; ++i) {
            commoditiesSold_.add(new CommoditySoldWithSettings());
        }
    }

    // Internal methods

    /**
     * Returns the economy index of {@code this} trader.
     *
     * <p>
     *  It is the position of {@code this} trader in the list containing all traders in the
     *  {@link Economy}.
     * </p>
     */
    @Pure
    int getEconomyIndex(@ReadOnly TraderWithSettings this) {
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
    @NonNull TraderWithSettings setEconomyIndex(int economyIndex) {
        checkArgument(economyIndex >= 0);
        economyIndex_ = economyIndex;
        return this;
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
    // TODO: add a corresponding method to economy.
    @Deterministic
    @NonNull Trader setState(@NonNull @ReadOnly TraderState state) {
        state_ = state;
        return this;
    }

    /**
     * Returns a modifiable {@link ListMultimap} with the mapping from the markets {@code this}
     * buyer participates in to the buyer participations it has in those markets.
     *
     * <p>
     *  A trader does not know how to modify this map, so it just returns it for the economy to
     *  modify.
     * </p>
     */
    @Pure
    @NonNull @PolyRead ListMultimap<@NonNull @ReadOnly Market, @NonNull @PolyRead BuyerParticipation>
            getMarketsAsBuyer(@PolyRead TraderWithSettings this) {
        return marketsAsBuyer_;
    }

    /**
     * Returns a modifiable List with the markets {@code this} seller participates in.
     *
     * <p>
     *  A trader does not know how to modify this list, so it just returns it for the economy to
     *  modify.
     * </p>
     */
    @Pure
    @NonNull @PolyRead List<@NonNull @PolyRead Market> getMarketsAsSeller(@PolyRead TraderWithSettings this) {
        return marketsAsSeller_;
    }

    /**
     * Returns a list of {@code this} trader's customers.
     *
     * <p>
     *  A trader is a customer of another trader, iff the former is currently buying at least one
     *  commodity the latter is selling.
     * </p>
     *
     * <p>
     *  This method really returns buyer participations instead of discrete traders, so if a trader
     *  buys the same commodity specification more than once, the list will contain more than one
     *  buyer participation belonging to the same trader.
     * </p>
     */
    @Pure
    @NonNull @PolyRead List<@NonNull @PolyRead BuyerParticipation> getCustomers(@PolyRead TraderWithSettings this) {
        return customers_;
    }

    // Methods for Trader

    @Override
    @Pure
    public @NonNull @ReadOnly Basket getBasketSold(@ReadOnly TraderWithSettings this) {
        return basketSold_;
    }

    @Override
    @Pure
    public @NonNull List<@NonNull @ReadOnly CommoditySold> getCommoditiesSold(@ReadOnly TraderWithSettings this) {
        return unmodifiableCommoditiesSold_;
    }

    @Override
    @Pure
    public @PolyRead CommoditySold getCommoditySold(@PolyRead TraderWithSettings this,
                                                    @NonNull @ReadOnly CommoditySpecification specification) {
        int index = getBasketSold().indexOf(specification);

        return index != -1 ? commoditiesSold_.get(index) : null;
    }

    @Override
    public CommoditySold addCommoditySold(@NonNull @ReadOnly CommoditySpecification newSpecification) {
        basketSold_ = basketSold_.add(newSpecification);

        if (commoditiesSold_.size() < basketSold_.size()) {
            CommoditySoldWithSettings newCommoditySold = new CommoditySoldWithSettings();
            commoditiesSold_.add(basketSold_.indexOf(newSpecification), newCommoditySold);

            return newCommoditySold;
        }

        return null;
    }

    @Override
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

    @Override
    @Pure
    public @NonNull TraderSettings getSettings(@ReadOnly TraderWithSettings this) {
        return this;
    }

    @Override
    @Pure
    public int getType(@ReadOnly TraderWithSettings this) {
        return type_;
    }

    @Override
    @Pure
    public @NonNull TraderState getState(@ReadOnly TraderWithSettings this) {
        return state_;
    }

    // Methods for TraderSettings

    @Override
    @Pure
    public boolean isSuspendable(@ReadOnly TraderWithSettings this) {
        return suspendable_;
    }

    @Override
    @Pure
    public boolean isCloneable(@ReadOnly TraderWithSettings this) {
        return cloneable_;
    }

    @Override
    @Pure
    public boolean isMovable(@ReadOnly TraderWithSettings this) {
        return movable_;
    }

    @Override
    @Pure
    public double getMaxDesiredUtil(@ReadOnly TraderWithSettings this) {
        return maxDesiredUtilization_;
    }

    @Override
    @Pure
    public double getMinDesiredUtil(@ReadOnly TraderWithSettings this) {
        return minDesiredUtilization_;
    }

    @Override
    @Deterministic
    public @NonNull TraderWithSettings setSuspendable(boolean suspendable) {
        suspendable_ = suspendable;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderWithSettings setCloneable(boolean cloneable) {
        cloneable_ = cloneable;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderWithSettings setMovable(boolean movable) {
        movable_ = movable;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderWithSettings setMaxDesiredUtil(double maxDesiredUtilization) {
        checkArgument(maxDesiredUtilization <= 1.0);
        checkArgument(minDesiredUtilization_ <= maxDesiredUtilization);
        maxDesiredUtilization_ = maxDesiredUtilization;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderWithSettings setMinDesiredUtil(double minDesiredUtilization) {
        checkArgument(0.0 <= minDesiredUtilization);
        checkArgument(minDesiredUtilization <= maxDesiredUtilization_);
        minDesiredUtilization_ = minDesiredUtilization;
        return this;
    }

} // end TraderWithSettings class

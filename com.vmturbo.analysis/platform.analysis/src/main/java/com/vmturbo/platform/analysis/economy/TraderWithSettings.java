package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

final class TraderWithSettings extends Trader implements TraderSettings {
    // Internal fields
    private final @NonNull Map<@NonNull ShoppingList,@NonNull Market> marketsAsBuyer_ = new LinkedHashMap<>();
    private final @NonNull List<Market> marketsAsSeller_ = new ArrayList<>();

    // Fields for TraderSettings
    private boolean suspendable_ = false;
    private boolean cloneable_ = false;
    private boolean guaranteedBuyer_ = false;
    private boolean canAcceptNewCustomers_ = false;
    private double maxDesiredUtilization_ = 1.0;
    private double minDesiredUtilization_ = 0.0;

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
        super(economyIndex,type,state,basketSold);
    }

    // Internal methods

    /**
     * Returns a modifiable {@link Map} with the mapping from shopping lists of {@code this}
     * buyer to the markets he participates in with these shopping lists.
     *
     * <p>
     *  A trader does not know how to modify this map, so it just returns it for the economy to
     *  modify.
     * </p>
     */
    @Pure
    @NonNull @PolyRead Map<@NonNull ShoppingList, @NonNull Market> getMarketsAsBuyer(@PolyRead TraderWithSettings this) {
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

    // Methods for Trader

    @Override
    @Pure
    public @NonNull TraderSettings getSettings(@ReadOnly TraderWithSettings this) {
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderState changeState(@NonNull TraderState newState) {
        return Market.changeTraderState(this, newState);
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
    public boolean isGuaranteedBuyer(@ReadOnly TraderWithSettings this) {
        return guaranteedBuyer_;
    }

    @Override
    @Pure
    public boolean canAcceptNewCustomers() {
        return canAcceptNewCustomers_;
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
    public @NonNull TraderWithSettings setMaxDesiredUtil(double maxDesiredUtilization) {
        checkArgument(maxDesiredUtilization <= 1.0, "maxDesiredUtilization = " + maxDesiredUtilization);
        checkArgument(minDesiredUtilization_ <= maxDesiredUtilization,
            "minDesiredUtilization_ = " + minDesiredUtilization_ + " maxDesiredUtilization = " + maxDesiredUtilization);
        maxDesiredUtilization_ = maxDesiredUtilization;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderWithSettings setMinDesiredUtil(double minDesiredUtilization) {
        checkArgument(0.0 <= minDesiredUtilization, "minDesiredUtilization = " + minDesiredUtilization);
        checkArgument(minDesiredUtilization <= maxDesiredUtilization_,
            "minDesiredUtilization = " + minDesiredUtilization + " maxDesiredUtilization_ = " + maxDesiredUtilization_);
        minDesiredUtilization_ = minDesiredUtilization;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderSettings setGuaranteedBuyer(boolean guaranteedBuyer) {
        guaranteedBuyer_ = guaranteedBuyer;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull TraderSettings setCanAcceptNewCustomers(boolean canAcceptNewCustomers) {
        canAcceptNewCustomers_ = canAcceptNewCustomers;
        return this;
    }

} // end TraderWithSettings class

package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

final class TraderWithSettings implements Trader, TraderSettings {
    // Fields for TraderSettings
    private boolean suspendable_ = false;
    private boolean cloneable_ = false;
    private double maxDesiredUtilization_ = 1.0;
    private double minDesiredUtilization_ = 0.0;

    // Fields for Trader
    private final int type_; // this should never change once the object is created.
    private @NonNull TraderState state_;
    private @NonNull Basket basketSold_;
    private final @NonNull List<@NonNull CommoditySold> commoditiesSold_ = new ArrayList<>();
    private final @NonNull ListMultimap<@NonNull Market, @NonNull BuyerParticipation> marketsAsBuyer_ = ArrayListMultimap.create();
    private final @NonNull List<Market> marketsAsSeller_ = new ArrayList<>();

    // Internal fields
    private int economyIndex_;

    // Constructors
    public TraderWithSettings(int economyIndex, int type, @NonNull TraderState state, @NonNull Basket basketSold) {
        type_ = type;
        state_ = state;
        basketSold_ = basketSold;
        economyIndex_ = economyIndex;

        for(int i = 0 ; i < basketSold.size() ; ++i) {
            commoditiesSold_.add(new CommoditySoldWithSettings());
        }
    }

    // Methods

    @Override
    public boolean isSuspendable() {
        return suspendable_;
    }

    @Override
    public boolean isCloneable() {
        return cloneable_;
    }

    @Override
    public double getMaxDesiredUtil() {
        return maxDesiredUtilization_;
    }

    @Override
    public double getMinDesiredUtil() {
        return minDesiredUtilization_;
    }

    /**
     * Returns the economy index of {@code this} trader.
     *
     * <p>
     *  It is the position of {@code this} trader in the list containing all traders in the
     *  {@link Economy}.
     * </p>
     */
    public int getEconomyIndex() {
        return economyIndex_;
    }

    @Override
    public @NonNull TraderWithSettings setSuspendable(boolean suspendable) {
        suspendable_ = suspendable;
        return this;
    }

    @Override
    public @NonNull TraderWithSettings setCloneable(boolean cloneable) {
        cloneable_ = cloneable;
        return this;
    }

    @Override
    public @NonNull TraderWithSettings setMaxDesiredUtil(double maxDesiredUtilization) {
        checkArgument(maxDesiredUtilization <= 1.0);
        checkArgument(minDesiredUtilization_ <= maxDesiredUtilization);
        maxDesiredUtilization_ = maxDesiredUtilization;
        return this;
    }

    @Override
    public @NonNull TraderWithSettings setMinDesiredUtil(double minDesiredUtilization) {
        checkArgument(0.0 <= minDesiredUtilization);
        checkArgument(minDesiredUtilization <= maxDesiredUtilization_);
        minDesiredUtilization_ = minDesiredUtilization;
        return this;
    }

    /**
     * Sets the value of the <b>economy index</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param economyIndex the new value for the field.
     * @return {@code this}
     */
    public @NonNull TraderWithSettings setEconomyIndex(int economyIndex) {
        economyIndex_ = economyIndex;
        return this;
    }

    @Override
    public @NonNull List<@NonNull @ReadOnly CommoditySold> getCommoditiesSold() {
        return Collections.unmodifiableList(commoditiesSold_);
    }

    @Override
    public @NonNull TraderSettings getSettings() {
        return this;
    }

    @Override
    public int getType() {
        return type_;
    }

    @Override
    public @NonNull TraderState getState() {
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
    // TODO: add a corresponding method to economy.
    public @NonNull Trader setState(@NonNull @ReadOnly TraderState state) {
        state_ = state;
        return this;
    }

    @Override
    public @NonNull @ReadOnly Basket getBasketSold(@ReadOnly TraderWithSettings this) {
        return basketSold_;
    }

    @Override
    public @NonNull CommoditySold addCommoditySold(@NonNull @ReadOnly CommoditySpecification newCommoditySpecification) {
        // TODO: improve implementation. Maybe include add method to Basket?
        @NonNull List<@NonNull CommoditySpecification> newSpecifications = new ArrayList<>(basketSold_.getCommoditySpecifications());
        newSpecifications.add(newCommoditySpecification);
        basketSold_ = new Basket(newSpecifications);

        CommoditySoldWithSettings newCommoditySold = new CommoditySoldWithSettings();
        commoditiesSold_.add(basketSold_.indexOf(newCommoditySpecification), newCommoditySold);

        return newCommoditySold;
    }

    @Override
    public @NonNull CommoditySold removeCommoditySold(@NonNull @ReadOnly CommoditySpecification typeToRemove) {
        // TODO: improve implementation. Maybe include remove method to Basket?
        CommoditySold removed = commoditiesSold_.remove(basketSold_.indexOf(typeToRemove));

        @NonNull List<@NonNull CommoditySpecification> newSpecifications = new ArrayList<>(basketSold_.getCommoditySpecifications());
        newSpecifications.remove(typeToRemove);
        basketSold_ = new Basket(newSpecifications);

        return removed;
    }

    @NonNull ListMultimap<@NonNull Market, @NonNull BuyerParticipation> getMarketsAsBuyer() {
        return marketsAsBuyer_;
    }

    @NonNull List<Market> getMarketsAsSeller() {
        return marketsAsSeller_;
    }

} // end TraderWithSettings class

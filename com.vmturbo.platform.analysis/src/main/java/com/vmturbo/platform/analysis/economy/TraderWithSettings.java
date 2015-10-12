package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

final class TraderWithSettings implements Trader, TraderSettings {
    // Fields for TraderSettings
    private boolean suspendable_ = false;
    private boolean cloneable_ = false;
    private double maxDesiredUtilization_ = 1.0;
    private double minDesiredUtilization_ = 0.0;

    // Fields for Trader
    private @NonNull TraderState state_;
    private final @NonNull TraderType type_; // this should never change once the object is created.
    private @NonNull Basket basketSold_;
    private @NonNull CommoditySold @NonNull [] commoditiesSold_;
    private @NonNull Market @NonNull [] marketsAsBuyer_ = new Market[0];

    // Constructors
    public TraderWithSettings(@NonNull TraderType type, @NonNull TraderState state,
                              @NonNull Basket basketSold) {
        type_ = type;
        state_ = state;
        basketSold_ = basketSold;

        commoditiesSold_ = new CommoditySoldWithSettings[basketSold.size()];
        for(int i = 0 ; i < basketSold.size() ; ++i) {
            commoditiesSold_[i] = new CommoditySoldWithSettings();
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

    @Override
    public @NonNull TraderSettings setSuspendable(boolean suspendable) {
        suspendable_ = suspendable;
        return this;
    }

    @Override
    public @NonNull TraderSettings setCloneable(boolean cloneable) {
        cloneable_ = cloneable;
        return this;
    }

    @Override
    public @NonNull TraderSettings setMaxDesiredUtil(double maxDesiredUtilization) {
        checkArgument(maxDesiredUtilization <= 1.0);
        checkArgument(minDesiredUtilization_ <= maxDesiredUtilization);
        maxDesiredUtilization_ = maxDesiredUtilization;
        return this;
    }

    @Override
    public @NonNull TraderSettings setMinDesiredUtil(double minDesiredUtilization) {
        checkArgument(0.0 <= minDesiredUtilization);
        checkArgument(minDesiredUtilization <= maxDesiredUtilization_);
        minDesiredUtilization_ = minDesiredUtilization;
        return this;
    }

    @Override
    public @NonNull List<@NonNull @ReadOnly CommoditySold> getCommoditiesSold() {
        return Collections.unmodifiableList(Arrays.asList(commoditiesSold_));
    }

    @Override
    public @NonNull TraderSettings getSettings() {
        return this;
    }

    @Override
    public @NonNull TraderType getType() {
        return type_;
    }

    @Override
    public @NonNull TraderState getState() {
        return state_;
    }

    @Override
    public @NonNull Trader setState(@NonNull TraderState state) {
        state_ = state;
        return this;
    }

    @Override
    public @NonNull @ReadOnly List<@NonNull @ReadOnly Trader> getCustomers() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public @NonNull @ReadOnly List<@NonNull @ReadOnly Trader> getSuppliers() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public @Nullable @ReadOnly Trader getSupplier(@NonNull @ReadOnly Market market) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public @NonNull @ReadOnly List<@NonNull @ReadOnly Market> getMarketsAsBuyer() {
        return Collections.unmodifiableList(Arrays.asList(marketsAsBuyer_));
    }

    @Override
    public @NonNull @ReadOnly List<@NonNull @ReadOnly Market> getMarketsAsSeller() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public @NonNull @ReadOnly Basket getBasketSold(@ReadOnly TraderWithSettings this) {
        return basketSold_;
    }

    @Override
    public @NonNull Trader addCommoditySold(@NonNull @ReadOnly CommodityType newCommodityType,
                                            @NonNull CommoditySold newCommoditySold) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public @NonNull CommoditySold removeCommoditySold(@NonNull @ReadOnly CommodityType typeToRemove) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public @NonNull Trader addCommodityBought(@NonNull Basket basketToAddTo,
                                              @NonNull @ReadOnly CommodityType commodityTypeToAdd) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public @NonNull Trader removeCommodityBought(@NonNull Basket basketToRemoveFrom,
                                        @NonNull @ReadOnly CommodityType commodityTypeToRemove) {
        // TODO Auto-generated method stub
        return null;
    }

}

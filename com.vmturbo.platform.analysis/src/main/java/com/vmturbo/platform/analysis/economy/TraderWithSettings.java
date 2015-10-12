package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;

final class TraderWithSettings implements Trader, TraderSettings {
    // Fields
    private boolean suspendable_ = false;
    private boolean cloneable_ = false;
    private double maxDesiredUtilization_ = 1.0;
    private double minDesiredUtilization_ = 0.0;

    private TraderState state_;
    private final TraderType type_; // this should never change once object is created.
    private CommoditySold[] commoditiesSold_;

    // Constructors
    public TraderWithSettings(TraderType type, TraderState state) {
        type_ = type;
        setState(state);
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
    public @NonNull List<@NonNull Basket> getBasketsBought() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public @NonNull List<@NonNull UnmodifiableCommoditySold> getCommoditiesSold() {
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
    public @NonNull Trader setState(TraderState state) {
        state_ = state;
        return this;
    }

}

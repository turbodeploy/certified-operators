package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.UnaryOperator;

import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

final class CommoditySoldWithSettings implements CommoditySold, CommoditySoldSettings {

    // Fields for CommoditySold
    private final @NonNull ArrayList<@NonNull Trader> buyers_ = new ArrayList<>();
    private double capacity_ = 0.0;
    private boolean thin_ = false;

    // Fields for CommoditySoldSettings
    private boolean resizable_ = true;
    private double capacityLowerBound_ = 0.0;
    private double capacityUpperBound_ = Double.MAX_VALUE;
    private double capacityIncrement_ = 1;
    private double utilizationUpperBound_ = 1.0;
    // TODO: change to named price function when implemented.
    private @NonNull UnaryOperator<@NonNull Double> priceFunction_ = x -> 1/((1-x)*(1-x));

    // Constructors

    /**
     * Constructs a default-initialized commodity sold. It has an empty buyers list and a default
     * price function.
     */
    public CommoditySoldWithSettings() {}

    // Methods

    @Override
    @Pure
    public @NonNull @ReadOnly List<@NonNull @ReadOnly Trader> getBuyers(@ReadOnly CommoditySoldWithSettings this) {
        return Collections.unmodifiableList(buyers_);
    }

    /**
     * Returns a modifiable list of traders buying {@code this} commodity.
     */
    @Pure
    @NonNull List<@NonNull @ReadOnly Trader> getModifiableBuyersList() {
        return buyers_;
    }

    @Override
    @Pure
    public @NonNull @PolyRead CommoditySoldSettings getSettings(@PolyRead CommoditySoldWithSettings this) {
        return this;
    }

    @Override
    @Pure
    public double getCapacity(@ReadOnly CommoditySoldWithSettings this) {
        return capacity_;
    }

    @Override
    @Pure
    public boolean isThin(@ReadOnly CommoditySoldWithSettings this) {
        return thin_;
    }

    @Override
    @Pure
    public boolean isResizable(@ReadOnly CommoditySoldWithSettings this) {
        return resizable_;
    }

    @Override
    @Pure
    public double getCapacityUpperBound(@ReadOnly CommoditySoldWithSettings this) {
        return capacityUpperBound_;
    }

    @Override
    @Pure
    public double getCapacityLowerBound(@ReadOnly CommoditySoldWithSettings this) {
        return capacityLowerBound_;
    }

    @Override
    @Pure
    public double getCapacityIncrement(@ReadOnly CommoditySoldWithSettings this) {
        return capacityIncrement_;
    }

    @Override
    @Pure
    public double getUtilizationUpperBound(@ReadOnly CommoditySoldWithSettings this) {
        return utilizationUpperBound_;
    }

    @Override
    public @NonNull @PolyRead UnaryOperator<@NonNull Double> getPriceFunction(@PolyRead CommoditySoldWithSettings this) {
        return priceFunction_;
    }

    @Override
    public @NonNull CommoditySoldSettings setResizable(boolean resizable) {
        resizable_ = resizable;
        return this;
    }

    @Override
    public @NonNull CommoditySoldSettings setCapacityUpperBound(double capacityUpperBound) {
        checkArgument(getCapacityLowerBound() <= capacityUpperBound);
        capacityUpperBound_ = capacityUpperBound;
        return this;
    }

    @Override
    public @NonNull CommoditySoldSettings setCapacityLowerBound(double capacityLowerBound) {
        checkArgument(0 <= capacityLowerBound);
        checkArgument(capacityLowerBound <= getCapacityUpperBound());
        capacityLowerBound_ = capacityLowerBound;
        return this;
    }

    @Override
    public @NonNull CommoditySoldSettings setCapacityIncrement(double capacityIncrement) {
        checkArgument(0 <= capacityIncrement);
        capacityIncrement_ = capacityIncrement;
        return this;
    }

    @Override
    public @NonNull CommoditySoldSettings setUtilizationUpperBound(double utilizationUpperBound) {
        checkArgument(0.0 <= utilizationUpperBound && utilizationUpperBound <= 1.0);
        utilizationUpperBound_ = utilizationUpperBound;
        return this;
    }

    @Override
    public @NonNull CommoditySold setCapacity(double capacity) {
        checkArgument(0 <= capacity);
        capacity_ = capacity;
        return this;
    }

    @Override
    public @NonNull CommoditySold setThin(boolean thin) {
        thin_ = thin;
        return this;
    }

    @Override
    public @NonNull CommoditySoldSettings setPriceFunction(@NonNull UnaryOperator<@NonNull Double> priceFunction) {
        priceFunction_ = priceFunction;
        return this;
    }

} // end class CommoditySoldWithSettings

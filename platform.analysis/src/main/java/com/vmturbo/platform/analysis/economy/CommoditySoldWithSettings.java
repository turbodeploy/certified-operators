package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.pricefunction.PriceFunction;

final class CommoditySoldWithSettings extends CommoditySold implements CommoditySoldSettings {
    // Fields for CommoditySoldSettings
    private boolean resizable_ = true;
    private double capacityLowerBound_ = 0.0;
    private double capacityUpperBound_ = Double.MAX_VALUE;
    private double capacityIncrement_ = 1;
    private double utilizationUpperBound_ = 1.0;
    private @NonNull PriceFunction priceFunction_ = PriceFunction.Cache.createStandardWeightedPriceFunction(1.0);

    // Constructors

    /**
     * Constructs a default-initialized commodity sold. It has an empty buyers list and a default
     * price function.
     */
    public CommoditySoldWithSettings() {}

    // Methods for CommoditySold

    @Override
    @Pure
    public @NonNull @PolyRead CommoditySoldSettings getSettings(@PolyRead CommoditySoldWithSettings this) {
        return this;
    }

    @Override
    @Pure
    public double getEffectiveCapacity(@ReadOnly CommoditySoldWithSettings this) {
        return getUtilizationUpperBound()*getCapacity();
    }

    // Methods for CommoditySoldSettings

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
    @Pure
    public @NonNull @PolyRead PriceFunction getPriceFunction(@PolyRead CommoditySoldWithSettings this) {
        return priceFunction_;
    }

    @Override
    @Deterministic
    public @NonNull CommoditySoldSettings setResizable(boolean resizable) {
        resizable_ = resizable;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull CommoditySoldSettings setCapacityUpperBound(double capacityUpperBound) {
        checkArgument(getCapacityLowerBound() <= capacityUpperBound, "capacityUpperBound = " + capacityUpperBound);
        capacityUpperBound_ = capacityUpperBound;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull CommoditySoldSettings setCapacityLowerBound(double capacityLowerBound) {
        checkArgument(0 <= capacityLowerBound, "capacityLowerBound = " + capacityLowerBound);
        checkArgument(capacityLowerBound <= getCapacityUpperBound());
        capacityLowerBound_ = capacityLowerBound;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull CommoditySoldSettings setCapacityIncrement(double capacityIncrement) {
        checkArgument(0 <= capacityIncrement, "capacityIncrement = " + capacityIncrement);
        capacityIncrement_ = capacityIncrement;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull CommoditySoldSettings setUtilizationUpperBound(double utilizationUpperBound) {
        checkArgument(0.0 < utilizationUpperBound && utilizationUpperBound <= 1.0,
                      "utilizationUpperBound = " + utilizationUpperBound);
        utilizationUpperBound_ = utilizationUpperBound;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull CommoditySoldSettings setPriceFunction(@NonNull PriceFunction priceFunction) {
        priceFunction_ = priceFunction;
        return this;
    }

} // end class CommoditySoldWithSettings

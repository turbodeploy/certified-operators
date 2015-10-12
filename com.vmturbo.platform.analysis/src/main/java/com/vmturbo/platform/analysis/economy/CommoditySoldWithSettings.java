package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

final class CommoditySoldWithSettings implements CommoditySold, CommoditySoldSettings {

    // Fields for CommoditySold
    private double capacity_ = 0.0;
    private boolean thin_ = false;
    private CommodityType type_;

    // Fields for CommoditySoldSettings
    private boolean resizable_ = true;
    private double capacityLowerBound_ = 0.0;
    private double capacityUpperBound_ = Double.MAX_VALUE;
    private double capacityIncrement_ = 1;
    private double utilizationUpperBound_ = 1.0;

    // Constructors
    public CommoditySoldWithSettings() {
        // TODO Auto-generated constructor stub
    }

    // Methods

    @Override
    @Pure
    public @NonNull CommoditySoldSettings getSettings() {
        return this;
    }

    @Override
    @Pure
    public @NonNull CommodityType getType() {
        return type_;
    }

    @Override
    @Pure
    public long getKey() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    @Pure
    public double getCapacity() {
        return capacity_;
    }

    @Override
    @Pure
    public boolean isThin() {
        return thin_;
    }

    @Override
    @Pure
    public boolean isResizable() {
        return resizable_;
    }

    @Override
    @Pure
    public double getCapacityUpperBound() {
        return capacityUpperBound_;
    }

    @Override
    @Pure
    public double getCapacityLowerBound() {
        return capacityLowerBound_;
    }

    @Override
    @Pure
    public double getCapacityIncrement() {
        return capacityIncrement_;
    }

    @Override
    @Pure
    public double getUtilizationUpperBound() {
        return utilizationUpperBound_;
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
    public @NonNull @ReadOnly List<@NonNull @ReadOnly Trader> getBuyers() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public @NonNull @ReadOnly Trader getSeller() {
        // TODO Auto-generated method stub
        return null;
    }

} // end class CommoditySoldWithSettings

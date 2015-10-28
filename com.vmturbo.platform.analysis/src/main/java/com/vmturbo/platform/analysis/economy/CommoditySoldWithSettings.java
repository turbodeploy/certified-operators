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

import com.vmturbo.platform.analysis.pricefunction.PFUtility;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;

final class CommoditySoldWithSettings implements CommoditySold, CommoditySoldSettings {

    // Fields for CommoditySold
    private final @NonNull ArrayList<@NonNull BuyerParticipation> buyers_ = new ArrayList<>();
    private double quantity_ = 0.0;
    private double peakQuantity_ = 0.0;
    private double capacity_ = Double.MAX_VALUE;
    private boolean thin_ = false;

    // Fields for CommoditySoldSettings
    private boolean resizable_ = true;
    private double capacityLowerBound_ = 0.0;
    private double capacityUpperBound_ = Double.MAX_VALUE;
    private double capacityIncrement_ = 1;
    private double utilizationUpperBound_ = 1.0;
    private @NonNull PriceFunction priceFunction_ = PFUtility.createStandardWeightedPriceFunction(1.0);

    // Constructors

    /**
     * Constructs a default-initialized commodity sold. It has an empty buyers list and a default
     * price function.
     */
    public CommoditySoldWithSettings() {}

    // Methods for CommoditySold

    @Override
    @Pure
    public @NonNull @ReadOnly List<@NonNull @ReadOnly BuyerParticipation> getBuyers(@ReadOnly CommoditySoldWithSettings this) {
        return Collections.unmodifiableList(buyers_);
    }

    /**
     * Returns a modifiable list of traders buying {@code this} commodity.
     */
    @Pure
    @NonNull List<@NonNull @ReadOnly BuyerParticipation> getModifiableBuyersList() {
        return buyers_;
    }

    @Override
    @Pure
    public @NonNull @PolyRead CommoditySoldSettings getSettings(@PolyRead CommoditySoldWithSettings this) {
        return this;
    }

    @Override
    @Pure
    public double getUtilization(@ReadOnly CommoditySoldWithSettings this) {
        return getQuantity()/getCapacity();
    }

    @Override
    @Pure
    public double getPeakUtilization(@ReadOnly CommoditySoldWithSettings this) {
        return getPeakQuantity()/getCapacity();
    }

    @Override
    @Pure
    public double getQuantity(@ReadOnly CommoditySoldWithSettings this) {
        return quantity_;
    }

    @Override
    @Pure
    public double getPeakQuantity(@ReadOnly CommoditySoldWithSettings this) {
        return peakQuantity_;
    }

    @Override
    @Pure
    public double getCapacity(@ReadOnly CommoditySoldWithSettings this) {
        return capacity_;
    }

    @Override
    @Pure
    public double getEffectiveCapacity(@ReadOnly CommoditySoldWithSettings this) {
        return getUtilizationUpperBound()*getCapacity();
    }

    @Override
    @Pure
    public boolean isThin(@ReadOnly CommoditySoldWithSettings this) {
        return thin_;
    }

    @Override
    @Deterministic
    public @NonNull CommoditySold setQuantity(double quantity) {
        checkArgument(0 <= quantity && quantity <= getCapacity());
        quantity_ = quantity;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull CommoditySold setPeakQuantity(double peakQuantity) {
        checkArgument(0 <= peakQuantity && peakQuantity <= getCapacity());
        peakQuantity_ = peakQuantity;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull CommoditySold setCapacity(double capacity) {
        checkArgument(0 <= capacity); // should we check that this is >= max(quantity,peakQuantity)?
        capacity_ = capacity;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull CommoditySold setThin(boolean thin) {
        thin_ = thin;
        return this;
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
        checkArgument(getCapacityLowerBound() <= capacityUpperBound);
        capacityUpperBound_ = capacityUpperBound;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull CommoditySoldSettings setCapacityLowerBound(double capacityLowerBound) {
        checkArgument(0 <= capacityLowerBound);
        checkArgument(capacityLowerBound <= getCapacityUpperBound());
        capacityLowerBound_ = capacityLowerBound;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull CommoditySoldSettings setCapacityIncrement(double capacityIncrement) {
        checkArgument(0 <= capacityIncrement);
        capacityIncrement_ = capacityIncrement;
        return this;
    }

    @Override
    @Deterministic
    public @NonNull CommoditySoldSettings setUtilizationUpperBound(double utilizationUpperBound) {
        checkArgument(0.0 <= utilizationUpperBound && utilizationUpperBound <= 1.0);
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

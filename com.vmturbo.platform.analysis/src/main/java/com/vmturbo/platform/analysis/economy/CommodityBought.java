package com.vmturbo.platform.analysis.economy;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

public final class CommodityBought {
    // Fields
    private @NonNull Market market_;
    private @NonNull Trader buyer_;
    private @NonNull CommoditySpecification type_;

    // Constructors
    CommodityBought(@NonNull Market market, @NonNull Trader buyer, @NonNull CommoditySpecification type) {
        market_ = market;
        buyer_ = buyer;
        type_ = type;
    }

    // Methods
    @Pure
    public @NonNull @ReadOnly CommoditySpecification getType(@ReadOnly CommodityBought this) {
        return type_;
    }

    @Pure
    public double getQuantity(@ReadOnly CommodityBought this) {
        // TODO: get the Quantity from market.
        return 0.0;
    }

    @Pure
    public double getPeakQuantity(@ReadOnly CommodityBought this) {
        // TODO: get the Quantity from market.
        return 0.0;
    }

    @Deterministic
    public CommodityBought setQuantity(double newQuantity) {
        // TODO: set the Quantity in market.
        return this;
    }

    @Deterministic
    public CommodityBought setPeakQuantity(double newPeakQuantity) {
        // TODO: set the Quantity in market.
        return this;
    }


}

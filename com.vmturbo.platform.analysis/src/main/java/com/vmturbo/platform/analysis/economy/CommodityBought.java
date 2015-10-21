
package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

public final class CommodityBought {
    // Fields
    private @NonNull Market market_;
    private @NonNull BuyerParticipation buyer_;
    private @NonNull CommoditySpecification type_;

    // Constructors
    CommodityBought(@NonNull Market market, @NonNull BuyerParticipation buyer, @NonNull CommoditySpecification type) {
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
        return buyer_.getQuantity(market_.getBasket().indexOf(type_));
    }

    @Pure
    public double getPeakQuantity(@ReadOnly CommodityBought this) {
        return buyer_.getPeakQuantity(market_.getBasket().indexOf(type_));
    }

    @Deterministic
    public CommodityBought setQuantity(double newQuantity) {
        if(buyer_.getSupplierIndex() != BuyerParticipation.NO_SUPPLIER)
        {
            final Trader supplier = market_.getEconomy().getTraders().get(buyer_.getSupplierIndex());
            // TODO: should this be capacity or utilizationUpperBound*capacity?
            checkArgument(newQuantity <= supplier.getCommoditiesSold().get(market_.getBasket().indexOf(type_)).getCapacity());
        }
        buyer_.setQuantity(market_.getBasket().indexOf(type_),newQuantity);
        return this;
    }

    @Deterministic
    public CommodityBought setPeakQuantity(double newPeakQuantity) {
        if(buyer_.getSupplierIndex() != BuyerParticipation.NO_SUPPLIER)
        {
            final Trader supplier = market_.getEconomy().getTraders().get(buyer_.getSupplierIndex());
            // TODO: should this be capacity or utilizationUpperBound*capacity?
            checkArgument(newPeakQuantity <= supplier.getCommoditiesSold().get(market_.getBasket().indexOf(type_)).getCapacity());
        }
        buyer_.setPeakQuantity(market_.getBasket().indexOf(type_),newPeakQuantity);
        return this;
    }


} // end CommodityBought class

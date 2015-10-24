
package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

/**
 * Represents a commodity bought by a specific buyer participation in a specific market.
 *
 * <p>
 *  E.g. a buyer buying the same basket two times from two different sellers will have two
 *  participations in the corresponding market and two CommodityBought instances of a given type,
 *  one for each seller.
 * </p>
 */
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
            checkArgument(newQuantity <= supplier.getCommoditySold(type_).getCapacity());
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
            checkArgument(newPeakQuantity <= supplier.getCommoditySold(type_).getCapacity());
        }
        buyer_.setPeakQuantity(market_.getBasket().indexOf(type_),newPeakQuantity);
        return this;
    }


} // end CommodityBought class

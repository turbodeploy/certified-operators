
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
    private final @NonNull BuyerParticipation participation_;
    private final int commodityIndex_;

    // Constructors

    /**
     * Constructs a new modifiable CommodityBought view of the (quantity, peak quantity) pair
     * corresponding to the given buyer participation, and index into the quantity and peak quantity
     * vectors.
     *
     * <p>
     *  CommodityBought objects themselves are immutable, but they present a modifiable view of the
     *  (quantity, peak quantity) pairs they refer to. They are never invalidated, but using them
     *  after the supplied buyer participation has been removed from the respective market, makes
     *  little sense.
     * </p>
     *
     * @param participation The buyer participation for which the view will be created.
     * @param commodityIndex The index of the pair for which the view should be created.
     *                       Must be non-negative and less than the common length of the quantity
     *                       and peak vectors.
     */
    // TODO: are they invalidated in other cases? what about addCommodityBought?
    CommodityBought(@NonNull BuyerParticipation participation, int commodityIndex) {
        checkArgument(0 <= commodityIndex && commodityIndex <= participation.getQuantities().length);

        participation_ = participation;
        commodityIndex_ = commodityIndex;
    }

    @Pure
    public double getQuantity(@ReadOnly CommodityBought this) {
        return participation_.getQuantity(commodityIndex_);
    }

    @Pure
    public double getPeakQuantity(@ReadOnly CommodityBought this) {
        return participation_.getPeakQuantity(commodityIndex_);
    }

    @Deterministic
    public CommodityBought setQuantity(double newQuantity) {
        checkArgument(0 <= newQuantity);
        // TODO: should we check anything else about newQuantity like comparing it with capacity?
        participation_.setQuantity(commodityIndex_,newQuantity);
        return this;
    }

    @Deterministic
    public CommodityBought setPeakQuantity(double newPeakQuantity) {
        checkArgument(0 <= newPeakQuantity);
        // TODO: should we check anything else about newPeakQuantity like comparing it with capacity?
        participation_.setPeakQuantity(commodityIndex_,newPeakQuantity);
        return this;
    }

} // end CommodityBought class

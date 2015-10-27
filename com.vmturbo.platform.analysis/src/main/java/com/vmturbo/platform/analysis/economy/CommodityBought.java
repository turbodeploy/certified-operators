
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
    private final @NonNull BuyerParticipation participation_; // the buyer participation backing the view.
    private final int commodityIndex_; // the index into the quantity and peak quantity vectors.

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
        checkArgument(0 <= commodityIndex && commodityIndex < participation.getQuantities().length);

        participation_ = participation;
        commodityIndex_ = commodityIndex;
    }

    /**
     * Returns the <b>quantity</b> of {@code this} commodity bought.
     *
     * <p>
     *  This is the quantity one buyer participation is buying or intends to buy from a given type.
     * </p>
     */
    @Pure
    public double getQuantity(@ReadOnly CommodityBought this) {
        return participation_.getQuantity(commodityIndex_);
    }

    /**
     * Returns the <b>peak quantity</b> of {@code this} commodity bought.
     *
     * <p>
     *  The peak quantity is the running max of quantity in some rolling time window, but the width
     *  of this window is not known or customizable form inside the Economy and it is assumed that
     *  the Mediation can get more accurate values for peak quantity from the Hypervisor than could
     *  be calculated using the quantity samples alone.
     * </p>
     */
    @Pure
    public double getPeakQuantity(@ReadOnly CommodityBought this) {
        return participation_.getPeakQuantity(commodityIndex_);
    }

    /**
     * Sets the value of the <b>quantity</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param newQuantity the new value for the field. Must be non-negative.
     * @return {@code this}
     *
     * @see #getQuantity()
     */
    @Deterministic
    public CommodityBought setQuantity(double newQuantity) {
        checkArgument(0 <= newQuantity);
        // TODO: should we check anything else about newQuantity like comparing it with capacity?
        participation_.setQuantity(commodityIndex_,newQuantity);
        return this;
    }

    /**
     * Sets the value of the <b>peak quantity</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param newPeakQuantity the new value for the field. Must be non-negative.
     * @return {@code this}
     *
     * @see #getQuantity()
     */
    @Deterministic
    public CommodityBought setPeakQuantity(double newPeakQuantity) {
        checkArgument(0 <= newPeakQuantity);
        // TODO: should we check anything else about newPeakQuantity like comparing it with capacity?
        participation_.setPeakQuantity(commodityIndex_,newPeakQuantity);
        return this;
    }

} // end CommodityBought class

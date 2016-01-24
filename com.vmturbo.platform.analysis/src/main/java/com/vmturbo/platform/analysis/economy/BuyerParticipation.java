package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;
import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

/**
 * One of the participations of a given trader in a given market.
 *
 * <p>
 *  A given trader can participate multiple times as a buyer in a single market. (e.g. if it buys
 *  form multiple storages) We call each of those participations a BuyerParticipation.
 * </p>
 *
 * <p>
 *  If a buyer stops participating in the market, the corresponding buyer participation is
 *  invalidated.
 * </p>
 */
public final class BuyerParticipation {
    // Fields
    private final @NonNull Trader buyer_; // @see #getBuyer().
    private @Nullable Trader supplier_; // @see #setSupplier(Trader).
                                       // New traders may temporarily buy from no one.
    private final double @NonNull [] quantities_; // @see #getQuantities()
    private final double @NonNull [] peakQuantities_; // @see #getPeakQuantities().
                                                     // Must be same size as quantities_.

    // Constructors

    /**
     * Constructs a new BuyerParticipation instance with the specified properties.
     *
     * @param buyer see {@link #getBuyer()}
     * @param numberOfCommodities The number of commodities bought that should be associated with
     *         the new BuyerParticipation instance. It should be equal to the basket size of the
     *         market this participation belongs to.
     */
    BuyerParticipation(@NonNull Trader buyer, int numberOfCommodities) {
        buyer_ = buyer;
        supplier_ = null;
        quantities_ = new double[numberOfCommodities];
        peakQuantities_ = new double[numberOfCommodities];
    }

    // Methods

    /**
     * Returns the {@link Trader buyer} {@code this} buyer participation belongs to.
     */
    @Pure
    public @NonNull Trader getBuyer(@ReadOnly BuyerParticipation this) {
        return buyer_;
    }

    /**
     * Returns the (current) {@link Trader supplier} of {@code this} buyer participation, or
     * {@code null} if the buyer participation is not currently buying from anyone.
     *
     * @see #setSupplier(Trader)
     */
    @Pure
    public @Nullable Trader getSupplier(@ReadOnly BuyerParticipation this) {
        return supplier_;
    }

    /**
     * Returns the quantity vector of the commodities bought by {@code this} participation of the
     * buyer.
     *
     * <p>
     *  This array contains one quantity entry for each commodity specification in the basket of the
     *  market {@code this} buyer participation belongs to, in the same order.
     * </p>
     *
     * @see #setQuantity(int, double)
     */
    @Pure
    public @PolyRead double @NonNull [] getQuantities(@PolyRead BuyerParticipation this) {
        return quantities_;
    }

    /**
     * Returns the peak quantity vector of the commodities bought by {@code this} participation of
     * the buyer.
     *
     * <p>
     *  This array contains one peak quantity entry for each commodity specification in the basket
     *  of the market {@code this} buyer participation belongs to, in the same order.
     * </p>
     *
     * @see #setPeakQuantity(int, double)
     */
    @Pure
    public @PolyRead double @NonNull [] getPeakQuantities(@PolyRead BuyerParticipation this) {
        return peakQuantities_;
    }

    /**
     * Returns the quantity at the specified index of the quantity vector.
     *
     * @see #getQuantities()
     */
    @Pure
    public double getQuantity(@ReadOnly BuyerParticipation this, int index) {
        return quantities_[index];
    }

    /**
     * Returns the peak quantity at the specified index of the peak quantity vector.
     *
     * @see #getPeakQuantities()
     */
    @Pure
    public double getPeakQuantity(@ReadOnly BuyerParticipation this, int index) {
        return peakQuantities_[index];
    }

    /**
     * Sets the value of the <b>supplier</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param newSupplier the new value for the field.
     * @return {@code this}
     *
     * @see #getSupplier()
     */
    @Deterministic
    @NonNull BuyerParticipation setSupplier(@Nullable Trader newSupplier) {
        supplier_ = newSupplier;
        return this;
    }

    /**
     * Sets the value of the <b>quantity</b> at the specified index.
     *
     * <p>
     *  Has no observable side-effects except updating the above index.
     * </p>
     *
     * @param index The index in the quantities array that should be updated.
     * @param newQuantity The new value for that position of the array. Should be non-negative.
     * @return {@code this}
     *
     * @see #getQuantity(int)
     */
    @Deterministic
    public @NonNull BuyerParticipation setQuantity(int index, double newQuantity) {
        checkArgument(newQuantity >= 0);
        quantities_[index] = newQuantity;
        return this;
    }

    /**
     * Sets the value of the <b>peak quantity</b> at the specified index.
     *
     * <p>
     *  Has no observable side-effects except updating the above index.
     * </p>
     *
     * @param index The index in the peak quantities array that should be updated.
     * @param newPeakQuantity The new value for that position of the array. Should be non-negative.
     * @return {@code this}
     *
     * @see #getPeakQuantity(int)
     */
    @Deterministic
    public @NonNull BuyerParticipation setPeakQuantity(int index, double newPeakQuantity) {
        checkArgument(newPeakQuantity >= 0);
        peakQuantities_[index] = newPeakQuantity;
        return this;
    }

    /**
     * Moves {@code this} buyer participation of a buyer to a new supplier, causing the customer
     * lists of former and future supplier to be updated.
     *
     * <p>
     *  It can be used to first position a buyer participation buying from no-one (like the one of a
     *  newly created trader) to its first supplier, or to make a buyer participation seize buying
     *  from anyone.
     * </p>
     *
     * @param newSupplier The new supplier of {@code this}.
     * @return {@code this}
     */
    @Deterministic
    public @NonNull BuyerParticipation move(Trader newSupplier) {
        // Update old supplier to exclude this from its customers.
        if (getSupplier() != null) {
            checkArgument(((TraderWithSettings)getSupplier()).getCustomers().remove(this));
        }

        // Update new supplier to include this to its customers.
        if (newSupplier != null) {
            ((TraderWithSettings)newSupplier).getCustomers().add(this);
        }
        setSupplier(newSupplier);

        return this;
    }

} // end BuyerParticipation class

package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
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
    static final int NO_SUPPLIER = -1; // new traders may temporarily buy from no one.

    private int buyerIndex_; // @see #setBuyerIndex(int). It may need to be updated when traders are
        // removed from the economy.
    private int supplierIndex_; // @see #setSupplierIndex(int). It may need to be updated when
        // traders are removed from the economy.
    private final double @NonNull [] quantities_; // @see #getQuantities()
    private final double @NonNull [] peakQuantities_; // @see #getPeakQuantities().
                                                     // Must be same size as quantities_.

    // Constructors

    /**
     * Constructs a new BuyerParticipation instance with the specified properties.
     *
     * @param buyerIndex see {@link #setBuyerIndex(int)}
     * @param supplierIndex see {@link #setSupplierIndex(int)}
     * @param numberOfCommodities The number of commodities bought that should be associated with
     *         the new BuyerParticipation instance. It should be equal to the basket size of the
     *         market this participation belongs to.
     */
    BuyerParticipation(int buyerIndex, int supplierIndex, int numberOfCommodities) {
        setBuyerIndex(buyerIndex);
        setSupplierIndex(supplierIndex);
        quantities_ = new double[numberOfCommodities];
        peakQuantities_ = new double[numberOfCommodities];
    }

    // Methods

    /**
     * Returns the economy index of the buyer {@code this} participation belongs to.
     *
     * <p>
     *  Each trader in the economy is associated with a unique number that is equal to the index
     *  the trader has in the traders list of the economy. This is the economy index of the trader.
     * </p>
     *
     * <p>
     *  Moreover if the trader participates in one or more markets, each of its participations will
     *  be associated with a market index.
     * </p>
     *
     * @see #setBuyerIndex(int)
     */
    @Pure
    int getBuyerIndex(@ReadOnly BuyerParticipation this) {
        return buyerIndex_;
    }

    /**
     * Returns the economy index of the buyer {@code this} participation belongs to.
     *
     * <p>
     *  Each trader in the economy is associated with a unique number that is equal to the index
     *  the trader has in the traders list of the economy. This is the economy index of the trader.
     * </p>
     *
     * <p>
     *  Moreover if the trader participates in one or more markets, each of its participations will
     *  be associated with a market index.
     * </p>
     *
     * @see #setBuyerIndex(int)
     */
    @Pure
    int getSupplierIndex(@ReadOnly BuyerParticipation this) {
        return supplierIndex_;
    }

    /**
     * Returns the quantity vector of the commodities bought by {@code this} participation of the
     * buyer.
     *
     * <p>
     *  This array contains one quantity entry for each commodity specification in the basket of the
     *  market {@code this} buyer participation belongs to, in the same order.
     * </p>
     */
    @Pure
    public @ReadOnly double @NonNull [] getQuantities(@ReadOnly BuyerParticipation this) {
        return Arrays.copyOf(quantities_, quantities_.length); // if @ReadOnly was checked statically
            // I wouldn't need to return a copy.
    }

    /**
     * Returns the peak quantity vector of the commodities bought by {@code this} participation of
     * the buyer.
     *
     * <p>
     *  This array contains one peak quantity entry for each commodity specification in the basket
     *  of the market {@code this} buyer participation belongs to, in the same order.
     * </p>
     */
    @Pure
    public @ReadOnly double @NonNull [] getPeakQuantities(@ReadOnly BuyerParticipation this) {
        return Arrays.copyOf(peakQuantities_, peakQuantities_.length); // if @ReadOnly was checked
            // statically I wouldn't need to return a copy.
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
     * Sets the value of the <b>buyer index</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param newBuyerIndex the new value for the field. Must be non-negative.
     * @return {@code this}
     *
     * @see #getBuyerIndex()
     */
    @Deterministic
    @NonNull BuyerParticipation setBuyerIndex(int newBuyerIndex) {
        checkArgument(newBuyerIndex >= 0);
        buyerIndex_ = newBuyerIndex;
        return this;
    }

    /**
     * Sets the value of the <b>supplier index</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param newSupplierIndex the new value for the field. Must be non-negative or NO_SUPPLIER.
     * @return {@code this}
     *
     * @see #getSupplierIndex()
     */
    @Deterministic
    @NonNull BuyerParticipation setSupplierIndex(int newSupplierIndex) {
        checkArgument(newSupplierIndex >= 0  || newSupplierIndex == NO_SUPPLIER);
        supplierIndex_ = newSupplierIndex;
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
    @NonNull BuyerParticipation setQuantity(int index, double newQuantity) {
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
    @NonNull BuyerParticipation setPeakQuantity(int index, double newPeakQuantity) {
        checkArgument(newPeakQuantity >= 0);
        peakQuantities_[index] = newPeakQuantity;
        return this;
    }

} // end BuyerParticipation class

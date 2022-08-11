package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;
import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

/**
 * One of the shopping lists of a given trader in a given market.
 *
 * <p>
 *  A given trader can participate multiple times as a buyer in a single market. (e.g. if it buys
 *  form multiple storages) We call each of those participations a ShoppingList.
 * </p>
 *
 * <p>
 *  If a buyer stops participating in the market, the corresponding shopping list is
 *  invalidated.
 * </p>
 */
public final class ShoppingList {
    // Fields
    private final @NonNull Trader buyer_; // @see #getBuyer().
    private @Nullable Trader supplier_; // @see #setSupplier(Trader).
                                       // New traders may temporarily buy from no one.
    private final double @NonNull [] quantities_; // @see #getQuantities()
    private final double @NonNull [] peakQuantities_; // @see #getPeakQuantities().
                                                     // Must be same size as quantities_.
    private boolean movable_ = false; // Whether analysis is allowed to move this shopping list to
                                     // another supplier.
    private final @NonNull Basket basket_; // The basket for this shopping list

    // Constructors

    /**
     * Constructs a new ShoppingList instance with the specified properties.
     *
     * @param buyer see {@link #getBuyer()}
     * @param basket The basket this shopping list corresponds to. It should
     * be equal to the one belonging to the Market this shopping list participates in.
     */
    ShoppingList(@NonNull Trader buyer, @NonNull Basket basket) {
        buyer_ = buyer;
        supplier_ = null;
        basket_ = basket;
        quantities_ = new double[basket.size()];
        peakQuantities_ = new double[basket.size()];
    }

    // Methods

    /**
     * Returns the {@link Trader buyer} {@code this} shopping list belongs to.
     */
    @Pure
    public @NonNull Trader getBuyer(@ReadOnly ShoppingList this) {
        return buyer_;
    }

    /**
     * Returns the {@link Basket basket} that belongs to {@code this} shopping list.
     */
    @Pure
    public @NonNull Basket getBasket(@ReadOnly ShoppingList this) {
        return basket_;
    }

    /**
     * Returns the (current) {@link Trader supplier} of {@code this} shopping list, or
     * {@code null} if the shopping list is not currently buying from anyone.
     *
     * @see #setSupplier(Trader)
     */
    @Pure
    public @Nullable Trader getSupplier(@ReadOnly ShoppingList this) {
        return supplier_;
    }

    /**
     * Returns the quantity vector of the commodities bought by {@code this} shopping list of the
     * buyer.
     *
     * <p>
     *  This array contains one quantity entry for each commodity specification in the basket of the
     *  market {@code this} shopping list belongs to, in the same order.
     * </p>
     *
     * @see #setQuantity(int, double)
     */
    @Pure
    public @PolyRead double @NonNull [] getQuantities(@PolyRead ShoppingList this) {
        return quantities_;
    }

    /**
     * Returns the peak quantity vector of the commodities bought by {@code this} shopping list of
     * the buyer.
     *
     * <p>
     *  This array contains one peak quantity entry for each commodity specification in the basket
     *  of the market {@code this} shopping list belongs to, in the same order.
     * </p>
     *
     * @see #setPeakQuantity(int, double)
     */
    @Pure
    public @PolyRead double @NonNull [] getPeakQuantities(@PolyRead ShoppingList this) {
        return peakQuantities_;
    }

    /**
     * Returns the quantity at the specified index of the quantity vector.
     *
     * @see #getQuantities()
     */
    @Pure
    public double getQuantity(@ReadOnly ShoppingList this, int index) {
        return quantities_[index];
    }

    /**
     * Returns the peak quantity at the specified index of the peak quantity vector.
     *
     * @see #getPeakQuantities()
     */
    @Pure
    public double getPeakQuantity(@ReadOnly ShoppingList this, int index) {
        return peakQuantities_[index];
    }

    /**
     * Whether {@code this} {@link ShoppingList} should be considered for moving.
     *
     * <p>
     *  The placement algorithm should ignore immovable shopping lists.
     * </p>
     */
    @Pure
    public boolean isMovable(ShoppingList this) {
        return movable_;
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
    @NonNull ShoppingList setSupplier(@Nullable Trader newSupplier) {
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
    public @NonNull ShoppingList setQuantity(int index, double newQuantity) {
        checkArgument(newQuantity >= 0, "newQuantity = " + newQuantity);
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
    public @NonNull ShoppingList setPeakQuantity(int index, double newPeakQuantity) {
        checkArgument(newPeakQuantity >= 0, "newPeakQuantity = " + newPeakQuantity);
        peakQuantities_[index] = Math.max(quantities_[index], newPeakQuantity);
        return this;
    }

    /**
     * Sets the value of the <b>movable</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param movable the new value for the field.
     * @return {@code this}
     */
    @Deterministic
    public @NonNull ShoppingList setMovable(boolean movable) {
        movable_ = movable;
        return this;
    }

    /**
     * Moves {@code this} shopping list of a buyer to a new supplier, causing the customer
     * lists of former and future supplier to be updated.
     *
     * <p>
     *  It can be used to first position a shopping list buying from no-one (like the one of a
     *  newly created trader) to its first supplier, or to make a shopping list seize buying
     *  from anyone.
     * </p>
     *
     * @param newSupplier The new supplier of {@code this}.
     * @return {@code this}
     */
    @Deterministic
    public @NonNull ShoppingList move(Trader newSupplier) {
        // Update old supplier to exclude this from its customers.
        if (getSupplier() != null) {
            checkArgument(getSupplier().getModifiableCustomers().remove(this), "this = " + this);
        }

        // Update new supplier to include this to its customers.
        if (newSupplier != null) {
            newSupplier.getModifiableCustomers().add(this);
        }
        setSupplier(newSupplier);

        return this;
    }

} // end ShoppingList class

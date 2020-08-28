package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.commons.lang3.ArrayUtils;
import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.Context;

/**
 * One of the shopping lists of a given trader in a given market.
 *
 * <p>A given trader can participate multiple times as a buyer in a single market. (e.g. if it buys
 *  form multiple storages) We call each of those participations a ShoppingList.
 * </p>
 *
 * <p>If a buyer stops participating in the market, the corresponding shopping list is
 *  invalidated.
 * </p>
 */
public class ShoppingList implements Serializable {
    private static final long serialVersionUID = -3998819999266896549L;
    // Fields
    private final @NonNull Trader buyer_; // @see #getBuyer().
    private @Nullable Trader supplier_; // @see #setSupplier(Trader).
                                       // New traders may temporarily buy from no one.
    private final double @NonNull [] quantities_; // @see #getQuantities()
    private final double @NonNull [] peakQuantities_; // @see #getPeakQuantities().
                                                     // Must be same size as quantities_.
    private boolean movable_ = false; // Whether analysis is allowed to move this shopping list to
                                     // another supplier.
    private float moveCost_ = 0; // Cost to move this shopping list to another supplier
    private final @NonNull Basket basket_; // The basket for this shopping list
    private UUID shoppingListId = UUID.randomUUID(); // unique identifier for this shopping list.
    /**
     * The cost on the supplier.
     */
    private Double cost_ = null;
    /*
     * The group factor is used for consistent resizing groups.  It defaults to 1 for SLs that are
     * not in a group that needs to consistently resize.  Otherwise it is the number of elements in
     * the group if the SL is the designated group leader, and zero if the SL is in a group but is
     * not the group leader.
     */
    private long groupFactor_ = 1;

    // The list of commodities' base_types that have ability to not increase quote for the shopping
    // based on some criteria.
    private List<Integer> unquotedCommoditiesBaseTypeList_ = new ArrayList<Integer>();

    // The list of commodities' base_types that are not going to increase quote for the
    // shopping list.
    private List<Integer> modifiableUnquotedCommoditiesBaseTypeList_ = new ArrayList<Integer>();

    private Context moveContext;

    // Assigned capacities related to commodities sold by buyer.
    // Added for shoppingList representing cloud volume, preserving commodity sold capacities for volume,
    // which is used to compare with desired demand and generate action for commodity resize case.
    private Map<Integer, Double> assignedCapacities = new HashMap<>();

    // Whether demand is scalable within M2. Added for cloud volume Savings/Reversibility mode -
    // In Reversibility mode, demand_scalable is false, which indicates that volume size cannot
    // be increased by default.
    // In Savings mode, demand_scalable is true.
    private boolean demandScalable;

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
     * Returns the moveCost for this {@link ShoppingList}.
     *
     * @return the cost to move this shopping list
     */
    @Pure
    public float getMoveCost(@ReadOnly ShoppingList this) {
        return moveCost_;
    }

    /**
     * Returns debug information about this {@link ShoppingList}.
     * Use only for debugging and logging purposes, never for business logic.
     */
    @Pure
    public String getDebugInfoNeverUseInCode(@ReadOnly ShoppingList this) {
        return "SL_" + buyer_.getDebugInfoNeverUseInCode() + "|" + shoppingListId;
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
     * Sets the value of the <b>moveCost</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param moveCost the new value for the field.
     * @return {@code this}
     */
    @Deterministic
    public @NonNull ShoppingList setMoveCost(float moveCost) {
        checkArgument(moveCost >= 0f, "value less than 0, moveCost = " + moveCost);
        moveCost_ = moveCost;
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
            checkArgument(getSupplier().getModifiableCustomers().remove(this), "this = %s", this);
        }

        // Update new supplier to include this to its customers.
        if (newSupplier != null) {
            newSupplier.getModifiableCustomers().add(this);
        }
        setSupplier(newSupplier);

        return this;
    }

    /**
     * Get the cost on the supplier of {@code this}.
     *
     * @return the cost on the supplier
     */
    public Optional<Double> getCost() {
        return Optional.ofNullable(cost_);
    }

    /**
     * Set the cost on the supplier of {@code this}.
     *
     * @param cost_ cost of the supplier
     */
    public void setCost(double cost_) {
        this.cost_ = cost_;
    }

    /**
    * Get list of unquoted commodities base types for this shopping list.
    * @return unquotedCommoditiesBaseTypeList_
    */
    public List<Integer> getUnquotedCommoditiesBaseTypeList() {
        return unquotedCommoditiesBaseTypeList_;
    }

    /**
    * Add an unquoted commodity base type to the list.
    */
    public void addUnquotedCommodityBaseType(Integer i) {
        unquotedCommoditiesBaseTypeList_.add(i);
    }

    /**
    * Get modifiable list of unquoted commodities base types for this shopping list.
    * @return modifiableUnquotedCommoditiesBaseTypeList_
    */
    public List<Integer> getModifiableUnquotedCommoditiesBaseTypeList() {
        return modifiableUnquotedCommoditiesBaseTypeList_;
    }

    /**
    * Add an unquoted commodity base type to the modifiable list.
    */
    public void addModifiableUnquotedCommodityBaseType(Integer i) {
        modifiableUnquotedCommoditiesBaseTypeList_.add(i);
    }

    /**
     * Get unique identifier for this shopping list.
     * @return shopping list's UUID.
     */
    public UUID getShoppingListId() {
        return shoppingListId;
    }

    /**
     * Set the group factor of {@code this}.
     *
     * @param groupFactor factor (multiplier)
     */
    public void setGroupFactor(long groupFactor) {
        this.groupFactor_ = groupFactor;
    }

    /**
     * Get group factor for this shopping list.
     * @return shopping list's group factor
     */
    public long getGroupFactor() {
        return groupFactor_;
    }

    /**
     * Sets unique UUID for this shopping list.
     *
     * <p>UUID is automatically assigned to every shopping list during its creation.
     * It is used in clones to establish relation with its source.</p>
     *
     * @param shoppingListId The new UUID for this shopping list
     */
    public void setShoppingListId(UUID shoppingListId) {
        this.shoppingListId = shoppingListId;
    }

    @Override
    public String toString() {
        return getDebugInfoNeverUseInCode();
    }

    /**
     * Returns a debug string consisting of two lists: one is the list of commodities in the
     * basket, and the other is the corresponding list of quantities of those commodities.
     *
     * @return a debug string consisting of the list of commodities and their quantities
     */
    public String toDebugString() {
        return String.format("basket: %s, quantities: %s", getBasket().toDebugString(),
                Arrays.asList(ArrayUtils.toObject(quantities_)));
    }

    /**
     * Set the move context on the shopping list based on the move context from the move action.
     *
     * @param context The given context
     */
    public void setContext(Context context) {
        this.moveContext = context;
    }

    /**
     * Return the context set on the shopping list.
     *
     * @return An optional move context
     */
    public Optional<Context> getContext() {
        return Optional.ofNullable(moveContext);
    }

    /**
     * Return the total allocated coupons from the context set on the shopping list.
     * @param economy the economy
     * @param seller trader whose compute tier family to use
     * @return the total allocated coupons for the seller's family, or zero if not available
     */
    public double getTotalAllocatedCoupons(UnmodifiableEconomy economy, Trader seller) {
        Optional<com.vmturbo.platform.analysis.economy.Context> optionalContext = this.getBuyer()
                .getSettings().getContext();
        if (!optionalContext.isPresent()) {
            return 0.0;
        }
        return optionalContext.get()
                .getTotalAllocatedCoupons(seller.getOid())
                .orElse(0.0);
    }

    /**
     * Return the total requested coupons from the context set on the shopping list.
     * @param economy the economy
     * @param seller trader whose compute tier family to use
     * @return the total allocated coupons for the seller's family, or zero if not available
     */
    public double getTotalRequestedCoupons(UnmodifiableEconomy economy, Trader seller) {
        Optional<com.vmturbo.platform.analysis.economy.Context> optionalContext = this.getBuyer()
                .getSettings().getContext();
        if (!optionalContext.isPresent()) {
            return 0.0;
        }
        return optionalContext.get()
                .getTotalRequestedCoupons(seller.getOid())
                .orElse(0.0);
    }

    /**
     * Add entry for commodity type with assigned capacity sold by buyer.
     *
     * @param commodityType commodityType.
     * @param assignedCapacity assignedCapacity for the commodityType sold by buyer.
     */
    public void addAssignedCapacity(Integer commodityType, double assignedCapacity) {
        assignedCapacities.put(commodityType, assignedCapacity);
    }

    /**
     * Get assigned capacity for certain commodity type sold by buyer.
     *
     * @param commodityType commodityType.
     * @return assigned capacity for given commodityType.
     */
    @Nullable
    public Double getAssignedCapacity(Integer commodityType) {
        return assignedCapacities.get(commodityType);
    }

    /**
     * Set demandScalable flag on the shopping list.
     *
     * @param demandScalable given demandScalable value.
     */
    public void setDemandScalable(boolean demandScalable) {
        this.demandScalable = demandScalable;
    }

    /**
     * Get demandScalable flag for the shopping list.
     *
     * @return demandScalable value.
     */
    public boolean getDemandScalable() {
        return demandScalable;
    }
} // end ShoppingList class

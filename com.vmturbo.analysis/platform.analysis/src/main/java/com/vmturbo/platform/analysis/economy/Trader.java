package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

/**
 * An entity that trades goods in a {@link Market}.
 *
 * <p>
 *  It can participate in multiple markets either as a seller or a buyer and even participate in a
 *  single market multiple times (though never as a buyer and seller simultaneously). The latter can
 *  happen e.g. if a buyer buys multiple storage commodities. It is also possible that a Trader
 *  buys from another trader that is not in the current market, if a policy is created that is not
 *  enforced initially.
 * </p>
 */
public abstract class Trader implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 8356471354794423341L;
    // Fields
    private int economyIndex_;
    private int cloneOf_;
    private long oid_ = Long.MIN_VALUE; // OID of the trader
    private final int type_; // this should never change once the object is created.
    private @NonNull TraderState state_;
    private @NonNull Basket basketSold_;
    private final @NonNull List<@NonNull CommoditySold> commoditiesSold_ = new ArrayList<>();
    private final @NonNull Set<@NonNull Long> cliques_ = new HashSet<>();
    private final @NonNull List<@NonNull ShoppingList> customers_ = new ArrayList<>();
    // TODO: (Jun 22, 2016) This field is intended to be temporarily used for debugging in the initial stages of M2. To avoid making drastic change in
    // market2, we use a setter and getter to set and get this field instead of putting it part of the constructor even though it should
    // only be set once and never changed.
    // If in future we want to keep it, we should make it part of the constructor, remove the setter, also modify all places that call it
    // and the corresponding tests.
    private String debugInfoNeverUseInCode_; // a field keeps information about the eclass and uuid of the trader.
    // Cached data

    // Cached unmodifiable view of the commoditiesSold_ list.
    private final @NonNull List<@NonNull CommoditySold> unmodifiableCommoditiesSold_ = Collections.unmodifiableList(commoditiesSold_);
    // Cached unmodifiable view of the cliques_ set.
    private final @NonNull Set<@NonNull Long> unmodifiableCliques_ = Collections.unmodifiableSet(cliques_);
    // Cached unmodifiable view of the customers_ list.
    private final @NonNull List<@NonNull ShoppingList> unmodifiableCustomers_ = Collections.unmodifiableList(customers_);
    // This field specifies whether we want to print debug info for the trader
    private boolean debugEnabled = false;
    // This field specifies whether this trader represents a template
    private boolean templateProvider_ = false;
    // This field specifies whether the trader's debug info is already printed
    private boolean sellersInfoPrinted = false;
    // Identifies the scaling group that this trader belongs to
    private final static String NO_SCALING_GROUP = "";
    private String scalingGroupId_ = NO_SCALING_GROUP;
    // Trader coming in to analysis in deploy market as new entity for placement.
    private boolean isPlacementEntity_ = false;

    // Constructors
    /**
     * Constructs a new TraderWithSettings instance with the specified attributes.
     *
     * @param economyIndex see {@link #setEconomyIndex(int)}.
     * @param type see {@link #getType()}.
     * @param state see {@link #setState(TraderState)}.
     * @param basketSold see {@link #getBasketSold()}.
     */
    public Trader(int economyIndex, int type, @NonNull TraderState state, @NonNull Basket basketSold) {
        checkArgument(type >= 0, "type = " + type);

        type_ = type;
        state_ = state;
        basketSold_ = basketSold;
        cloneOf_ = -1;
        setEconomyIndex(economyIndex);
        for(int i = 0 ; i < basketSold.size() ; ++i) {
            commoditiesSold_.add(new CommoditySoldWithSettings());
        }
    }

    // Methods

    /**
     * Returns the basket sold by {@code this} seller.
     */
    @Pure
    public @NonNull @ReadOnly Basket getBasketSold(@ReadOnly Trader this) {
        return basketSold_;
    }

    /**
     * Returns an unmodifiable list of the commodities {@code this} trader is selling.
     */
    @Pure
    public @NonNull @ReadOnly List<@NonNull @ReadOnly CommoditySold> getCommoditiesSold(@ReadOnly Trader this) {
        return unmodifiableCommoditiesSold_;
    }

    /**
     * Returns the commodity sold by {@code this} trader that corresponds to the given
     * {@link CommoditySpecification}.
     *
     * @param specification The commodity specification specifying the commodity sold that should be
     *         returned.
     * @return The commodity sold by {@code this} trader that corresponds to the given commodity
     *         specification, or {@code null} iff !{@link #getBasketSold()}.
     *         {@link Basket#contains(CommoditySpecification) contains(specification)}
     */
    @Pure
    public @PolyRead CommoditySold getCommoditySold(@PolyRead Trader this, @NonNull @ReadOnly CommoditySpecification specification) {
        int index = getBasketSold().indexOf(specification);

        return index != -1 ? commoditiesSold_.get(index) : null;
    }

    // TODO: consider making addCommoditySold and removeCommoditySold throw in cases they now return
    // null. Same for the corresponding Basket methods.

    /**
     * Adds a new commodity to the list of commodities sold by {@code this} seller and returns it.
     *
     * @param newSpecification The type of the new commodity. It will be added to {@code this}
     *          seller's basket, or ignored if it already exists.
     * @return The new commodity that was created and added, or {@code null} if it already existed.
     */
    @Deterministic
    public CommoditySold addCommoditySold(@NonNull @ReadOnly CommoditySpecification newSpecification) {
        basketSold_ = basketSold_.add(newSpecification);

        if (commoditiesSold_.size() < basketSold_.size()) {
            CommoditySoldWithSettings newCommoditySold = new CommoditySoldWithSettings();
            commoditiesSold_.add(basketSold_.indexOf(newSpecification), newCommoditySold);

            return newCommoditySold;
        }

        return null;
    }

    /**
     * Removes an existing commodity from the list of commodities sold by {@code this} seller.
     *
     * <p>
     *  A commodity sold by a single trader is uniquely identified by its type.
     *  Both the list of commodities sold and the basket sold are updated.
     * </p>
     *
     * @param specificationToRemove The specification of the commodity that needs to be removed.
     *              It will be removed from {@code this} seller's basket, or ignored if it was never
     *              in the basket.
     * @return The removed commodity sold, or {@code null} if it wasn't in the basket.
     */
    @Deterministic // in the sense that for the same referents of this and typeToRemove the result will
    // be the same. Calling this two times on the same topology will produce different results
    // because the topology is modified.
    public CommoditySold removeCommoditySold(@NonNull @ReadOnly CommoditySpecification specificationToRemove) {
        int index = basketSold_.indexOf(specificationToRemove);

        if (index == -1) {
            return null;
        } else {
            CommoditySold removed = commoditiesSold_.remove(index);
            basketSold_ = basketSold_.remove(specificationToRemove);

            return removed;
        }
    }

    /**
     * Returns an unmodifiable set of the k-partite cliques {@code this} trader is a member of.
     *
     * <p>
     *  There are situations when a buyer needs to buy a number of {@link ShoppingList}s from
     *  different {@link Market}s and that not all combinations of sellers from those markets are
     *  valid. In these situations the k-partite graph of valid placements is covered by k-partite
     *  cliques so that any combination of sellers from different parts of the graph and in the same
     *  k-partite clique are valid. This clique cover is constructed before the topology is sent to
     *  the analysis engine. Refer to the VMTurbo wiki for more details.
     * </p>
     *
     * <p>
     *  Each of those cliques is represented as an integer.
     * </p>
     *
     * <p>
     *  {@link Market}s organize their sellers based on k-partite clique membership.
     * </p>
     */
    @Pure
    public @NonNull @ReadOnly Set<@NonNull Long> getCliques(@ReadOnly Trader this) {
        return unmodifiableCliques_;
    }

    /**
     * Returns a modifiable set of the k-partite cliques {@code this} trader is a member of.
     *
     * <p>
     *  This is a modifiable version of the set returned by {@link #getCliques()}.
     * </p>
     *
     * @see #getCliques()
     */
    @Pure
    @NonNull @PolyRead Set<@NonNull @PolyRead Long> getModifiableCliques(@PolyRead Trader this) {
        return cliques_;
    }

    /**
     * Returns an unmodifiable list of {@code this} trader's customers.
     *
     * <p>
     *  A trader is a customer of another trader, iff the former is active and currently buying at
     *  least one commodity the latter is selling.
     * </p>
     *
     * <p>
     *  This method really returns shopping lists instead of discrete traders, so if a trader
     *  buys the same commodity specification more than once, the list will contain more than one
     *  shopping list belonging to the same trader. For a method returning a list of unique
     *  traders that are customers of {@code this} trader, see {@link #getUniqueCustomers()}.
     * </p>
     *
     * @see #getUniqueCustomers()
     */
    @Pure
    public @NonNull @ReadOnly List<@NonNull ShoppingList> getCustomers(@ReadOnly Trader this) {
        return unmodifiableCustomers_;
    }

    /**
     * Returns a a modifiable set of {@code this} trader's customers which are buyers in market m.
     *
     * <p>
     *  A trader is a customer of another trader, iff the former is active and currently buying at
     *  least one commodity the latter is selling.
     * </p>
     */
    @Pure
    public @NonNull @PolyRead Set<@NonNull ShoppingList> getCustomers(
            @ReadOnly Trader this, @ReadOnly Market m) {
        @NonNull Set<@NonNull @ReadOnly ShoppingList> customersInMarket = new HashSet<>(
                unmodifiableCustomers_);
        customersInMarket.retainAll(new HashSet<>(m.getBuyers()));
        return customersInMarket;
    }

    /**
     * Returns a modifiable list of {@code this} trader's customers.
     *
     * <p>
     *  This is a modifiable version of the list returned by {@link #getCustomers()}.
     * </p>
     *
     * @see #getCustomers()
     * @see #getUniqueCustomers()
     */
    @Pure
    @NonNull @PolyRead List<@NonNull @PolyRead ShoppingList> getModifiableCustomers(@PolyRead Trader this) {
        return customers_;
    }

    /**
     * Returns an unmodifiable set of {@code this} trader's customers.
     *
     * <p>
     *  A trader is a customer of another trader iff the former is active and currently buys any
     *  subset of the commodities the latter is selling.
     * </p>
     *
     * <p>
     *  This method returns a set of unique traders. For a list of all the shopping lists
     *  buying from {@code this} trader, see {@link #getCustomers()}.
     * </p>
     *
     * @see #getCustomers()
     */
    @Pure
    public @NonNull @ReadOnly Set<@NonNull @ReadOnly Trader> getUniqueCustomers(@ReadOnly Trader this) {
        @NonNull Set<@NonNull @ReadOnly Trader> customers = new HashSet<>();

        for (@NonNull ShoppingList shoppingList : getCustomers()) {
            customers.add(shoppingList.getBuyer());
        }

        return Collections.unmodifiableSet(customers);
    }

    // May need to add some reference to the associated reservation later...

    /**
     * The {@link TraderSettings settings} controlling {@code this} trader's behavior.
     */
    @Pure
    public abstract @NonNull TraderSettings getSettings();

    /**
     * Returns the <em>economy index</em> of {@code this} trader.
     *
     * <p>
     *  The economy index of a trader is its position in the {@link Economy#getTraders() traders
     *  list} and it's non-negative and non-increasing. It will be decreased iff a trader with lower
     *  economy index is removed from the economy.
     * </p>
     *
     * <p>
     *  This is an O(1) operation.
     * </p>
     */
    @Pure
    public int getEconomyIndex(@ReadOnly Trader this) {
        return economyIndex_;
    }

    /**
     * Sets the value of the <b>economy index</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param economyIndex the new value for the field. Must be non-negative.
     * @return {@code this}
     *
     * @see #getEconomyIndex()
     */
    @Deterministic
    @NonNull Trader setEconomyIndex(int economyIndex) {
        checkArgument(economyIndex >= 0, "economyIndex = " + economyIndex);
        economyIndex_ = economyIndex;
        return this;
    }

    /**
     * Returns the <em>oid</em> of {@code this} trader.
     * <p>
     *  The oid is the "Object ID" of the trader and matches the OID
     *  of the corresponding entity in the topology used to generate
     *  the economy.
     * </p>
     *
     * <p>
     *  This is an O(1) operation.
     * </p>
     * @return the Trader's OID.
     */
    @Pure
    public long getOid(@ReadOnly Trader this) {
        return oid_;
    }

    /**
     * Sets the value of the <b>oid</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param oid the new value for the field. Must be non-negative.
     * @return {@code this}
     *
     * @see #getOid()
     */
    @Deterministic
    @NonNull
    public Trader setOid(long oid) {
        checkArgument(oid_ == Long.MIN_VALUE, "oid_ already assigned value of " + oid_);
        oid_ = oid;
        return this;
    }

    /**
     * Check whether the <b>oid</b> field has been set.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @return whether the Trader's OID has already been set.
     * @see #getOid()
     */
    @Pure
    public boolean isOidSet(@ReadOnly Trader this) {
        return oid_ != Long.MIN_VALUE;
    }

    /**
     * Returns the type of the trader.
     *
     * <p>
     *  Its a numerical representation of the type. An ID of sorts that may e.g. correspond to
     *  "physical machine" or "storage", but the correspondence is not important to the economy and
     *  kept (potentially) outside the economy. It is non-negative.
     * </p>
     */
    @Pure
    public int getType(@ReadOnly Trader this) {
        return type_;
    }

    /**
     * Returns the current {@link TraderState state} of {@code this} trader.
     */
    @Pure
    public @NonNull TraderState getState(@ReadOnly Trader this) {
        return state_;
    }

    /**
     * Sets the value of the <b>state</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param state the new value for the field.
     * @return {@code this}
     */
    @Deterministic
    @NonNull Trader setState(@NonNull @ReadOnly TraderState state) {
        state_ = state;
        return this;
    }

    /**
     * Changes the state of {@code this} trader, updating the corresponding markets he participates
     * in as a buyer or seller to reflect the change.
     *
     * @param newState The new state for the trader.
     * @return The old state of trader.
     */
    @Deterministic
    public abstract @NonNull TraderState changeState(@NonNull TraderState newState);

    /**
     * Sets the debugInfo field. It contains information about the eclass and uuid of the trader.
     * @param debugInfo a string contains eclass|uuid of the trader
     */
    public @NonNull Trader setDebugInfoNeverUseInCode(@NonNull String debugInfo) {
        debugInfoNeverUseInCode_ = debugInfo;
        return this;
    }

    /**
     * Returns the debugInfo field.
     */
    public String getDebugInfoNeverUseInCode(@ReadOnly Trader this) {
        return debugInfoNeverUseInCode_;
    }

    /**
     * Returns the cloneOf field.
     */
    public int getCloneOf() {
        return cloneOf_;
    }

    /**
     * Sets the cloneOf field. It contains economyIndex of the modelSeller
     * @param modelSeller the {@link Trader} that we clone
     */
    public void setCloneOf(Trader modelSeller) {
        cloneOf_ = modelSeller.getEconomyIndex();
    }

    /**
     * @return true if the entity is a clone
     */
    public boolean isClone() {
        return cloneOf_ != -1;
    }

    /**
     * @return true if the trader is debug enabled
     */
    public boolean isDebugEnabled() {
        return debugEnabled;
    }

    /**
     * set the debugEnabled flag to true or false
     */
    public void setDebugEnabled(boolean debugEnabled) {
        this.debugEnabled = debugEnabled;
    }

    /**
     * @return true if the trader is a templateProvider
     */
    public boolean isTemplateProvider() {
        return templateProvider_;
    }

    /**
     * set the templateProvider_ flag to true or false
     */
    public void setTemplateProvider(boolean templateProvider) {
        templateProvider_ = templateProvider;
    }

    /**
     * @return true if the trader's possible sellers' info is already printed
     */
    public boolean isSellersInfoPrinted() {
        return sellersInfoPrinted;
    }

    /**
     * set the sellersInfoPrinted flag to true or false
     */
    public void setSellersInfoPrinted(boolean printed) {
        this.sellersInfoPrinted = printed;
    }

    /*
     * Returns the debugInfoNeverUseInCode
     */
    @Override
    public String toString() {
        return debugInfoNeverUseInCode_;
    }

    /**
     * @return the scaling group ID.
     */
    public String getScalingGroupId() {
        return scalingGroupId_;
    }

    /**
     * Sets the scaling group ID.
     * @param scalingGroupId group ID
     */
    public void setScalingGroupId(@Nonnull String scalingGroupId) {
        this.scalingGroupId_ = scalingGroupId;
    }

    /**
     * @return whether this Trader is in a scaling group
     */
    public boolean isInScalingGroup(Economy e) {
        return !scalingGroupId_.isEmpty() && e.getNumberOfMembersInScalingGroup(scalingGroupId_) > 1;
    }

    /**
     * @return whether Trader is in deploy market as new entity for placement.
     */
    public boolean isPlacementEntity() {
        return isPlacementEntity_;
    }

    /**
     * Set whether Trader is in deploy market as new entity for placement.
     * @param isPlacementEntity
     */
    public void setPlacementEntity(boolean isPlacementEntity) {
        this.isPlacementEntity_ = isPlacementEntity;
    }

    /**
     * Clear information about this trader's customers, markets this trader
     * participates in, clique memberships, etc.
     * <p/>
     * This is useful when retaining Traders for ActionReplay without transitively
     * having to keep the whole old economy in-memory.
     */
    public void clearShoppingAndMarketData() {
        cliques_.clear();
        customers_.clear();
    }
} // end interface Trader

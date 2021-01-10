package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

/**
 * A trading place where a particular basket of goods is sold and bought.
 *
 * <p>
 *  A {@code Market} is associated with a {@link Basket} and comprises a list of buyers and sellers
 *  trading that particular basket.
 * </p>
 *
 * <p>
 *  Market is responsible for keeping the contained lists as well as the
 *  {@link TraderWithSettings#getMarketsAsBuyer() markets-as-buyer} and
 *  {@link TraderWithSettings#getMarketsAsSeller() markets-as-seller} lists of the participating
 *  traders in sync.
 * </p>
 */
public final class Market implements Serializable {
    // Fields

    private final @NonNull Basket basket_; // see #getBasket()
    private final @NonNull List<@NonNull ShoppingList> buyers_ = new ArrayList<>(); // see #getBuyers()
    // active sellers, inactive sellers and each of the lists in cliques may be lists, but are
    // utilized as sets in the sense that they are not supposed to contain duplicate elements. Lists
    // were selected for fast iteration.
    private final @NonNull Set<@NonNull Trader> activeSellers_ = new HashSet<>(); // see #getActiveSellers()
    private final @NonNull Map<@NonNull Long, @NonNull List<@NonNull Trader>> cliques_ = new TreeMap<>(); // see #getCliques
    private final @NonNull Set<@NonNull Trader> inactiveSellers_ = new HashSet<>(); // see #getInactiveSellers()
    // active sellers that can accept new customers. see #getActiveSellersAvailableForPlacement()
    private final @NonNull List<@NonNull Trader> activeSellersAvailableForPlacement_ = new ArrayList<>();

    // Cached data
    // used in placement termination condition
    private double expenseBaseline_;
    private double placementSavings_;

    // Cached unmodifiable view of the buyers_ list.
    private final @NonNull List<@NonNull ShoppingList> unmodifiableBuyers_ = Collections.unmodifiableList(buyers_);
    // Cached unmodifiable view of the activeSellers_ list.
    private final @NonNull Set<@NonNull Trader> unmodifiableActiveSellers_ = Collections.unmodifiableSet(activeSellers_);
    // Cached unmodifiable view of the activeSellersAvailableForPlacement_ list.
    private final @NonNull List<@NonNull Trader> unmodifiableActiveSellersAvailableForPlacement_ =
                    Collections.unmodifiableList(activeSellersAvailableForPlacement_);
    // Cached unmodifiable view of the cliques_ map.
    // TODO: find a way to make the contained lists unmodifiable as well.
    private final @NonNull Map<@NonNull Long, @NonNull List<@NonNull Trader>> unmodifiableCliques_ = Collections.unmodifiableMap(cliques_);
    // Cached unmodifiable view of the inactiveSellers_ list.
    private final @NonNull Set<@NonNull Trader> unmodifiableInactiveSellers_ = Collections.unmodifiableSet(inactiveSellers_);

    // Constructors

    /**
     * Constructs an empty Market and attaches the given basket.
     *
     * @param basketToAssociate The basket to associate with the new market. It it referenced and
     *                          not copied.
     */
    Market(@NonNull Basket basketToAssociate) {
        basket_ = basketToAssociate;
    }

    // Methods

    /**
     * Returns the associated {@link Basket}.
     *
     * <p>
     *  All buyers in the market buy that basket.
     * </p>
     */
    @Pure
    public @NonNull Basket getBasket(@ReadOnly Market this) {
        return basket_;
    }

    /**
     * Returns an unmodifiable list of active sellers participating in {@code this} {@code Market}.
     *
     * <p>
     *  A {@link Trader} participates in the market as a seller iff he is active and the basket he
     *  is selling satisfies the one associated with the market.
     * </p>
     *
     * @see #getInactiveSellers()
     */
    @Pure
    public @NonNull @ReadOnly Set<@NonNull Trader> getActiveSellers(@ReadOnly Market this) {
        return unmodifiableActiveSellers_;
    }

    /**
     * Returns an unmodifiable list of active sellers available for placement that are
     * participating in {@code this} {@code Market}.
     *
     * <p>
     *  A {@link Trader} participates in the market as a seller iff he is active and the basket he
     *  is selling satisfies the one associated with the market.
     * </p>
     *
     * @see #getInactiveSellers()
     */
    @Pure
    public @NonNull @ReadOnly List<@NonNull Trader> getActiveSellersAvailableForPlacement(@ReadOnly Market this) {
        return unmodifiableActiveSellersAvailableForPlacement_;
    }

    /**
     * Returns an unmodifiable map from k-partite clique number to the <em>active</em>
     * {@link Trader}s in {@code this} market that are members of that clique.
     *
     * <p>
     *  The iteration order in this map is from smallest to largest clique number.
     * </p>
     */
    @Pure
    public @NonNull Map<@NonNull Long, @NonNull List<@NonNull Trader>> getCliques(@ReadOnly Market this) {
        return unmodifiableCliques_;
    }

    /**
     * Returns an unmodifiable list of inactive sellers in {@code this} {@code Market}.
     *
     * <p>
     *  An inactive {@link Trader} selling a basket that satisfies the one associated with
     *  {@code this} market is still kept in the market, but in a separate list, so that it's easy
     *  to consider inactive traders for reactivation when generating actions to add resources.
     * </p>
     *
     * @see #getActiveSellers()
     */
    @Pure
    public @NonNull @ReadOnly Set<@NonNull Trader> getInactiveSellers(@ReadOnly Market this) {
        return unmodifiableInactiveSellers_;
    }

    /**
     * Returns steam of all the sellers in the Market active and inactive.
     *
     * @return Returns stream of all the sellers in the Market active and inactive.
     */
    public @NonNull @ReadOnly Stream<@NonNull Trader> getSellers(@ReadOnly Market this) {
        return Stream.concat(activeSellers_.stream(),
            inactiveSellers_.stream());
    }

    @Pure
    public @NonNull @ReadOnly double getExpenseBaseline(@ReadOnly Market this) {
        return expenseBaseline_;
    }

    @Pure
    public @NonNull @ReadOnly double getPlacementSavings(@ReadOnly Market this) {
        return placementSavings_;
    }

    @Deterministic
    public Market setExpenseBaseline(double expenseBaseline) {
        checkArgument(expenseBaseline >= 0, "expenseBaseline = %s", expenseBaseline);
        expenseBaseline_ = expenseBaseline;
        return this;
    }

    @Deterministic
    public Market setPlacementSavings(double placementSavings) {
        checkArgument(placementSavings >= 0, "placementSavings = %s", placementSavings);
        placementSavings_ = placementSavings;
        return this;
    }

    /**
     * Adds a new seller to {@code this} market. If he is already in the market the results are
     * undefined.
     *
     * <p>
     *  The trader's own {@link TraderWithSettings#getMarketsAsSeller() markets as seller} list is
     *  updated.
     * </p>
     *
     * @param newSeller The new trader to add to the market as a seller. His basket sold must match
     *                  the market's one and he must be active. He should not be already in the market.
     * @return {@code this}
     */
    @Deterministic
    public @NonNull Market addSeller(@NonNull TraderWithSettings newSeller) {
        checkArgument(getBasket().isSatisfiedBy(newSeller.getBasketSold()),
                "basket = %s, sellerBasket = %s, newSeller = %s",
                getBasket(), newSeller.getBasketSold(), newSeller);

        if (newSeller.getState().isActive()) {
            activeSellers_.add(newSeller);
            if (newSeller.canAcceptNewCustomers()) {
                activeSellersAvailableForPlacement_.add(newSeller);
            }
        } else {
            inactiveSellers_.add(newSeller);
        }
        newSeller.getMarketsAsSeller().add(this);

        // Add seller to corresponding cliques
        for (@NonNull Long cliqueNumber : newSeller.getCliques()) {
            List<@NonNull Trader> cliquePart = cliques_.get(cliqueNumber);
            if (cliquePart == null) {
                cliques_.put(cliqueNumber, cliquePart = new ArrayList<>());
            }
            cliquePart.add(newSeller);
        }

        return this;
    }

    /**
     * Sort the buyers that belong to this market by current quote, high to low.
     *
     * @param economy - the economy where the market belongs to
     */

    public void sortBuyers(@NonNull Economy economy) {
        List<ShoppingList> sortedBuyers = new ArrayList<>(economy.sortShoppingLists(getBuyers()));
        // update buyers_ with the sorted list.
        buyers_.clear();
        buyers_.addAll(sortedBuyers);
    }




    /**
     * Removes an existing seller from {@code this} market. If he was not in {@code this} market in
     * the first place, the results are undefined.
     *
     * <p>
     *  The trader's own {@link TraderWithSettings#getMarketsAsSeller() markets as seller} list is
     *  updated.
     * </p>
     *
     * <p>
     *  Note that if there are shopping lists in the market buying from sellerToRemove, they
     *  are left unchanged. That's because it's legal from a buyer to temporarily buy from a seller
     *  in another market if e.g. an access commodity has been removed, but the recommendation to
     *  move the buyer hasn't been taken yet.
     * </p>
     *
     * @param sellerToRemove The existing trader that should seize selling in this market.
     * @return {@code this}
     */
    @Deterministic
    public @NonNull Market removeSeller(@NonNull TraderWithSettings sellerToRemove) {
        if (sellerToRemove.getState().isActive()) {
            checkArgument(activeSellers_.remove(sellerToRemove),
                    "sellerToRemove = %s", sellerToRemove);
            if (sellerToRemove.canAcceptNewCustomers()) {
                checkArgument(activeSellersAvailableForPlacement_.remove(sellerToRemove));
            }
        } else {
            checkArgument(inactiveSellers_.remove(sellerToRemove),
                    "sellerToRemove = %s", sellerToRemove);
        }
        sellerToRemove.getMarketsAsSeller().remove(this);

        // Remove seller from corresponding cliques
        for (@NonNull Long cliqueNumber : sellerToRemove.getCliques()) {
            @NonNull List<@NonNull Trader> cliquePart = cliques_.get(cliqueNumber);
            checkArgument(cliquePart.remove(sellerToRemove),
                    "sellerToRemove = %s", sellerToRemove);
        }

        return this;
    }

    /**
     * Returns an unmodifiable list of buyers participating in {@code this} {@code Market}.
     *
     * <p>
     *  A {@link Trader} participates in the market as a buyer iff he is active and he buys at least
     *  one basket equal to the one associated with the market. He may appear more than once in the
     *  returned list as different participations iff he buys that basket more than once.
     * </p>
     */
    @Pure
    public @NonNull @ReadOnly List<@NonNull ShoppingList> getBuyers(@ReadOnly Market this) {
        return unmodifiableBuyers_;
    }

    /**
     * Adds a new buyer to {@code this} market or an existing one again.
     *
     * <p>
     *  This will make the trader buy {@code this} market's basket if he wasn't, or buy it again
     *  if he was. That implies modification of his {@link TraderWithSettings#getMarketsAsBuyer()
     *  markets as buyer} map.
     * </p>
     *
     * @param newBuyer The new trader to add to the market as a buyer. He must be active.
     *                 If his economy index is incorrect, the results are undefined.
     * @param existingShoppingList An existing shopping list to use when adding the buyer.
     * @return The existing shopping list for the buyer that was passed in.
     */
    @NonNull ShoppingList addBuyer(@NonNull TraderWithSettings newBuyer,
                                   @NonNull ShoppingList existingShoppingList) {
        if (newBuyer.getState().isActive()) {
            buyers_.add(existingShoppingList);
        }
        newBuyer.getMarketsAsBuyer().put(existingShoppingList, this);
        return existingShoppingList;
    }

    /**
     * Adds a new buyer to {@code this} market or an existing one again.
     *
     * <p>
     *  This will make the trader buy {@code this} market's basket if he wasn't, or buy it again
     *  if he was. That implies modification of his {@link TraderWithSettings#getMarketsAsBuyer()
     *  markets as buyer} map.
     * </p>
     *
     * @param newBuyer The new trader to add to the market as a buyer. He must be active.
     *                 If his economy index is incorrect, the results are undefined.
     * @return The shopping list that was created for the buyer.
     */
    @NonNull ShoppingList addBuyer(@NonNull TraderWithSettings newBuyer) {
        return addBuyer(newBuyer, new ShoppingList(newBuyer, basket_));
    }

    /**
     * Removes an existing shopping list from {@code this} market. If it was not in
     * {@code this} market in the first place, the results are undefined.
     * The trader's own {@link TraderWithSettings#getMarketsAsBuyer() markets as seller} map is
     * updated.
     *
     * @param shoppingListToRemove The existing shopping list that should be removed from
     *                              {@code this} market. It should be in the market.
     * @return {@code this}
     */
    @NonNull Market removeShoppingList(@NonNull ShoppingList shoppingListToRemove) {
        if (shoppingListToRemove.getBuyer().getState().isActive()) {
            checkArgument(buyers_.remove(shoppingListToRemove),
                    "shoppingListToRemove = %s", shoppingListToRemove);
        }
        shoppingListToRemove.move(null);
        checkArgument(((TraderWithSettings)shoppingListToRemove.getBuyer()).getMarketsAsBuyer().remove(shoppingListToRemove, this),
                      "shoppingListToRemove = %s this = %s",
                shoppingListToRemove, this);

        return this;
    }

    /**
     * Total sellers in market.
     *
     * @return count of sellers in market.
     */
    public int getSellerCount() {
        return activeSellers_.size() + inactiveSellers_.size();
    }

    /**
     * Checks if a trader is present in the market as a seller active or inactive.
     *
     * @param seller the trader to check.
     * @return Checks if a trader is present in the market as a seller active or inactive.
     */
    public boolean isSellerPresent(Trader seller) {
        return activeSellers_.contains(seller) || inactiveSellers_.contains(seller);
    }

    /**
     * Changes the state of a trader, updating the corresponding markets he participates in as a
     * buyer or seller to reflect the change.
     *
     * @param trader The trader whose state should be changed.
     * @param newState The new state for the trader.
     * @return The old state of trader.
     */
    static @NonNull TraderState changeTraderState(@NonNull TraderWithSettings trader, @NonNull TraderState newState) {
        @NonNull TraderState oldState = trader.getState();

        if (oldState.isActive() != newState.isActive()) { // if there was a change.
            if (newState.isActive()) { // activate
                // As buyer
                for (Entry<@NonNull ShoppingList, @NonNull Market> entry : trader.getMarketsAsBuyer().entrySet()) {
                    entry.getValue().buyers_.add(entry.getKey());
                }
                // As seller
                for (@NonNull @PolyRead Market market : trader.getMarketsAsSeller()) {
                    checkArgument(market.inactiveSellers_.remove(trader), "trader = %s", trader);
                    market.activeSellers_.add(trader);
                    market.activeSellersAvailableForPlacement_.add(trader);
                    for (@NonNull Long cliqueNumber : trader.getCliques()) {
                        market.cliques_.get(cliqueNumber).add(trader);
                    }
                }
            } else { // deactivate
                // As buyer
                for (Entry<@NonNull ShoppingList, @NonNull Market> entry : trader.getMarketsAsBuyer().entrySet()) {
                    checkArgument(entry.getValue().buyers_.remove(entry.getKey()),
                            "entry.getKey() = %s", entry.getKey());
                }
                // As seller
                for (@NonNull @PolyRead Market market : trader.getMarketsAsSeller()) {
                    checkArgument(market.activeSellers_.remove(trader), "trader = %s", trader);
                    market.activeSellersAvailableForPlacement_.remove(trader);
                    market.inactiveSellers_.add(trader);
                    for (@NonNull Long cliqueNumber : trader.getCliques()) {
                        checkArgument(market.cliques_.get(cliqueNumber).remove(trader),
                                "trader = %s", trader);
                    }
                }
            }
        }

        trader.setState(newState);
        return oldState;
    }

} // end Market class

package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.DoubleBinaryOperator;
import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.dataflow.qual.SideEffectFree;

import com.google.common.primitives.Ints;

/**
 * A set of related markets and the traders participating in them.
 *
 * <p>
 *  It is responsible for creating and removing traders while simultaneously creating, updating and
 *  destroying markets while that happens.
 * </p>
 */
public final class Economy implements UnmodifiableEconomy {
    // Fields

    // The map that associates Baskets with Markets.
    private final @NonNull Map<@NonNull @ReadOnly Basket,@NonNull Market> markets_ = new TreeMap<>();
    // The list of all Traders participating in the Economy.
    private final @NonNull List<@NonNull TraderWithSettings> traders_ = new ArrayList<>();
    // Map of quantity calculation functions by (sold) commodity specification. If an entry is
    // missing, the corresponding commodity specification is 'additive'.
    private final @NonNull Map<@NonNull CommoditySpecification, @NonNull DoubleBinaryOperator>
        quantityFunctions_ = new TreeMap<>();
    // An aggregate of all the parameters configuring this economy's behavior.
    private final @NonNull EconomySettings settings_ = new EconomySettings();
    // Map of commodity resize dependency calculation by commodity type.
    private final @NonNull Map<@NonNull Integer, @NonNull List<@NonNull CommodityResizeSpecification>>
        commodityResizeDependency_ = new HashMap<>();
    // Map from raw processedCommodity -> rawCommodity
    private final @NonNull Map<@NonNull Integer, @NonNull List<@NonNull Integer>> rawMaterial_ = new HashMap<>();
    // a flag to indicate if analysis should stop immediately or not
    private volatile boolean forceStop = false;
    // Cached data

    // Cached unmodifiable view of the markets_.values() collection.
    private final @NonNull Collection<@NonNull Market> unmodifiableMarkets_ = Collections.unmodifiableCollection(markets_.values());
    // Cached unmodifiable view of the traders_ list.
    private final @NonNull List<@NonNull Trader> unmodifiableTraders_ = Collections.unmodifiableList(traders_);
    // Cached unmodifiable view of the quantityFunctions_ map.
    private final @NonNull Map<@NonNull CommoditySpecification, @NonNull DoubleBinaryOperator>
        unmodifiableQuantityFunctions_ = Collections.unmodifiableMap(quantityFunctions_);

    // Constructors

    /**
     * Constructs an empty Economy.
     *
     * <p>
     *  It will initially contain no Markets nor Traders.
     * </p>
     */
    public Economy() {}

    // Methods

    @Override
    @Pure
    public @ReadOnly @NonNull Map<@NonNull CommoditySpecification, @NonNull DoubleBinaryOperator>
            getQuantityFunctions(@ReadOnly Economy this) {
        return unmodifiableQuantityFunctions_;
    }

    /**
     * Returns a modifiable map from {@link CommoditySpecification} to the corresponding quantity
     * updating function, if there is one.
     *
     * @see UnmodifiableEconomy#getQuantityFunctions()
     */
    @Pure
    public @PolyRead @NonNull Map<@NonNull CommoditySpecification, @NonNull DoubleBinaryOperator>
            getModifiableQuantityFunctions(@PolyRead Economy this) {
        return quantityFunctions_;
    }

    @Pure
    public @NonNull Map<@NonNull Integer, @NonNull List<@NonNull CommodityResizeSpecification>>
                                                    getModifiableCommodityResizeDependencyMap() {
        return commodityResizeDependency_;
    }

    @Pure
    public @NonNull Map<@NonNull Integer, @NonNull List<@NonNull Integer>>
                                              getModifiableRawCommodityMap() {
        return rawMaterial_;
    }

    @Override
    @Pure
    public @NonNull @PolyRead EconomySettings getSettings(@PolyRead Economy this) {
        return settings_;
    }

    @Override
    @Pure
    public @NonNull @ReadOnly Collection<@NonNull @ReadOnly Market> getMarkets(@ReadOnly Economy this) {
        return unmodifiableMarkets_;
    }

    @Override
    @Pure
    public @NonNull @ReadOnly Market getMarket(@ReadOnly Economy this, @NonNull @ReadOnly Basket basket) {
        Market result = markets_.get(basket);
        checkArgument(result != null, "basket = " + basket);

        return result;
    }

    @Override
    @Pure
    public @NonNull @ReadOnly Market getMarket(@ReadOnly Economy this, @NonNull @ReadOnly ShoppingList shoppingList) {
        return ((TraderWithSettings)shoppingList.getBuyer()).getMarketsAsBuyer().get(shoppingList);
    }

    @Override
    @SideEffectFree
    public @NonNull @ReadOnly List<@NonNull CommodityBought> getCommoditiesBought(@ReadOnly Economy this,
                                               @NonNull @ReadOnly ShoppingList shoppingList) {
        final int basketSize = getMarket(shoppingList).getBasket().size();
        final @NonNull List<@NonNull CommodityBought> result = new ArrayList<>(basketSize);

        for (int i = 0; i < basketSize; ++i) { // should be same size as the shopping list
            result.add(new CommodityBought(shoppingList, i));
        }

        return result;
    }

    @Override
    @SideEffectFree
    public @NonNull @PolyRead CommodityBought getCommodityBought(@PolyRead Economy this,
                                         @NonNull @PolyRead ShoppingList shoppingList,
                                         @NonNull @ReadOnly CommoditySpecification specification) {
        return new CommodityBought(shoppingList,getMarket(shoppingList).getBasket().indexOf(specification));
    }

    @Override
    @Pure
    public @NonNull @ReadOnly List<@NonNull @ReadOnly Trader> getTraders(@ReadOnly Economy this) {
        return unmodifiableTraders_;
    }

    @Override
    @Pure
    public @NonNull @ReadOnly List<@NonNull Integer> getRawMaterials(@ReadOnly Economy this,
                                                                     int processedCommodityType) {
        return rawMaterial_.get(processedCommodityType);
    }

    @Override
    @Pure
    public @NonNull @ReadOnly List<@NonNull CommodityResizeSpecification>
                    getResizeDependency(@ReadOnly Economy this, int processedCommodityType) {
        return commodityResizeDependency_.get(processedCommodityType);
    }

    /**
     * Creates a new {@link Trader trader} with the given characteristics and adds it to
     * {@code this} economy.
     *
     * <p>
     *  Ignoring cliques, it is an O(M*C) operation, where M is the number of markets present in
     *  {@code this} economy and C is the number of commodities sold by the trader.
     * </p>
     *
     * <p>
     *  New traders are always added to the end of the {@link #getTraders() traders list}.
     * </p>
     *
     * @param type The type of the trader (e.g. host or virtual machine) represented as an integer.
     * @param state The initial state of the new trader.
     * @param basketSold The basket that will be sold by the new trader.
     * @param cliques The numbers of the k-partite cliques the new trader should be a member of.
     * @return The newly created trader, so that its properties can be updated.
     */
    public @NonNull Trader addTrader(int type, @NonNull TraderState state, @NonNull Basket basketSold,
                                     @NonNull Collection<@NonNull Integer> cliques) {
        TraderWithSettings newTrader = new TraderWithSettings(traders_.size(), type, state, basketSold);

        // Populate cliques list before adding to markets
        newTrader.getModifiableCliques().addAll(cliques);

        // Add as seller (it won't be buying anything yet)
        for(Market market : markets_.values()) {
            if (market.getBasket().isSatisfiedBy(basketSold)) {
                market.addSeller(newTrader);
            }
        }

        // update traders list
        traders_.add(newTrader);
        return newTrader;
    }

    /**
     * Creates a new {@link Trader trader} with the given characteristics and adds it to
     * {@code this} economy.
     *
     * <p>
     *  Same as {@link #addTrader(int, TraderState, Basket, Collection)}, but doesn't include the
     *  new trader in any k-partite cliques and makes it initially buy a number of baskets instead.
     * </p>
     *
     * <p>
     *  The complexity is as for {@link #addTrader(int, TraderState, Basket, Collection)} plus an
     *  O(B*logM) term, where M is the number of markets present in {@code this} economy and B is
     *  the number of baskets bought. That is if no new markets are created as a result of adding
     *  the trader.
     * </p>
     *
     * <p>
     *  If new markets have to be created as a result of adding the trader, then there is an extra
     *  O(T*C) cost for each basket bought creating a new market, where T is the number of traders
     *  in the economy and C is the number of commodities in the basket creating the market.
     * </p>
     *
     * @param basketsBought The baskets the new trader should buy. {@link ShoppingList}s will be
     *                      created for each one and will appear in the {@link #getMarketsAsBuyer(Trader)}
     *                      map in the same order.
     */
    public @NonNull Trader addTrader(int type, @NonNull TraderState state, @NonNull Basket basketSold,
                                     @NonNull Basket... basketsBought) {
        @NonNull Trader newTrader = addTrader(type, state, basketSold, Ints.asList());

        // Add as buyer
        for (Basket basketBought : basketsBought) {
            addBasketBought(newTrader, basketBought);
        }

        return newTrader;
    }

    /**
     * Removes an existing {@link Trader trader} from the economy.
     *
     * <p>
     *  All buyers buying from traderToRemove will afterwards buy from no-one and all sellers
     *  selling to traderToRemove will remove him from their lists of customers.
     * </p>
     *
     * <p>
     *  It is an O(T) operation, where T is the number of traders in the economy.
     *  Shopping lists of the trader are invalidated, as are the commodities bought by these
     *  shopping lists.
     * </p>
     *
     * <p>
     *  Any shopping lists of traderToRemove and corresponding commodities bought, will become
     *  invalid.
     * </p>
     *
     * @param traderToRemove The trader to be removed. Must be in {@code this} economy.
     * @return {@code this}
     */
    // TODO: consider removing markets that no longer have buyers. (may keep them in case they
    // acquire again, to avoid recalculation of sellers)
    @Deterministic
    public @NonNull Economy removeTrader(@NonNull Trader traderToRemove) {
        final TraderWithSettings castTraderToRemove = (TraderWithSettings)traderToRemove;

        // Stop everyone from buying from the trader.
        for (@NonNull ShoppingList shoppingList : new ArrayList<>(castTraderToRemove.getCustomers())) {
            shoppingList.move(null); // this is not the cheapest way, but the safest...
        }

        // Remove the trader from all markets it participated as seller
        for (Market market : new ArrayList<>(castTraderToRemove.getMarketsAsSeller())) {
            market.removeSeller(castTraderToRemove);
        }

        // Remove the trader from all markets it participated as buyer
        for (Entry<@NonNull ShoppingList, @NonNull Market> entry
                : new ArrayList<>(castTraderToRemove.getMarketsAsBuyer().entrySet())) {
            entry.getValue().removeShoppingList(entry.getKey());
        }

        // Update economy indices of all traders and remove trader from list.
        for (TraderWithSettings trader : traders_) {
            if (trader.getEconomyIndex() > traderToRemove.getEconomyIndex()) {
                trader.setEconomyIndex(trader.getEconomyIndex() - 1);
            }
        }
        checkArgument(traders_.remove(traderToRemove), "traderToRemove = " + traderToRemove);

        return this;
    }

    @Override
    @Pure
    public @NonNull @ReadOnly List<@NonNull @ReadOnly Trader> getSuppliers(@ReadOnly Economy this,
                                                                           @NonNull @ReadOnly Trader trader) {
        @NonNull List<@NonNull @ReadOnly Trader> suppliers = new ArrayList<>();

        for (ShoppingList shoppingList : getMarketsAsBuyer(trader).keySet()) {
            if (shoppingList.getSupplier() != null) {
                suppliers.add(shoppingList.getSupplier());
            }
        }

        return Collections.unmodifiableList(suppliers);
    }

    @Override
    @Pure
    public @NonNull @ReadOnly Map<@NonNull ShoppingList, @NonNull Market>
            getMarketsAsBuyer(@ReadOnly Economy this, @NonNull @ReadOnly Trader trader) {
        return Collections.unmodifiableMap(((TraderWithSettings)trader).getMarketsAsBuyer());
    }

    @Override
    @Pure
    public @NonNull @ReadOnly List<@NonNull @ReadOnly Market> getMarketsAsSeller(@ReadOnly Economy this,
                                                                                 @NonNull @ReadOnly Trader trader) {
        return Collections.unmodifiableList(((TraderWithSettings)trader).getMarketsAsSeller());
    }

    /**
     * Makes a {@link Trader buyer} start buying a new {@link Basket basket}, or an old one one more
     * time.
     *
     * <p>
     *  The buyer's {@link #getMarketsAsBuyer(Trader) market-to-buyer-participation map} and the
     *  economy's markets are updated accordingly.
     * </p>
     *
     * @param buyer The trader that should start buying the new basket.
     * @param basketBought The basket that the buyer should start buying.
     * @return The new shopping list of the buyer in the market corresponding to the
     *         basketBought.
     */
    public @NonNull ShoppingList addBasketBought(@NonNull Trader buyer, @NonNull @ReadOnly Basket basketBought) {
        // create a market if it doesn't already exist.
        if (!markets_.containsKey(basketBought)) {
            Market newMarket = new Market(basketBought);

            markets_.put(basketBought, newMarket);

            // Populate new market
            for (TraderWithSettings seller : traders_) {
                if (newMarket.getBasket().isSatisfiedBy(seller.getBasketSold())) {
                    newMarket.addSeller(seller);
                }
            }
        }

        // add the buyer to the correct market.
        return markets_.get(basketBought).addBuyer((@NonNull TraderWithSettings)buyer);
    }

    /**
     * Makes a {@link Trader buyer} stop buying a specific instance of a {@link Basket basket},
     * designated by a {@link ShoppingList shopping list}.
     *
     * <p>
     *  Normally you would supply the basket to be removed. But when a buyer buys the same basket
     *  multiple times, the question arises: which instance of the basket should be removed? The
     *  solution is to distinguish the baskets using the shopping lists that are unique. Then
     *  only one of the multiple instances will be removed.
     * </p>
     *
     * @param shoppingList The shopping list uniquely identifying the basket instance that
     *                      should be removed.
     * @return The basket that was removed.
     */
    // TODO: consider removing markets that no longer have buyers. (may keep them in case they
    // acquire again, to avoid recalculation of sellers)
    public @NonNull @ReadOnly Basket removeBasketBought(@NonNull ShoppingList shoppingList) {
        @NonNull Market market = getMarket(shoppingList);

        market.removeShoppingList(shoppingList);

        return market.getBasket();
    }

    /**
     * Adds a new commodity specification and corresponding commodity bought to a given buyer
     * shopping list, updating markets and baskets as needed.
     *
     * <p>
     *  Normally a commodity specification is added to a basket. But when a buyer buys the same
     *  basket multiple times, the question arises: which instance of the basket should the
     *  specification be added to? The solution is to distinguish the baskets using the buyer
     *  shopping lists that are unique. Then only one of the multiple instances will be updated.
     * </p>
     *
     * @param shoppingList The shopping list that should be changed. It will be invalidated.
     * @param commoditySpecificationToAdd The commodity specification of the commodity that will be
     *                                    added to the shopping list.
     * @return The shopping list that will replace the one that was changed.
     */
    @Deterministic
    public @NonNull ShoppingList addCommodityBought(@NonNull ShoppingList shoppingList,
                              @NonNull @ReadOnly CommoditySpecification commoditySpecificationToAdd) {
        @NonNull TraderWithSettings trader = (TraderWithSettings)shoppingList.getBuyer();

        // remove the shopping list from the old market
        Basket newBasketBought = removeBasketBought(shoppingList).add(commoditySpecificationToAdd);

        // add the trader to the new market.
        ShoppingList newShoppingList = addBasketBought(trader, newBasketBought);

        // copy quantity and peak quantity values from old shopping list.
        int specificationIndex = newBasketBought.indexOf(commoditySpecificationToAdd);
        for (int i = 0 ; i < specificationIndex ; ++i) {
            newShoppingList.setQuantity(i, shoppingList.getQuantity(i));
            newShoppingList.setPeakQuantity(i, shoppingList.getPeakQuantity(i));
        }
        for (int i = specificationIndex + 1 ; i < newBasketBought.size() ; ++i) {
            newShoppingList.setQuantity(i, shoppingList.getQuantity(i-1));
            newShoppingList.setPeakQuantity(i, shoppingList.getPeakQuantity(i-1));
        }

        return newShoppingList;
    }

    /**
     * Removes an existing commodity specification and corresponding commodity bought from a given
     * shopping list, updating markets and baskets as needed.
     *
     * <p>
     *  Normally a commodity specification is removed from a basket. But when a buyer buys the same
     *  basket multiple times, the question arises: which instance of the basket should the
     *  specification be removed from? The solution is to distinguish the baskets using the buyer
     *  shopping lists that are unique. Then only one of the multiple instances will be updated.
     * </p>
     *
     * @param shoppingList The shopping list that should be changed. It will be invalidated.
     * @param commoditySpecificationToRemove The commodity specification of the commodity that will
     *            be removed from the shopping list. If this commodity isn't in the buyer
     *            shopping list, the call will just replace the shopping list with a new one.
     * @return The shopping list that will replace the one that was changed.
     */
    @Deterministic
    public @NonNull ShoppingList removeCommodityBought(@NonNull ShoppingList shoppingList,
                              @NonNull @ReadOnly CommoditySpecification commoditySpecificationToRemove) {
        @NonNull TraderWithSettings trader = (TraderWithSettings)shoppingList.getBuyer();

        // remove the shopping list from the old market
        @NonNull Basket oldBasket = removeBasketBought(shoppingList);

        Basket newBasketBought = oldBasket.remove(commoditySpecificationToRemove);

        // add the trader to the new market.
        ShoppingList newShoppingList = addBasketBought(trader, newBasketBought);

        // copy quantity and peak quantity values from old shopping list.
        int specificationIndex = oldBasket.indexOf(commoditySpecificationToRemove);
        if (specificationIndex == -1) {
            specificationIndex = newBasketBought.size();
        }
        for (int i = 0 ; i < specificationIndex ; ++i) {
            newShoppingList.setQuantity(i, shoppingList.getQuantity(i));
            newShoppingList.setPeakQuantity(i, shoppingList.getPeakQuantity(i));
        }
        for (int i = specificationIndex ; i < newBasketBought.size() ; ++i) {
            newShoppingList.setQuantity(i, shoppingList.getQuantity(i+1));
            newShoppingList.setPeakQuantity(i, shoppingList.getPeakQuantity(i+1));
        }
        return newShoppingList;
    }

    /**
     * Resets {@code this} {@link Economy} to the state it was in just after construction.
     *
     * <p>
     *  It has no other observable side-effects.
     * </p>
     */
    public void clear() {
        markets_.clear();
        traders_.clear();
        quantityFunctions_.clear();
        settings_.clear();
        forceStop = false;
    }

    /**
     * Set the flag to indicate if analysis should stop immediately.
     */
    @Override
    public void setForceStop(boolean forcePlanStop) {
        this.forceStop = forcePlanStop;
    }

    /**
     * Get the flag which indicates if plan should stop immediately
     */
    @Override
    public boolean getForceStop() {
        return forceStop;
    }
} // end class Economy

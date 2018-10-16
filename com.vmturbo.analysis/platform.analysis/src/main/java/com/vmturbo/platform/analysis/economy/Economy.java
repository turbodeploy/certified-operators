package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.dataflow.qual.SideEffectFree;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;

import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.ede.ActionClassifier;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.utilities.PlacementStats;

/**
 * A set of related markets and the traders participating in them.
 *
 * <p>
 *  It is responsible for creating and removing traders while simultaneously creating, updating and
 *  destroying markets while that happens.
 * </p>
 */
public final class Economy implements UnmodifiableEconomy, Serializable {
    // Fields

    // The map that associates Baskets with Markets.
    private final @NonNull Map<@NonNull @ReadOnly Basket,@NonNull Market> markets_ = new TreeMap<>();
    // The list of all Traders participating in the Economy.
    private final @NonNull List<@NonNull TraderWithSettings> traders_ = new ArrayList<>();
    // An aggregate of all the parameters configuring this economy's behavior.
    private final @NonNull EconomySettings settings_ = new EconomySettings();
    // Map of commodity resize dependency calculation by commodity type.
    private final @NonNull Map<@NonNull Integer, @NonNull List<@NonNull CommodityResizeSpecification>>
        commodityResizeDependency_ = new HashMap<>();
    // Map from raw processedCommodity -> rawCommodity
    private final @NonNull Map<@NonNull Integer, @NonNull List<@NonNull Integer>> rawMaterial_ = new HashMap<>();
    // a flag to indicate if analysis should stop immediately or not
    private volatile boolean forceStop = false;
    // The list of all Markets with at least one buyer that can move
    private final @NonNull List<@NonNull Market> marketsForPlacement_ = new ArrayList<>();
    // list of preferential traders in the economy
    private final @NonNull List<@NonNull ShoppingList> preferentialSls_ = new ArrayList<>();
    // list of shop together traders in the economy
    private final @NonNull List<@NonNull Trader> shopTogetherTraders_ = new ArrayList<>();
    private final List<TraderTO> tradersForHeadroom_ = new ArrayList<>();
    private Topology topology_;
    // the map for user to  its balance account
    private Map<Long, BalanceAccount> balanceAccountMap = new HashMap<>();;
    // Cached data

    // Cached unmodifiable view of the markets_.values() collection.
    private transient final @NonNull Collection<@NonNull Market> unmodifiableMarkets_ = Collections.unmodifiableCollection(markets_.values());
    // Cached unmodifiable view of the traders_ list.
    private final @NonNull List<@NonNull Trader> unmodifiableTraders_ = Collections.unmodifiableList(traders_);
    // Cached unmodifiable view of the preferentilShoppingLists_.
    private final @NonNull List<@NonNull ShoppingList> unmodifiablePreferentialSls_ = Collections.unmodifiableList(preferentialSls_);
    // Cached unmodifiable view of the shopTogetherTraders_ list.
    private final @NonNull List<@NonNull Trader> unmodifiableShopTogetherTraders_ =
                    Collections.unmodifiableList(shopTogetherTraders_);
    // Cached unmodifiable view of the marketsForPlacement_ list.
    private final @NonNull List<@NonNull Market> unmodifiableMarketsForPlacement_ = Collections.unmodifiableList(marketsForPlacement_);

    // An index to speed up looking for traders that satisfy the basket in a given market.
    // 32 is chosen as a stop threshold because when that scanning a list of 32 traders
    // for satisfiability may be faster than scanning all commodities in a basket for very large baskets.
    // The number is not scientifically chosen but works well in practice.
    private final @NonNull InvertedIndex sellersInvertedIndex_ = new InvertedIndex(this, 32);

    private final @NonNull Set<@NonNull CommoditySpecification> commsToAdjustOverhead_ = new HashSet<>();

    /**
     * The placement statistics associated with this {@link Economy}.
     */
    private final PlacementStats placementStats = new PlacementStats();

    private boolean marketsPopulated = false;

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

    /**
     * @return unmodifiable list of Markets that has at least one movable buyer
     *
     */
    @Override
    @Pure
    public @ReadOnly @NonNull List<@NonNull Market>
            getMarketsForPlacement(@ReadOnly Economy this) {
        return unmodifiableMarketsForPlacement_;
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

    /**
     * For all markets in the economy, scan each individual market and add all satisfying
     * sellers to that market.
     *
     * <p>
     * For an economy with M markets with T traders where the average number
     * of commodities sold by a given trader is k, the algorithm runs in O(M*T) time (the
     * bound may be lower but I cannot prove it so I chose the conservative number).
     * In practice on real topologies the algorithm runs in O(M*k) time. A survey of all
     * entities in all customer topologies gathered from customer diagnostics, the average
     * across all service entities is k=6.1.
     * </p>
     *
     * <p>
     * For an explanation of why this optimization performs well in practice, see
     * https://vmturbo.atlassian.net/wiki/pages/viewpage.action?pageId=170271654
     * </p>
     *
     * <p>
     * This method should only be run once and only after all traders have been added to the
     * economy.
     * </p>
     */
    public void populateMarketsWithSellers() {
        Preconditions.checkArgument(!marketsPopulated);

        for (Market market : markets_.values()) {
            populateMarketWithSellers(market);
        }
        marketsPopulated = true;
    }

    /**
     * Add all satisfying sellers to the {@link Market} being evaluated.
     *
     * @param market for which sellers are to be populated
     */
    public void populateMarketWithSellers(Market market) {
        sellersInvertedIndex_.getSatisfyingTraders(market.getBasket()).forEach(
                seller -> market.addSeller((TraderWithSettings) seller)
        );
    }

    /**
     * returns an unmodifiable list of preferential shoppingLists
     *
     * @param this the economy that the preferential shoppingLists participate in
     * @return an unmodifiable list of preferential shoppingLists
     */
    @Override
    @Pure
    public @NonNull @ReadOnly List<@NonNull @ReadOnly ShoppingList> getPreferentialShoppingLists(@ReadOnly Economy this) {
        return unmodifiablePreferentialSls_;
    }

    /**
     * returns a modifiable list of Idle VMs
     *
     * @param this the economy that the preferential ShoppingLists participate in
     * @return a modifiable list of preferential {@link ShoppingList}s
     */
    public List<ShoppingList> getModifiablePreferentialSls() {
        return preferentialSls_;
    }

    /**
     * returns a modifiable list of shop together traders
     *
     * @param this the economy that the shop together traders participate in
     * @return a modifiable list of shop together traders
     */
    public List<Trader> getModifiableShopTogetherTraders() {
        return shopTogetherTraders_;
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
     * returns an unmodifiable list of shop together traders
     *
     * @param this the economy that the shop together traders participate in
     * @return an unmodifiable list of shop together traders
     */
    @Override
    @Pure
    public @NonNull @ReadOnly List<@NonNull @ReadOnly Trader>
                    getShopTogetherTraders(@ReadOnly Economy this) {
        return unmodifiableShopTogetherTraders_;
    }

    /**
     * Creates a new {@link Trader trader} with the given characteristics and adds it to
     * {@code this} economy.
     *
     * <p>
     *  Ignoring cliques, it is an approximately constant time operation.
     * </p>
     *
     * <p>
     *  New traders are always added to the end of the {@link #getTraders() traders list}.
     * </p>
     *
     * <p>
     * Note well that adding a trader does NOT add that trader as a seller in the markets that
     * trader satisfies. Instead, call {@link #populateMarketsWithSellers()} after adding all
     * traders to the economy and this will match all traders with the markets in which they
     * should sell.
     * </p>
     *
     * @param type The type of the trader (e.g. host or virtual machine) represented as an integer.
     * @param state The initial state of the new trader.
     * @param basketSold The basket that will be sold by the new trader.
     * @param cliques The numbers of the k-partite cliques the new trader should be a member of.
     * @return The newly created trader, so that its properties can be updated.
     */
    public @NonNull Trader addTrader(int type, @NonNull TraderState state, @NonNull Basket basketSold,
                                     @NonNull Collection<@NonNull Long> cliques) {
        TraderWithSettings newTrader = new TraderWithSettings(traders_.size(), type, state, basketSold);

        // Populate cliques list before adding to markets
        newTrader.getModifiableCliques().addAll(cliques);

        // update traders list
        traders_.add(newTrader);
        sellersInvertedIndex_.add(newTrader);

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
     *  O(B) term, where B is the number of baskets bought.
     * </p>
     *
     * @param basketsBought The baskets the new trader should buy. {@link ShoppingList}s will be
     *                      created for each one and will appear in the {@link #getMarketsAsBuyer(Trader)}
     *                      map in the same order.
     */
    public @NonNull Trader addTrader(int type, @NonNull TraderState state, @NonNull Basket basketSold,
                                     @NonNull Basket... basketsBought) {
        @NonNull Trader newTrader = addTrader(type, state, basketSold, Longs.asList());

        // Add as buyer
        for (Basket basketBought : basketsBought) {
            addBasketBought(newTrader, basketBought);
        }

        return newTrader;
    }

    /**
     * Provides the same behavior as {@link #addTrader(int, TraderState, Basket, Collection)}
     * but also adds the trader to the markets associated with the basketSold. The modelSeller
     * must already be a member of the economy.
     *
     * If the model seller belongs to m markets and there are M markets in the overall economy,
     * this method has complexity O(m) when the modelSeller's basket is equal to the new
     * trader's basket sold and complexity O(M) when the modelSeller's basket is different from
     * the new trader's basket sold.
     *
     * @param modelSeller The seller whose properties match the trader to be added.
     * @param state state of the new seller
     * @param basketSold basket sold of the new seller
     * @param cliques a set of cliques the new seller is part of
     * @return the new seller
     */
    public @NonNull Trader addTraderByModelSeller(@NonNull Trader modelSeller,
                    @NonNull TraderState state, @NonNull Basket basketSold,
                    @NonNull Collection<@NonNull Long> cliques) {
        @NonNull
        Trader newTrader = addTrader(modelSeller.getType(), state, basketSold, cliques);

        // necessary in order to add the new seller to the list of active sellers
        // available for placement
        newTrader.getSettings().setCanAcceptNewCustomers(modelSeller.getSettings().
                        canAcceptNewCustomers());

        Collection<Market> marketsToScan = basketSold.equals(modelSeller.getBasketSold()) ?
            getMarketsAsSeller(modelSeller) : markets_.values();

        for (Market market : marketsToScan) {
            if (market.getBasket().isSatisfiedBy(basketSold)) {
                market.addSeller((TraderWithSettings) newTrader);
            }
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
        checkArgument(traders_.remove(traderToRemove), "traderToRemove = {}", traderToRemove);
        sellersInvertedIndex_.remove(traderToRemove);

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
        return Collections.unmodifiableMap(((TraderWithSettings) trader).getMarketsAsBuyer());
    }

    @Override
    @Pure
    public @NonNull @ReadOnly List<@NonNull @ReadOnly Market> getMarketsAsSeller(@ReadOnly Economy this,
                                                                                 @NonNull @ReadOnly Trader trader) {
        return Collections.unmodifiableList(((TraderWithSettings) trader).getMarketsAsSeller());
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
     * @param shoppingList An existing shopping list to use when adding the buyer.  Can be null.
     *                     If null, a new shopping list will be created.
     * @return The shopping list of the buyer in the market corresponding to the
     *         basketBought.  If an existing shopping list was passed in, that will be returned.
     */
    public @NonNull ShoppingList addBasketBought(@NonNull Trader buyer,
                                                 @NonNull @ReadOnly Basket basketBought,
                                                 ShoppingList shoppingList) {
        // create a market if it doesn't already exist.
        Market market = markets_.get(basketBought);
        if (market == null) {
            market = new Market(basketBought);

            markets_.put(basketBought, market);
        }

        // add the buyer to the correct market.
        if (shoppingList != null) {
            return market.addBuyer((@NonNull TraderWithSettings) buyer, shoppingList);
        } else {
            return market.addBuyer((@NonNull TraderWithSettings)buyer);
        }
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
    public @NonNull ShoppingList addBasketBought(@NonNull Trader buyer,
                                                 @NonNull @ReadOnly Basket basketBought) {
        return addBasketBought(buyer, basketBought, null);
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
        sellersInvertedIndex_.clear();
        settings_.clear();
        preferentialSls_.clear();
        commodityResizeDependency_.clear();
        rawMaterial_.clear();
        marketsForPlacement_.clear();
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

    /**
     * retrieve the {@link Trader} (that was part of the plan scope) that "trader" is a clone of
     * if a clone is of an existing clone (that is cloned from an entity in the scope), we return
     * the entity in the scope
     */
    @Override
    public Trader getCloneOfTrader (Trader trader) {
        while (trader.isClone()) {
            trader = this.getTraders().get(trader.getCloneOf());
        }
        return trader;
    }

    /**
     * sort the buyers of all markets based on the current quote (on-prem) or
     * current cost (cloud) sorted high to low. The list buyers_ is updated for
     * each market.
     */
    public void sortBuyersofMarket() {
        for (Market market : getMarkets()) {
            market.sortBuyers(this);
        }
    }

    /**
     * create a list containing a  subset of Markets that have atleast one trader that is movable
     */
    public void composeMarketSubsetForPlacement() {
        marketsForPlacement_.clear();
        getMarkets().stream().filter(market -> market.getBuyers().stream().filter(sl ->
            sl.isMovable()).count() != 0).collect(Collectors.toCollection(() ->
            marketsForPlacement_));
    }

    /**
     * Create a minimal clone of the economy for the purpose of simulating {@link Move} actions
     * and figuring whether the action is executable or not. It is "minimal" in the sense that
     * it only has the properties necessary for that simulation, namely traders, shopping lists,
     * and commodities sold, and nothing else.
     * @see {@link ActionClassifier}
     * @return a minimal clone of {@code this} economy
     */
    public @NonNull Economy simulationClone() {
        Economy clone = new Economy();
        for (Trader trader : getTraders()) {
            clone.addTrader(trader.getType(), trader.getState(), new Basket(trader.getBasketSold()));
        }
        getTraders().stream()
            .forEach(clone::simulationCloneTrader);
        return clone;
    }

    protected static final String SIM_CLONE_SUFFIX = " SIMCLONE";

    /**
     * Clone a trader. Notice that the method is called from the clone economy and the
     * argument is a trader from the original economy.
     * @param trader the trader to clone
     */
    private void simulationCloneTrader(Trader trader) {
        Trader cloneTrader = getTraders().get(trader.getEconomyIndex());
        cloneTrader.setDebugInfoNeverUseInCode(
                trader.getDebugInfoNeverUseInCode() + SIM_CLONE_SUFFIX);
        cloneTrader.getSettings().setQuoteFunction(trader.getSettings().getQuoteFunction());
        cloneTrader.getSettings().setBalanceAccount(trader.getSettings().getBalanceAccount());
        cloneTrader.getSettings().setCostFunction(trader.getSettings().getCostFunction());
        cloneCommoditiesSold(trader, cloneTrader);
        cloneShoppingLists(trader, cloneTrader);
    }

    /**
     * Clones the commodities sold from one trader (the original) to its clone.
     * @param trader the original trader
     * @param cloneTrader the clone of the original trader
     */
    private void cloneCommoditiesSold(Trader trader, Trader cloneTrader) {
        List<CommoditySold> commoditiesSold = trader.getCommoditiesSold();
        List<CommoditySold> cloneCommoditiesSold = cloneTrader.getCommoditiesSold();
        for (int commIndex = 0; commIndex < commoditiesSold.size(); commIndex++) {
            CommoditySold commSold = commoditiesSold.get(commIndex);
            CommoditySold cloneCommSold = cloneCommoditiesSold.get(commIndex);
            cloneCommSold.setCapacity(commSold.getCapacity());
            cloneCommSold.setQuantity(commSold.getQuantity());
            cloneCommSold.setPeakQuantity(commSold.getPeakQuantity());
            cloneCommSold.getSettings().setPriceFunction(commSold.getSettings().getPriceFunction());
            cloneCommSold.getSettings().setUpdatingFunction(commSold.getSettings().getUpdatingFunction());
        }
    }

    /**
     * Clones the shopping lists bought from one trader (the original) to its clone.
     * @param trader the original trader
     * @param cloneTrader the clone of the original trader
     */
    private void cloneShoppingLists(Trader trader, Trader cloneTrader) {
        Set<ShoppingList> shoppingListsOfTrader = getMarketsAsBuyer(trader).keySet();
        for (ShoppingList sl : shoppingListsOfTrader) {
            Basket cloneBasketBought = sl.getBasket(); // reuse baskets in original and clone
            ShoppingList cloneShoppingList = addBasketBought(cloneTrader, cloneBasketBought);
            cloneShoppingList.setShoppingListId(sl.getShoppingListId());
            double[] quantities = sl.getQuantities();
            double[] peakQuantities = sl.getPeakQuantities();
            double[] cloneQuantities = cloneShoppingList.getQuantities();
            double[] clonePeakQuantities = cloneShoppingList.getPeakQuantities();
            for (int q = 0; q < quantities.length; q++) {
                // Setters contain extra logic that that may prevent setting of exact values.
                // Hence, copy values directly in clone arrays for quantities and peakQuantities.
                // s.t clone has exactly same values.
                cloneQuantities[q] = quantities[q];
                clonePeakQuantities[q] = peakQuantities[q];
            }
            // find supplier in clone economy and move sl to that supplier.
            if (sl.getSupplier() != null) {
                int economyIndexofSupplier = sl.getSupplier().getEconomyIndex();
                Trader cloneSupplier = getTraders().get(economyIndexofSupplier);
                cloneShoppingList.move(cloneSupplier);
            }
        }
    }

    /**
     * list of traderTOs
     * @return list of {@link TraderTO}s
     */
    @Override
    public List<TraderTO> getTradersForHeadroom() {
        return tradersForHeadroom_;
    }

    /**
     * save the {@link Topology} associated with this {@link Economy}
     */
    @Override
    public void setTopology(Topology topology) {
        topology_ = topology;
    }

    /**
     * @return return the {@link Topology} associated with this {@link Economy}
     */
    @Override
    @Nullable
    public Topology getTopology() {
        return topology_;
    }

    /**
     * @return return the balance account map associates with this {@link Economy}
     */
    @Override
    public Map<Long, BalanceAccount> getBalanceAccountMap() {
        return balanceAccountMap;
    }


    public Set<CommoditySpecification> getCommsToAdjustOverhead() {
        return commsToAdjustOverhead_;
    }

    /**
     * Get the inverted index that maps sellers to markets.
     *
     * @return the inverted index that maps sellers to markets.
     */
    public InvertedIndex getSellersInvertedIndex() {
        return sellersInvertedIndex_;
    }

    /**
     * Find the common cliques for a given trader. Consider only markets
     * with non-empty cliques maps.
     *
     * @param trader the trader for which we need the common cliques
     * @return a set containing common clique numbers
     */
    @VisibleForTesting
    Set<Long> getCommonCliquesNonEmpty(Trader trader) {
        return getCommonCliques(trader, false);
    }

    /**
     * Find the common cliques for a given trader. Consider all markets,
     * regardless of whether their cliques map is empty or not.
     *
     * @param trader the trader for which we need the common cliques
     * @return a set containing common clique numbers
     */
    public Set<Long> getCommonCliques(Trader trader) {
        return getCommonCliques(trader, true);
    }

    /**
     * Find the common cliques for a given trader.
     *
     * @param trader the trader for which we need the common cliques
     * @param allMarkets whether to consider all markets in the construction ({@code true} value)
     * or just markets with non-empty cliques maps ({@code false} value).
     * @return a set containing common clique numbers
     */
    private Set<Long> getCommonCliques(Trader trader, boolean allMarkets) {
        return getMarketsAsBuyer(trader).entrySet().stream()
                // when allMarkets is true, the filter passes for all entries,
                // otherwise the second term in the boolean expression is evaluated
                .filter(entry -> allMarkets || !entry.getValue().getCliques().isEmpty())
                 // if shopping list is movable
                .map(entry -> entry.getKey().isMovable()
                    // use the cliques of the market
                    ? entry.getValue().getCliques().keySet()
                     // else if shopping list is placed
                    : (new TreeSet<>(entry.getKey().getSupplier() != null
                             // use clique that contain supplier
                            ? entry.getKey().getSupplier().getCliques()
                             // else there is no valid placement.
                            : Collections.emptyList())))
                .reduce(Sets::intersection).orElse(Collections.emptySet());
    }

    /**
     * A mapping (represented as list of entries) from shopping list to market,
     * including only movable shopping lists.
     *
     * @param trader the trader for which to compute the mapping
     * @return a list of entries of shopping list to market mappings
     */
    public List<Entry<ShoppingList, Market>> moveableSlByMarket(Trader trader) {
        return getMarketsAsBuyer(trader).entrySet().stream()
            .filter(e -> e.getKey().isMovable())
            .collect(Collectors.toList());
    }

    /**
     * Compute all the potential sellers of a trader.
     * A trader can be a buyer in multiple markets. Some of these markets would have
     * a non-empty cliques map and for some the map will be empty. If for a given market
     * the cliques map is non-empty then potential sellers are those that are members of the
     * cliques that repeat in all such markets. If for a given market the cliques map is empty
     * then potential sellers are all the active sellers in this market.
     *
     * @param trader the trader for which to compute the sellers
     * @return all the sellers that this trader is buying from
     */
    @Override
    public Set<Trader> getPotentialSellers(Trader trader) {
        Collection<Market> markets = getMarketsAsBuyer(trader).values();
        Set<Trader> nonCliqueSellers = new HashSet<Trader>();
        markets.stream()
            .filter(market -> market.getCliques().isEmpty())
            .forEach(m -> {
                nonCliqueSellers.addAll(m.getActiveSellers());
                nonCliqueSellers.addAll(m.getInactiveSellers());
            });
        Set<Long> cliques = getCommonCliquesNonEmpty(trader);
        if (cliques.isEmpty()) {
            return nonCliqueSellers;
        }
        Set<Trader> cliqueSellers = markets.stream()
            .map(Market::getCliques)
            .map(Map::entrySet)
            .flatMap(Set::stream)
            .filter(entry -> cliques.contains(entry.getKey()))
            .map(Entry::getValue)
            .flatMap(List::stream)
            .collect(Collectors.toSet());
        return Sets.union(cliqueSellers, nonCliqueSellers);
    }

    /**
     * Get the placement statistics associated with this economy.
     *
     * @return {@link PlacementStats} associated with this economy.
     */
    @Override
    @NonNull
    public PlacementStats getPlacementStats() {
        return placementStats;
    }
} // end class Economy

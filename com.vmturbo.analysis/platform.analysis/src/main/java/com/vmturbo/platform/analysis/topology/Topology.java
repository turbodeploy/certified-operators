package com.vmturbo.platform.analysis.topology;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.ByProducts;
import com.vmturbo.platform.analysis.economy.CommodityResizeSpecification;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.RawMaterials;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;

/**
 * Encapsulates an {@link Economy} together with a number of auxiliary maps, for use by code that
 * interfaces with the outside world. e.g. receiving a topology from the network and/or loading it
 * from a file.
 *
 * <p>
 *  It maintains the relationship between {@link Trader}s and {@link ShoppingList}s and their
 *  corresponding OIDs so that this information can be used to send back {@link Action}s that need
 *  to refer to them across process boundaries.
 * </p>
 *
 * <p>
 *  It is responsible for keeping the managed maps and economy consistent with each other.
 * </p>
 */
public final class Topology implements Serializable {
    // Fields
    private final @NonNull Economy economy_ = new Economy(); // The managed economy.
    private final @NonNull Map<@NonNull Long, @NonNull Trader> tradersByOid_ = new HashMap<>();
    private long provisionedShoppingListIndex_ = 0;
    private final @NonNull BiMap<@NonNull ShoppingList, @NonNull Long> shoppingListOids_ = HashBiMap.create();
    // A map from OIDs of traders we haven't seen yet to shopping lists that need to be
    // placed on them. It is needed if we can receive a customer before its supplier.
    private final @NonNull Map<@NonNull Long, @NonNull List<@NonNull ShoppingList>> danglingShoppingLists_ = new HashMap<>();
    // A map to record the newly provisioned trader's shopping list to the provisioned trader
    private final @NonNull Map<@NonNull Long, @NonNull Long> newShoppingListToBuyerMap_ = new HashMap<>();
    // Id that uniquely identifies the topology
    private long topologyId_;

    // Cached data

    // Cached unmodifiable view of the tradersByOid_ Map.
    private final @NonNull Map<@NonNull Long, @NonNull Trader>
        unmodifiableTradersByOid_ = Collections.unmodifiableMap(tradersByOid_);
    // Cached unmodifiable view of the shoppingListOids_ BiMap.
    private final @NonNull BiMap<@NonNull ShoppingList, @NonNull Long>
        unmodifiableShoppingListOids_ = Maps.unmodifiableBiMap(shoppingListOids_);
    // Cached unmodifiable view of the danglingShoppingLists_ Map.
    private final @NonNull Map<@NonNull Long, @NonNull List<@NonNull ShoppingList>>
        unmodifiableDanglingShoppingLists_ = Collections.unmodifiableMap(danglingShoppingLists_);
    // Cached unmodifiable view of the newShoppingListToBuyerMap.
    private final @NonNull Map<@NonNull Long, @NonNull Long>
        unmodifiableNewShoppingListToBuyerMap_ = Collections.unmodifiableMap(newShoppingListToBuyerMap_);

    // Constructors

    /**
     * Constructs an empty Topology
     */
    public Topology() {
        economy_.setTopology(this);
    }

    // Methods

    /**
     * Adds a new trader to the topology.
     *
     * @param oid The OID of the new trader.
     * @param type The numeric type of the new trader.
     * @param state The state of the new trader. e.g. active or inactive
     * @param basketSold The basket sold by the new trader.
     * @param cliques The numbers of the k-partite cliques the new trader should be a member of.
     * @return The trader newly added to {@code this} topology.
     *
     * @see Economy#addTrader(int, TraderState, Basket, Basket...)
     */
    public @NonNull Trader addTrader(long oid, int type, @NonNull TraderState state, @NonNull Basket basketSold,
                                     @NonNull Collection<@NonNull Long> cliques) {
        @NonNull Trader trader = economy_.addTrader(type, state, basketSold, cliques);
        trader.setOid(oid);
        tradersByOid_.put(oid, trader);

        // Check if the topology already contains shopping lists that refer to this trader...
        List<@NonNull ShoppingList> shoppingLists = danglingShoppingLists_.get(oid);
        if (shoppingLists != null) {
            for (ShoppingList shoppingList : shoppingLists) {
                // ...and place them on this trader.
                shoppingList.move(trader);
            }

            danglingShoppingLists_.remove(oid);
        }

        return trader;
    }

    /**
     * Populates the sellers in the markets and merge's coverage of traders part of scalingGroups.
     *
     * <p>
     * This method should only be run once and only after all traders have been added to the economy.
     * </p>
     */
    public void populateMarketsWithSellersAndMergeConsumerCoverage() {
        economy_.populateMarketsWithSellersAndMergeConsumerCoverage();
    }

    /**
     * Adds a new basket bought to the specified buyer.
     *
     * @param oid The oid that should be associated with the new shopping list that will be
     *            created for the basket bought.
     * @param buyer The buyer that should start buying the new basket.
     * @param basketBought The basket that <b>buyer</b> should start buying.
     * @return The newly created shopping list of <b>buyer</b> in the market corresponding to
     *         basket bought.
     *
     * @see Economy#addBasketBought(Trader, Basket)
     */
    public @NonNull ShoppingList addBasketBought(long oid, @NonNull Trader buyer, @NonNull Basket basketBought) {
        @NonNull ShoppingList shoppingList = economy_.addBasketBought(buyer, basketBought);
        shoppingListOids_.put(shoppingList, oid);

        return shoppingList;
    }

    /**
     * Adds a new basket bought to the specified buyer, that should be provided by a particular
     * supplier.
     *
     * <p>
     *  Clients should use this method if the supplier for the shopping list might not have
     *  been added to {@code this} topology yet, so that the topology can automatically update the
     *  reference to the supplier when the supplier is finally added.
     * </p>
     *
     * @param oid Same as for {@link #addBasketBought(long, Trader, Basket)}
     * @param buyer Same as for {@link #addBasketBought(long, Trader, Basket)}
     * @param basketBought Same as for {@link #addBasketBought(long, Trader, Basket)}
     * @param supplierOid The OID of the trader that should become the supplier of the buyer
     *                    shopping list that will be created for the new basket bought.
     * @return Same as for {@link #addBasketBought(long, Trader, Basket)}
     *
     * @see Economy#addBasketBought(Trader, Basket)
     * @see #addBasketBought(long, Trader, Basket)
     */
    public @NonNull ShoppingList addBasketBought(long oid, @NonNull Trader buyer,
                                                       @NonNull Basket basketBought, long supplierOid) {
        @NonNull ShoppingList shoppingList = addBasketBought(oid, buyer, basketBought);

        // Check whether the supplier has been added to the topology...
        @NonNull Trader supplier = tradersByOid_.get(supplierOid);
        if (supplier != null) {
            shoppingList.move(supplier);
        } else {
            // ...and if not, make a note of the fact so that we can update the shopping list
            // when it's added.
            @NonNull List<@NonNull ShoppingList> shoppingLists = danglingShoppingLists_.get(supplierOid);
            if (shoppingLists == null) {
                shoppingLists = new ArrayList<>();
                danglingShoppingLists_.put(supplierOid, shoppingLists);
            }
            shoppingLists.add(shoppingList);
        }

        return shoppingList;
    }

    /**
     *
     * @return A modifiable map from commodity sold to the dependent commodities bought
     *         that have to be changed in case of a resize.
     */
    public @NonNull Map<@NonNull Integer, @NonNull List<@NonNull CommodityResizeSpecification>>
                                           getModifiableCommodityResizeDependencyMap() {
        return economy_.getModifiableCommodityResizeDependencyMap();
    }

    /**
     *
     * @return A modifiable map from commodity sold to the dependent commodities bought
     *         that have to be changed in case of a resize.
     */
    public void addToModifiableCommodityProducesDependencyMap(@NonNull Integer key, @NonNull List<@NonNull Integer> value) {
        economy_.addToModifiableCommodityProducesDependencyMap(key, value);
    }

    public @NonNull Map<Integer, Set<Integer>> getModifiableCommoditiesToIgnoreForProvisionAndSuspensionMap() {
        return economy_.getModifiableCommoditiesToIgnoreForProvisionAndSuspensionMap();
    }

    /**
    *
    * @return A modifiable map from commodity sold to the dependent commodities bought
    *         that the change to them should be skipped in case of resize based on
    *         historical usage.
    */
   public @NonNull Map<@NonNull Integer, @NonNull List<@NonNull Integer>>
                                           getModifiableHistoryBasedResizeSkipDependency() {
       return economy_.getModifiableHistoryBasedResizeDependencySkipMap();
   }

    /**
     *
     * @return A modifiable map from processed commodity to the raw commodities used by it.
     */
    public @NonNull Map<@NonNull Integer, @NonNull RawMaterials>
                                            getModifiableRawCommodityMap() {
        return economy_.getModifiableRawCommodityMap();
    }

    /**
     *
     * @return A modifiable map from processed commodity to the by-products used by it.
     */
    public @NonNull Map<@NonNull Integer, ByProducts>
                                            getModifiableByProductsMap() {
        return economy_.getModifiableByProductMap();
    }

    /**
     * Returns an unmodifiable view of the managed {@link Economy} that can be copied.
     */
    public @NonNull UnmodifiableEconomy getEconomy(@ReadOnly Topology this) {
        return economy_;
    }

    /**
     * Returns a view of the managed {@link Economy} that is used only for testing.
     * This is used for JUnit tests, use getEconomy for all other purposes.
     */
    public @NonNull Economy getEconomyForTesting() {
        return economy_;
    }

    /**
     * Returns an unmodifiable Map mapping OID to {@link Trader}.
     *
     * @return an unmodifiable Map mapping OID to {@link Trader}.
     */
    public @ReadOnly @NonNull Map<@NonNull Long, @NonNull Trader> getTradersByOid(@ReadOnly Topology this) {
        return unmodifiableTradersByOid_;
    }

    /**
     * Returns a modifiable Map mapping OID to corresponding {@link Trader}.
     *
     * @return a modifiable Map mapping OID to corresponding {@link Trader}.
     */
    public @NonNull Map<@NonNull Long, @NonNull Trader> getModifiableTraderOids() {
        return tradersByOid_;
    }

    /**
     * Returns an unmodifiable BiMap mapping {@link ShoppingList}s to their OIDs.
     */
    public @ReadOnly @NonNull BiMap<@NonNull ShoppingList, @NonNull Long>
            getShoppingListOids(@ReadOnly Topology this) {
        return unmodifiableShoppingListOids_;
    }

    /**
    * Returns a modifiable BiMap mapping {@link ShoppingList}s to their OIDs.
    */
   public @NonNull BiMap<@NonNull ShoppingList, @NonNull Long> getModifiableShoppingListOids() {
       return shoppingListOids_;
   }

    /**
     * Returns an unmodifiable Map mapping OIDs of traders that haven't been added to {@code this}
     * topology yet to lists of shopping lists that should be placed on those traders.
     *
     * <p>
     *  We called those shopping lists 'dangling' by analogy to the 'dangling' pointers which
     *  point to something that doesn't exist. (although dangling pointers point to something that
     *  used to exist, while dangling shopping lists point to something that will exist in the
     *  future)
     * </p>
     */
    public @ReadOnly @NonNull Map<@NonNull Long, @NonNull List<@NonNull ShoppingList>>
            getDanglingShoppingLists(@ReadOnly Topology this) {
        return unmodifiableDanglingShoppingLists_;
    }

    /**
     * Returns an unmodifiable Map mapping OIDs of shopping list of newly provisioned traders and
     * OIDs of its corresponding traders
     */
    public @ReadOnly @NonNull Map<@NonNull Long, @NonNull Long>
            getNewShoppingListToBuyerMap(@ReadOnly Topology this) {
        return unmodifiableNewShoppingListToBuyerMap_;
    }

    /**
     * Resets {@code this} {@link Topology} to the state it was in just after construction.
     *
     * <p>
     *  It has no other observable side-effects.
     * </p>
     */
    public void clear() {
        economy_.clear();
        tradersByOid_.clear();
        shoppingListOids_.clear();
        danglingShoppingLists_.clear();
        newShoppingListToBuyerMap_.clear();
    }

    /**
     * Assign a negative oid to the newly provisioned shopping list, update the shoppingListOids_ map,
     * update the newShoppingListToBuyer map.
     */
    public long addProvisionedShoppingList(@NonNull ShoppingList provisionedShoppingList) {
        provisionedShoppingListIndex_--;
        shoppingListOids_.put(provisionedShoppingList, provisionedShoppingListIndex_);
        // put oid of shopping list from newly provisioned trader and the oid of newly provisioned
        // trader to a map
        newShoppingListToBuyerMap_.put(provisionedShoppingListIndex_, provisionedShoppingList.getBuyer().getOid());
        return provisionedShoppingListIndex_;
    }

    /**
     * Assign an OID for newly provisioned trader and update the tradersByOid_ map. Use the
     * {@link IdentityGenerator} to give a globally unique OID.
     */
    public long addProvisionedTrader(@NonNull Trader provisionedTrader) {
        long newId = IdentityGenerator.next();
        provisionedTrader.setOid(newId);
        tradersByOid_.put(newId, provisionedTrader);
        return newId;

    }

    /**
     * adding a {@link ShoppingList} of an preferential ShoppingList in the economy
     */
    public void addPreferentialSl(@NonNull ShoppingList shoppingList) {
        economy_.getModifiablePreferentialSls().add(shoppingList);
    }

    /**
     * adding a {@link Trader} which should shop together to the shopTogetherTraders list in the economy
     */
    public void addShopTogetherTraders(@NonNull Trader trader) {
        economy_.getModifiableShopTogetherTraders().add(trader);
    }

    public long getTopologyId() {
        return topologyId_;
    }

    public void setTopologyId(long topologyId) {
        topologyId_ = topologyId;
    }

    public boolean addTradersForHeadroom(TraderTO trader) {
        return economy_.getTradersForHeadroom().add(trader);
    }

    public boolean addCommsToAdjustOverhead(CommoditySpecification cs) {
        return economy_.getCommsToAdjustOverhead().add(cs);
    }
} // end Topology class

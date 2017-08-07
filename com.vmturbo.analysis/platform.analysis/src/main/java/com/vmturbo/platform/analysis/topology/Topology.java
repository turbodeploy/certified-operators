package com.vmturbo.platform.analysis.topology;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommodityResizeSpecification;
import com.vmturbo.platform.analysis.economy.Economy;
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
    private final @NonNull BiMap<@NonNull Trader, @NonNull Long> traderOids_ = HashBiMap.create();
    private long provisionedShoppingListIndex_ = 0;
    private final @NonNull BiMap<@NonNull ShoppingList, @NonNull Long> shoppingListOids_ = HashBiMap.create();
    // A map from OIDs of traders we haven't seen yet to shopping lists that need to be
    // placed on them. It is needed if we can receive a customer before its supplier.
    private final @NonNull Map<@NonNull Long, @NonNull List<@NonNull ShoppingList>> danglingShoppingLists_ = new HashMap<>();
    // A map to record the newly provisioned trader's shoppinglist to the provisioned trader
    private final @NonNull Map<@NonNull Long, @NonNull Long> newShoppingListToBuyerMap_ = new HashMap<>();
    // Id that uniquely identifies the topology
    private long topologyId_;

    // Cached data

    // Cached unmodifiable view of the traderOids_ BiMap.
    private final @NonNull BiMap<@NonNull Trader, @NonNull Long>
        unmodifiableTraderOids_ = Maps.unmodifiableBiMap(traderOids_);
    // Cached unmodifiable view of the traderOids_ BiMap.
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
        traderOids_.put(trader, oid);

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

    public void populateMarketsWithSellers() {
        economy_.populateMarketsWithSellers();
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
     *  Clients should use this method if the supplier for the buyer participation might not have
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
        @NonNull Trader supplier = traderOids_.inverse().get(supplierOid);
        if (supplier != null) {
            shoppingList.move(supplier);
        } else {
            // ...and if not, make a note of the fact so that we can update the buyer participation
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
     * @return A modifiable map from processed commodity to the raw commodities used by it.
     */
    public @NonNull Map<@NonNull Integer, @NonNull List<@NonNull Integer>>
                                            getModifiableRawCommodityMap() {
        return economy_.getModifiableRawCommodityMap();
    }

    /**
     * Returns an unmodifiable view of the managed {@link Economy} that can be copied.
     */
    public @NonNull UnmodifiableEconomy getEconomy(@ReadOnly Topology this) {
        return economy_;
    }

    /**
     * Returns an unmodifiable BiMap mapping {@link Trader}s to their OIDs.
     */
    public @ReadOnly @NonNull BiMap<@NonNull Trader, @NonNull Long> getTraderOids(@ReadOnly Topology this) {
        return unmodifiableTraderOids_;
    }

    /**
     * Returns an unmodifiable BiMap mapping {@link ShoppingList}s to their OIDs.
     */
    public @ReadOnly @NonNull BiMap<@NonNull ShoppingList, @NonNull Long>
            getShoppingListOids(@ReadOnly Topology this) {
        return unmodifiableShoppingListOids_;
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
        traderOids_.clear();
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
        newShoppingListToBuyerMap_.put(provisionedShoppingListIndex_, traderOids_
                        .get(provisionedShoppingList.getBuyer()));
        return provisionedShoppingListIndex_;
    }

    /**
     * Assign an OID for newly provisioned trader and update the traderOids_ map. Use the
     * {@link IdentityGenerator} to give a globally unique OID.
     */
    public long addProvisionedTrader(@NonNull Trader provisionedTrader) {
        long newId = IdentityGenerator.next();
        traderOids_.put(provisionedTrader, newId);
        return newId;

    }

    /**
     * Add the given OID for newly provisioned trader to the traderOids_ map. This OID must have
     * been generated with {@link IdentityGenerator} to give a globally unique OID.
     *
     * @param provisionedTrader The provisioned {@link Trader}
     * @param newOid The alias OID that the trader should be associated with
     * @return the old OID if any
     */
    public long addProvisionedTrader(@NonNull Trader provisionedTrader, long newOid) {
        long oldId = -1;
        if (traderOids_.containsKey(provisionedTrader)) {
            oldId = traderOids_.get(provisionedTrader);
        }
        traderOids_.put(provisionedTrader, newOid);
        return oldId;
    }

    /**
     * adding a {@link ShoppingList} of an idleVM to the idleVMs list in the economy
     */
    public void addIdleVmSl (@NonNull ShoppingList shoppingList) {
        economy_.getModifiableIdleVmSls().add(shoppingList);
    }

    /**
     * adding a {@link trader} which should shop together to the shopTogetherTraders list in the economy
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

} // end Topology class

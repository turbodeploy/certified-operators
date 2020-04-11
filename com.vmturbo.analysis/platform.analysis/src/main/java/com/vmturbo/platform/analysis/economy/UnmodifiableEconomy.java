package com.vmturbo.platform.analysis.economy;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nullable;

import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.dataflow.qual.SideEffectFree;

import com.vmturbo.platform.analysis.economy.Context.BalanceAccount;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.analysis.utilities.PlacementStats;

/**
 * An unmodifiable view of an {@link Economy}.
 *
 * <p>
 *  It includes all read-only operations.
 * </p>
 */
public interface UnmodifiableEconomy {

    /**
     * The {@link EconomySettings settings} parameterizing {@code this} economy's behavior.
     */
    @Pure
    @NonNull @PolyRead EconomySettings getSettings(@PolyRead UnmodifiableEconomy this);

    /**
     * Returns an unmodifiable list of the {@link Market markets} currently present in the economy.
     *
     * <p>
     *  This changes dynamically as new {@link Trader traders} are added and/or removed from the
     *  economy. It is an O(1) operation.
     * </p>
     *
     * <p>
     *  Whether the returned list will be updated or not after it is returned and a call to
     *  add/removeTrader and/or add/removeCommodityBought is made, is undefined.
     * </p>
     */
    @Pure
    @NonNull @ReadOnly Collection<@NonNull @ReadOnly Market> getMarkets(@ReadOnly UnmodifiableEconomy this);

    /**
     * Returns the {@link Market market} where the commodities specified by the given
     * {@link Basket basket bought} are traded.
     *
     * @param basket The basket bought by some trader in the market. If it is not bought by any
     *               trader in {@code this} economy, the results are undefined.
     * @return The market where the commodities specified by the basket are traded.
     */
    @Pure
    @NonNull @ReadOnly Market getMarket(@ReadOnly UnmodifiableEconomy this,@NonNull Basket basket);

    /**
     * Returns the {@link Market market} that created and owns the given {@link ShoppingList
     * shopping list}.
     *
     * <p>
     *  If given shopping list has been invalidated, the results are undefined. The latter
     *  can happen for example if the associated buyer is removed from the economy or the market
     *  that owned the shopping list.
     * </p>
     *
     * @param shoppingList The valid shopping list for which the market should be returned.
     * @return The market that created and owns the shopping list.
     */
    @Pure
    @NonNull @ReadOnly Market getMarket(@ReadOnly UnmodifiableEconomy this,@NonNull ShoppingList shoppingList);

    /**
     * Returns an unmodifiable list of the {@link CommodityBought commodities} the given
     * {@link ShoppingList shopping list} is buying in {@code this} economy.
     *
     * <p>
     *  If the given shopping list is not currently buying these commodities from anyone, then
     *  they just represent the quantities and peak quantities the buyer intends to buy.
     * </p>
     *
     * <p>
     *  The commodities bought, are returned in the same order that quantities and peak quantities
     *  appear in the respective vectors, which in turn is the same as the order in which the
     *  commodity specifications appear in the respective basket bought.
     * </p>
     *
     * <p>
     *  The returned commodities remains valid for as long as the shopping list remains valid.
     *  After this point the results of using them are undefined.
     * </p>
     */
    @SideEffectFree
    @NonNull @ReadOnly List<@NonNull CommodityBought> getCommoditiesBought(@ReadOnly UnmodifiableEconomy this,
                                                            @NonNull ShoppingList shoppingList);

    /**
     * Returns the {@link CommodityBought commodity} bought by the given {@link ShoppingList
     * shopping list} and specified by the given {@link CommoditySpecification commodity
     * specification}.
     *
     * <p>
     *  It remains valid for as long as the shopping list remains valid. After this point the
     *  results of using it are undefined.
     * </p>
     *
     * @param shoppingList The shopping list buying the returned commodity.
     * @param specification The specification specifying the returned commodity. It must be in the
     *                      basket bought by shopping list.
     * @return The commodity bought by the given shopping list and specified by the given
     *         commodity specification.
     */
    @SideEffectFree
    @NonNull @PolyRead CommodityBought getCommodityBought(@PolyRead UnmodifiableEconomy this,
        @NonNull @PolyRead ShoppingList shoppingList, @NonNull @ReadOnly CommoditySpecification specification);

    /**
     * Returns an unmodifiable list of all the {@link Trader traders} currently participating in the
     * economy.
     *
     * <p>
     *  This changes dynamically as new {@link Trader traders} are added and/or removed from the
     *  economy. It is an O(1) operation.
     * </p>
     *
     * <p>
     *  Whether the returned list will be updated or not after it is returned and a call to
     *  add/removeTrader is made, is undefined.
     * </p>
     */
    @Pure
    @NonNull @ReadOnly List<@NonNull @ReadOnly Trader> getTraders(@ReadOnly UnmodifiableEconomy this);

    /**
     * Returns an unmodifiable list of the given trader's suppliers.
     *
     * <p>
     *  It may contain the same supplier multiple times, one for each shopping list of the
     *  trader that has the same supplier.
     * </p>
     *
     * <p>
     *  A trader is a supplier of another trader, iff the former is currently selling some commodity
     *  to the latter.
     * </p>
     */
    @Pure
    @NonNull @ReadOnly List<@NonNull @ReadOnly Trader> getSuppliers(@ReadOnly UnmodifiableEconomy this,
                                                                    @NonNull @ReadOnly Trader trader);

    /**
     * Returns an unmodifiable map of the markets the given trader participates in as a buyer.
     *
     * <p>
     *  It maps shopping lists to the markets the trader participates in with these shopping lists.
     *  The iteration order of the returned map is guaranteed to be deterministic.
     * </p>
     */
    @Pure
    @NonNull @ReadOnly Map<@NonNull ShoppingList, @NonNull Market> getMarketsAsBuyer(
        @ReadOnly UnmodifiableEconomy this, @NonNull @ReadOnly Trader trader);

    /**
     * Returns an unmodifiable list of the markets the given trader participates in as a seller.
     */
    @Pure
    @NonNull @ReadOnly List<@NonNull @ReadOnly Market> getMarketsAsSeller(@ReadOnly UnmodifiableEconomy this,
                                                                          @NonNull @ReadOnly Trader trader);

    /**
     * Returns an unmodifiable List of commodityType that are rawMaterials of a particular processed type
     */
    @NonNull @ReadOnly List<Integer> getRawMaterials(int processedCommodityType);

    /**
     *
     * @param processedCommodityType The commodity base type.
     * @return An unmodifiable List of Commodity Resize Specification for the provided type.
     */
    @NonNull
    @ReadOnly
    List<@NonNull CommodityResizeSpecification> getResizeDependency(int processedCommodityType);

    @NonNull
    @ReadOnly
    List<@NonNull Integer> getHistoryBasedResizeSkippedDependentCommodities(int processedCommodityType);

    @ReadOnly
    boolean getForceStop();

    @Pure
    void setForceStop(boolean forcePlanStop);

    /**
     * @return An unmodifiable List of Markets that have at least one movable trader
     */
    @ReadOnly
    @NonNull
    List<@NonNull Market> getMarketsForPlacement();

    /**
     * @return the {@link Trader} from which "trader" is cloned
     */
    Trader getCloneOfTrader(Trader trader);

    /**
     * @return An unmodifiable List of preferential {@link ShoppingList}s
     */
    @NonNull
    @ReadOnly
    List<@NonNull @ReadOnly ShoppingList> getPreferentialShoppingLists();

    /**
     * @return An unmodifiable list of shop together VMs
     */
    @NonNull
    @ReadOnly
    List<@NonNull @ReadOnly Trader> getShopTogetherTraders();

    /**
     * @return list of {@link TraderTO}s
     */
    List<TraderTO> getTradersForHeadroom();

    /**
     * @return balance account map associates with the {@link Economy}
     */
    Map<Long, BalanceAccount> getBalanceAccountMap();

    /**
     * Sets the {@link Topology} associated with this {@link Economy}.
     *
     * @param topology The new associated topology
     */
    void setTopology(Topology topology);

    /**
     * Return the {@link Topology} associated with this {@link Economy}.
     *
     * @return return the {@link Topology} associated with this {@link Economy}
     */
    @Nullable
    @ReadOnly
    Topology getTopology();

    /**
     * Compute all sellers that this trader can buy from.
     *
     * @param trader the trader for which to compute the set.
     * @return all the traders that the argument trader can buy from.
     */
    @NonNull
    Set<Trader> getPotentialSellers(Trader trader);

    /**
     * Get the placement statistics associated with this economy.
     *
     * @return {@link PlacementStats} associated with this economy.
     */
    @NonNull
    PlacementStats getPlacementStats();

    /**
     * Get the placement entities for deploy market.
     *
     * @return placement entities for deploy market.
     */
    List<Trader> getPlacementEntities();

    /**
     * Get the peer {@link ShoppingList} for the leader of a scaling group.
     *
     * @param shoppingList shopping list to check.
     * @return list of peer {@link ShoppingList}s
     */
    List<ShoppingList> getPeerShoppingLists(ShoppingList shoppingList);

    /**
     * Return whether the shopping list's scaling group is currently consistently sized.
     * @param shoppingList shopping list to check
     * @return true if the scaling group is currently consistently sized, or false if it is not.
     */
    boolean isScalingGroupConsistentlySized(final ShoppingList shoppingList);

    /**
     * Return the scaling group peer information for a given shopping list.
     *
     * @param shoppingList shopping list to check
     * @return if the shopping list belongs to a scaling group, return the peer information for
     * the scaling group, which includes whether the group is already consistently sized and the
     * list of peer shopping lists.  If the shopping list is null or does not belong to a scaling
     * group, dummy empty peer information will be returned instead.
     */
    ScalingGroupPeerInfo getScalingGroupPeerInfo(ShoppingList shoppingList);
} // end UnmodifiableEconomy interface

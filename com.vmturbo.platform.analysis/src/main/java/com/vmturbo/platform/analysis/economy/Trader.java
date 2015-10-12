package com.vmturbo.platform.analysis.economy;

import java.util.List;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
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
public interface Trader {

    /**
     * Returns an unmodifiable list of {@code this} seller's customers.
     */
    @Pure
    @NonNull @ReadOnly List<@NonNull @ReadOnly Trader> getCustomers(@ReadOnly Trader this);

    /**
     * Returns an unmodifiable list of {@code this} buyer's suppliers.
     */
    @Pure
    @NonNull @ReadOnly List<@NonNull @ReadOnly Trader> getSuppliers(@ReadOnly Trader this);

    /**
     * Returns the supplier of {@code this} buyer for the specified market or {@code null} if there
     * is no such supplier.
     *
     * @param market the market for which we query the supplier.
     * @return the supplier or {@code null} if {@code this} buyer is currently not buying the
     *          corresponding basket from anyone.
     */
    @Pure
    @Nullable @ReadOnly Trader getSupplier(@ReadOnly Trader this, @NonNull @ReadOnly Market market);

    /**
     * Returns an unmodifiable list of the markets {@code this} trader participates in as a buyer.
     */
    @Pure
    @NonNull @ReadOnly List<@NonNull @ReadOnly Market> getMarketsAsBuyer(@ReadOnly Trader this);

    /**
     * Returns an unmodifiable list of the markets {@code this} trader participates in as a seller.
     */
    @Pure
    @NonNull @ReadOnly List<@NonNull @ReadOnly Market> getMarketsAsSeller(@ReadOnly Trader this);

    /**
     * Returns the basket sold by {@code this} seller.
     */
    @Pure
    @NonNull @ReadOnly Basket getBasketSold(@ReadOnly Trader this);

    /**
     * Returns an unmodifiable list of the commodities {@code this} trader is selling.
     */
    @Pure
    @NonNull @ReadOnly List<@NonNull @ReadOnly CommoditySold> getCommoditiesSold(@ReadOnly Trader this);

    /**
     * Returns an unmodifiable list of the commodities {@code this} trader is buying in the given market.
     */
    @Pure
    @NonNull @ReadOnly List<@NonNull CommodityBought> getCommoditiesBought(@ReadOnly Trader this, @NonNull @ReadOnly Market market);

    /**
     * Adds a new commodity to the list of commodities sold by {@code this} seller.
     *
     * @param newCommodityType The type of the new commodity. It will be added to {@code this}
     *          seller's basket.
     * @param newCommoditySold The new commodity to be added. It will be added to {@code this}
     *          seller's commodities sold list.
     * @return {@code this}
     */
    @Deterministic
    @NonNull Trader addCommoditySold(@NonNull @ReadOnly CommodityType newCommodityType, @NonNull CommoditySold newCommoditySold);

    /**
     * Removes an existing commodity from the list of commodities sold by {@code this} seller.
     *
     * <p>
     *  A commodity sold by a single trader is uniquely identified by its type.
     *  Both the list of commodities sold and the basket sold are updated.
     * </p>
     *
     * @param typeToRemove the type of the commodity that needs to be removed.
     * @return the removed {@link CommoditySold commodity sold}.
     */
    @Deterministic // in the sense that for the same referents of this and typeToRemove the result will
    // be the same. Calling this two times on the same topology will produce different results
    // because the topology is modified.
    @NonNull CommoditySold removeCommoditySold(@NonNull @ReadOnly CommodityType typeToRemove);

    /**
     * Adds a new commodity type to a given basket bought by this buyer.
     *
     * @param basketToAddTo the basket where the new commodity type should be added.
     * @param commodityTypeToAdd the commodity type to add to the basket bought.
     * @return {@code this}
     */
    @Deterministic
    @NonNull Trader addCommodityBought(@NonNull Basket basketToAddTo, @NonNull @ReadOnly CommodityType commodityTypeToAdd);

    /**
     * Removes an existing commodity type from a given basket bought by this buyer.
     *
     * <p>
     *  Baskets contain at most one of each commodity type.
     * </p>
     *
     * @param basketToRemoveFrom the basket bought from which the commodity type should be removed.
     * @param commodityTypeToRemove the commodity type that should be removed from the basket.
     * @return {@code this}
     */
    @Deterministic
    @NonNull Trader removeCommodityBought(@NonNull Basket basketToRemoveFrom, @NonNull @ReadOnly CommodityType commodityTypeToRemove);

    // May need to add methods to add/remove baskets bought later...

    // May need to add some reference to the associated reservation later...

    /**
     * The {@link TraderSettings settings} controlling {@code this} trader's behavior.
     */
    @Pure
    @NonNull TraderSettings getSettings();

    /**
     * The {@link TraderType type} of {@code this} trader.
     */
    @Pure
    @NonNull TraderType getType();

    /**
     * Returns the current {@link TraderState state} of {@code this} trader.
     */
    @Pure
    @NonNull TraderState getState();

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
    @NonNull Trader setState(TraderState state);

} // end interface Trader

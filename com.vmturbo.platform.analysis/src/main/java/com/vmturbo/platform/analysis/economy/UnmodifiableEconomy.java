package com.vmturbo.platform.analysis.economy;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.dataflow.qual.SideEffectFree;

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
     * Returns the {@link Market market} that created and owns the given {@link BuyerParticipation
     * buyer participation}.
     *
     * <p>
     *  If given buyer participation has been invalidated, the results are undefined. The latter
     *  can happen for example if the associated buyer is removed from the economy or the market
     *  that owned the participation.
     * </p>
     *
     * @param participation The valid buyer participation for which the market should be returned.
     * @return The market that created and owns participation.
     */
    @Pure
    @NonNull @ReadOnly Market getMarket(@ReadOnly UnmodifiableEconomy this,@NonNull BuyerParticipation participation);

    /**
     * Returns an unmodifiable list of the {@link CommodityBought commodities} the given
     * {@link BuyerParticipation buyer participation} is buying in {@code this} economy.
     *
     * <p>
     *  If the given buyer participation is not currently buying these commodities from anyone, then
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
     *  The returned commodities remains valid for as long as the buyer participation remains valid.
     *  After this point the results of using them are undefined.
     * </p>
     */
    @SideEffectFree
    @NonNull @ReadOnly List<@NonNull CommodityBought> getCommoditiesBought(@ReadOnly UnmodifiableEconomy this,
                                                            @NonNull BuyerParticipation participation);

    /**
     * Returns the {@link CommodityBought commodity} bought by the given {@link BuyerParticipation
     * buyer participation} and specified by the given {@link CommoditySpecification commodity
     * specification}.
     *
     * <p>
     *  It remains valid for as long as the buyer participation remains valid. After this point the
     *  results of using it are undefined.
     * </p>
     *
     * @param participation The buyer participation buying the returned commodity.
     * @param specification The specification specifying the returned commodity. It must be in the
     *                      basket bought by participation.
     * @return The commodity bought by the given buyer participation and specified by the given
     *         commodity specification.
     */
    @SideEffectFree
    @NonNull @PolyRead CommodityBought getCommodityBought(@PolyRead UnmodifiableEconomy this,
        @NonNull @PolyRead BuyerParticipation participation, @NonNull @ReadOnly CommoditySpecification specification);

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
     *  It may contain the same supplier multiple times, one for each buyer participation of the
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
     *  It maps buyer participations to the markets the trader participates in with these
     *  participations.
     * </p>
     */
    @Pure
    @NonNull @ReadOnly Map<@NonNull BuyerParticipation, @NonNull Market> getMarketsAsBuyer(
        @ReadOnly UnmodifiableEconomy this, @NonNull @ReadOnly Trader trader);

    /**
     * Returns an unmodifiable list of the markets the given trader participates in as a seller.
     */
    @Pure
    @NonNull @ReadOnly List<@NonNull @ReadOnly Market> getMarketsAsSeller(@ReadOnly UnmodifiableEconomy this,
                                                                          @NonNull @ReadOnly Trader trader);

} // end UnmodifiableEconomy interface

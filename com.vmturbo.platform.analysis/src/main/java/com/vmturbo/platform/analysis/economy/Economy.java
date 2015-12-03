package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;
import java.lang.AssertionError;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

/**
 * A set of related markets and the traders participating in them.
 *
 * <p>
 *  It is responsible for creating and removing traders while simultaneously creating, updating and
 *  destroying markets while that happens.
 * </p>
 */
public final class Economy implements Cloneable {
    // Fields

    // The map that associates Baskets with Markets.
    private final @NonNull Map<@NonNull @ReadOnly Basket,@NonNull Market> markets_ = new TreeMap<>();
    // The list of all Traders participating in the Economy.
    private final @NonNull List<@NonNull TraderWithSettings> traders_ = new ArrayList<>();

    // Cached data

    // Cached unmodifiable view of the markets_.values() collection.
    private final @NonNull Collection<@NonNull Market> unmodifiableMarkets_ = Collections.unmodifiableCollection(markets_.values());
    // Cached unmodifiable view of the traders_ list.
    private final @NonNull List<@NonNull Trader> unmodifiableTraders_ = Collections.unmodifiableList(traders_);

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
    public @NonNull @ReadOnly Collection<@NonNull @ReadOnly Market> getMarkets(@ReadOnly Economy this) {
        return unmodifiableMarkets_;
    }

    /**
     * Returns the {@link Market market} where the commodities specified by the given
     * {@link Basket basket bought} are traded.
     *
     * @param basket The basket bought by some trader in the market. If it is not bought by any
     *               trader in {@code this} economy, the results are undefined.
     * @return The market where the commodities specified by the basket are traded.
     */
    @Pure
    public @NonNull @ReadOnly Market getMarket(@ReadOnly Economy this, @NonNull @ReadOnly Basket basket) {
        checkArgument(markets_.containsKey(basket));

        return markets_.get(basket);
    }

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
    public @NonNull @ReadOnly Market getMarket(@ReadOnly Economy this, @NonNull @ReadOnly BuyerParticipation participation) {
        checkArgument(((TraderWithSettings)getBuyer(participation)).getMarketsAsBuyer().containsValue(participation));

        return Multimaps.invertFrom(((TraderWithSettings)getBuyer(participation)).getMarketsAsBuyer(),
            ArrayListMultimap.create()).get(participation).get(0); // only one market in inverse view
    }

    /**
     * Returns the <em>economy index</em> of the given trader.
     *
     * <p>
     *  The economy index of a trader is its position in the {@link #getTraders() traders list} and
     *  it's non-increasing. It will be decreased iff a trader with lower economy index is removed
     *  from the economy.
     * </p>
     *
     * <p>
     *  This is an O(1) operation.
     * </p>
     *
     * @param trader The trader whose economy index should be returned.
     * @return The economy index of the given trader. It's non-negative.
     */
    @Pure
    public int getIndex(@ReadOnly Economy this, @NonNull @ReadOnly Trader trader) {
        return ((TraderWithSettings)trader).getEconomyIndex();
    }

    /**
     * Returns the {@link Trader buyer} associated with the given {@link BuyerParticipation buyer
     * participation}.
     *
     * <p>
     *  If the given buyer participation has been invalidated, the results are undefined. The latter
     *  can happen for example if the associated buyer has been removed from the economy.
     * </p>
     *
     * @param participation The valid buyer participation for which the buyer should be returned.
     * @return The buyer participating in a market as this buyer participation.
     */
    @Pure
    public @NonNull Trader getBuyer(@ReadOnly Economy this, @NonNull @ReadOnly BuyerParticipation participation) {
        // Note: this may throw IndexOutOfBoundsException or just return another trader if
        // participation is invalid. Hard to check here, but can update buyerIndex accordingly in
        // removeTrader.
        return traders_.get(participation.getBuyerIndex());
    }

    /**
     * Returns the {@link Trader supplier} of the given {@link BuyerParticipation buyer participation}
     * in {@code this} economy or {@code null} if there is no such supplier.
     *
     * <p>
     *  If the given buyer participation has been invalidated, the results are undefined. The latter
     *  can happen for example if the associated buyer has been removed from the economy.
     * </p>
     *
     * @param participation The valid buyer participation for which the supplier should be returned.
     * @return The supplier or {@code null} if the given buyer participation is currently not buying
     *          the corresponding basket from anyone.
     */
    @Pure
    public @Nullable @ReadOnly Trader getSupplier(@ReadOnly Economy this, @NonNull @ReadOnly BuyerParticipation participation) {
        // Note: hard to check validity here. Best way would probably set indices appropriately in
        // removeTrader.
        return participation.getSupplierIndex() == BuyerParticipation.NO_SUPPLIER
            ? null : traders_.get(participation.getSupplierIndex());
    }

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
    @Pure
    public @NonNull @ReadOnly List<@NonNull CommodityBought> getCommoditiesBought(@ReadOnly Economy this,
                                               @NonNull @ReadOnly BuyerParticipation participation) {
        @NonNull Market market = getMarket(participation);
        @NonNull List<@NonNull CommodityBought> result = new ArrayList<>(market.getBasket().size());

        for (int i = 0; i < market.getBasket().size(); ++i) { // should be same size as participation
            result.add(new CommodityBought(participation, i));
        }

        return result;
    }

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
    @Pure
    public @NonNull @PolyRead CommodityBought getCommodityBought(@PolyRead Economy this,
                                         @NonNull @PolyRead BuyerParticipation participation,
                                         @NonNull @ReadOnly CommoditySpecification specification) {
        return new CommodityBought(participation,getMarket(participation).getBasket().indexOf(specification));
    }

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
    public @NonNull @ReadOnly List<@NonNull @ReadOnly Trader> getTraders(@ReadOnly Economy this) {
        return unmodifiableTraders_;
    }

    /**
     * Creates a new {@link Trader trader} with the given characteristics and adds it to the economy.
     *
     * <p>
     *  It is an O(B*logM + M*SC) operation, where M is the number of markets present in the economy,
     *  SC is the number of commodities sold by the trader and B is the number of baskets bought, if
     *  no new markets are created as a result of adding the trader
     * </p>
     *
     * <p>
     *  If new markets have to be created as a result of adding the trader, then there is an extra
     *  O(T*BC) cost for each basket bought creating a new market, where T is the number of traders
     *  in the economy and BC is the number of commodities in the basket creating the market.
     * </p>
     *
     * <p>
     *  New traders are always added to the end of the {@link #getTraders() traders list}.
     * </p>
     *
     * @return The newly created trader, so that its properties can be updated.
     */
    @Deterministic
    public @NonNull Trader addTrader(int type, @NonNull TraderState state,
                                      @NonNull Basket basketSold, @NonNull Basket... basketsBought) {
        TraderWithSettings newTrader = new TraderWithSettings(traders_.size(), type, state, basketSold);

        // Add as buyer
        for (Basket basketBought : basketsBought) {
            addBasketBought(newTrader, basketBought);
        }

        // Add as seller
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
     * Removes an existing {@link Trader trader} from the economy.
     *
     * <p>
     *  All buyers buying from traderToRemove will afterwards buy from no-one and all sellers
     *  selling to traderToRemove will remove him from their lists of customers.
     * </p>
     *
     * <p>
     *  It is an O(T) operation, where T is the number of traders in the economy.
     *  Buyer participations of the trader are invalidated, as are the commodities bought by these
     *  participations.
     * </p>
     *
     * <p>
     *  Any buyer participations of traderToRemove and corresponding commodities bought, will become
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
        checkArgument(traders_.contains(traderToRemove));

        // Stop everyone from buying from the trader.
        for (@NonNull BuyerParticipation participation : getCustomerParticipations(traderToRemove)) {
            moveTrader(participation, null); // this is not the cheapest way, but the safest...
        }

        // Remove the trader from all markets it participated as seller
        // may need to copy list to avoid exception...
        for (Market market : ((TraderWithSettings)traderToRemove).getMarketsAsSeller()) {
            market.removeSeller((TraderWithSettings)traderToRemove);
        }

        // Remove the trader from all markets it participated as buyer
        // may need to copy the map to avoid exception.
        for (Map.Entry<Market,BuyerParticipation> entry : ((TraderWithSettings)traderToRemove).getMarketsAsBuyer().entries()) {
            entry.getKey().removeBuyerParticipation(this,entry.getValue());
            // TODO: consider invalidating the participation.
        }

        // Update indices for all buyer participations.
        for (Market market : markets_.values()) {
            for (BuyerParticipation participation : market.getBuyers()) {
                if (participation.getBuyerIndex() > getIndex(traderToRemove)) {
                    participation.setBuyerIndex(participation.getBuyerIndex() - 1);
                }
                if (participation.getSupplierIndex() > getIndex(traderToRemove)) {
                    participation.setSupplierIndex(participation.getSupplierIndex() - 1);
                }
            }
        }

        // Update economy indices of all traders and remove trader from list.
        for (TraderWithSettings trader : traders_) {
            if (trader.getEconomyIndex() > getIndex(traderToRemove)) {
                trader.setEconomyIndex(trader.getEconomyIndex() - 1);
            }
        }
        traders_.remove(traderToRemove);

        return this;
    }

    /**
     * Moves one buyer participation of a buyer to a new supplier, causing customer and supplier
     * lists to be updated.
     *
     * <p>
     *  It can be used to first position a buyer participation buying from no-one (like the one of a
     *  newly created trader) to its first supplier, or to make a buyer participation seize buying
     *  from anyone.
     * </p>
     *
     * @param participationToMove The buyer participation that should change supplier.
     * @param newSupplier The new supplier of participationToMove.
     * @return {@code this}
     */
    // TODO: if newSupplier does not really sell the required basket, should it be counted as a
    // customer?
    @Deterministic
    public @NonNull Economy moveTrader(@NonNull BuyerParticipation participationToMove,
                                       @NonNull Trader newSupplier) {
        // Update old supplier to exclude participationToMove from its customers.
        if (participationToMove.getSupplierIndex() != BuyerParticipation.NO_SUPPLIER) {
            ((TraderWithSettings)getSupplier(participationToMove)).getCustomers().remove(participationToMove);
        }

        // Update new supplier to include participationToMove to its customers.
        if (newSupplier != null) {
            ((TraderWithSettings)newSupplier).getCustomers().add(participationToMove);
            participationToMove.setSupplierIndex(getIndex(newSupplier));
        }
        else
            participationToMove.setSupplierIndex(BuyerParticipation.NO_SUPPLIER);

        return this;
    }

    /**
     * Returns an unmodifiable set of the given trader's customers.
     *
     * <p>
     *  A trader is a customer of another trader iff the former currently buys any subset of the
     *  commodities the latter is selling.
     * </p>
     *
     * @see #getCustomerParticipations(Trader)
     */
    @Pure
    public @NonNull @ReadOnly Set<@NonNull @ReadOnly Trader> getCustomers(@ReadOnly Economy this,
                                                                          @NonNull @ReadOnly Trader trader) {
        @NonNull Set<@NonNull @ReadOnly Trader> customers = new TreeSet<>();

        for (@NonNull BuyerParticipation participation : getCustomerParticipations(trader)) {
            customers.add(getBuyer(participation));
        }

        return Collections.unmodifiableSet(customers);
    }

    /**
     * Returns an unmodifiable set of the given trader's customer participations as a list.
     *
     * <p>
     *  A customer participation of a trader, is a buyer participation that has the trader as its
     *  supplier.
     * </p>
     *
     * <p>
     *  This is similar to {@link #getCustomers(Trader)}, except that if a buyer buys multiple times
     *  from the same seller, he will appear only once as a customer, but will have both of his
     *  buyer participations appear as customer participations.
     * </p>
     *
     * @see #getCustomers(Trader)
     */
    @Pure
    public @NonNull @ReadOnly List<@NonNull BuyerParticipation> getCustomerParticipations(@ReadOnly Economy this,
                                                                                          @NonNull @ReadOnly Trader trader) {
        return Collections.unmodifiableList(((TraderWithSettings)trader).getCustomers());
    }

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
    public @NonNull @ReadOnly List<@NonNull @ReadOnly Trader> getSuppliers(@ReadOnly Economy this,
                                                                           @NonNull @ReadOnly Trader trader) {
        @NonNull List<@NonNull @ReadOnly Trader> suppliers = new ArrayList<>();

        for (BuyerParticipation participation : getMarketsAsBuyer(trader).values()) {
            if (participation.getSupplierIndex() != BuyerParticipation.NO_SUPPLIER) {
                suppliers.add(getSupplier(participation));
            }
        }

        return suppliers;
    }

    /**
     * Returns an unmodifiable multimap of the markets the given trader participates in as a buyer.
     *
     * <p>
     *  It maps each market to the list of buyer participations the given trader has in the market.
     * </p>
     */
    @Pure
    public @NonNull @ReadOnly ListMultimap<@NonNull Market, @NonNull BuyerParticipation>
            getMarketsAsBuyer(@ReadOnly Economy this, @NonNull @ReadOnly Trader trader) {
        return Multimaps.unmodifiableListMultimap(((TraderWithSettings)trader).getMarketsAsBuyer());
    }

    /**
     * Returns an unmodifiable list of the markets the given trader participates in as a seller.
     */
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
     * @return The new buyer participation of the buyer in the market corresponding to the
     *         basketBought.
     */
    public @NonNull BuyerParticipation addBasketBought(@NonNull Trader buyer, @NonNull @ReadOnly Basket basketBought) {
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
     * designated by a {@link BuyerParticipation buyer participation}.
     *
     * <p>
     *  Normally you would supply the basket to be removed. But when a buyer buys the same basket
     *  multiple times, the question arises: which instance of the basket should be removed? The
     *  solution is to distinguish the baskets using the buyer participations that are unique. Then
     *  only one of the multiple instances will be removed.
     * </p>
     *
     * @param participation The buyer participation uniquely identifying the basket instance that
     *                      should be removed.
     * @return The basket that was removed.
     */
    // TODO: consider removing markets that no longer have buyers. (may keep them in case they
    // acquire again, to avoid recalculation of sellers)
    public @NonNull @ReadOnly Basket removeBasketBought(@NonNull BuyerParticipation participation) {
        @NonNull Market market = getMarket(participation);

        market.removeBuyerParticipation(this, participation);

        return market.getBasket();
    }

    /**
     * Adds a new commodity specification and corresponding commodity bought to a given buyer
     * participation, updating markets and baskets as needed.
     *
     * <p>
     *  Normally a commodity specification is added to a basket. But when a buyer buys the same
     *  basket multiple times, the question arises: which instance of the basket should the
     *  specification be added to? The solution is to distinguish the baskets using the buyer
     *  participations that are unique. Then only one of the multiple instances will be updated.
     * </p>
     *
     * @param participation The buyer participation that should be changed. It will be invalidated.
     * @param commoditySpecificationToAdd The commodity specification of the commodity that will be
     *                                    added to the buyer participation.
     * @return {@code this}
     */
    @Deterministic
    public @NonNull Economy addCommodityBought(@NonNull BuyerParticipation participation,
                                              @NonNull @ReadOnly CommoditySpecification commoditySpecificationToAdd) {
        @NonNull TraderWithSettings trader = (TraderWithSettings)getBuyer(participation);

        // remove the participation from the old market
        Basket newBasketBought = removeBasketBought(participation).add(commoditySpecificationToAdd);

        // add the trader to the new market.
        BuyerParticipation newParticipation = addBasketBought(trader, newBasketBought);

        // copy quantity and peak quantity values from old participation.
        int specificationIndex = newBasketBought.indexOf(commoditySpecificationToAdd);
        for (int i = 0 ; i < specificationIndex ; ++i) {
            newParticipation.setQuantity(i, participation.getQuantity(i));
            newParticipation.setPeakQuantity(i, participation.getPeakQuantity(i));
        }
        for (int i = specificationIndex + 1 ; i < newBasketBought.size() ; ++i) {
            newParticipation.setQuantity(i, participation.getQuantity(i-1));
            newParticipation.setPeakQuantity(i, participation.getPeakQuantity(i-1));
        }

        return this;
    }

    /**
     * Removes an existing commodity specification and corresponding commodity bought from a given
     * buyer participation, updating markets and baskets as needed.
     *
     * <p>
     *  Normally a commodity specification is removed from a basket. But when a buyer buys the same
     *  basket multiple times, the question arises: which instance of the basket should the
     *  specification be removed from? The solution is to distinguish the baskets using the buyer
     *  participations that are unique. Then only one of the multiple instances will be updated.
     * </p>
     *
     * @param participation The buyer participation that should be changed. It will be invalidated.
     * @param commoditySpecificationToRemove The commodity specification of the commodity that will
     *                                       be removed from the buyer participation.
     * @return {@code this}
     */
    @Deterministic
    public @NonNull Economy removeCommodityBought(@NonNull BuyerParticipation participation,
                                                  @NonNull @ReadOnly CommoditySpecification commoditySpecificationToRemove) {
        @NonNull TraderWithSettings trader = (TraderWithSettings)getBuyer(participation);

        // remove the participation from the old market
        @NonNull Basket oldBasket = removeBasketBought(participation);

        Basket newBasketBought = oldBasket.remove(commoditySpecificationToRemove);

        // add the trader to the new market.
        BuyerParticipation newParticipation = addBasketBought(trader, newBasketBought);

        // copy quantity and peak quantity values from old participation.
        int specificationIndex = oldBasket.indexOf(commoditySpecificationToRemove);
        for (int i = 0 ; i < specificationIndex ; ++i) {
            newParticipation.setQuantity(i, participation.getQuantity(i));
            newParticipation.setPeakQuantity(i, participation.getPeakQuantity(i));
        }
        for (int i = specificationIndex ; i < newBasketBought.size() ; ++i) {
            newParticipation.setQuantity(i, participation.getQuantity(i+1));
            newParticipation.setPeakQuantity(i, participation.getPeakQuantity(i+1));
        }
        return this;
    }

    /**
     * Returns a deep copy of {@code this} economy.
     *
     * <p>
     *  It is an O(T) operation, where T is the number of traders in the economy.
     * </p>
     */
    @Override
    @Pure
    public Economy clone(@ReadOnly Economy this) {
        try {
            Economy copy = (Economy)super.clone();
            // TODO Auto-generated method stub
            return copy;
        }
        catch (CloneNotSupportedException e) {
            throw new AssertionError("Object.clone threw a CloneNotSupportedException for a Clonable object!");
        }
    }

} // end class Economy

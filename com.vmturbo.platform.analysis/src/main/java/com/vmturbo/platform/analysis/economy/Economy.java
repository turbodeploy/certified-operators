package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.ToDoubleFunction;

import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.dataflow.qual.SideEffectFree;

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
    // Map of quantity calculation functions by (sold) commodity specification.
    private final @NonNull Map<@NonNull CommoditySpecification, @NonNull ToDoubleFunction<List<Double>>>
                quantityFunctions_ = new TreeMap<>();
    // An aggregate of all the parameters configuring this economy's behavior.
    private final @NonNull EconomySettings settings_ = new EconomySettings();

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
     * @return
     * A modifiable map of quantity calculation functions per (sold) {@link CommoditySpecification}.
     * If a commodity specification is not in the map then its quantity calculation is additive.
     */
    public Map<CommoditySpecification, ToDoubleFunction<List<Double>>> getQuantityFunctions() {
        return quantityFunctions_;
    }

    /**
     * Check whether this commodity specification uses the default (additive) quantity update or
     * it has its own function.
     * @return true when the commodity specification is additive, false when it uses its own function
     */
    @Pure
    public boolean isAdditive(CommoditySpecification commSpec) {
        return !quantityFunctions_.containsKey(commSpec);
    }

    @Override
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
        return markets_.get(basket);
    }

    @Override
    @Pure
    public @NonNull @ReadOnly Market getMarket(@ReadOnly Economy this, @NonNull @ReadOnly BuyerParticipation participation) {
        return ((TraderWithSettings)participation.getBuyer()).getMarketsAsBuyer().get(participation);
    }

    @Override
    @SideEffectFree
    public @NonNull @ReadOnly List<@NonNull CommodityBought> getCommoditiesBought(@ReadOnly Economy this,
                                               @NonNull @ReadOnly BuyerParticipation participation) {
        final int basketSize = getMarket(participation).getBasket().size();
        final @NonNull List<@NonNull CommodityBought> result = new ArrayList<>(basketSize);

        for (int i = 0; i < basketSize; ++i) { // should be same size as participation
            result.add(new CommodityBought(participation, i));
        }

        return result;
    }

    @Override
    @SideEffectFree
    public @NonNull @PolyRead CommodityBought getCommodityBought(@PolyRead Economy this,
                                         @NonNull @PolyRead BuyerParticipation participation,
                                         @NonNull @ReadOnly CommoditySpecification specification) {
        return new CommodityBought(participation,getMarket(participation).getBasket().indexOf(specification));
    }

    @Override
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
        final TraderWithSettings castTraderToRemove = (TraderWithSettings)traderToRemove;

        // Stop everyone from buying from the trader.
        for (@NonNull BuyerParticipation participation : new ArrayList<>(castTraderToRemove.getCustomers())) {
            participation.move(null); // this is not the cheapest way, but the safest...
        }

        // Remove the trader from all markets it participated as seller
        for (Market market : new ArrayList<>(castTraderToRemove.getMarketsAsSeller())) {
            market.removeSeller(castTraderToRemove);
        }

        // Remove the trader from all markets it participated as buyer
        for (Entry<@NonNull BuyerParticipation, @NonNull Market> entry
                : new ArrayList<>(castTraderToRemove.getMarketsAsBuyer().entrySet())) {
            entry.getValue().removeBuyerParticipation(entry.getKey());
        }

        // Update economy indices of all traders and remove trader from list.
        for (TraderWithSettings trader : traders_) {
            if (trader.getEconomyIndex() > traderToRemove.getEconomyIndex()) {
                trader.setEconomyIndex(trader.getEconomyIndex() - 1);
            }
        }
        checkArgument(traders_.remove(traderToRemove));

        return this;
    }

    @Override
    @Pure
    public @NonNull @ReadOnly List<@NonNull @ReadOnly Trader> getSuppliers(@ReadOnly Economy this,
                                                                           @NonNull @ReadOnly Trader trader) {
        @NonNull List<@NonNull @ReadOnly Trader> suppliers = new ArrayList<>();

        for (BuyerParticipation participation : getMarketsAsBuyer(trader).keySet()) {
            if (participation.getSupplier() != null) {
                suppliers.add(participation.getSupplier());
            }
        }

        return Collections.unmodifiableList(suppliers);
    }

    @Override
    @Pure
    public @NonNull @ReadOnly Map<@NonNull BuyerParticipation, @NonNull Market>
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

        market.removeBuyerParticipation(participation);

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
     * @return The buyer participation that will replace the one that was changed.
     */
    @Deterministic
    public @NonNull BuyerParticipation addCommodityBought(@NonNull BuyerParticipation participation,
                              @NonNull @ReadOnly CommoditySpecification commoditySpecificationToAdd) {
        @NonNull TraderWithSettings trader = (TraderWithSettings)participation.getBuyer();

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

        return newParticipation;
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
     *            be removed from the buyer participation. If this commodity isn't in the buyer
     *            participation, the call will just replace the buyer participation with a new one.
     * @return The buyer participation that will replace the one that was changed.
     */
    @Deterministic
    public @NonNull BuyerParticipation removeCommodityBought(@NonNull BuyerParticipation participation,
                              @NonNull @ReadOnly CommoditySpecification commoditySpecificationToRemove) {
        @NonNull TraderWithSettings trader = (TraderWithSettings)participation.getBuyer();

        // remove the participation from the old market
        @NonNull Basket oldBasket = removeBasketBought(participation);

        Basket newBasketBought = oldBasket.remove(commoditySpecificationToRemove);

        // add the trader to the new market.
        BuyerParticipation newParticipation = addBasketBought(trader, newBasketBought);

        // copy quantity and peak quantity values from old participation.
        int specificationIndex = oldBasket.indexOf(commoditySpecificationToRemove);
        if (specificationIndex == -1) {
            specificationIndex = newBasketBought.size();
        }
        for (int i = 0 ; i < specificationIndex ; ++i) {
            newParticipation.setQuantity(i, participation.getQuantity(i));
            newParticipation.setPeakQuantity(i, participation.getPeakQuantity(i));
        }
        for (int i = specificationIndex ; i < newBasketBought.size() ; ++i) {
            newParticipation.setQuantity(i, participation.getQuantity(i+1));
            newParticipation.setPeakQuantity(i, participation.getPeakQuantity(i+1));
        }
        return newParticipation;
    }

} // end class Economy

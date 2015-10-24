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
 *  It is responsible for creating and destroying markets as traders are added/removed.
 * </p>
 */
public final class Economy implements Cloneable {
    // Fields

    // The map that associates Baskets with Markets.
    private @NonNull Map<@NonNull @ReadOnly Basket,@NonNull Market> markets_ = new TreeMap<>();
    // The list of all Traders participating in the Economy.
    private @NonNull List<@NonNull TraderWithSettings> traders_ = new ArrayList<>();

    // Constructors

    /**
     * Constructs an empty Economy.
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
     */
    @Pure
    public @NonNull @ReadOnly Collection<@NonNull @ReadOnly Market> getMarkets(@ReadOnly Economy this) {
        return Collections.unmodifiableCollection(markets_.values());
    }

    @Pure
    public @NonNull @ReadOnly Market getMarket(@ReadOnly Economy this, @NonNull @ReadOnly Basket basket) {
        return markets_.get(basket);
    }

    @Pure
    public @NonNull Trader getBuyer(@ReadOnly Economy this, @NonNull @ReadOnly BuyerParticipation participation) {
        return traders_.get(participation.getBuyerIndex());
    }

    /**
     * Returns the supplier of the given buyer participation in {@code this} economy or {@code null}
     * if there is no such supplier.
     *
     * @param participation The buyer participation for which we query the supplier.
     * @return The supplier or {@code null} if the given buyer participation is currently not buying
     *          the corresponding basket from anyone.
     */
    @Pure
    public @Nullable @ReadOnly Trader getSupplier(@ReadOnly Economy this, @NonNull @ReadOnly BuyerParticipation participation) {
        return participation.getSupplierIndex() == BuyerParticipation.NO_SUPPLIER
            ? null : traders_.get(participation.getSupplierIndex());
    }

    /**
     * Returns an unmodifiable list of the commodities the given buyer participation is buying in
     * {@code this} market.
     *
     * <p>
     *  If the given buyer participation is not currently buying these commodities from anyone, then
     *  they just represent the quantities and peak quantities the buyer intends to buy.
     * </p>
     */
    @Pure
    public @NonNull @ReadOnly List<@NonNull CommodityBought> getCommoditiesBought(@ReadOnly Economy this,
                                               @NonNull @ReadOnly BuyerParticipation participation) {
        @NonNull Market market = Multimaps.invertFrom(((TraderWithSettings)getBuyer(participation)).getMarketsAsBuyer(),
            ArrayListMultimap.create()).get(participation).get(0); // only one market in inverse view
        @NonNull List<@NonNull CommodityBought> result = new ArrayList<>(market.getBasket().size());

        for (int i = 0; i < market.getBasket().size(); ++i) { // should be same size as participation
            result.add(new CommodityBought(participation, i));
        }

        return result;
    }

    @Pure
    public @NonNull @PolyRead CommodityBought getCommodityBought(@PolyRead Economy this,
                                         @NonNull @PolyRead BuyerParticipation participation,
                                         @NonNull @ReadOnly CommoditySpecification specification) {
        return new CommodityBought(participation, Multimaps.invertFrom(
            ((TraderWithSettings)getBuyer(participation)).getMarketsAsBuyer(), ArrayListMultimap.create())
           .get(participation).get(0).getBasket().indexOf(specification));
    }

    /**
     * Returns an unmodifiable list of all the {@link Trader traders} currently participating in the
     * economy.
     *
     * <p>
     *  It is an O(1) operation.
     * </p>
     */
    @Pure
    public @NonNull @ReadOnly List<@NonNull @ReadOnly Trader> getTraders(@ReadOnly Economy this) {
        return Collections.unmodifiableList(traders_);
    }

    /**
     * Adds a new {@link Trader trader} to the economy.
     *
     * <p>
     *  It is an O(M*C) operation, where M is the number of markets present in the economy and C is
     *  the number of commodities sold by the trader.
     * </p>
     *
     * @return {@code this}
     */
    @Deterministic
    public @NonNull Trader addTrader(int type, @NonNull TraderState state,
                                      @NonNull Basket basketSold, @NonNull Basket... basketsBought) {
        TraderWithSettings newTrader = new TraderWithSettings(traders_.size(), type, state, basketSold);

        // Add a buyer
        for (Basket basketBought : basketsBought) {
            if (!markets_.containsKey(basketBought)) {
                Market newMarket = new Market(basketBought);

                markets_.put(basketBought, newMarket);

                // Populate new market
                for (Trader seller : traders_) {
                    if (newMarket.getBasket().isSatisfiedBy(seller.getBasketSold())) {
                        newMarket.addSeller(seller);
                    }
                }
            }

            newTrader.getMarketsAsBuyer().put(markets_.get(basketBought), markets_.get(basketBought).addBuyer(newTrader));
        }

        // Add as seller
        for(Market market : markets_.values()) {
            if (market.getBasket().isSatisfiedBy(basketSold)) {
                market.addSeller(newTrader);
                newTrader.getMarketsAsSeller().add(market);
            }
        }

        traders_.add(newTrader);
        return newTrader;
    }

    /**
     * Removes an existing {@link Trader trader} from the economy.
     *
     * <p>
     *  It is an O(T) operation, where T is the number of traders in the economy.
     *  All existing indices to traders are potentially invalidated.
     * </p>
     *
     * @param existingTrader The trader to be removed.
     * @return {@code this}
     */
    @Deterministic
    public @NonNull Economy removeTrader(@NonNull Trader existingTrader) {
        for (Market market : ((TraderWithSettings)existingTrader).getMarketsAsSeller()) {
            market.removeSeller(existingTrader);
        }
        for (Map.Entry<Market,BuyerParticipation> entry : ((TraderWithSettings)existingTrader).getMarketsAsBuyer().entries()) {
            entry.getKey().removeBuyerParticipation(entry.getValue());
        }
        for (TraderWithSettings trader : traders_) {
            if (trader.getEconomyIndex() > ((TraderWithSettings)existingTrader).getEconomyIndex()) {
                trader.setEconomyIndex(trader.getEconomyIndex() - 1);
            }
        }
        traders_.remove(existingTrader);
        return this;
    }

    @Deterministic
    public @NonNull Economy moveTrader(@NonNull BuyerParticipation participationToMove,
                                       @NonNull Trader newSupplier) {
        @NonNull TraderWithSettings trader = (TraderWithSettings)getBuyer(participationToMove);
        @NonNull Market market = Multimaps.invertFrom(trader.getMarketsAsBuyer(),ArrayListMultimap.create())
                                                                    .get(participationToMove).get(0);

        Trader supplier = getSupplier(participationToMove);
        if (supplier != null) {
            int i = 0;
            for (CommoditySpecification specification : market.getBasket().getCommoditySpecifications()) {
                // there should be at least one that matches
                while(!specification.isSatisfiedBy((supplier.getBasketSold().getCommoditySpecifications().get(i)))) {
                    ++i;
                }
                ((CommoditySoldWithSettings)supplier.getCommoditiesSold().get(i)).getModifiableBuyersList().remove(participationToMove);
            }
        }

        if (newSupplier != null) {
            checkArgument(market.getBasket().isSatisfiedBy(newSupplier.getBasketSold()));
            int i = 0;
            for (CommoditySpecification specification : market.getBasket().getCommoditySpecifications()) {
                // there should be at least one that matches
                while(!specification.isSatisfiedBy((newSupplier.getBasketSold().getCommoditySpecifications().get(i)))) {
                    ++i;
                }
                ((CommoditySoldWithSettings)newSupplier.getCommoditiesSold().get(i)).getModifiableBuyersList().add(participationToMove);
            }

            participationToMove.setSupplierIndex(traders_.indexOf(newSupplier));
        }
        else
            participationToMove.setSupplierIndex(BuyerParticipation.NO_SUPPLIER);

        return this;
    }

    /**
     * Returns an unmodifiable list of {@code this} seller's customers.
     */
    @Pure
    public @NonNull @ReadOnly Set<@NonNull @ReadOnly Trader> getCustomers(@ReadOnly Economy this, @NonNull @ReadOnly Trader trader) {
        @NonNull Set<@NonNull @ReadOnly Trader> customers = new TreeSet<>();

        for (CommoditySold commSold : trader.getCommoditiesSold()) {
            for (BuyerParticipation customer : commSold.getBuyers()) {
                customers.add(traders_.get(customer.getBuyerIndex()));
            }
        }

        return Collections.unmodifiableSet(customers);
    }

    /**
     * Returns an unmodifiable list of {@code this} buyer's suppliers.
     */
    @Pure
    public @NonNull @ReadOnly List<@NonNull @ReadOnly Trader> getSuppliers(@ReadOnly Economy this, @NonNull @ReadOnly Trader trader) {
        @NonNull List<@NonNull @ReadOnly Trader> suppliers = new ArrayList<>();

        for (BuyerParticipation participation : ((TraderWithSettings)trader).getMarketsAsBuyer().values()) {
            suppliers.add(getSupplier(participation));
        }

        return suppliers;
    }

    /**
     * Returns an unmodifiable multimap of the markets {@code this} trader participates in as a buyer.
     */
    @Pure
    public @NonNull @ReadOnly ListMultimap<@NonNull Market, @NonNull BuyerParticipation>
            getMarketsAsBuyer(@ReadOnly Economy this, @NonNull @ReadOnly Trader trader) {
        return Multimaps.unmodifiableListMultimap(((TraderWithSettings)trader).getMarketsAsBuyer());
    }

    /**
     * Returns an unmodifiable list of the markets {@code this} trader participates in as a seller.
     */
    @Pure
    public @NonNull @ReadOnly List<@NonNull @ReadOnly Market> getMarketsAsSeller(@ReadOnly Economy this, @NonNull @ReadOnly Trader trader) {
        return Collections.unmodifiableList(((TraderWithSettings)trader).getMarketsAsSeller());
    }

    /**
     * Adds a new commodity specification to a given basket bought by this buyer.
     *
     * @param commodityTypeToAdd the commodity specification to add to the basket bought.
     * @return {@code this}
     */
    @Deterministic
    public @NonNull Economy addCommodityBought(@NonNull BuyerParticipation participation,
                                              @NonNull @ReadOnly CommoditySpecification commodityTypeToAdd) {
        @NonNull TraderWithSettings trader = (TraderWithSettings)getBuyer(participation);
        @NonNull Market market = Multimaps.invertFrom(trader.getMarketsAsBuyer(),ArrayListMultimap.create()).get(participation).get(0);

        market.removeBuyerParticipation(participation);
        trader.getMarketsAsBuyer().remove(market, participation);

        Basket newBasketBought = market.getBasket().add(commodityTypeToAdd);
        if (!markets_.containsKey(newBasketBought)) {
            Market newMarket = new Market(newBasketBought);

            markets_.put(newBasketBought, newMarket);

            // Populate new market
            for (Trader seller : traders_) {
                if (newMarket.getBasket().isSatisfiedBy(seller.getBasketSold())) {
                    newMarket.addSeller(seller);
                }
            }
        }

        // TODO: should existing commodities bought be somehow preserved?
        trader.getMarketsAsBuyer().put(markets_.get(newBasketBought),
                                                             markets_.get(newBasketBought).addBuyer(trader));
        return this;
    }

    /**
     * Removes an existing commodity specification from a given basket bought by this buyer.
     *
     * <p>
     *  Baskets contain at most one of each commodity specification.
     * </p>
     *
     * @param commodityTypeToRemove the commodity specification that should be removed from the basket.
     * @return {@code this}
     */
    @Deterministic
    public @NonNull Economy removeCommodityBought(@NonNull BuyerParticipation participation,
                                                  @NonNull @ReadOnly CommoditySpecification commodityTypeToRemove) {
        @NonNull TraderWithSettings trader = (TraderWithSettings)getBuyer(participation);
        @NonNull Market market = Multimaps.invertFrom(trader.getMarketsAsBuyer(),ArrayListMultimap.create()).get(participation).get(0);

        market.removeBuyerParticipation(participation);
        trader.getMarketsAsBuyer().remove(market, participation);

        Basket newBasketBought = market.getBasket().remove(commodityTypeToRemove);
        if (!markets_.containsKey(newBasketBought)) {
            Market newMarket = new Market(newBasketBought);

            markets_.put(newBasketBought, newMarket);

            // Populate new market
            for (Trader seller : traders_) {
                if (newMarket.getBasket().isSatisfiedBy(seller.getBasketSold())) {
                    newMarket.addSeller(seller);
                }
            }
        }

        // TODO: should existing commodities bought be somehow preserved?
        trader.getMarketsAsBuyer().put(markets_.get(newBasketBought),
                                                             markets_.get(newBasketBought).addBuyer(trader));
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

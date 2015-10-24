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

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

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
    private @NonNull Map<@NonNull @ReadOnly Basket,@NonNull Market> markets = new TreeMap<>();
    // The list of all Traders participating in the Economy.
    private @NonNull List<@NonNull TraderWithSettings> traders = new ArrayList<>();

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
        return Collections.unmodifiableCollection(markets.values());
    }

    @Pure
    public @NonNull @ReadOnly Market getMarket(@ReadOnly Economy this, @NonNull @ReadOnly Basket basket) {
        return markets.get(basket);
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
        return Collections.unmodifiableList(traders);
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
        TraderWithSettings newTrader = new TraderWithSettings(traders.size(), type, state, basketSold);

        // Add a buyer
        for (Basket basketBought : basketsBought) {
            if (!markets.containsKey(basketBought)) {
                Market newMarket = new Market(this, basketBought);

                markets.put(basketBought, newMarket);

                // Populate new market
                for (Trader seller : traders) {
                    if (newMarket.getBasket().isSatisfiedBy(seller.getBasketSold())) {
                        newMarket.addSeller(seller);
                    }
                }
            }

            newTrader.getMarketsAsBuyer().put(markets.get(basketBought), markets.get(basketBought).addBuyer(newTrader));
        }

        // Add as seller
        for(Market market : markets.values()) {
            if (market.getBasket().isSatisfiedBy(basketSold)) {
                market.addSeller(newTrader);
                newTrader.getMarketsAsSeller().add(market);
            }
        }

        traders.add(newTrader);
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
        for (TraderWithSettings trader : traders) {
            if (trader.getEconomyIndex() > ((TraderWithSettings)existingTrader).getEconomyIndex()) {
                trader.setEconomyIndex(trader.getEconomyIndex() - 1);
            }
        }
        traders.remove(existingTrader);
        return this;
    }

    @Deterministic
    public @NonNull Economy moveTrader(@NonNull Trader trader, @NonNull BuyerParticipation participationToMove,
                                       @NonNull Trader newSupplier) {
        checkArgument(((TraderWithSettings)trader).getMarketsAsBuyer().containsValue(participationToMove));

        @NonNull Market market = new Market(this, new Basket()); // dummy initialization to avoid errors.

        // Find the correct market.
        for (Map.Entry<Market,BuyerParticipation> entry : ((TraderWithSettings)trader).getMarketsAsBuyer().entries()) {
            if (participationToMove == entry.getValue()) {
                market = entry.getKey();
                break;
            }
        }

        Trader supplier = market.getSupplier(participationToMove);
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

            participationToMove.setSupplierIndex(traders.indexOf(newSupplier));
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
                customers.add(traders.get(customer.getBuyerIndex()));
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

        for (Map.Entry<Market,BuyerParticipation> entry : ((TraderWithSettings)trader).getMarketsAsBuyer().entries()) {
            suppliers.add(entry.getKey().getSupplier(entry.getValue()));
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
    public @NonNull Economy addCommodityBought(@NonNull Trader trader, @NonNull BuyerParticipation participation,
                                              @NonNull @ReadOnly CommoditySpecification commodityTypeToAdd) {
        checkArgument(((TraderWithSettings)trader).getMarketsAsBuyer().containsValue(participation));

        @NonNull Market market = new Market(this, new Basket()); // dummy initialization to avoid errors.

        // Find the correct market.
        for (Map.Entry<Market,BuyerParticipation> entry : ((TraderWithSettings)trader).getMarketsAsBuyer().entries()) {
            if (participation == entry.getValue()) {
                market = entry.getKey();
                break;
            }
        }

        market.removeBuyerParticipation(participation);
        ((TraderWithSettings)trader).getMarketsAsBuyer().remove(market, participation);

        Basket newBasketBought = market.getBasket().add(commodityTypeToAdd);
        if (!markets.containsKey(newBasketBought)) {
            Market newMarket = new Market(this, newBasketBought);

            markets.put(newBasketBought, newMarket);

            // Populate new market
            for (Trader seller : traders) {
                if (newMarket.getBasket().isSatisfiedBy(seller.getBasketSold())) {
                    newMarket.addSeller(seller);
                }
            }
        }

        // TODO: should existing commodities bought be somehow preserved?
        ((TraderWithSettings)trader).getMarketsAsBuyer().put(markets.get(newBasketBought),
                                                             markets.get(newBasketBought).addBuyer((TraderWithSettings)trader));
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
    public @NonNull Economy removeCommodityBought(@NonNull Trader trader, @NonNull BuyerParticipation participation,
                                                  @NonNull @ReadOnly CommoditySpecification commodityTypeToRemove) {
        checkArgument(((TraderWithSettings)trader).getMarketsAsBuyer().containsValue(participation));

        @NonNull Market market = new Market(this, new Basket()); // dummy initialization to avoid errors.

        // Find the correct market.
        for (Map.Entry<Market,BuyerParticipation> entry : ((TraderWithSettings)trader).getMarketsAsBuyer().entries()) {
            if (participation == entry.getValue()) {
                market = entry.getKey();
                break;
            }
        }

        market.removeBuyerParticipation(participation);
        ((TraderWithSettings)trader).getMarketsAsBuyer().remove(market, participation);

        Basket newBasketBought = market.getBasket().remove(commodityTypeToRemove);
        if (!markets.containsKey(newBasketBought)) {
            Market newMarket = new Market(this, newBasketBought);

            markets.put(newBasketBought, newMarket);

            // Populate new market
            for (Trader seller : traders) {
                if (newMarket.getBasket().isSatisfiedBy(seller.getBasketSold())) {
                    newMarket.addSeller(seller);
                }
            }
        }

        // TODO: should existing commodities bought be somehow preserved?
        ((TraderWithSettings)trader).getMarketsAsBuyer().put(markets.get(newBasketBought),
                                                             markets.get(newBasketBought).addBuyer((TraderWithSettings)trader));
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

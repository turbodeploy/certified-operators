package com.vmturbo.platform.analysis.topology;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderSettings;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;

/**
 * Used to create an {@link Economy} by some client - either mediation, UI or other.
 * Maintains mapping of {@link Trader trader}s and {@link BuyerParticipation shopping list}s
 * and their OIDs, in order to create {@link Action action}s and relate them back to the client.
 */
public final class Topology {
    // Fields
    private final @NonNull Economy economy_ = new Economy();
    private final @NonNull BiMap<@NonNull Trader, @NonNull Long> traders_ = HashBiMap.create();
    private final @NonNull BiMap<@NonNull BuyerParticipation, @NonNull Long> shoppingLists_ = HashBiMap.create();
    // Used for buyer participations created before their buyer is created
    // Key is an OID of a Trader
    private final Map<Long, List<BuyerParticipation>> unplaced_ = Maps.newHashMap();

    private static final PriceFunction SWPF = PriceFunction.Cache.createStandardWeightedPriceFunction(1.0);

     /**
     * Constructs an empty Topology
     */
    public Topology() {
    }

     /**
     * Returns the {@link Trader} associated with a given OID.
     *
     * @param oid the OID uniquely identifying the trader to return
     * @return the trader associated with supplied OID.
     */
    @Pure
    public Trader getTrader(long oid) {
        return traders_.inverse().get(oid);
    }

    /**
     * Returns the OID of a {@link Trader}
     * @param trader
     * @return the OID of the trader
     * @throws NullPointerException if the argument trader was not created through {@code this}
     * interface
     */
    public long getTraderOID(@NonNull Trader trader) {
        return traders_.get(trader);
    }

    /**
     * Add a trader to the economy with sold commodities. If the trader is also a buyer then you should call
     * addShoppingList for each shopping list that it buys.
     * @param traderOID trader OID
     * @param type trader type
     * @param state trader state as a boolean. True means ACTIVE.
     * @param soldTypes numerical types of commodities sold by the trader
     * @param soldQuantities sold commodities' quantities
     * @param soldPeakQuantities sold commodities' peak quantities
     * @param soldCapacities sold commodities' capacities
     * @param soldThins whether each sold commodity is thin
     * @param soldResizables whether each sold commodity is resizeable
     * @param soldCapacityLowerBounds lower bounds (for resize) of sold commodities' capacity
     * @param soldCapacityUpperBounds upper bounds (for resize) of sold commodities' capacity
     * @param soldCapacityIncrements  capacity increments (for resize) sold commodities'
     * @param soldUtilizationUpperBounds utilization upper bounds of sold commodities'
     * @param soldPriceFunctions price functions to be used by each sold commodity
     * @param cloneable whether the trader is cloneable
     * @param suspendable whether the trader is suspendable
     * @param minDesiredUtilization minimum desired utilization of the trader
     * @param maxDesiredUtilization maximum desired utilization of the trader
     * @return a new trader
     */
    public @NonNull Trader addTrader(
            // Trader properties
            long traderOID, int type, boolean state,
            // Commodities sold
            @NonNull List<Integer> soldTypes, @NonNull List<Double> soldQuantities, @NonNull List<Double> soldPeakQuantities,
            @NonNull List<Double> soldCapacities, @NonNull List<Boolean> soldThins, @NonNull List<Boolean> soldResizables,
            @NonNull List<Double> soldCapacityLowerBounds, @NonNull List<Double> soldCapacityUpperBounds,
            @NonNull List<Double> soldCapacityIncrements, @NonNull List<Double> soldUtilizationUpperBounds,
            @NonNull List<@Nullable PriceFunction> soldPriceFunctions,
            // Shopping Lists: use addShoppingList(.)
            // Trader settings
            boolean cloneable, boolean suspendable, double minDesiredUtilization, double maxDesiredUtilization
            ) {

        Trader trader = addTrader(traderOID, type, state, soldTypes, cloneable, suspendable, minDesiredUtilization, maxDesiredUtilization);
        IntStream.range(0, soldTypes.size()).forEach(
                i -> {
                    setCommoditySoldProps(
                            traderOID,
                            // Sold commodities
                            soldTypes.get(i),
                            soldCapacities.get(i), soldQuantities.get(i), soldPeakQuantities.get(i),
                            soldThins.get(i),
                            // Sold commodity settings
                            soldResizables.get(i),
                            soldCapacityIncrements.get(i),
                            soldCapacityLowerBounds.get(i), soldCapacityUpperBounds.get(i),
                            soldUtilizationUpperBounds.get(i), soldPriceFunctions.get(i));
                });

        return trader;
    }

    /**
     * Add a trader to the economy. After calling this method, use setCommoditySoldProps to set properties of commodities sold.
     * If the trader is also a buyer then you should call addShoppingList for each shopping list that it buys.
     * @param traderOID trader OID
     * @param type trader type
     * @param state trader state as a boolean. True means ACTIVE.
     * @param soldTypes numerical types of commodities sold by the trader
     * @param cloneable whether the trader is cloneable
     * @param suspendable whether the trader is suspendable
     * @param minDesiredUtilization minimum desired utilization of the trader
     * @param maxDesiredUtilization maximum desired utilization of the trader
     * @return a new trader with an empty basket sold
     */
    public @NonNull Trader addTrader(
            // Trader properties
            long traderOID, int type, @NonNull boolean state,
            // Commodities sold
            @NonNull List<Integer> soldTypes,
            // Trader settings
            boolean cloneable, boolean suspendable, double minDesiredUtilization, double maxDesiredUtilization
            ) {
        TraderState traderState = state ? TraderState.ACTIVE : TraderState.INACTIVE;
        Basket basketSold = new Basket(soldTypes.stream().map(CommoditySpecification::new).collect(Collectors.toList()));
        @NonNull Trader trader = economy_.addTrader(type, traderState, basketSold);
        TraderSettings settings = trader.getSettings();
        settings.setCloneable(cloneable);
        settings.setSuspendable(suspendable);
        settings.setMinDesiredUtil(minDesiredUtilization);
        settings.setMaxDesiredUtil(maxDesiredUtilization);

        traders_.inverse().put(traderOID, trader);
        List<BuyerParticipation> unplaced = unplaced_.get(traderOID);
        if (unplaced != null) {
            unplaced.stream().forEach(bp -> bp.move(trader));
            unplaced_.remove(traderOID);
        }

        return trader;
    }

    /**
     * Sets the properties of a {@link CommoditySold}
     * @param traderOID the OID of the trader that sells the commodity
     * @param type commodity specification numeric type
     * @param capacity sold commodity capacity
     * @param quantity sold commodity quantity
     * @param peakQuantity sold commodity peak quantity
     * @param thin whether the sold commodity is thin
     * @param resizable whether the sold commodity is resizable
     * @param capacityLowerBound lower bound of sold commodity capacity
     * @param capacityUpperBound upper bound of sold commodity capacity
     * @param capacityIncrement capacity increment of sold commodity
     * @param utilizationUpperBound upper bound of sold commodity utilization
     * @param priceFunction price function to be used by sold commodity. If null the use StandardWeightedPriceFunction with argument 1.0.
     * @throws IllegalArgumentException if the trader doesn't sell a commodity of this type
     * @throws NullPointerException if {@code this} topology doesn't have a trader with the specified OID
     */
    public void setCommoditySoldProps(
            long traderOID,
            int type, double capacity, double quantity, double peakQuantity,
            boolean thin,
            // Settings
            boolean resizable,
            double capacityLowerBound, double capacityUpperBound,
            double capacityIncrement, double utilizationUpperBound,
            @NonNull PriceFunction priceFunction
            ) {
        CommoditySpecification spec = new CommoditySpecification(type);
        Trader trader = traders_.inverse().get(traderOID);
        CommoditySold commoditySold = trader.getCommoditySold(spec);
        if (commoditySold == null) {
            throw new IllegalArgumentException(
                    "Trader with OID " + 1 + " doesn't sell commodity type " + type);
        }
        commoditySold.setCapacity(capacity);
        commoditySold.setQuantity(quantity);
        commoditySold.setPeakQuantity(peakQuantity);
        commoditySold.setThin(thin);
        CommoditySoldSettings settings = commoditySold.getSettings();
        settings.setCapacityIncrement(capacityIncrement);
        settings.setCapacityLowerBound(capacityLowerBound);
        settings.setCapacityUpperBound(capacityUpperBound);
        settings.setUtilizationUpperBound(utilizationUpperBound);
        settings.setResizable(resizable);
        settings.setPriceFunction(priceFunction == null ? SWPF : priceFunction);
    }

    /**
     * Returns the {@link BuyerParticipation} associated with a given OID.
     *
     * @param oid the OID uniquely identifying shopping list
     * @return the shopping list associated with the oid
     */
    public @NonNull BuyerParticipation getShoppingList(long oid) {
        return shoppingLists_.inverse().get(oid);
    }

    /**
     * Returns the OID of a {@link BuyerParticipation}
     * @param bp a shopping list
     * @return the shopping list OID
     * @throws NullPointerException if the argument shopping list was not created
     * through {@code this} interface
     */
    public long getShoppingListOID(BuyerParticipation bp) {
        return shoppingLists_.get(bp);
    }

    /**
     * Add one shopping list to a trader
     * @param buyerOID the OID of the {@link Trader} buying this shopping list
     * @param shoppingListOID an OID uniquely identifying the resulted shopping list
     * @param supplierOID the OID of the {@link Trader} that the shopping list is bought from
     * @param movable whether the shopping list is movable
     * @param commodityTypesBought the types of commodities bought in this shopping lists
     * @param boughtQuantities quantities bought in this shopping list
     * @param boughtPeakQuantities peak quantities bought in this shopping list
     * @return the new shopping list
     */
    public @NonNull BuyerParticipation addShoppingList(
            long buyerOID, long shoppingListOID, long supplierOID, boolean movable,
            // Commodities bought
            @NonNull List<Integer> commodityTypesBought, @NonNull List<Double> boughtQuantities,
            @NonNull List<Double> boughtPeakQuantities
            ) {
        Trader buyer = traders_.inverse().get(buyerOID);
        // Create the basket bought
        Basket basketBought = new Basket(commodityTypesBought.stream()
                .map(typeBought -> new CommoditySpecification(typeBought))
                .collect(Collectors.toList()));
        BuyerParticipation bp = economy_.addBasketBought(buyer, basketBought);
        bp.setMovable(movable);
        IntStream.range(0, commodityTypesBought.size()).forEach(
                i -> {
                    bp.setQuantity(i, boughtQuantities.get(i));
                    bp.setPeakQuantity(i, boughtPeakQuantities.get(i));
                }
                );
        // Placement
        Trader supplier = traders_.inverse().get(supplierOID);
        if (supplier == null) {
            unplaced_.compute(supplierOID, (oid, list) -> list == null ? Lists.newArrayList() : list).add(bp);
            // queue
        } else {
            bp.move(supplier);
        }
        shoppingLists_.put(bp, shoppingListOID);

        return bp;
    }

    /**
     * Returns the Economy that corresponds to {@code this} topology.
     * TODO: return UnmodifiableEconomy? That way traders and shopping lists can be added only through this Topology
     */
    @Pure
    public @NonNull @ReadOnly Economy getEconomy() {
        return economy_;
    }
}
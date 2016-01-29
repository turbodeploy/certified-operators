package com.vmturbo.platform.analysis.topology;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.utilities.NumericIDAllocator;
import com.vmturbo.platform.analysis.utilities.UnmodifiableNumericIDAllocator;

/**
 * A class representing a loaded legacy topology together with its auxiliary maps capturing
 * information not contained in today's topologies.
 *
 * <p>
 *  It is responsible for keeping the managed maps and economy consistent with each other.
 * </p>
 */
public final class LegacyTopology {
    // Fields
    private final @NonNull Economy economy_ = new Economy();
    private final @NonNull BiMap<@NonNull Trader, @NonNull String> uuids_ = HashBiMap.create();
    private final @NonNull Map<@NonNull Trader, @NonNull String> names_ = new LinkedHashMap<>();
    private final @NonNull NumericIDAllocator traderTypes_ = new NumericIDAllocator();
    private final @NonNull NumericIDAllocator commodityTypes_ = new NumericIDAllocator();

    // Cached data

    // Cached unmodifiable view of the uuids_ BiMap.
    private final @NonNull BiMap<@NonNull Trader, @NonNull String> unmodifiableUuids_ = Maps.unmodifiableBiMap(uuids_);
    // Cached unmodifiable view of the names_ map.
    private final @NonNull Map<@NonNull Trader, @NonNull String> unmodifiableNames_ = Collections.unmodifiableMap(names_);

    // Constructors

    /**
     * Constructs an empty LegacyTopology
     */
    public LegacyTopology() {
        // nothing to do
    }

    // Methods

    /**
     * Adds a new trader to the legacy topology.
     *
     * @param uuid The UUID of the new trader.
     * @param name The human-readable name of the new trader.
     * @param type The human-readable type of the new trader.
     * @param state The state of the new trader.
     * @param commodityTypesSold A map which keys are human-readable commodity type strings from which
     *                           the basket sold of the trader will be created, and values are the
     *                           functions used to calculate the quantity sold by a commodity of
     *                           this type (or null, if the quantity sold is additive.) The keys are
     *                           allocated to numerical IDs in the order they are returned by the
     *                           collecion's iterator.
     * @return The new trader.
     *
     * @see Economy#addTrader(int, TraderState, Basket, Basket...)
     */
    public @NonNull Trader addTrader(@NonNull String uuid, @NonNull String name, @NonNull String type,
            @NonNull TraderState state,
            @NonNull Map<@NonNull String, Function<List<Double>, Double>> commodityTypesSoldMap) {
        @NonNull Basket basketSold = new Basket(commodityTypesSoldMap.entrySet().stream()
            .map(e -> new CommoditySpecification(commodityTypes_.allocate(e.getKey()), e.getValue()))
            .collect(Collectors.toList()));
        @NonNull Trader trader = economy_.addTrader(traderTypes_.allocate(type), state, basketSold);
        uuids_.put(trader, uuid);
        names_.put(trader, name);

        return trader;
    }

    /**
     * Adds a new trader with additive commodities to the legacy topology.
     *
     * @param uuid The UUID of the new trader.
     * @param name The human-readable name of the new trader.
     * @param type The human-readable type of the new trader.
     * @param state The state of the new trader.
     * @param commodityTypesSold A collection of human-readable commodity type strings from which
     *                           the basket sold of the trader will be created. They will be
     *                           allocated to numerical IDs in the order they are returned by the
     *                           collecion's iterator.
     * @return The new trader.
     *
     * @see Economy#addTrader(int, TraderState, Basket, Basket...)
     */
    public @NonNull Trader addTrader(@NonNull String uuid, @NonNull String name, @NonNull String type,
            @NonNull TraderState state, @NonNull Collection<@NonNull String> commodityTypesSold) {

        Map<@NonNull String, Function<List<Double>, Double>> commodityTypesSoldMap = new HashMap<>();
        commodityTypesSold.forEach(key -> commodityTypesSoldMap.put(key,  null));
        return addTrader(uuid, name, type, state, commodityTypesSoldMap);
    }

    /**
     * Adds a new basket bought to the specified buyer.
     *
     * @param buyer The buyer who should start buying the new basket.
     * @param commodityTypesBought A collection of human-readable commodity type strings from which
     *                             the basket bought of the buyer will be created.
     * @return The new buyer participation of buyer in the market corresponding to basket bought.
     *
     * @see Economy#addBasketBought(Trader, Basket)
     */
    public @NonNull BuyerParticipation addBasketBought(@NonNull Trader buyer,
                                       @NonNull Collection<@NonNull String> commodityTypesBought) {
        return economy_.addBasketBought(buyer, new Basket(commodityTypesBought.stream()
            .map(typeBought -> new CommoditySpecification(commodityTypes_.allocate(typeBought)))
            .collect(Collectors.toList())));
    }

    /**
     * Returns an unmodifiable view of the managed {@link Economy} that can be copied.
     */
    public @NonNull UnmodifiableEconomy getEconomy(@ReadOnly LegacyTopology this) {
        return economy_;
    }

    /**
     * Returns an unmodifiable BiMap mapping Traders to their UUIDs.
     */
    public @NonNull @ReadOnly BiMap<@NonNull Trader, @NonNull String> getUuids(@ReadOnly LegacyTopology this) {
        return unmodifiableUuids_;
    }

    /**
     * Returns an unmodifiable Map mapping Traders to their human-readable names.
     */
    public @NonNull @PolyRead Map<@NonNull Trader, @NonNull String> getNames(@ReadOnly LegacyTopology this) {
        return unmodifiableNames_;
    }

    /**
     * Returns an unmodifiable NumericIDAllocator with the allocations of numerical IDs to human-
     * readable trader types.
     */
    public @NonNull @ReadOnly UnmodifiableNumericIDAllocator getTraderTypes(@ReadOnly LegacyTopology this) {
        return traderTypes_;
    }

    /**
     * Returns an unmodifiable NumericIDAllocator with the allocations of numerical IDs to human-
     * readable commodity types.
     */
    public @NonNull @ReadOnly UnmodifiableNumericIDAllocator getCommodityTypes(@ReadOnly LegacyTopology this) {
        return commodityTypes_;
    }

} // end LegacyTopology class

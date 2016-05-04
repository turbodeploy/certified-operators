package com.vmturbo.platform.analysis.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.DoubleBinaryOperator;
import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;

/**
 * Encapsulates an {@link Economy} together with a number of auxiliary maps, for use by code that
 * interfaces with the outside world. e.g. receiving a topology from the network and/or loading it
 * from a file.
 *
 * <p>
 *  It maintains the relationship between {@link Trader}s and {@link BuyerParticipation}s and their
 *  corresponding OIDs so that this information can be used to send back {@link Action}s that need
 *  to refer to them across process boundaries.
 * </p>
 *
 * <p>
 *  It is responsible for keeping the managed maps and economy consistent with each other.
 * </p>
 */
public final class Topology {
    // Fields
    private final @NonNull Economy economy_ = new Economy(); // The managed economy.
    private final @NonNull BiMap<@NonNull Trader, @NonNull Long> traderOids_ = HashBiMap.create();
    private final @NonNull BiMap<@NonNull BuyerParticipation, @NonNull Long> participationOids_ = HashBiMap.create();
    // A map from OIDs of traders we haven't seen yet to buyer participations that need to be
    // placed on them. It is needed if we can receive a customer before its supplier.
    private final @NonNull Map<@NonNull Long, @NonNull List<@NonNull BuyerParticipation>> danglingBuyerParticipations_ = new HashMap<>();

    // Cached data

    // Cached unmodifiable view of the traderOids_ BiMap.
    private final @NonNull BiMap<@NonNull Trader, @NonNull Long>
        unmodifiableTraderOids_ = Maps.unmodifiableBiMap(traderOids_);
    // Cached unmodifiable view of the traderOids_ BiMap.
    private final @NonNull BiMap<@NonNull BuyerParticipation, @NonNull Long>
        unmodifiableParticipationOids_ = Maps.unmodifiableBiMap(participationOids_);
    // Cached unmodifiable view of the danglingBuyerParticipations_ Map.
    private final @NonNull Map<@NonNull Long, @NonNull List<@NonNull BuyerParticipation>>
        unmodifiableDanglingBuyerParticipations_ = Collections.unmodifiableMap(danglingBuyerParticipations_);

    // Constructors

    /**
     * Constructs an empty Topology
     */
    public Topology() {
        // nothing to do
    }

    // Methods

    /**
     * Adds a new trader to the topology.
     *
     * @param oid The OID of the new trader.
     * @param type The numeric type of the new trader.
     * @param state The state of the new trader. e.g. active or inactive
     * @param basketSold The basket sold by the new trader.
     * @return The trader newly added to {@code this} topology.
     *
     * @see Economy#addTrader(int, TraderState, Basket, Basket...)
     */
    public @NonNull Trader addTrader(long oid, int type, @NonNull TraderState state, @NonNull Basket basketSold) {
        @NonNull Trader trader = economy_.addTrader(type, state, basketSold);
        traderOids_.put(trader, oid);

        // Check if the topology already contains buyer participations that refer to this trader...
        List<@NonNull BuyerParticipation> participations = danglingBuyerParticipations_.get(oid);
        if (participations != null) {
            for (BuyerParticipation participation : participations) {
                // ...and place them on this trader.
                participation.move(trader);
            }

            danglingBuyerParticipations_.remove(oid);
        }

        return trader;
    }

    /**
     * Adds a new basket bought to the specified buyer.
     *
     * @param oid The oid that should be associated with the new buyer participation that will be
     *            created for the basket bought.
     * @param buyer The buyer that should start buying the new basket.
     * @param basketBought The basket that <b>buyer</b> should start buying.
     * @return The newly created buyer participation of <b>buyer</b> in the market corresponding to
     *         basket bought.
     *
     * @see Economy#addBasketBought(Trader, Basket)
     */
    public @NonNull BuyerParticipation addBasketBought(long oid, @NonNull Trader buyer, @NonNull Basket basketBought) {
        @NonNull BuyerParticipation participation = economy_.addBasketBought(buyer, basketBought);
        participationOids_.put(participation, oid);

        return participation;
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
     *                    participation that will be created for the new basket bought.
     * @return Same as for {@link #addBasketBought(long, Trader, Basket)}
     *
     * @see Economy#addBasketBought(Trader, Basket)
     * @see #addBasketBought(long, Trader, Basket)
     */
    public @NonNull BuyerParticipation addBasketBought(long oid, @NonNull Trader buyer,
                                                       @NonNull Basket basketBought, long supplierOid) {
        @NonNull BuyerParticipation participation = addBasketBought(oid, buyer, basketBought);

        // Check whether the supplier has been added to the topology...
        @NonNull Trader supplier = traderOids_.inverse().get(supplierOid);
        if (supplier != null) {
            participation.move(supplier);
        } else {
            // ...and if not, make a note of the fact so that we can update the buyer participation
            // when it's added.
            @NonNull List<@NonNull BuyerParticipation> participations = danglingBuyerParticipations_.get(supplierOid);
            if (participations == null) {
                participations = new ArrayList<>();
                danglingBuyerParticipations_.put(supplierOid, participations);
            }
            participations.add(participation);
        }

        return participation;
    }

    /**
     * Returns a modifiable view of the map associating commodity specifications to quantity
     * updating functions that is part of the internal economy.
     *
     * @see Economy#getModifiableQuantityFunctions()
     */
    public @PolyRead @NonNull Map<@NonNull CommoditySpecification, @NonNull DoubleBinaryOperator>
            getModifiableQuantityFunctions(@PolyRead Topology this) {
        return economy_.getModifiableQuantityFunctions();
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
     * Returns an unmodifiable BiMap mapping {@link BuyerParticipation}s to their OIDs.
     */
    public @ReadOnly @NonNull BiMap<@NonNull BuyerParticipation, @NonNull Long>
            getParticipationOids(@ReadOnly Topology this) {
        return unmodifiableParticipationOids_;
    }

    /**
     * Returns an unmodifiable Map mapping OIDs of traders that haven't been added to {@code this}
     * topology yet to lists of buyer participations that should be placed on those traders.
     *
     * <p>
     *  We called those buyer participations 'dangling' by analogy to the 'dangling' pointers which
     *  point to something that doesn't exist. (although dangling pointers point to something that
     *  used to exist, while dangling buyer participations point to something that will exist in the
     *  future)
     * </p>
     */
    public @ReadOnly @NonNull Map<@NonNull Long, @NonNull List<@NonNull BuyerParticipation>>
            getDanglingBuyerParticipations(@ReadOnly Topology this) {
        return unmodifiableDanglingBuyerParticipations_;
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
        participationOids_.clear();
        danglingBuyerParticipations_.clear();
    }

} // end Topology class

package com.vmturbo.platform.analysis.topology;

import java.util.HashMap;
import java.util.Map;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

/**
 * A representation of the outside world's state capable of communicating with Mediation and UI.
 *
 * <p>
 *  It supports referring to objects using <b>OIDs</b> common between <b>Market</b>, <b>Mediation</b> and
 *  <b>UI</b>.
 * </p>
 * <p>
 *  It encapsulates a single-writer multiple-readers lock where the expected writers are the
 *  <b>Mediation</b> and <b>UI</b> and the expected readers are the <b>UI</b> and <b>Market</b>.
 * </p>
 */
public final class Topology {
    // Fields
    private @NonNull Map<@NonNull @ReadOnly OID,@NonNull Trader> traders_ = new HashMap<>();
    private @NonNull Economy economy_ = new Economy(); // topology will just delegate to economy for now.

    // Constructors

    /**
     * Constructs an empty Topology
     */
    public Topology() {}

    // Methods

    /**
     * Returns the trader associated with a given OID.
     *
     * @param oid the OID uniquely identifying the trader to return
     * @return the trader associated with supplied OID.
     */
    @Pure
    public @NonNull Trader getTrader(@ReadOnly Topology this, @NonNull @ReadOnly OID oid) {
        return traders_.get(oid);
    }

    /**
     * Adds a new trader to the topology.
     *
     * @param traderOID the OID to associate with the new trader.
     * @param trader the new trader to add to the topology.
     * @return {@code this}
     */
    @Deterministic
    public @NonNull Trader addTrader(@NonNull @ReadOnly OID traderOID, int type, @NonNull TraderState state,
                                     @NonNull Basket basketSold, @NonNull Basket... basketsBought) {
        @NonNull Trader trader = economy_.addTrader(type,state,basketSold,basketsBought);
        traders_.put(traderOID, trader);

        return trader;
    }

    /**
     * Removes the trader with the given OID from the topology and returns it.
     * @param traderOID the OID of the trader that should be removed.
     * @return the trader that was just removed from the topology.
     */
    @Deterministic // in the sense that for the same referents of this and traderOID the result will
    // be the same. Calling this two times on the same topology will produce different results
    // because the topology is modified.
    public @NonNull Trader removeTrader(@NonNull @ReadOnly OID traderOID) {
        @NonNull Trader toBeRemoved = traders_.get(traderOID);
        economy_.removeTrader(toBeRemoved);
        return toBeRemoved;
    }

    /**
     * Returns the Economy that corresponds to {@code this} topology.
     *
     * <p>
     *  It can be copied for use as an input to the Economy Simulation Algorithms.
     * </p>
     */
    @Pure
    public @NonNull @ReadOnly Economy getEconomy(@ReadOnly Topology this) {
        return economy_;
    }
} // end class Topology

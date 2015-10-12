package com.vmturbo.platform.analysis.economy;

import java.lang.AssertionError;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

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
    private @NonNull Map<@NonNull @ReadOnly Basket,@NonNull Market> markets =
                                           new TreeMap<@NonNull @ReadOnly Basket,@NonNull Market>();
    // The list of all Traders participating in the Economy.
    private @NonNull List<@NonNull Trader> traders = new ArrayList<@NonNull Trader>();

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
     * @param newTrader The trader to be added. The referent itself is added and not a copy.
     * @return {@code this}
     */
    @Deterministic
    public @NonNull Economy addTrader(@NonNull Trader newTrader, @NonNull Basket... basketsBought) {
        // TODO: implement this.
        return this;
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
        // TODO: implement this.
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

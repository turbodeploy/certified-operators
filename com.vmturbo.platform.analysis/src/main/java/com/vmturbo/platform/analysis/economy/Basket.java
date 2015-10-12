package com.vmturbo.platform.analysis.economy;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * A set of commodity (type, quality) pairs a trader may try to buy.
 *
 * <p>
 *  They are usually associated with a {@link Market}.
 * </p>
 */
public interface Basket extends Comparable<Basket> {
    /**
     * Returns the associated {@link Market} or {@code null} if it is not associated with a market.
     */
    Market getMarket();

    /**
     * Returns whether a buyer shopping for {@code this} basket, can be satisfied by a given basket.
     *
     * <p>
     *  e.g. a buyer buying CPU with 4 cores and memory, can be satisfied by a seller selling CPU
     *  with 8 cores, memory and some access commodities.
     * </p>
     *
     * @param other the Basket to the tested against {@code this}.
     * @return {@code true} if {@code this} basket is satisfied by {@code other}.
     */
    boolean isSatisfiedBy(@NonNull Basket other);

} // end Basket interface

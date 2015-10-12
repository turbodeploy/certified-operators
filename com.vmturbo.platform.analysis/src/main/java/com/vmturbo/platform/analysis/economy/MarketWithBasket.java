package com.vmturbo.platform.analysis.economy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

/**
 * A {@link Market} with an attached {@link Basket}.
 *
 * <p>
 *  A design decision was made to present baskets and markets as separate entities externally, but
 *  implement them as a single object internally to conserve space, decrease dereferencing and copy
 *  time and improve cache locality.
 * </p>
 */
final class MarketWithBasket extends Basket implements Market {
    // Fields
    private Trader[] buyers; // all active Traders buying {@code this} basket.
    private Trader[] sellers; // all active Traders selling a basket that matches {@code this}.
    private int[] mapping; // The mapping between sellers and buyers of this Market. Can support up
                  // to 2 billion Traders. Must be the same size as buyers.
    private double[][] amountAndPeakVectors; // Must be the same size as buyers. For each buyer,
        // holds an array of as many (amount,peak) pairs as there are commodity types in this Basket.
        // The layout is {a1,p1,a2,p2,...,an,pn} where commodityTypes.length == n.

    // Constructors

    /**
     * Constructs and empty Market with an attached basket constructed from the given commodity types.
     *
     * @param commodityTypes The commodity types from which the attached basket will be constructed.
     */
    public MarketWithBasket(CommodityType... commodityTypes) {
        super(commodityTypes);
    }

    // Methods

    @Override
    @Pure
    public @NonNull Basket getBasket() {
        return this;
    }

    @Override
    @Pure
    public @NonNull List<@NonNull Trader> getSellers() {
        return Collections.unmodifiableList(Arrays.asList(sellers));
    }

    @Override
    @Pure
    public @NonNull List<@NonNull Trader> getBuyers() {
        return Collections.unmodifiableList(Arrays.asList(buyers));
    }

} // end MarketWithBasket class

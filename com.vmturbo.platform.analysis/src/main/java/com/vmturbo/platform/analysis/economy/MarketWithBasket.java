package com.vmturbo.platform.analysis.economy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.checkerframework.checker.javari.qual.ReadOnly;
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
final class MarketWithBasket implements Basket, Market {

    // Fields for Basket
    private CommodityType[] commodityTypes; // must have the same size as commodityKeys and the pair
    private long[] commodityKeys; // must be sorted by commodity type and then by key.

    // Fields for Market
    private Trader[] buyers; // all active Traders buying {@code this} basket.
    private Trader[] sellers; // all active Traders selling a basket that matches {@code this}.
    private int[] mapping; // The mapping between sellers and buyers of this Market. Can support up
                  // to 2 billion Traders. Must be the same size as buyers.
    private double[][] amountAndPeakVectors; // Must be the same size as buyers. For each buyer,
        // holds an array of as many (amount,peak) pairs as there are commodity types in this Basket.
        // The layout is {a1,p1,a2,p2,...,an,pn} where commodityTypes.length == n.

    // Constructors

    // Methods

    @Override
    @Pure
    public int compareTo(@NonNull @ReadOnly MarketWithBasket this, @NonNull @ReadOnly Basket other) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    @Pure
    public boolean isSatisfiedBy(@NonNull @ReadOnly Basket other) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Pure
    public @NonNull Market getMarket() {
        return this;
    }

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

    @Override
    public @NonNull @ReadOnly List<@NonNull @ReadOnly CommodityType> getCommodityTypes() {
        // TODO Auto-generated method stub
        return null;
    }

} // end MarketWithBasket class

package com.vmturbo.platform.analysis.topology;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommodityType;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;

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

    // Fields

    private CommodityType[] commodityTypes; // must have the same size as commodityKeys and the pair
    private long[] commodityKeys; // must be sorted by commodity type and then by key.

    private Trader[] buyers; // all active Merchants buying {@code this} basket.
    private Trader[] sellers; // all active Merchants selling a basket that matches {@code this}.
    private int[] mapping; // The mapping between sellers and buyers of this Market. Can support up
                  // to 2 billion Merchants. Must be the same size as buyers.
    private double[][] amountAndPeakVectors; // Must be the same size as buyers. For each buyer,
        // holds an array of as many (amount,peak) pairs as there are commodity types in this Basket.
        // The layout is {a1,p1,a2,p2,...,an,pn} where commodityTypes.length == n.

    // Constructors

    // Methods

    @Override
    public int compareTo(@NonNull Basket other) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isSatisfiedBy(@NonNull Basket other) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public @NonNull Market getMarket() {
        return this;
    }

    @Override
    public @NonNull Basket getBasket() {
        return this;
    }

    @Override
    public @NonNull List<@NonNull Trader> getSellers() {
        return Collections.unmodifiableList(Arrays.asList(sellers));
    }

    @Override
    public @NonNull List<@NonNull Trader> getBuyers() {
        return Collections.unmodifiableList(Arrays.asList(buyers));
    }

} // end MarketWithBasket class

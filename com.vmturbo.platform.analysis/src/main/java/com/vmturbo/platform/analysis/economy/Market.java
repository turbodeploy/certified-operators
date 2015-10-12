package com.vmturbo.platform.analysis.economy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

/**
 * A trading place where a particular basket of goods is sold and bought.
 *
 * <p>
 *  A {@code Market} is associated with a {@link Basket} and comprises a list of buyers and sellers
 *  trading that particular basket.
 * </p>
 */
public final class Market {
    // Fields

    // the associated basket all buyers in the market buy that basket.
    private final @NonNull Basket basket_;
    // all active Traders buying {@code this} basket.
    private @NonNull Trader @NonNull [] buyers = new Trader[0];
    // all active Traders selling a basket that matches {@code this}.
    private @NonNull Trader @NonNull [] sellers = new Trader[0];
    // The mapping between sellers and buyers of this Market. Can support up to 2 billion Traders.
    // Must be the same size as buyers.
    private int @NonNull [] currentSuppliers = new int[0];
    // Must be the same size as buyers. For each buyer, holds an array of as many (amount,peak)
    // pairs as there are commodity types in this Basket. The layout is {a1,p1,a2,p2,...,an,pn}
    // where commodityTypes.length == n.
    private double @NonNull [] @NonNull [] amountAndPeakVectors = new double[0][0];

    // Constructors

    /**
     * Constructs and empty Market and attaches the given basket.
     *
     * @param basketToAssociate The basket to associate with the new market. It it referenced and
     *                          not copied.
     */
    public Market(@NonNull Basket basketToAssociate) {
        basket_ = basketToAssociate;
    }

    // Methods

    /**
     * Returns the associated {@link Basket}
     */
    @Pure
    public @NonNull Basket getBasket(@ReadOnly Market this) {
        return basket_;
    }

    /**
     * Returns an unmodifiable list of sellers participating in {@code this} {@code Market}.
     */
    @Pure
    public @NonNull @ReadOnly List<@NonNull Trader> getSellers(@ReadOnly Market this) {
        return Collections.unmodifiableList(Arrays.asList(sellers));
    }

    /**
     * Returns an unmodifiable list of buyers participating in {@code this} {@code Market}.
     */
    @Pure
    public @NonNull @ReadOnly List<@NonNull Trader> getBuyers(@ReadOnly Market this) {
        return Collections.unmodifiableList(Arrays.asList(buyers));
    }

} // end Market class

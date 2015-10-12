package com.vmturbo.platform.analysis.economy;

import java.util.List;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;


/**
 * A place where a particular basket of goods is sold and bought.
 *
 * <p>
 *  A {@code Market} is associated with a {@link Basket} and comprises a list of buyers and sellers
 *  trading that particular basket.
 * </p>
 */
public interface Market {
    /**
     * Returns the associated {@link Basket}
     */
    @Pure
    @NonNull Basket getBasket(@ReadOnly Market this);

    /**
     * Returns an unmodifiable list of sellers participating in {@code this} {@code Market}.
     */
    @Pure
    @NonNull @ReadOnly List<@NonNull Trader> getSellers(@ReadOnly Market this);

    /**
     * Returns an unmodifiable list of buyers participating in {@code this} {@code Market}.
     */
    @Pure
    @NonNull @ReadOnly List<@NonNull Trader> getBuyers(@ReadOnly Market this);

} // end Market interface

package com.vmturbo.platform.analysis.economy;

import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;


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
    @NonNull Basket getBasket();

    /**
     * Returns an unmodifiable list of sellers participating in {@code this} {@code Market}.
     */
    @NonNull List<Trader> getSellers();

    /**
     * Returns an unmodifiable list of buyers participating in {@code this} {@code Market}.
     */
    @NonNull List<Trader> getBuyers();

} // end Market interface

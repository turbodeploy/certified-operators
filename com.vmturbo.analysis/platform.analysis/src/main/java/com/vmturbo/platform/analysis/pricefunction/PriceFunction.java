package com.vmturbo.platform.analysis.pricefunction;

import java.io.Serializable;

import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;

@FunctionalInterface
public interface PriceFunction extends Serializable {
    // Methods

    /**
     * The price of one unit of normalized utilization. When a trader wants to
     * buy say 30% utilization, it will be charged 0.3 of the unit price.
     * @param normalizedUtilization the utilization as a percentage of the utilization
     * admissible for the {@link CommoditySold}
     * @param seller is the {@link Trader} selling the commodity
     * @param cs is the {@link CommoditySold} by the seller
     * @param e is the {@link Economy} that the seller resides in
     * @return the price that will be charged for 100% of the capacity for a particular commodity
     *          sold by a seller
     */
    @Pure
    public double unitPrice(double normalizedUtilization, ShoppingList shoppingList, Trader seller, CommoditySold cs
                            , UnmodifiableEconomy e);

    // Inner classes
    public static class Cache extends com.vmturbo.platform.analysis.pricefunction.Cache {/* workaround */};
}

package com.vmturbo.platform.analysis.pricefunction;

import java.io.Serializable;

import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;

public interface PriceFunction extends Serializable {
    // Methods

    /**
     * The price of one unit of normalized utilization. When a trader wants to
     * buy say 30% utilization, it will be charged 0.3 of the unit price.
     * @param normalizedUtilization the utilization as a percentage of the utilization
     * admissible for the {@link CommoditySold}
     * @param shoppingList is the consumer's shoppingList.
     * @param seller is the {@link Trader} selling the commodity
     * @param cs is the {@link CommoditySold} by the seller
     * @param e is the {@link Economy} that the seller resides in
     * @return the price that will be charged for 100% of the capacity for a particular commodity
     *          sold by a seller
     */
    @Pure
    double unitPrice(double normalizedUtilization, ShoppingList shoppingList, Trader seller, CommoditySold cs,
                            UnmodifiableEconomy e);

    /**
     * The mechanism to update a {@link PriceFunction} with any new weight specified.
     * @param weight is the weight on the new PriceFunction.
     * @return this {@link PriceFunction}.
     */
    default PriceFunction updatePriceFunctionWithWeight(double weight) {
        return this;
    }

    /**
     * Returns the input parameter associated with the {@link PriceFunction}.
     *
     * @return an array of input parameters. The order follows the order of input arguments in constructor.
     */
    double[] getParams();
}

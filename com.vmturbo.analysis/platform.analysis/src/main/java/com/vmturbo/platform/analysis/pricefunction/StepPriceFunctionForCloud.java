package com.vmturbo.platform.analysis.pricefunction;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;

/**
 * StepPriceFunctionForCloud.
 */
public class StepPriceFunctionForCloud implements PriceFunction {
    /*
     * weight assigned to the priceFunction.
     */
    double weight_;

    StepPriceFunctionForCloud(double weight) {
        weight_ = weight;
    }

    /**
     * The price of one unit of normalized utilization. When a trader wants to
     * buy say 30% utilization, it will be charged 0.3 of the unit price.
     * @param normalizedUtilization the utilization as a percentage of the utilization
     * admissible for the {@link CommoditySold}
     * @param shoppingList is the consumer's shoppingList.
     * @param seller is the {@link Trader} selling the commodity
     * @param cs is the {@link CommoditySold} by the seller
     * @param e is the {@link UnmodifiableEconomy} that the seller resides in
     * @return the price that will be charged for 100% of the capacity for a particular commodity
     *          sold by a seller
     */
    @Override
    public double unitPrice(double normalizedUtilization, ShoppingList shoppingList, Trader seller, CommoditySold cs,
                            UnmodifiableEconomy e) {
        return normalizedUtilization == 0 ? 0 : PriceFunctionFactory.isInvalid(normalizedUtilization)
                ? Double.POSITIVE_INFINITY : weight_;
    }

    @Override
    public double[] getParams() {
        return new double[] { this.weight_ };
    }
}

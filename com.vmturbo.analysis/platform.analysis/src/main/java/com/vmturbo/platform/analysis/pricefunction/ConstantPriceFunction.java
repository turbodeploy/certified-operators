package com.vmturbo.platform.analysis.pricefunction;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;

/**
 * ConstantPriceFunction.
 */
public class ConstantPriceFunction implements PriceFunction {

    /*
     * Constant price to be returned as part of the function.
     */
    double constant_;

    ConstantPriceFunction(double constant) {
        constant_ = constant;
    }

    /**
     * The price of one unit of normalized utilization. When a trader wants to
     * buy say 30% utilization, it will be charged 0.3 of the unit price.
     * @param normalizedUtilization the utilization as a percentage of the utilization
     * @param shoppingList is the consumer's shoppingList.
     * admissible for the {@link CommoditySold}
     * @param seller is the {@link Trader} selling the commodity
     * @param cs is the {@link CommoditySold} by the seller
     * @param e is the {@link UnmodifiableEconomy} that the seller resides in
     * @return the price that will be charged for 100% of the capacity for a particular commodity
     *          sold by a seller
     */
    public double unitPrice(double normalizedUtilization, ShoppingList shoppingList, Trader seller, CommoditySold cs,
                            UnmodifiableEconomy e) {
        return PriceFunctionFactory.isInvalid(normalizedUtilization) ? Double.POSITIVE_INFINITY : constant_;
    }
}

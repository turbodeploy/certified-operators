package com.vmturbo.platform.analysis.pricefunction;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;

/**
 * StepPriceFunction.
 */
public class StepPriceFunction implements PriceFunction {
    /*
     * junction point where the step occurs.
     */
    double stepAt_;

    /*
     * price below step.
     */
    double priceBelow_;

    /*
     * price above step.
     */
    double priceAbove_;

    StepPriceFunction(double stepAt, double priceBelow, double priceAbove) {
        stepAt_ = stepAt;
        priceBelow_ = priceBelow;
        priceAbove_ = priceAbove;
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
    public double unitPrice(double normalizedUtilization, ShoppingList shoppingList, Trader seller, CommoditySold cs,
                            UnmodifiableEconomy e) {
        return PriceFunctionFactory.isInvalid(normalizedUtilization) ? Double.POSITIVE_INFINITY
                : normalizedUtilization < stepAt_ ? priceBelow_ : priceAbove_;
    }
}

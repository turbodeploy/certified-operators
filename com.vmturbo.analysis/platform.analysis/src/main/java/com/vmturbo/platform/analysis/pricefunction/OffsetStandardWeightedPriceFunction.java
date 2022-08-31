package com.vmturbo.platform.analysis.pricefunction;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;

/**
 * The offset standard price function. The function returns a flat price upto 100% util, and then increases from
 * 100% to 200% util (the same way standard price function rises from 0% to 100%), and then returns Infinity after 200%.
 * The formula is
 *          w for u < 1,
 * P(u) =   min(w / (1-nu)^2, MAX_UNIT_PRICE) where nu = u - 1 for 1 < u < 2
 *          Double.POSITIVE_INFINITY for u > 2.
 */
public class OffsetStandardWeightedPriceFunction implements PriceFunction {

    /*
     * weight assigned to the priceFunction.
     */
    double weight_;

    OffsetStandardWeightedPriceFunction(double weight) {
        weight_ = weight;
    }

    /**
     * The offset standard price function.
     * The formula is
     *          w for u < 1,
     * P(u) =   min(w / (1-nu)^2, MAX_UNIT_PRICE) where nu = u - 1 for 1 < u < 2
     *          Double.POSITIVE_INFINITY for u > 2.
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
        if (normalizedUtilization <= 1) {
            return weight_;
        } else {
            return normalizedUtilization > 2
                    ? Double.POSITIVE_INFINITY
                    : pricingBetweenSteps(normalizedUtilization);
        }
    }

    private double pricingBetweenSteps(double util) {
        double normalizedUtil = util - 1;
        return Math.min(weight_ / ((1.0f - normalizedUtil) * (1.0f - normalizedUtil)),
                PriceFunctionFactory.MAX_UNIT_PRICE);
    }

    @Override
    public double[] getParams() {
        return new double[] { this.weight_ };
    }
}

package com.vmturbo.platform.analysis.pricefunction;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;

/**
 * SquaredReciprocalBoughtUtilizationPriceFunction.
 */
public class SquaredReciprocalBoughtUtilizationPriceFunction implements PriceFunction {
    /*
     * weight assigned to the priceFunction.
     */
    double weight_;

    SquaredReciprocalBoughtUtilizationPriceFunction(double weight) {
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
    public double unitPrice(double normalizedUtilization, ShoppingList shoppingList, Trader seller, CommoditySold cs,
                            UnmodifiableEconomy e) {
        // if shopping list is passed, disregard u
        // fetch the bought commodity by specification and consider it's utilization
        double util = normalizedUtilization;
        if (shoppingList != null && cs.getCapacity() > 0) {
            int soldIndex = seller.getCommoditiesSold().indexOf(cs);
            if (soldIndex >= 0) {
                int boughtIndex = shoppingList.getBasket().indexOf(seller.getBasketSold().get(soldIndex));
                if (boughtIndex >= 0) {
                    util = shoppingList.getQuantity(boughtIndex) / cs.getCapacity();
                }
            }
        }
        if (PriceFunctionFactory.isInvalid(util)) {
            return Double.POSITIVE_INFINITY;
        } else if (util == 0) {
            return PriceFunctionFactory.MAX_UNIT_PRICE;
        } else {
            return Math.min(weight_ / util / util, PriceFunctionFactory.MAX_UNIT_PRICE);
        }
    }
}
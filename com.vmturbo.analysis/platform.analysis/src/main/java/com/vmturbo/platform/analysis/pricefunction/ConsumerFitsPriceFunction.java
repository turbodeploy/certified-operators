package com.vmturbo.platform.analysis.pricefunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;

/**
 * ConsumerFitsPriceFunction.
 */
public class ConsumerFitsPriceFunction implements PriceFunction {

    /**
     * Price function that checks if the consumption of the SL can fit within the capacity of the seller.
     * If the consumer's usage is less than the provider's capacity, then we return 0.
     * Otherwise we return Infinite so the provider won't be a possible provider to place on.
     * If there is no consumer/sl passed, such as in Provision phase, then we return price of 0.
     *
     * @param normalizedUtilization the utilization as a percentage of the utilization
     * admissible for the {@link CommoditySold}
     * @param shoppingList is the consumer's shoppingList.
     * @param seller is the {@link Trader} selling the commodity
     * @param cs is the {@link CommoditySold} by the seller
     * @param e is the {@link UnmodifiableEconomy} that the seller resides in
     * @return the computed price.
     */
    @Override
    public double unitPrice(double normalizedUtilization, @Nullable ShoppingList shoppingList,
        @Nonnull Trader seller, @Nonnull CommoditySold cs, @Nonnull UnmodifiableEconomy e) {
        // Not used in provision.
        if (shoppingList == null) {
            return 0;
        }
        // Used in placements.
        CommoditySpecification commSpec = seller.getBasketSold().get(seller.getCommoditiesSold().indexOf(cs));
        return shoppingList.getQuantity(shoppingList.getBasket().indexOf(commSpec)) > cs.getCapacity()
            ? Double.POSITIVE_INFINITY : 0;
    }

    /**
     * Returns the input parameter associated with the {@link PriceFunction}.
     *
     * @return an array of input parameters. The order follows the order of input arguments in constructor.
     */
    @Override
    public @Nonnull double[] getParams() {
        return new double[0];
    }
}
